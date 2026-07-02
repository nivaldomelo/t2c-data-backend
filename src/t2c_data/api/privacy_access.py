from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import Select, desc, func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.access_control.abac import record_abac_denial
from t2c_data.features.export_jobs import ExportArtifactResult, enqueue_export_job, register_export_request_audit, serialize_export_job
from t2c_data.features.export_security import safe_csv_writer, DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, redact_export_value, resolve_export_limit
from t2c_data.features.audit import build_table_history_snapshot, table_history_changes
from t2c_data.features.pagination import normalize_page_params
from t2c_data.features.privacy_access import (
    ACCESS_ROLE_LABELS,
    ACCESS_ROLE_OPTIONS,
    ACCESS_SCOPE_LABELS,
    ACCESS_SCOPE_OPTIONS,
    LEGAL_BASIS_LABELS,
    LEGAL_BASIS_OPTIONS,
    SENSITIVITY_LABELS,
    SENSITIVITY_LEVELS,
    can_edit_privacy,
    can_view_table,
    normalize_access_roles,
    privacy_summary_payload,
    role_tokens_for_user,
    suspected_personal_data_columns,
)
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.platform.sensitive_data import can_view_sensitive_data, mask_payload_by_policy
from t2c_data.features.platform.visibility import mask_privacy_summary_payload, table_visibility_decision_from_entity
from t2c_data.models.auth import User
from t2c_data.models.catalog import Database, Schema, TableEntity
from t2c_data.models.governance import PrivacyReviewEvent
from t2c_data.schemas.pagination import PageOut
from t2c_data.schemas.privacy_access import (
    PrivacyAccessOptionsOut,
    PrivacyAccessPatch,
    PrivacyBySchemaOut,
    PrivacyPeriodicReviewIn,
    PrivacyPriorityOut,
    PrivacyRiskBucketsOut,
    PrivacySummaryOutPage,
    PrivacySummaryTotalsOut,
    PrivacyTableDetailOut,
    PrivacyTableListItemOut,
    PrivacyTopBlockerOut,
    PrivacyReviewChangedFieldOut,
    PrivacyReviewEventOut,
    PrivacyReviewEventPageOut,
    PrivacyReviewEventSummaryOut,
)
from t2c_data.schemas.platform import IntegrationSyncJobOut
from t2c_data.features.audit.support import AuditFieldChange
from t2c_data.services.audit import log_field_changes, request_audit_kwargs

router = APIRouter(prefix="/privacy-access", tags=["privacy-access"])

PRIVACY_REVIEW_TYPES = {
    "classification",
    "access_change",
    "legal_basis_change",
    "purpose_change",
    "retention_change",
    "masking_change",
    "external_sharing_change",
    "periodic_review",
    "manual_review",
    "automatic_signal_review",
    "mixed_change",
}
PRIVACY_REVIEW_SOURCES = {"manual", "system", "import", "migration"}
PRIVACY_EVENT_FIELDS = {
    "classification",
    "has_personal_data",
    "has_sensitive_personal_data",
    "legal_basis",
    "privacy_purpose",
    "retention_policy",
    "access_scope",
    "access_roles",
    "is_masked",
    "external_sharing",
    "privacy_notes",
    "privacy_reviewed_at",
    "privacy_reviewed_by_user_id",
}


def _table_query() -> Select[Any]:
    return (
        select(TableEntity)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .options(
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.columns),
            selectinload(TableEntity.privacy_reviewed_by_user),
        )
    )


def _serialize_table(
    table: TableEntity,
    *,
    masked: bool = False,
    current_user: User | None = None,
) -> PrivacyTableListItemOut:
    privacy_payload = privacy_summary_payload(table)
    if masked:
        privacy_payload = mask_privacy_summary_payload(privacy_payload)
    payload = {
        "id": table.id,
        "name": table.name,
        "table_type": table.table_type,
        "schema_name": table.schema.name,
        "database_name": table.schema.database.name,
        "datasource_name": table.schema.database.datasource.name,
        "engine": table.schema.database.datasource.db_type,
        "owner": table.owner,
        "owner_email": table.owner_email,
        "data_owner": table.data_owner,
        "privacy": privacy_payload,
        "updated_at": table.updated_at,
    }
    if not can_view_sensitive_data(current_user, table=table):
        payload = mask_payload_by_policy(payload, can_view_sensitive=False)
        payload["owner"] = "[masked]" if payload.get("owner") is not None else None
        payload["owner_email"] = "[masked]" if payload.get("owner_email") is not None else None
        if isinstance(payload.get("data_owner"), dict):
            payload["data_owner"]["name"] = "[masked]"
            payload["data_owner"]["email"] = "[masked]"
    return PrivacyTableListItemOut(**payload)


def _apply_table_filters(
    stmt: Select[Any],
    *,
    q: str | None,
    sensitivity_level: str | None,
    has_personal_data: bool | None,
    access_scope: str | None,
) -> Select[Any]:
    if q:
        term = f"%{q.strip()}%"
        stmt = stmt.where(
            or_(
                TableEntity.name.ilike(term),
                TableEntity.owner.ilike(term),
                TableEntity.owner_email.ilike(term),
                TableEntity.privacy_notes.ilike(term),
                TableEntity.privacy_purpose.ilike(term),
                Schema.name.ilike(term),
            )
        )
    if sensitivity_level:
        stmt = stmt.where(TableEntity.sensitivity_level == sensitivity_level)
    if has_personal_data is not None:
        stmt = stmt.where(TableEntity.has_personal_data.is_(has_personal_data))
    if access_scope:
        stmt = stmt.where(TableEntity.access_scope == access_scope)
    return stmt


def _filtered_visible_tables(
    db: Session,
    *,
    current_user: User,
    q: str | None,
    sensitivity_level: str | None,
    has_personal_data: bool | None,
    access_scope: str | None,
) -> list[TableEntity]:
    stmt = _apply_table_filters(
        _table_query().order_by(TableEntity.updated_at.desc(), TableEntity.id.desc()),
        q=q,
        sensitivity_level=sensitivity_level,
        has_personal_data=has_personal_data,
        access_scope=access_scope,
    )
    tables = db.scalars(stmt).all()
    return [table for table in tables if can_view_table(current_user, table)]


def _is_wide_access(scope: str | None) -> bool:
    return not scope or scope in {"public", "authenticated"}


def _is_restricted_access(scope: str | None) -> bool:
    return scope in {"confidential", "restricted", "personal_data"}


def _needs_legal_basis(privacy: dict[str, Any]) -> bool:
    return bool(privacy.get("has_personal_data") or privacy.get("has_sensitive_personal_data"))


def _owner_missing(table: TableEntity) -> bool:
    return not bool(table.owner or table.owner_email or getattr(table, "data_owner", None))


def _top_blocker_key(table: TableEntity, privacy: dict[str, Any]) -> str:
    if privacy.get("possible_personal_data") and not privacy.get("sensitivity_level"):
        return "possible_personal_unclassified"
    if privacy.get("has_sensitive_personal_data") and _is_wide_access(privacy.get("access_scope")):
        return "sensitive_wide_access"
    if _needs_legal_basis(privacy) and not privacy.get("legal_basis"):
        return "without_legal_basis"
    if privacy.get("possible_personal_data") and _is_wide_access(privacy.get("access_scope")):
        return "wide_access_with_suspicion"
    if _owner_missing(table):
        return "without_owner"
    if not privacy.get("privacy_reviewed_at"):
        return "without_review"
    if not privacy.get("privacy_purpose") and _needs_legal_basis(privacy):
        return "without_purpose"
    if not privacy.get("sensitivity_level"):
        return "unclassified"
    return "controlled"


_BLOCKER_METADATA: dict[str, tuple[str, str, str]] = {
    "possible_personal_unclassified": (
        "Possível dado pessoal sem classificação",
        "Ativos com sinal automático de dado pessoal, mas sem decisão formal.",
        "Revisar sensibilidade",
    ),
    "sensitive_wide_access": (
        "Dado sensível com acesso amplo",
        "Ativos sensíveis expostos além do necessário para o nível de proteção esperado.",
        "Restringir acesso",
    ),
    "without_legal_basis": (
        "Sem base legal",
        "Ativos com dado pessoal confirmado sem fundamento jurídico registrado.",
        "Informar base legal LGPD",
    ),
    "wide_access_with_suspicion": (
        "Acesso amplo com suspeita",
        "Ativos com sinal de dado pessoal ainda acessíveis a públicos mais amplos.",
        "Revisar acesso",
    ),
    "without_owner": (
        "Sem owner",
        "Ativos sem responsável dificultam validação de privacidade, aceite de risco e revisão periódica.",
        "Definir owner",
    ),
    "without_review": (
        "Sem revisão",
        "Ativos sem revisão registrada não deixam claro quem confirmou a política atual.",
        "Registrar revisão",
    ),
    "without_purpose": (
        "Sem finalidade",
        "Ativos com dado pessoal confirmado sem finalidade LGPD estruturada.",
        "Registrar finalidade",
    ),
    "unclassified": (
        "Não classificado",
        "Ativos ainda sem decisão formal de sensibilidade.",
        "Classificar sensibilidade",
    ),
}


def _risk_assessment(table: TableEntity, privacy: dict[str, Any]) -> tuple[str, str, str]:
    owner_missing = _owner_missing(table)
    wide_access = _is_wide_access(privacy.get("access_scope"))
    possible_personal = bool(privacy.get("possible_personal_data"))
    confirmed_personal = bool(privacy.get("has_personal_data"))
    confirmed_sensitive = bool(privacy.get("has_sensitive_personal_data"))
    has_classification = bool(privacy.get("sensitivity_level") or confirmed_personal or confirmed_sensitive)
    no_legal_basis = _needs_legal_basis(privacy) and not privacy.get("legal_basis")
    no_review = not privacy.get("privacy_reviewed_at")
    no_purpose = _needs_legal_basis(privacy) and not privacy.get("privacy_purpose")

    if confirmed_sensitive and wide_access:
        return (
            "critical",
            "Dado sensível confirmado com acesso amplo.",
            "Restringir acesso a perfis autorizados antes de ampliar consumo.",
        )
    if confirmed_personal and no_legal_basis and wide_access:
        return (
            "critical",
            "Dado pessoal confirmado sem base legal e com acesso amplo.",
            "Registrar base legal e revisar imediatamente o escopo de acesso.",
        )
    if possible_personal and not has_classification and wide_access and owner_missing:
        return (
            "critical",
            "Possível dado pessoal, sem classificação, com acesso amplo e sem owner.",
            "Definir responsável e formalizar classificação antes de manter o acesso.",
        )
    if possible_personal and not has_classification:
        return (
            "high",
            "Possível dado pessoal sem classificação formal.",
            "Confirmar sensibilidade e revisar o ativo com base nas colunas suspeitas.",
        )
    if confirmed_personal and no_legal_basis:
        return (
            "high",
            "Dado pessoal confirmado sem base legal registrada.",
            "Informar a base legal LGPD antes de manter ou ampliar o uso do ativo.",
        )
    if _is_restricted_access(privacy.get("access_scope")) and no_review:
        return (
            "high",
            "Acesso restrito sem revisão recente registrada.",
            "Registrar revisão da política e validar se as restrições continuam adequadas.",
        )
    if no_purpose:
        return (
            "medium",
            "Dado pessoal confirmado sem finalidade estruturada.",
            "Registrar a finalidade do tratamento para delimitar o uso permitido.",
        )
    if not has_classification or owner_missing or no_review:
        return (
            "medium",
            "Ativo com pendências de classificação, owner ou revisão.",
            "Completar a política de privacidade e registrar o responsável pela decisão.",
        )
    return (
        "low",
        "Ativo com classificação, revisão e acesso compatíveis com a leitura atual.",
        "Manter a revisão periódica e revalidar quando houver mudança relevante.",
    )


def _risk_score(level: str) -> int:
    return {"critical": 100, "high": 75, "medium": 45, "low": 15}.get(level, 0)


def _normalize_event_roles(value: Any) -> list[str] | None:
    if isinstance(value, list):
        normalized = normalize_access_roles([str(item) for item in value if item is not None])
        return normalized or None
    return None


def _privacy_state_from_snapshot(table: TableEntity, snapshot: Any) -> dict[str, Any]:
    return {
        "sensitivity_level": snapshot.sensitivity_level.get("value") if snapshot.sensitivity_level else None,
        "has_personal_data": bool(snapshot.has_personal_data),
        "has_sensitive_personal_data": bool(snapshot.has_sensitive_personal_data),
        "legal_basis": snapshot.legal_basis,
        "privacy_purpose": snapshot.privacy_purpose,
        "retention_policy": snapshot.retention_policy,
        "is_masked": bool(snapshot.is_masked),
        "external_sharing": bool(snapshot.external_sharing),
        "access_scope": snapshot.access_scope,
        "access_roles": normalize_access_roles(snapshot.access_roles or []) or None,
        "privacy_notes": snapshot.privacy_notes,
        "privacy_reviewed_at": table.privacy_reviewed_at,
        "possible_personal_data": bool(suspected_personal_data_columns(getattr(table, "columns", None))),
    }


def _event_changed_fields(
    before_privacy: dict[str, Any],
    after_privacy: dict[str, Any],
    *,
    before_reviewed_at: datetime | None,
    after_reviewed_at: datetime | None,
    before_reviewer_user_id: int | None,
    after_reviewer_user_id: int | None,
) -> list[dict[str, Any]]:
    field_order = [
        "classification",
        "has_personal_data",
        "has_sensitive_personal_data",
        "legal_basis",
        "privacy_purpose",
        "retention_policy",
        "access_scope",
        "access_roles",
        "is_masked",
        "external_sharing",
        "privacy_notes",
    ]
    items: list[dict[str, Any]] = []
    for field in field_order:
        if before_privacy.get(field) != after_privacy.get(field):
            items.append({"field": field, "previous": before_privacy.get(field), "new": after_privacy.get(field)})
    if before_reviewed_at != after_reviewed_at:
        items.append(
            {
                "field": "privacy_reviewed_at",
                "previous": before_reviewed_at.isoformat() if before_reviewed_at else None,
                "new": after_reviewed_at.isoformat() if after_reviewed_at else None,
            }
        )
    if before_reviewer_user_id != after_reviewer_user_id:
        items.append(
            {
                "field": "privacy_reviewed_by_user_id",
                "previous": before_reviewer_user_id,
                "new": after_reviewer_user_id,
            }
        )
    return items


def _event_review_type(changed_fields: list[dict[str, Any]]) -> str:
    field_names = {str(item.get("field")) for item in changed_fields}
    effective_fields = field_names - {"privacy_reviewed_at", "privacy_reviewed_by_user_id"}
    if not effective_fields:
        return "periodic_review"
    category_map = {
        "classification": "classification",
        "has_personal_data": "classification",
        "has_sensitive_personal_data": "classification",
        "access_scope": "access_change",
        "access_roles": "access_change",
        "legal_basis": "legal_basis_change",
        "privacy_purpose": "purpose_change",
        "retention_policy": "retention_change",
        "is_masked": "masking_change",
        "external_sharing": "external_sharing_change",
        "privacy_notes": "manual_review",
    }
    categories = {category_map.get(field, "manual_review") for field in effective_fields}
    if len(categories) == 1:
        return next(iter(categories))
    return "mixed_change"


def _privacy_risk_level(table: TableEntity, privacy: dict[str, Any]) -> str:
    owner_missing = _owner_missing(table)
    wide_access = _is_wide_access(privacy.get("access_scope"))
    possible_personal = bool(privacy.get("possible_personal_data"))
    confirmed_personal = bool(privacy.get("has_personal_data"))
    confirmed_sensitive = bool(privacy.get("has_sensitive_personal_data"))
    has_classification = bool(privacy.get("sensitivity_level") or confirmed_personal or confirmed_sensitive)
    no_legal_basis = _needs_legal_basis(privacy) and not privacy.get("legal_basis")
    no_review = not privacy.get("privacy_reviewed_at")
    no_purpose = _needs_legal_basis(privacy) and not privacy.get("privacy_purpose")
    external_sharing = bool(privacy.get("external_sharing"))
    no_retention = not privacy.get("retention_policy")

    if external_sharing and (confirmed_personal or confirmed_sensitive):
        return "critical"
    if confirmed_sensitive and wide_access:
        return "critical"
    if confirmed_personal and no_legal_basis and wide_access:
        return "critical"
    if possible_personal and not has_classification and wide_access:
        return "high"
    if confirmed_personal and no_legal_basis:
        return "high"
    if confirmed_personal and no_purpose:
        return "high"
    if confirmed_sensitive:
        return "high"
    if not has_classification or owner_missing or no_review or no_retention:
        return "medium"
    return "low"


def _next_review_at_for_table(table: TableEntity, *, db: Session) -> datetime:
    settings = get_governance_settings_snapshot(db)
    interval_days = (
        settings.sensitive_privacy_review_interval_days
        if table.has_sensitive_personal_data
        else settings.privacy_review_interval_days
    )
    return datetime.now(timezone.utc) + timedelta(days=interval_days)


def _serialize_privacy_event(event: PrivacyReviewEvent) -> PrivacyReviewEventOut:
    changed_fields_payload = []
    raw_changed = event.metadata_json.get("changed_fields") if isinstance(event.metadata_json, dict) else []
    for item in raw_changed or []:
        if not isinstance(item, dict):
            continue
        changed_fields_payload.append(
            PrivacyReviewChangedFieldOut(
                field=str(item.get("field") or ""),
                previous=item.get("previous"),
                new=item.get("new"),
            )
        )
    return PrivacyReviewEventOut(
        id=event.id,
        table_id=event.table_id,
        table_name=event.table_name,
        schema_name=event.schema_name,
        database_name=event.database_name,
        review_type=event.review_type,
        review_source=event.review_source,
        reviewer_user_id=event.reviewer_user_id,
        reviewer_name=event.reviewer_name,
        reviewer_email=event.reviewer_email,
        notes=event.notes,
        risk_before=event.risk_before,
        risk_after=event.risk_after,
        next_review_at=event.next_review_at,
        created_at=event.created_at,
        changed_fields=changed_fields_payload,
    )


def _privacy_event_query() -> Select[Any]:
    return (
        select(PrivacyReviewEvent)
        .join(TableEntity, TableEntity.id == PrivacyReviewEvent.table_id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
    )


def _resolve_privacy_event_page(
    *,
    db: Session,
    current_user: User,
    table_id: int | None = None,
    review_type: str | None = None,
    review_source: str | None = None,
    reviewer_user_id: int | None = None,
    reviewer: str | None = None,
    database_name: str | None = None,
    schema_name: str | None = None,
    table_name: str | None = None,
    owner: str | None = None,
    sensitivity_level: str | None = None,
    access_scope: str | None = None,
    risk_before: str | None = None,
    risk_after: str | None = None,
    only_risk_increased: bool = False,
    only_risk_reduced: bool = False,
    date_from: date | None = None,
    date_to: date | None = None,
    field: str | None = None,
    page: int = 1,
    page_size: int = 25,
) -> PrivacyReviewEventPageOut:
    query = _privacy_event_query()
    if table_id is not None:
        query = query.where(PrivacyReviewEvent.table_id == table_id)
    if review_type:
        query = query.where(PrivacyReviewEvent.review_type == review_type.strip().lower())
    if review_source:
        query = query.where(PrivacyReviewEvent.review_source == review_source.strip().lower())
    if reviewer_user_id is not None:
        query = query.where(PrivacyReviewEvent.reviewer_user_id == reviewer_user_id)
    if reviewer and reviewer.strip():
        token = f"%{reviewer.strip()}%"
        query = query.where(or_(PrivacyReviewEvent.reviewer_name.ilike(token), PrivacyReviewEvent.reviewer_email.ilike(token)))
    if database_name and database_name.strip():
        query = query.where(func.lower(PrivacyReviewEvent.database_name) == database_name.strip().lower())
    if schema_name and schema_name.strip():
        query = query.where(func.lower(PrivacyReviewEvent.schema_name) == schema_name.strip().lower())
    if table_name and table_name.strip():
        query = query.where(func.lower(PrivacyReviewEvent.table_name) == table_name.strip().lower())
    if owner and owner.strip():
        token = f"%{owner.strip()}%"
        query = query.where(or_(TableEntity.owner.ilike(token), TableEntity.owner_email.ilike(token)))
    if sensitivity_level and sensitivity_level.strip():
        query = query.where(PrivacyReviewEvent.new_sensitivity_level == sensitivity_level.strip().lower())
    if access_scope and access_scope.strip():
        query = query.where(PrivacyReviewEvent.new_access_scope == access_scope.strip().lower())
    if risk_before and risk_before.strip():
        query = query.where(PrivacyReviewEvent.risk_before == risk_before.strip().lower())
    if risk_after and risk_after.strip():
        query = query.where(PrivacyReviewEvent.risk_after == risk_after.strip().lower())
    if date_from is not None:
        query = query.where(PrivacyReviewEvent.created_at >= datetime.combine(date_from, datetime.min.time(), tzinfo=timezone.utc))
    if date_to is not None:
        query = query.where(PrivacyReviewEvent.created_at <= datetime.combine(date_to, datetime.max.time(), tzinfo=timezone.utc))
    rows = db.scalars(query.order_by(desc(PrivacyReviewEvent.created_at), desc(PrivacyReviewEvent.id))).unique().all()
    visible_rows: list[PrivacyReviewEvent] = []
    for event in rows:
        table = db.get(TableEntity, event.table_id)
        if not table or not can_view_table(current_user, table):
            continue
        before_score = _risk_score(event.risk_before or "unknown")
        after_score = _risk_score(event.risk_after or "unknown")
        if only_risk_increased and not (after_score > before_score):
            continue
        if only_risk_reduced and not (after_score < before_score):
            continue
        if field and field.strip():
            metadata_items = event.metadata_json.get("changed_fields") if isinstance(event.metadata_json, dict) else []
            if not any(isinstance(item, dict) and str(item.get("field")) == field.strip() for item in metadata_items or []):
                continue
        visible_rows.append(event)

    total = len(visible_rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    effective_page = min(page, total_pages) if total else 1
    start = (effective_page - 1) * page_size
    end = start + page_size
    return PrivacyReviewEventPageOut(
        items=[_serialize_privacy_event(item) for item in visible_rows[start:end]],
        total=total,
        page=effective_page,
        page_size=page_size,
    )


def _build_privacy_event_summary(events: list[PrivacyReviewEvent], *, db: Session, current_user: User) -> PrivacyReviewEventSummaryOut:
    by_type: dict[str, int] = defaultdict(int)
    by_reviewer: dict[str, int] = defaultdict(int)
    by_schema: dict[str, int] = defaultdict(int)
    increased = 0
    reduced = 0
    unchanged = 0
    periodic_reviews = 0
    access_changes = 0
    legal_basis_changes = 0
    purpose_changes = 0
    assets_with_review_due: set[int] = set()
    upcoming_review_due = 0
    due_60_days = 0
    without_next_review = 0
    sensitive_without_next_review = 0
    current_risk_critical = 0
    current_risk_high = 0
    visible_total = 0
    recent_events: list[PrivacyReviewEventOut] = []
    now = datetime.now(timezone.utc)
    threshold_30 = now + timedelta(days=30)
    threshold_60 = now + timedelta(days=60)
    latest_events_by_table: dict[int, PrivacyReviewEvent] = {}
    for event in events:
        table = db.get(TableEntity, event.table_id)
        if not table or not can_view_table(current_user, table):
            continue
        visible_total += 1
        if len(recent_events) < 10:
            recent_events.append(_serialize_privacy_event(event))
        by_type[event.review_type] += 1
        by_reviewer[event.reviewer_name or event.reviewer_email or "Sistema"] += 1
        by_schema[f"{event.database_name}.{event.schema_name}"] += 1
        before_score = _risk_score(event.risk_before or "unknown")
        after_score = _risk_score(event.risk_after or "unknown")
        if after_score > before_score:
            increased += 1
        elif after_score < before_score:
            reduced += 1
        else:
            unchanged += 1
        if event.review_type == "periodic_review":
            periodic_reviews += 1
        if event.review_type == "access_change":
            access_changes += 1
        if event.review_type == "legal_basis_change":
            legal_basis_changes += 1
        if event.review_type == "purpose_change":
            purpose_changes += 1
        if event.table_id not in latest_events_by_table:
            latest_events_by_table[event.table_id] = event

    for event in latest_events_by_table.values():
        table = db.get(TableEntity, event.table_id)
        if not table or not can_view_table(current_user, table):
            continue
        if event.next_review_at and event.next_review_at <= now:
            assets_with_review_due.add(event.table_id)
        elif event.next_review_at and event.next_review_at <= threshold_30:
            upcoming_review_due += 1
        elif event.next_review_at and event.next_review_at <= threshold_60:
            due_60_days += 1
        elif not event.next_review_at:
            without_next_review += 1
            if bool(table.has_sensitive_personal_data):
                sensitive_without_next_review += 1
        current_risk = event.risk_after or "unknown"
        if current_risk == "critical":
            current_risk_critical += 1
        elif current_risk == "high":
            current_risk_high += 1

    return PrivacyReviewEventSummaryOut(
        total_events=visible_total,
        by_type=dict(sorted(by_type.items(), key=lambda item: item[0])),
        by_reviewer=dict(sorted(by_reviewer.items(), key=lambda item: item[0])),
        by_schema=dict(sorted(by_schema.items(), key=lambda item: item[0])),
        increased_risk=increased,
        reduced_risk=reduced,
        unchanged_risk=unchanged,
        periodic_reviews=periodic_reviews,
        access_changes=access_changes,
        legal_basis_changes=legal_basis_changes,
        purpose_changes=purpose_changes,
        assets_with_review_due=len(assets_with_review_due),
        upcoming_review_due=upcoming_review_due,
        due_60_days=due_60_days,
        without_next_review=without_next_review,
        sensitive_without_next_review=sensitive_without_next_review,
        current_risk_critical=current_risk_critical,
        current_risk_high=current_risk_high,
        review_due={
            "overdue": len(assets_with_review_due),
            "due_30_days": upcoming_review_due,
            "due_60_days": due_60_days,
            "without_next_review": without_next_review,
            "sensitive_without_next_review": sensitive_without_next_review,
        },
        recent_events=recent_events[:5],
    )


def _summary_payload_for_table(table: TableEntity, *, user: User) -> dict[str, Any]:
    decision = table_visibility_decision_from_entity(table, user=user)
    payload = privacy_summary_payload(table)
    if decision.masked:
        payload = mask_privacy_summary_payload(payload)
    return payload


def _build_summary(
    tables: list[TableEntity],
    *,
    current_user: User,
) -> PrivacySummaryOutPage:
    total_visible = len(tables)
    totals = PrivacySummaryTotalsOut(visible_assets=total_visible)
    risk = PrivacyRiskBucketsOut()
    blocker_counts: dict[str, int] = defaultdict(int)
    by_schema_counts: dict[tuple[str, str], dict[str, Any]] = {}
    priorities: list[PrivacyPriorityOut] = []

    for table in tables:
        privacy = _summary_payload_for_table(table, user=current_user)
        classified = bool(privacy.get("sensitivity_level"))
        confirmed_personal = bool(privacy.get("has_personal_data"))
        confirmed_sensitive = bool(privacy.get("has_sensitive_personal_data"))
        restricted = _is_restricted_access(privacy.get("access_scope"))
        possible_personal = bool(privacy.get("possible_personal_data"))
        without_legal_basis = _needs_legal_basis(privacy) and not privacy.get("legal_basis")
        wide_access_with_suspicion = possible_personal and _is_wide_access(privacy.get("access_scope"))
        without_owner = _owner_missing(table)
        without_review = not privacy.get("privacy_reviewed_at")

        totals.classified_assets += int(classified)
        totals.unclassified_assets += int(not classified)
        totals.confirmed_personal_data += int(confirmed_personal)
        totals.confirmed_sensitive_data += int(confirmed_sensitive)
        totals.restricted_assets += int(restricted)
        totals.possible_personal_data += int(possible_personal)
        totals.without_legal_basis += int(without_legal_basis)
        totals.wide_access_with_suspicion += int(wide_access_with_suspicion)
        totals.without_owner += int(without_owner)
        totals.without_review += int(without_review)

        risk_level, reason, recommended_action = _risk_assessment(table, privacy)
        setattr(risk, risk_level, getattr(risk, risk_level) + 1)

        blocker_key = _top_blocker_key(table, privacy)
        if blocker_key != "controlled":
          blocker_counts[blocker_key] += 1

        group_key = (table.schema.database.name, table.schema.name)
        if group_key not in by_schema_counts:
            by_schema_counts[group_key] = {
                "database": table.schema.database.name,
                "schema": table.schema.name,
                "total": 0,
                "unclassified": 0,
                "possible_personal_data": 0,
                "confirmed_personal_data": 0,
                "sensitive_data": 0,
                "restricted": 0,
                "wide_access_with_suspicion": 0,
                "without_legal_basis": 0,
                "risk_score_total": 0,
            }
        bucket = by_schema_counts[group_key]
        bucket["total"] += 1
        bucket["unclassified"] += int(not classified)
        bucket["possible_personal_data"] += int(possible_personal)
        bucket["confirmed_personal_data"] += int(confirmed_personal)
        bucket["sensitive_data"] += int(confirmed_sensitive)
        bucket["restricted"] += int(restricted)
        bucket["wide_access_with_suspicion"] += int(wide_access_with_suspicion)
        bucket["without_legal_basis"] += int(without_legal_basis)
        bucket["risk_score_total"] += _risk_score(risk_level)

        if risk_level in {"critical", "high", "medium"}:
            priorities.append(
                PrivacyPriorityOut(
                    asset_id=table.id,
                    asset_name=table.name,
                    database_name=table.schema.database.name,
                    schema_name=table.schema.name,
                    risk_level=risk_level,
                    reason=reason,
                    recommended_action=recommended_action,
                )
            )

    top_blockers = []
    for key, count in sorted(blocker_counts.items(), key=lambda item: item[1], reverse=True):
        label, description, action = _BLOCKER_METADATA.get(
            key,
            (key.replace("_", " "), "Pendência de privacidade identificada na leitura atual.", "Revisar ativo"),
        )
        top_blockers.append(
            PrivacyTopBlockerOut(
                key=key,
                label=label,
                count=count,
                percent=round((count / total_visible) * 100, 2) if total_visible else 0,
                description=description,
                action=action,
            )
        )

    by_schema = [
        PrivacyBySchemaOut(
            database=value["database"],
            schema_name=value["schema"],
            total=value["total"],
            unclassified=value["unclassified"],
            possible_personal_data=value["possible_personal_data"],
            confirmed_personal_data=value["confirmed_personal_data"],
            sensitive_data=value["sensitive_data"],
            restricted=value["restricted"],
            wide_access_with_suspicion=value["wide_access_with_suspicion"],
            without_legal_basis=value["without_legal_basis"],
            risk_score=round(value["risk_score_total"] / value["total"]) if value["total"] else 0,
        )
        for value in sorted(
            by_schema_counts.values(),
            key=lambda item: (item["risk_score_total"], item["possible_personal_data"], item["unclassified"]),
            reverse=True,
        )
    ]

    priorities.sort(key=lambda item: (_risk_score(item.risk_level), item.asset_name), reverse=True)
    return PrivacySummaryOutPage(
        totals=totals,
        risk=risk,
        top_blockers=top_blockers[:6],
        by_schema=by_schema[:12],
        priorities=priorities[:10],
    )


@router.get("/options", response_model=PrivacyAccessOptionsOut)
def privacy_options(
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacyAccessOptionsOut:
    return PrivacyAccessOptionsOut(
        sensitivity_levels=[{"value": value, "label": SENSITIVITY_LABELS[value]} for value in SENSITIVITY_LEVELS],
        legal_basis_options=[{"value": value, "label": LEGAL_BASIS_LABELS[value]} for value in LEGAL_BASIS_OPTIONS],
        access_scopes=[{"value": value, "label": ACCESS_SCOPE_LABELS[value]} for value in ACCESS_SCOPE_OPTIONS],
        access_roles=[{"value": value, "label": ACCESS_ROLE_LABELS[value]} for value in ACCESS_ROLE_OPTIONS],
    )


def _privacy_is_wide_access(scope: str | None) -> bool:
    return not scope or scope in {"authenticated", "public"}


def _privacy_is_restricted_access(scope: str | None) -> bool:
    return scope in {"confidential", "restricted", "personal_data"}


def _privacy_needs_legal_basis(privacy: PrivacySummaryOut) -> bool:
    return bool(privacy.has_personal_data or privacy.has_sensitive_personal_data)


def _matches_privacy_quick_filter(item: PrivacyTableListItemOut, quick_filter: str) -> bool:
    """Mirror the UI's matchesQuickFilter exactly, on the serialized item, so the
    server-side quick filter has identical semantics to the former client-side one."""
    privacy = item.privacy
    if quick_filter in ("", "all"):
        return True
    if quick_filter == "possible_personal_data":
        return bool(privacy.possible_personal_data)
    if quick_filter == "not_classified":
        return not privacy.sensitivity_level
    if quick_filter == "personal_confirmed":
        return bool(privacy.has_personal_data)
    if quick_filter == "sensitive":
        return bool(privacy.has_sensitive_personal_data)
    if quick_filter == "restricted":
        return _privacy_is_restricted_access(privacy.access_scope)
    if quick_filter == "wide_access":
        return _privacy_is_wide_access(privacy.access_scope)
    if quick_filter == "without_legal_basis":
        return _privacy_needs_legal_basis(privacy) and not privacy.legal_basis
    if quick_filter == "without_owner":
        return not item.owner
    if quick_filter == "without_review":
        return not privacy.privacy_reviewed_at
    if quick_filter == "high_risk":
        return bool(
            (privacy.possible_personal_data and _privacy_is_wide_access(privacy.access_scope))
            or (privacy.has_sensitive_personal_data and _privacy_is_wide_access(privacy.access_scope))
            or (_privacy_needs_legal_basis(privacy) and not privacy.legal_basis)
        )
    return True


@router.get("/tables", response_model=PageOut[PrivacyTableListItemOut])
def list_privacy_tables(
    q: str | None = Query(default=None),
    sensitivity_level: str | None = Query(default=None),
    has_personal_data: bool | None = Query(default=None),
    access_scope: str | None = Query(default=None),
    quick_filter: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[PrivacyTableListItemOut]:
    normalized_page, normalized_page_size = normalize_page_params(
        page=page,
        page_size=page_size,
        default_page_size=25,
        max_page_size=200,
    )
    stmt = _apply_table_filters(
        _table_query(),
        q=q,
        sensitivity_level=sensitivity_level,
        has_personal_data=has_personal_data,
        access_scope=access_scope,
    ).order_by(TableEntity.updated_at.desc(), TableEntity.id.desc())

    normalized_quick_filter = (quick_filter or "").strip()
    if normalized_quick_filter and normalized_quick_filter != "all":
        # Quick filters depend on derived per-item signals, so apply them over the full
        # (base-filtered) set in Python and paginate afterwards — keeping behavior consistent.
        all_tables = db.scalars(stmt).all()
        serialized = [
            _serialize_table(
                table,
                masked=table_visibility_decision_from_entity(table, user=current_user).masked,
                current_user=current_user,
            )
            for table in all_tables
            if can_view_table(current_user, table)
        ]
        filtered = [item for item in serialized if _matches_privacy_quick_filter(item, normalized_quick_filter)]
        total = len(filtered)
        offset = (normalized_page - 1) * normalized_page_size
        items = filtered[offset : offset + normalized_page_size]
        total_pages = max(1, (total + normalized_page_size - 1) // normalized_page_size) if total > 0 else 0
        return PageOut[PrivacyTableListItemOut](
            page=normalized_page,
            page_size=normalized_page_size,
            total=total,
            total_pages=total_pages,
            has_more=normalized_page * normalized_page_size < total,
            items=items,
        )

    total = int(db.scalar(select(func.count()).select_from(stmt.order_by(None).subquery())) or 0)
    offset = (normalized_page - 1) * normalized_page_size
    page_tables = db.scalars(stmt.offset(offset).limit(normalized_page_size)).all()
    visible = [table for table in page_tables if can_view_table(current_user, table)]
    items = [
        _serialize_table(
            table,
            masked=table_visibility_decision_from_entity(table, user=current_user).masked,
            current_user=current_user,
        )
        for table in visible
    ]
    total_pages = max(1, (total + normalized_page_size - 1) // normalized_page_size) if total > 0 else 0
    return PageOut[PrivacyTableListItemOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=items,
    )


@router.get("/summary", response_model=PrivacySummaryOutPage)
def privacy_summary(
    q: str | None = Query(default=None),
    sensitivity_level: str | None = Query(default=None),
    has_personal_data: bool | None = Query(default=None),
    access_scope: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacySummaryOutPage:
    visible = _filtered_visible_tables(
        db,
        current_user=current_user,
        q=q,
        sensitivity_level=sensitivity_level,
        has_personal_data=has_personal_data,
        access_scope=access_scope,
    )
    return _build_summary(visible, current_user=current_user)


@router.get("/tables/{table_id}", response_model=PrivacyTableDetailOut)
def get_privacy_table(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacyTableDetailOut:
    table = db.scalar(_table_query().where(TableEntity.id == table_id))
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    visibility = table_visibility_decision_from_entity(table, user=current_user)
    base = _serialize_table(table, masked=visibility.masked, current_user=current_user).model_dump()
    base.update(
        description_manual=table.description_manual,
        description_source=table.description_source,
        lifecycle_status=table.lifecycle_status,
        certification_status=table.certification_status,
        certification_criticality=table.certification_criticality,
        certification_badges=table.certification_badges,
        suspected_columns=[] if visibility.masked else suspected_personal_data_columns(getattr(table, "columns", None)),
    )
    if visibility.masked:
        base.update(
            description_manual=None,
            description_source=None,
            certification_criticality=None,
            certification_badges=[],
        )
    return PrivacyTableDetailOut(**base)


@router.patch("/tables/{table_id}", response_model=PrivacyTableDetailOut)
def patch_privacy_table(
    table_id: int,
    payload: PrivacyAccessPatch,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacyTableDetailOut:
    table = db.scalar(_table_query().where(TableEntity.id == table_id))
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_edit_privacy(current_user, table=table):
        record_abac_denial(
            db,
            request=request,
            current_user=current_user,
            action="update",
            table=table,
            reason="privacy_edit_denied",
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Profile cannot edit privacy policy")

    before = build_table_history_snapshot(table)
    before_reviewed_at = table.privacy_reviewed_at
    before_reviewer_user_id = table.privacy_reviewed_by_user_id
    before_privacy = _privacy_state_from_snapshot(table, before)
    updates = payload.model_dump(exclude_unset=True)

    if "sensitivity_level" in updates and updates["sensitivity_level"] not in SENSITIVITY_LEVELS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid sensitivity_level")
    if "legal_basis" in updates and updates["legal_basis"] and updates["legal_basis"] not in LEGAL_BASIS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid legal_basis")
    if "access_scope" in updates and updates["access_scope"] and updates["access_scope"] not in ACCESS_SCOPE_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid access_scope")
    if "access_roles" in updates:
        normalized_roles = normalize_access_roles(updates["access_roles"])
        if len(normalized_roles) != len([role for role in (updates["access_roles"] or []) if role]):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid access_roles")
        updates["access_roles"] = normalized_roles or None

    for key, value in updates.items():
        setattr(table, key, value)

    table.privacy_reviewed_by_user_id = current_user.id
    table.privacy_reviewed_at = datetime.now(timezone.utc)

    after = build_table_history_snapshot(table)
    after_privacy = _privacy_state_from_snapshot(table, after)
    changes = [
        change
        for change in table_history_changes(before, after)
        if change.field_name
        in {
            "classification",
            "has_personal_data",
            "has_sensitive_personal_data",
            "legal_basis",
            "privacy_purpose",
            "retention_policy",
            "is_masked",
            "external_sharing",
            "access_scope",
            "access_roles",
            "privacy_notes",
        }
    ]
    changed_fields = _event_changed_fields(
        before_privacy,
        after_privacy,
        before_reviewed_at=before_reviewed_at,
        after_reviewed_at=table.privacy_reviewed_at,
        before_reviewer_user_id=before_reviewer_user_id,
        after_reviewer_user_id=table.privacy_reviewed_by_user_id,
    )
    effective_changed_fields = [
        item for item in changed_fields if item["field"] not in {"privacy_reviewed_at", "privacy_reviewed_by_user_id"}
    ]
    if changes:
        log_field_changes(
            db,
            action="table.privacy.patch",
            entity_type="table",
            entity_id=table.id,
            changes=changes,
            source_module="privacy_access",
            metadata={"message": "Privacy and access policy updated"},
            audit_kwargs=request_audit_kwargs(request, current_user),
            actor_user_id=current_user.id,
        )

    if effective_changed_fields:
        review_type = _event_review_type(changed_fields)
        event = PrivacyReviewEvent(
            table_id=table.id,
            table_name=table.name,
            database_name=table.schema.database.name,
            schema_name=table.schema.name,
            previous_sensitivity_level=before_privacy.get("sensitivity_level"),
            new_sensitivity_level=after_privacy.get("sensitivity_level"),
            previous_has_personal_data=before_privacy.get("has_personal_data"),
            new_has_personal_data=after_privacy.get("has_personal_data"),
            previous_has_sensitive_personal_data=before_privacy.get("has_sensitive_personal_data"),
            new_has_sensitive_personal_data=after_privacy.get("has_sensitive_personal_data"),
            previous_legal_basis=before_privacy.get("legal_basis"),
            new_legal_basis=after_privacy.get("legal_basis"),
            previous_privacy_purpose=before_privacy.get("privacy_purpose"),
            new_privacy_purpose=after_privacy.get("privacy_purpose"),
            previous_retention_policy=before_privacy.get("retention_policy"),
            new_retention_policy=after_privacy.get("retention_policy"),
            previous_access_scope=before_privacy.get("access_scope"),
            new_access_scope=after_privacy.get("access_scope"),
            previous_access_roles=_normalize_event_roles(before_privacy.get("access_roles")),
            new_access_roles=_normalize_event_roles(after_privacy.get("access_roles")),
            previous_is_masked=before_privacy.get("is_masked"),
            new_is_masked=after_privacy.get("is_masked"),
            previous_external_sharing=before_privacy.get("external_sharing"),
            new_external_sharing=after_privacy.get("external_sharing"),
            previous_privacy_notes=before_privacy.get("privacy_notes"),
            new_privacy_notes=after_privacy.get("privacy_notes"),
            review_type=review_type,
            review_source="manual",
            reviewer_user_id=current_user.id,
            reviewer_name=current_user.name or current_user.full_name,
            reviewer_email=current_user.email,
            notes=after_privacy.get("privacy_notes"),
            risk_before=_privacy_risk_level(table, before_privacy),
            risk_after=_privacy_risk_level(table, after_privacy),
            next_review_at=_next_review_at_for_table(table, db=db),
            metadata_json={
                "changed_fields": changed_fields,
                "possible_personal_data": after_privacy.get("possible_personal_data"),
            },
        )
        db.add(event)

    db.commit()
    db.refresh(table)
    base = _serialize_table(table, current_user=current_user).model_dump()
    base.update(
        description_manual=table.description_manual,
        description_source=table.description_source,
        lifecycle_status=table.lifecycle_status,
        certification_status=table.certification_status,
        certification_criticality=table.certification_criticality,
        certification_badges=table.certification_badges,
        suspected_columns=suspected_personal_data_columns(getattr(table, "columns", None)),
    )
    return PrivacyTableDetailOut(**base)


@router.post("/tables/{table_id}/periodic-review", response_model=PrivacyTableDetailOut)
def register_privacy_periodic_review(
    table_id: int,
    payload: PrivacyPeriodicReviewIn,
    request: Request = None,  # injected by FastAPI for HTTP calls; optional for direct unit calls
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacyTableDetailOut:
    table = db.scalar(_table_query().where(TableEntity.id == table_id))
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_edit_privacy(current_user, table=table):
        record_abac_denial(
            db,
            request=request,
            current_user=current_user,
            action="approve",
            table=table,
            reason="privacy_review_denied",
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Profile cannot edit privacy policy")
    if not payload.confirmed:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Periodic review confirmation is required")
    if not (payload.notes or "").strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe a justificativa da revisão periódica para manter a trilha decisória.",
        )
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    if (table.has_personal_data or table.has_sensitive_personal_data) and not (table.legal_basis or "").strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Ativos com dado pessoal ou sensível exigem base legal registrada antes da revisão formal de privacidade.",
        )
    if (table.has_personal_data or table.has_sensitive_personal_data) and not (table.privacy_purpose or "").strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Ativos com dado pessoal ou sensível exigem finalidade estruturada antes da revisão formal de privacidade.",
        )
    if (table.has_personal_data or table.has_sensitive_personal_data) and payload.next_review_at is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe a próxima revisão formal para registrar o ciclo de privacidade.",
        )
    if table.has_sensitive_personal_data:
        reviewer_tokens = role_tokens_for_user(current_user)
        if not reviewer_tokens.intersection({"admin", "governance"}):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Revisões de privacidade sensível exigem aprovação de governança ou administração.",
            )

    before = build_table_history_snapshot(table)
    before_privacy = _privacy_state_from_snapshot(table, before)
    now = datetime.now(timezone.utc)
    table.privacy_reviewed_by_user_id = current_user.id
    table.privacy_reviewed_at = now
    after = build_table_history_snapshot(table)
    after_privacy = _privacy_state_from_snapshot(table, after)
    next_review_at = payload.next_review_at or _next_review_at_for_table(table, db=db)

    event = PrivacyReviewEvent(
        table_id=table.id,
        table_name=table.name,
        database_name=table.schema.database.name,
        schema_name=table.schema.name,
        previous_sensitivity_level=before_privacy.get("sensitivity_level"),
        new_sensitivity_level=after_privacy.get("sensitivity_level"),
        previous_has_personal_data=before_privacy.get("has_personal_data"),
        new_has_personal_data=after_privacy.get("has_personal_data"),
        previous_has_sensitive_personal_data=before_privacy.get("has_sensitive_personal_data"),
        new_has_sensitive_personal_data=after_privacy.get("has_sensitive_personal_data"),
        previous_legal_basis=before_privacy.get("legal_basis"),
        new_legal_basis=after_privacy.get("legal_basis"),
        previous_privacy_purpose=before_privacy.get("privacy_purpose"),
        new_privacy_purpose=after_privacy.get("privacy_purpose"),
        previous_retention_policy=before_privacy.get("retention_policy"),
        new_retention_policy=after_privacy.get("retention_policy"),
        previous_access_scope=before_privacy.get("access_scope"),
        new_access_scope=after_privacy.get("access_scope"),
        previous_access_roles=_normalize_event_roles(before_privacy.get("access_roles")),
        new_access_roles=_normalize_event_roles(after_privacy.get("access_roles")),
        previous_is_masked=before_privacy.get("is_masked"),
        new_is_masked=after_privacy.get("is_masked"),
        previous_external_sharing=before_privacy.get("external_sharing"),
        new_external_sharing=after_privacy.get("external_sharing"),
        previous_privacy_notes=before_privacy.get("privacy_notes"),
        new_privacy_notes=after_privacy.get("privacy_notes"),
        review_type="periodic_review",
        review_source="manual",
        reviewer_user_id=current_user.id,
        reviewer_name=current_user.name or current_user.full_name,
        reviewer_email=current_user.email,
        notes=(payload.notes or "").strip(),
        risk_before=_privacy_risk_level(table, before_privacy),
        risk_after=_privacy_risk_level(table, after_privacy),
        next_review_at=next_review_at,
        metadata_json={
            "changed_fields": [],
            "possible_personal_data": after_privacy.get("possible_personal_data"),
            "periodic_review": True,
            "review_without_policy_change": True,
            "justification_required": True,
        },
    )
    db.add(event)
    log_field_changes(
        db,
        action="table.privacy.periodic_review",
        entity_type="table",
        entity_id=table.id,
        changes=[
            AuditFieldChange(
                field_name="privacy_reviewed_by_user_id",
                before=before_privacy.get("privacy_reviewed_by_user_id"),
                after=table.privacy_reviewed_by_user_id,
            ),
            AuditFieldChange(
                field_name="privacy_reviewed_at",
                before=before_privacy.get("privacy_reviewed_at"),
                after=table.privacy_reviewed_at.isoformat() if table.privacy_reviewed_at else None,
            ),
        ],
        source_module="privacy_access.periodic_review",
        metadata={
            "message": "Periodic privacy review recorded",
            "review_type": "periodic_review",
            "review_source": "manual",
            "next_review_at": next_review_at.isoformat() if next_review_at else None,
            "justification": (payload.notes or "").strip(),
        },
        actor_user_id=current_user.id,
    )
    db.commit()
    db.refresh(table)

    base = _serialize_table(table, current_user=current_user).model_dump()
    base.update(
        description_manual=table.description_manual,
        description_source=table.description_source,
        lifecycle_status=table.lifecycle_status,
        certification_status=table.certification_status,
        certification_criticality=table.certification_criticality,
        certification_badges=table.certification_badges,
        suspected_columns=suspected_personal_data_columns(getattr(table, "columns", None)),
    )
    return PrivacyTableDetailOut(**base)


@router.get("/tables/{table_id}/events", response_model=PrivacyReviewEventPageOut)
def list_privacy_table_events(
    table_id: int,
    review_type: str | None = Query(default=None),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    reviewer_user_id: int | None = Query(default=None),
    field: str | None = Query(default=None),
    risk_level: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacyReviewEventPageOut:
    table = db.scalar(_table_query().where(TableEntity.id == table_id))
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    return _resolve_privacy_event_page(
        db=db,
        current_user=current_user,
        table_id=table_id,
        review_type=review_type,
        reviewer_user_id=reviewer_user_id,
        date_from=date_from,
        date_to=date_to,
        field=field,
        risk_after=risk_level,
        page=page,
        page_size=page_size,
    )


@router.get("/events", response_model=PrivacyReviewEventPageOut)
def list_privacy_events(
    review_type: str | None = Query(default=None),
    review_source: str | None = Query(default=None),
    reviewer_user_id: int | None = Query(default=None),
    reviewer: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    table_name: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    sensitivity_level: str | None = Query(default=None),
    access_scope: str | None = Query(default=None),
    risk_before: str | None = Query(default=None),
    risk_after: str | None = Query(default=None),
    only_risk_increased: bool = Query(default=False),
    only_risk_reduced: bool = Query(default=False),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    field: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacyReviewEventPageOut:
    return _resolve_privacy_event_page(
        db=db,
        current_user=current_user,
        review_type=review_type,
        review_source=review_source,
        reviewer_user_id=reviewer_user_id,
        reviewer=reviewer,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        owner=owner,
        sensitivity_level=sensitivity_level,
        access_scope=access_scope,
        risk_before=risk_before,
        risk_after=risk_after,
        only_risk_increased=only_risk_increased,
        only_risk_reduced=only_risk_reduced,
        date_from=date_from,
        date_to=date_to,
        field=field,
        page=page,
        page_size=page_size,
    )


@router.get("/events/summary", response_model=PrivacyReviewEventSummaryOut)
def privacy_events_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PrivacyReviewEventSummaryOut:
    rows = db.scalars(select(PrivacyReviewEvent).order_by(desc(PrivacyReviewEvent.created_at), desc(PrivacyReviewEvent.id))).all()
    return _build_privacy_event_summary(rows, db=db, current_user=current_user)


def build_privacy_access_export_artifact(
    db: Session,
    *,
    current_user: User,
    q: str | None = None,
    sensitivity_level: str | None = None,
    has_personal_data: bool | None = None,
    access_scope: str | None = None,
    **_: Any,
) -> ExportArtifactResult:
    tables = _filtered_visible_tables(
        db,
        current_user=current_user,
        q=q,
        sensitivity_level=sensitivity_level,
        has_personal_data=has_personal_data,
        access_scope=access_scope,
    )
    export_limit = resolve_export_limit(source_module="privacy_access", entity_type="privacy_asset")
    tables, truncated = enforce_export_limit(tables, limit=export_limit)
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "banco",
            "schema",
            "tabela",
            "owner",
            "sensibilidade",
            "dado_pessoal",
            "dado_sensivel",
            "sinal_automatico",
            "colunas_suspeitas",
            "base_legal",
            "finalidade",
            "retencao",
            "acesso",
            "roles",
            "revisao",
            "principais_riscos",
            "proxima_acao",
        ]
    )
    for table in tables:
        privacy = _summary_payload_for_table(table, user=current_user)
        suspected_columns = suspected_personal_data_columns(getattr(table, "columns", None))
        risk_level, reason, action = _risk_assessment(table, privacy)
        writer.writerow(
            [
                table.schema.database.name,
                table.schema.name,
                table.name,
                table.owner or "",
                privacy.get("sensitivity_label") or "Não classificado",
                "sim" if privacy.get("has_personal_data") else "nao",
                "sim" if privacy.get("has_sensitive_personal_data") else "nao",
                "sim" if privacy.get("possible_personal_data") else "nao",
                ", ".join(f"{item['column_name']} ({item['signal']})" for item in suspected_columns),
                privacy.get("legal_basis_label") or "",
                redact_export_value(privacy.get("privacy_purpose"), field_name="privacy_purpose"),
                redact_export_value(privacy.get("retention_policy"), field_name="retention_policy"),
                privacy.get("access_scope_label") or "",
                ", ".join(privacy.get("access_role_labels") or []),
                privacy.get("privacy_reviewed_at").isoformat() if isinstance(privacy.get("privacy_reviewed_at"), datetime) else "",
                f"{risk_level}: {reason}",
                action,
            ]
        )
    payload = buffer.getvalue().encode("utf-8")
    return ExportArtifactResult(
        payload=payload,
        filename="privacy_access_export.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(tables),
        truncated=truncated,
        export_format="csv",
    )


def build_privacy_review_events_export_artifact(
    db: Session,
    *,
    current_user: User,
    review_type: str | None = None,
    review_source: str | None = None,
    reviewer_user_id: int | None = None,
    reviewer: str | None = None,
    database_name: str | None = None,
    schema_name: str | None = None,
    table_name: str | None = None,
    owner: str | None = None,
    sensitivity_level: str | None = None,
    access_scope: str | None = None,
    risk_before: str | None = None,
    risk_after: str | None = None,
    only_risk_increased: bool = False,
    only_risk_reduced: bool = False,
    date_from: date | None = None,
    date_to: date | None = None,
    field: str | None = None,
    **_: Any,
) -> ExportArtifactResult:
    export_limit = resolve_export_limit(source_module="privacy_access", entity_type="privacy_review_event")
    page = _resolve_privacy_event_page(
        db=db,
        current_user=current_user,
        review_type=review_type,
        review_source=review_source,
        reviewer_user_id=reviewer_user_id,
        reviewer=reviewer,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        owner=owner,
        sensitivity_level=sensitivity_level,
        access_scope=access_scope,
        risk_before=risk_before,
        risk_after=risk_after,
        only_risk_increased=only_risk_increased,
        only_risk_reduced=only_risk_reduced,
        date_from=date_from,
        date_to=date_to,
        field=field,
        page=1,
        page_size=export_limit,
    )
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "data",
            "banco",
            "schema",
            "tabela",
            "tipo_revisao",
            "origem",
            "revisor",
            "risco_antes",
            "risco_depois",
            "sensibilidade_anterior",
            "nova_sensibilidade",
            "base_legal_anterior",
            "nova_base_legal",
            "finalidade_anterior",
            "nova_finalidade",
            "retencao_anterior",
            "nova_retencao",
            "acesso_anterior",
            "novo_acesso",
            "roles_anteriores",
            "novas_roles",
            "mascaramento_anterior",
            "novo_mascaramento",
            "compartilhamento_externo_anterior",
            "novo_compartilhamento_externo",
            "observacao",
            "proxima_revisao",
        ]
    )
    for item in page.items:
        event = db.get(PrivacyReviewEvent, item.id)
        if not event:
            continue
        writer.writerow(
            [
                item.created_at.isoformat(),
                item.database_name,
                item.schema_name,
                item.table_name,
                item.review_type,
                item.review_source,
                item.reviewer_name or item.reviewer_email or "",
                item.risk_before or "",
                item.risk_after or "",
                event.previous_sensitivity_level or "",
                event.new_sensitivity_level or "",
                event.previous_legal_basis or "",
                event.new_legal_basis or "",
                event.previous_privacy_purpose or "",
                event.new_privacy_purpose or "",
                event.previous_retention_policy or "",
                event.new_retention_policy or "",
                event.previous_access_scope or "",
                event.new_access_scope or "",
                ", ".join(event.previous_access_roles or []),
                ", ".join(event.new_access_roles or []),
                "sim" if event.previous_is_masked else "nao" if event.previous_is_masked is not None else "",
                "sim" if event.new_is_masked else "nao" if event.new_is_masked is not None else "",
                "sim" if event.previous_external_sharing else "nao" if event.previous_external_sharing is not None else "",
                "sim" if event.new_external_sharing else "nao" if event.new_external_sharing is not None else "",
                redact_export_value(item.notes, field_name="notes"),
                item.next_review_at.isoformat() if item.next_review_at else "",
            ]
        )
    payload = buffer.getvalue().encode("utf-8")
    return ExportArtifactResult(
        payload=payload,
        filename="privacy_review_events.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(page.items),
        truncated=page.total > len(page.items),
        export_format="csv",
    )


@router.get("/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_privacy_csv(
    request: Request,
    q: str | None = Query(default=None),
    sensitivity_level: str | None = Query(default=None),
    has_personal_data: bool | None = Query(default=None),
    access_scope: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("privacy_access:export")),
    ) -> IntegrationSyncJobOut:
    job = enqueue_export_job(
        db,
        job_type="privacy_access.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "q": q,
            "sensitivity_level": sensitivity_level,
            "has_personal_data": has_personal_data,
            "access_scope": access_scope,
            "export_format": "csv",
        },
        context_json={
            "filters": {
                "q": q,
                "sensitivity_level": sensitivity_level,
                "has_personal_data": has_personal_data,
                "access_scope": access_scope,
            }
        },
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="privacy_access.export_requested",
        entity_type="privacy_asset",
        source_module="privacy_access",
        export_format="csv",
        filters={
            "q": q,
            "sensitivity_level": sensitivity_level,
            "has_personal_data": has_personal_data,
            "access_scope": access_scope,
        },
    )
    return serialize_export_job(job, request=request)


@router.get("/events/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_privacy_events_csv(
    request: Request,
    review_type: str | None = Query(default=None),
    review_source: str | None = Query(default=None),
    reviewer_user_id: int | None = Query(default=None),
    reviewer: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    table_name: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    sensitivity_level: str | None = Query(default=None),
    access_scope: str | None = Query(default=None),
    risk_before: str | None = Query(default=None),
    risk_after: str | None = Query(default=None),
    only_risk_increased: bool = Query(default=False),
    only_risk_reduced: bool = Query(default=False),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    field: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("privacy_access:export")),
) -> IntegrationSyncJobOut:
    job = enqueue_export_job(
        db,
        job_type="privacy_access.events.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "review_type": review_type,
            "review_source": review_source,
            "reviewer_user_id": reviewer_user_id,
            "reviewer": reviewer,
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "owner": owner,
            "sensitivity_level": sensitivity_level,
            "access_scope": access_scope,
            "risk_before": risk_before,
            "risk_after": risk_after,
            "only_risk_increased": only_risk_increased,
            "only_risk_reduced": only_risk_reduced,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "field": field,
            "export_format": "csv",
        },
        context_json={
            "filters": {
                "review_type": review_type,
                "review_source": review_source,
                "reviewer_user_id": reviewer_user_id,
                "reviewer": reviewer,
                "database_name": database_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "owner": owner,
                "sensitivity_level": sensitivity_level,
                "access_scope": access_scope,
                "risk_before": risk_before,
                "risk_after": risk_after,
                "only_risk_increased": only_risk_increased,
                "only_risk_reduced": only_risk_reduced,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None,
                "field": field,
            }
        },
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="privacy_access.events.export_requested",
        entity_type="privacy_review_event",
        source_module="privacy_access",
        export_format="csv",
        filters={
            "review_type": review_type,
            "review_source": review_source,
            "reviewer_user_id": reviewer_user_id,
            "reviewer": reviewer,
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "owner": owner,
            "sensitivity_level": sensitivity_level,
            "access_scope": access_scope,
            "risk_before": risk_before,
            "risk_after": risk_after,
            "only_risk_increased": only_risk_increased,
            "only_risk_reduced": only_risk_reduced,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "field": field,
        },
    )
    return serialize_export_job(job, request=request)
