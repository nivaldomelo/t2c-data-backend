from __future__ import annotations

from dataclasses import dataclass
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import logging
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.certification.api_support import certification_status_label
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.ingestion.service import IngestionIntegrationUnavailable, load_table_ingestion_detail
from t2c_data.models.auth import User
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRun, DQTableMetric
from t2c_data.models.governance import GovernanceTrustSnapshot, OperationalStabilitySnapshot
from t2c_data.models.incident import Incident
from t2c_data.models.platform import TimelineEpisodeAction
from t2c_data.models.tag import TagIntelligenceEvent
from t2c_data.schemas.timeline import (
    TimelineAnalyticsBucketOut,
    TimelineAnalyticsOut,
    TimelineEpisodeActionIn,
    TimelineEpisodeActionOut,
    TimelineEpisodeMemberOut,
    TimelineEpisodeOut,
    TimelineEventOut,
    TimelinePageOut,
    TimelineSummaryOut,
)

_CATEGORY_LABELS = {
    "governance": "Governança",
    "operation": "Operação",
    "quality": "Qualidade",
    "incident": "Incidente",
    "audit": "Auditoria",
}

_SOURCE_LABELS = {
    "catalog": "Catálogo",
    "glossary": "Glossário",
    "tags": "Tags",
    "certification": "Certificação",
    "privacy_access": "Privacidade e acesso",
    "privacy-access": "Privacidade e acesso",
    "governance": "Governança",
    "search": "Busca",
    "dashboard": "Dashboard",
    "incidents": "Incidentes",
    "lineage": "Linhagem",
    "admin": "Administração",
    "ingestion": "Ingestão",
    "dq": "Data Quality",
    "audit": "Auditoria",
    "ops": "Operação",
    "system": "Sistema",
}

_LOGGER = logging.getLogger(__name__)

@dataclass(slots=True)
class TimelineQuery:
    date_from: datetime | None = None
    date_to: datetime | None = None
    source: str | None = None
    datasource: str | None = None
    schema_name: str | None = None
    owner: str | None = None
    certification_status: str | None = None
    event_type: str | None = None
    category: str | None = None
    severity: str | None = None
    q: str | None = None
    manual_only: bool = False
    automatic_only: bool = False
    contains_pii: bool | None = None
    contains_sensitive: bool | None = None
    contains_critical: bool | None = None
    open_incidents: bool | None = None
    dq_recent: bool | None = None
    table_id: int | None = None
    column_id: int | None = None
    episode_status: str | None = None
    episode_type: str | None = None
    min_importance_score: int | None = None


def _normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _ensure_window(filters: TimelineQuery) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    end = _normalize_dt(filters.date_to) or now
    start = _normalize_dt(filters.date_from)
    if start is None:
        start = end - timedelta(days=30)
    return start, end


def _profile_matches_filters(profile, filters: TimelineQuery) -> bool:
    if filters.table_id is not None and profile.table_id != filters.table_id:
        return False
    datasource_name = (profile.datasource_name or "").lower()
    if filters.source and filters.source.strip().lower() not in datasource_name:
        return False
    if filters.datasource and filters.datasource.strip().lower() not in datasource_name:
        return False
    schema_name = (profile.schema_name or "").lower()
    if filters.schema_name and filters.schema_name.strip().lower() not in schema_name:
        return False
    if filters.owner and filters.owner.strip().lower() not in (profile.owner_name or "").lower():
        return False
    certification_status = (profile.certification_status or "").lower()
    if filters.certification_status and filters.certification_status.strip().lower() != certification_status:
        return False
    if filters.contains_pii is True and not profile.has_personal_data:
        return False
    if filters.contains_sensitive is True and not profile.has_sensitive_personal_data:
        return False
    if filters.contains_critical is True and profile.critical_open_incidents <= 0:
        return False
    if filters.open_incidents is True and profile.open_incidents <= 0:
        return False
    if filters.dq_recent is True and not (profile.active_dq_violation or (profile.dq_score is not None and profile.dq_score < 90)):
        return False
    if filters.q:
        q = filters.q.strip().lower()
        haystack = " ".join(
            value
            for value in (
                profile.table_fqn,
                profile.table_name,
                profile.schema_name,
                profile.database_name,
                profile.datasource_name,
                profile.owner_name or "",
            )
        ).lower()
        if q not in haystack:
            return False
    return True


def _manual_mode(*, user_id: int | None, actor_name: str | None, actor_email: str | None, source_module: str | None) -> str:
    if user_id is not None or actor_name or actor_email:
        return "manual"
    if (source_module or "").strip().lower() in {"tags", "glossary", "certification", "privacy_access", "governance"}:
        return "manual"
    if (source_module or "").strip().lower() in {"ingestion", "dq", "ops", "system", "lineage", "catalog"}:
        return "automatic"
    return "unknown"


def _severity_from_priority(priority: int) -> str:
    if priority >= 90:
        return "critical"
    if priority >= 70:
        return "high"
    if priority >= 40:
        return "medium"
    return "low"


def _priority_from_event(
    *,
    category: str,
    severity: str | None = None,
    confidence_score: int | None = None,
    active_dq_violation: bool = False,
    is_sensitive_change: bool = False,
    manual: bool = False,
    recurrent: bool = False,
) -> int:
    score = 25
    if category == "incident":
        score = 88
    elif category == "quality":
        score = 74
    elif category == "operation":
        score = 52
    elif category == "governance":
        score = 64
    elif category == "audit":
        score = 45
    if severity == "critical":
        score += 20
    elif severity == "high":
        score += 12
    elif severity == "medium":
        score += 6
    if confidence_score is not None:
        score = max(score, min(100, confidence_score))
    if active_dq_violation:
        score += 10
    if is_sensitive_change:
        score += 8
    if manual:
        score += 2
    if recurrent:
        score += 6
    return max(0, min(100, score))


def _category_label(category: str) -> str:
    return _CATEGORY_LABELS.get(category, category.replace("_", " ").title())


def _source_label(source: str | None) -> str:
    if not source:
        return "—"
    return _SOURCE_LABELS.get(source, source.replace("_", " ").title())


def _asset_href(table_id: int | None, column_id: int | None = None) -> str | None:
    if table_id is None:
        return None
    href = f"/explorer?tableId={table_id}&tab=history"
    if column_id is not None:
        href = f"/explorer?tableId={table_id}&tab=columns&columnId={column_id}"
    return href


def _timeline_item(**kwargs: Any) -> TimelineEventOut:
    return TimelineEventOut.model_validate(kwargs)


def _extend_events_safely(
    session: Session,
    events: list[TimelineEventOut],
    *,
    source_name: str,
    scope: str,
    table_id: int | None,
    build_events,
) -> None:
    try:
        chunk = build_events()
    except Exception:
        session.rollback()
        _LOGGER.exception(
            "timeline source failed scope=%s table_id=%s source=%s",
            scope,
            table_id,
            source_name,
        )
        return
    if chunk:
        events.extend(chunk)


def _table_variant_keys(profile) -> set[str]:
    variants = {f"{profile.schema_name}.{profile.table_name}"}
    if profile.datasource_name:
        variants.add(f"{profile.datasource_name}.{profile.schema_name}.{profile.table_name}")
    return variants


def _column_lookup(session: Session, table_ids: list[int]) -> dict[int, ColumnEntity]:
    if not table_ids:
        return {}
    rows = session.scalars(
        select(ColumnEntity).where(ColumnEntity.table_id.in_(table_ids)).order_by(ColumnEntity.table_id, ColumnEntity.ordinal_position)
    ).all()
    return {column.id: column for column in rows}


def _profiles_map(session: Session, *, filters: TimelineQuery, current_user=None) -> list:
    now = datetime.now(timezone.utc)
    if filters.table_id is not None:
        profiles = load_table_profiles(session, now, table_ids=[filters.table_id], current_user=current_user)
    else:
        profiles = load_table_profiles(session, now, current_user=current_user)
    return [profile for profile in profiles if _profile_matches_filters(profile, filters)]


def _profile_by_table_id(profiles: list) -> dict[int, object]:
    return {profile.table_id: profile for profile in profiles}


def _profile_by_fqn(profiles: list) -> dict[str, object]:
    mapping: dict[str, object] = {}
    for profile in profiles:
        for key in _table_variant_keys(profile):
            mapping[key] = profile
    return mapping


def _column_profile_key(column: ColumnEntity) -> str:
    return f"{column.table_id}:{column.id}"


def _audit_event_type(field_name: str | None, change_type: str | None, source_module: str | None, action: str | None) -> tuple[str, str, str]:
    field_key = (field_name or "").strip().lower()
    change_key = (change_type or "").strip().lower()
    source_key = (source_module or "").strip().lower()
    action_key = (action or "").strip().lower()
    if field_key in {"owner", "data_owner_id"} or "owner" in action_key:
        return "owner_changed", "governance", "Owner alterado"
    if field_key in {"classification", "sensitivity_level", "has_personal_data", "has_sensitive_personal_data"}:
        return "classification_changed", "governance", "Classificação alterada"
    if field_key.startswith("certification_") or change_key in {"certify", "decertify"}:
        return "certification_changed", "governance", "Certificação atualizada"
    if field_key in {"glossary_terms", "definition", "description", "dictionary_description", "dictionary_comment", "existing_comment"}:
        return "metadata_updated", "governance", "Metadados atualizados"
    if field_key == "tags" or source_key == "tags":
        return "tag_changed", "governance", "Tags atualizadas"
    if source_key == "glossary":
        return "term_changed", "governance", "Termos atualizados"
    if source_key in {"dq", "data_quality"}:
        return "dq_change", "quality", "Data Quality atualizada"
    if source_key in {"ingestion", "platform", "ops"}:
        return "pipeline_change", "operation", "Operação atualizada"
    if source_key == "lineage":
        return "lineage_change", "operation", "Linhagem atualizada"
    if source_key == "incidents":
        return "incident_change", "incident", "Incidente atualizado"
    if "recommendation" in action_key or "recommendation" in field_key:
        return "recommendation_change", "governance", "Recomendação atualizada"
    return "audit_event", "audit", (action or "Mudança registrada").replace(".", " • ").replace("_", " ")


def _dq_severity(dq_score: float | None, failed_rules: int, recurrent_degradation: bool = False) -> str:
    if dq_score is None:
        return "medium"
    if dq_score < 60 or failed_rules >= 3 or recurrent_degradation:
        return "critical"
    if dq_score < 70 or failed_rules > 0:
        return "high"
    if dq_score < 85:
        return "medium"
    return "low"


def _trust_severity(trust_score: int) -> str:
    if trust_score >= 85:
        return "low"
    if trust_score >= 70:
        return "medium"
    if trust_score >= 50:
        return "high"
    return "critical"


def _incident_event_title(incident: Incident) -> tuple[str, str, str]:
    status = (incident.status or "").lower()
    if status in {"open", "investigating"}:
        return "incident_opened", "incident", "Incidente aberto"
    if status in {"mitigated", "resolved", "closed"}:
        return "incident_closed", "incident", "Incidente encerrado"
    return "incident_updated", "incident", "Incidente atualizado"


def _rows_to_events_from_audit(
    rows,
    *,
    profile_by_table_id: dict[int, object],
    column_lookup: dict[int, ColumnEntity],
    filters: TimelineQuery,
) -> list[TimelineEventOut]:
    events: list[TimelineEventOut] = []
    for row in rows:
        item: AuditLog = row.AuditLog if hasattr(row, "AuditLog") else row
        table_id = getattr(row, "table_id", None)
        if table_id is None and item.entity_type == "table":
            try:
                table_id = int(item.entity_id or 0)
            except (TypeError, ValueError):
                table_id = None
        if table_id is None and item.entity_type == "column":
            try:
                column_id = int(item.entity_id or 0)
            except (TypeError, ValueError):
                column_id = 0
            column = column_lookup.get(column_id)
            if column is not None:
                table_id = column.table_id
        profile = profile_by_table_id.get(int(table_id)) if table_id is not None else None
        if profile is None:
            continue
        if filters.column_id is not None and item.entity_type == "column" and int(item.entity_id or 0) != filters.column_id:
            continue
        if filters.table_id is not None and profile.table_id != filters.table_id:
            continue
        event_type, category, title = _audit_event_type(item.field_name, item.change_type, item.source_module, item.action)
        entity_id = item.entity_id
        column_id = None
        column_name = None
        if item.entity_type == "column":
            try:
                column_id = int(item.entity_id) if item.entity_id is not None else None
            except ValueError:
                column_id = None
            if column_id is not None and column_id in column_lookup:
                column_name = column_lookup[column_id].name
        details = item.metadata_json if isinstance(item.metadata_json, dict) else {}
        message = None
        if isinstance(details, dict):
            message = details.get("message") if isinstance(details.get("message"), str) else None
        before = item.before_json
        after = item.after_json
        if message is None and item.field_name:
            message = f"{item.field_name}: {before!s} → {after!s}"
        manual = _manual_mode(
            user_id=item.user_id,
            actor_name=item.actor_name,
            actor_email=item.user_email,
            source_module=item.source_module,
        ) == "manual"
        priority = _priority_from_event(
            category=category,
            severity="critical" if item.is_sensitive_change else "medium",
            active_dq_violation=bool(profile.active_dq_violation),
            is_sensitive_change=bool(item.is_sensitive_change),
            manual=manual,
        )
        events.append(
            _timeline_item(
                id=f"audit:{item.id}",
                occurred_at=item.created_at,
                category=category,
                event_type=event_type,
                title=title,
                detail=message,
                source_module=item.source_module or "audit",
                source_label=_source_label(item.source_module),
                actor_name=item.actor_name,
                actor_email=item.user_email,
                mode="manual" if manual else "automatic",
                severity="critical" if item.is_sensitive_change else _severity_from_priority(priority),
                priority=priority,
                entity_type=item.entity_type,
                entity_id=str(entity_id) if entity_id is not None else None,
                table_id=profile.table_id,
                column_id=column_id,
                table_name=profile.table_name,
                column_name=column_name,
                schema_name=profile.schema_name,
                database_name=profile.database_name,
                datasource_name=profile.datasource_name,
                table_fqn=profile.table_fqn,
                owner_name=profile.owner_name,
                certification_status=profile.certification_status,
                certification_status_label=certification_status_label(profile.certification_status),
                readiness_score=profile.readiness_score,
                active_dq_violation=bool(profile.active_dq_violation),
                active_dq_rule_names=list(profile.active_dq_rule_names or []),
                href=_asset_href(profile.table_id, column_id if column_id is not None else filters.column_id),
                metadata_json=details or None,
            )
        )
    return events


def _rows_to_events_from_tag_intelligence(
    rows,
    *,
    profile_by_table_id: dict[int, object],
    column_lookup: dict[int, ColumnEntity],
    filters: TimelineQuery,
) -> list[TimelineEventOut]:
    events: list[TimelineEventOut] = []
    for row in rows:
        column_id = int(row.entity_id) if row.entity_type == "column" else None
        table_id = int(row.entity_id) if row.entity_type == "table" else None
        tag_name = row.tag.name if row.tag is not None else "Tag"
        if column_id is not None:
            column = column_lookup.get(column_id)
            table_id = column.table_id if column is not None else None
        profile = profile_by_table_id.get(int(table_id)) if table_id is not None else None
        if profile is None:
            continue
        entity_type = row.entity_type
        if filters.column_id is not None and entity_type == "column" and int(row.entity_id or 0) != filters.column_id:
            continue
        if filters.table_id is not None and profile.table_id != filters.table_id:
            continue
        column_name = column_lookup.get(column_id).name if column_id is not None and column_id in column_lookup else None
        if row.review_status == "blocked":
            event_type = "tag_blocked"
            title = f"Tag bloqueada: {tag_name}"
            category = "governance"
        elif row.review_status == "manual_applied":
            event_type = "tag_applied"
            title = f"Tag aprovada manualmente: {tag_name}"
            category = "governance"
        elif row.review_status == "removed":
            event_type = "tag_removed"
            title = f"Tag removida: {tag_name}"
            category = "governance"
        elif row.applied_automatically:
            event_type = "tag_applied"
            title = f"Tag aplicada automaticamente: {tag_name}"
            category = "governance"
        else:
            event_type = "tag_suggestion"
            title = f"Sugestão de tag: {tag_name}"
            category = "governance"
        manual = row.review_status == "manual_applied"
        severity = "high" if row.review_status in {"blocked", "pending_review", "suggested"} and int(row.confidence_score or 0) < 70 else "medium"
        priority = _priority_from_event(
            category=category,
            severity="critical" if row.review_status == "blocked" else severity,
            confidence_score=int(row.confidence_score or 0),
            active_dq_violation=bool(profile.active_dq_violation),
            manual=manual,
        )
        events.append(
            _timeline_item(
                id=f"tag:{row.id}",
                occurred_at=row.created_at,
                category=category,
                event_type=event_type,
                title=title,
                detail=row.inference_reason,
                source_module="tags",
                source_label="Tags",
                actor_name=None,
                actor_email=None,
                mode="manual" if manual else "automatic",
                severity=_severity_from_priority(priority),
                priority=priority,
                entity_type=entity_type,
                entity_id=str(row.entity_id),
                table_id=profile.table_id,
                column_id=column_id,
                table_name=profile.table_name,
                column_name=column_name,
                schema_name=profile.schema_name,
                database_name=profile.database_name,
                datasource_name=profile.datasource_name,
                table_fqn=profile.table_fqn,
                owner_name=profile.owner_name,
                certification_status=profile.certification_status,
                certification_status_label=certification_status_label(profile.certification_status),
                readiness_score=profile.readiness_score,
                active_dq_violation=bool(profile.active_dq_violation),
                active_dq_rule_names=list(profile.active_dq_rule_names or []),
                href=_asset_href(profile.table_id, column_id if column_id is not None else filters.column_id),
                metadata_json={
                    "rule_key": row.rule_key,
                    "rule_label": row.rule_label,
                    "confidence_score": int(row.confidence_score or 0),
                    "review_status": row.review_status,
                    "applied_automatically": bool(row.applied_automatically),
                    "inference_source": row.inference_source,
                    "evidence": row.evidence,
                },
            )
        )
    return events


def _rows_to_events_from_incidents(
    rows: list[Incident],
    *,
    profile_by_fqn: dict[str, object],
    filters: TimelineQuery,
) -> list[TimelineEventOut]:
    events: list[TimelineEventOut] = []
    for incident in rows:
        if incident.table_fqn is None:
            continue
        profile = profile_by_fqn.get(incident.table_fqn)
        if profile is None:
            parts = [part for part in incident.table_fqn.split(".") if part]
            if len(parts) >= 2:
                profile = profile_by_fqn.get(".".join(parts[-2:]))
        if profile is None:
            continue
        if filters.table_id is not None and profile.table_id != filters.table_id:
            continue
        event_type, category, title = _incident_event_title(incident)
        if incident.status == "open" and incident.occurrences > 1:
            title = "Incidente recorrente"
        manual = not ((incident.source_type or "").strip().lower() in {"dq_profile", "dq_rule", "ingestion_ops", "pipeline_failure", "pipeline_stale", "platform_ops"} and incident.reporter_user_id is None)
        severity = {"sev1": "critical", "sev2": "high", "sev3": "medium", "sev4": "low"}.get(incident.severity, "medium")
        priority = _priority_from_event(
            category=category,
            severity=severity,
            manual=manual,
            recurrent=incident.occurrences > 1,
        )
        events.append(
            _timeline_item(
                id=f"incident:{incident.id}",
                occurred_at=incident.updated_at or incident.detected_at,
                category=category,
                event_type=event_type,
                title=title,
                detail=incident.description,
                source_module="incidents",
                source_label="Incidentes",
                actor_name=incident.owner_user.name if incident.owner_user else None,
                actor_email=incident.owner_user.email if incident.owner_user else None,
                mode="manual" if manual else "automatic",
                severity=severity,
                priority=priority,
                entity_type=incident.entity_type,
                entity_id=str(incident.id),
                table_id=profile.table_id,
                column_id=None,
                table_name=profile.table_name,
                column_name=None,
                schema_name=profile.schema_name,
                database_name=profile.database_name,
                datasource_name=profile.datasource_name,
                table_fqn=profile.table_fqn,
                owner_name=profile.owner_name,
                certification_status=profile.certification_status,
                certification_status_label=certification_status_label(profile.certification_status),
                readiness_score=profile.readiness_score,
                active_dq_violation=bool(profile.active_dq_violation),
                active_dq_rule_names=list(profile.active_dq_rule_names or []),
                href=f"/incidents/tickets?tableId={profile.table_id}",
                metadata_json={
                    "status": incident.status,
                    "severity": incident.severity,
                    "source_type": incident.source_type,
                    "source_ref_id": incident.source_ref_id,
                    "evidence_json": incident.evidence_json,
                    "occurrences": incident.occurrences,
                },
            )
        )
    return events


def _rows_to_events_from_dq_runs(
    rows,
    *,
    profile_by_table_id: dict[int, object],
    filters: TimelineQuery,
) -> list[TimelineEventOut]:
    events: list[TimelineEventOut] = []
    for dq_run, table_metric in rows:
        profile = profile_by_table_id.get(int(table_metric.table_id))
        if profile is None:
            continue
        if filters.table_id is not None and profile.table_id != filters.table_id:
            continue
        dq_score = float(table_metric.dq_score or 0.0)
        failed_rules = int(table_metric.failed_rules or 0)
        severity = _dq_severity(dq_score, failed_rules)
        if dq_score < 60 or failed_rules >= 3:
            title = "DQ crítica"
            event_type = "dq_run_failure"
        elif dq_score < 70 or failed_rules > 0:
            title = "DQ com falhas"
            event_type = "dq_run_degraded"
        else:
            title = "DQ executada com sucesso"
            event_type = "dq_run_success"
        priority = _priority_from_event(
            category="quality",
            severity=severity,
            confidence_score=int(round(dq_score)),
            active_dq_violation=bool(profile.active_dq_violation),
            recurrent=bool(table_metric.failed_rules or 0),
        )
        events.append(
            _timeline_item(
                id=f"dq:{dq_run.id}:{table_metric.id}",
                occurred_at=dq_run.finished_at or dq_run.started_at or dq_run.created_at,
                category="quality",
                event_type=event_type,
                title=title,
                detail=f"Score {round(dq_score, 1)} · {failed_rules} regra(s) em falha · {int(table_metric.duplicates_count or 0)} duplicado(s).",
                source_module="dq",
                source_label="Data Quality",
                actor_name=None,
                actor_email=None,
                mode="automatic",
                severity=severity,
                priority=priority,
                entity_type="table",
                entity_id=str(profile.table_id),
                table_id=profile.table_id,
                column_id=None,
                table_name=profile.table_name,
                column_name=None,
                schema_name=profile.schema_name,
                database_name=profile.database_name,
                datasource_name=profile.datasource_name,
                table_fqn=profile.table_fqn,
                owner_name=profile.owner_name,
                certification_status=profile.certification_status,
                certification_status_label=certification_status_label(profile.certification_status),
                readiness_score=profile.readiness_score,
                active_dq_violation=bool(profile.active_dq_violation),
                active_dq_rule_names=list(profile.active_dq_rule_names or []),
                href=f"/data-quality?tableId={profile.table_id}",
                metadata_json={
                    "run_id": dq_run.id,
                    "status": dq_run.status,
                    "execution_engine": dq_run.execution_engine,
                    "row_count": int(table_metric.row_count or 0),
                    "completeness_pct_avg": float(table_metric.completeness_pct_avg or 0.0),
                    "dq_score": dq_score,
                    "duplicates_count": int(table_metric.duplicates_count or 0),
                    "failed_rules": failed_rules,
                },
            )
        )
    return events


def _rows_to_events_from_pipeline_snapshots(
    rows: list[OperationalStabilitySnapshot],
    *,
    profile_by_table_id: dict[int, object],
    filters: TimelineQuery,
) -> list[TimelineEventOut]:
    events: list[TimelineEventOut] = []
    for snapshot in rows:
        profile = profile_by_table_id.get(int(snapshot.table_id))
        if profile is None:
            continue
        if filters.table_id is not None and profile.table_id != filters.table_id:
            continue
        status_label = snapshot.latest_status_label or "Pendente"
        if snapshot.currently_stale:
            title = "Pipeline sem atualização recente"
            event_type = "pipeline_stale"
            severity = "high"
        elif status_label.lower().startswith("fal"):
            title = "Pipeline com falha"
            event_type = "pipeline_failure"
            severity = "critical"
        elif snapshot.recurrent_degradation:
            title = "Pipeline degradado"
            event_type = "pipeline_degraded"
            severity = "high"
        else:
            title = "Pipeline executado"
            event_type = "pipeline_success"
            severity = "low"
        priority = _priority_from_event(
            category="operation",
            severity=severity,
            recurrent=bool(snapshot.recurrent_degradation),
        )
        events.append(
            _timeline_item(
                id=f"pipeline-snapshot:{snapshot.id}",
                occurred_at=snapshot.bucket_start_at,
                category="operation",
                event_type=event_type,
                title=title,
                detail=(
                    f"{snapshot.pipeline_name or snapshot.dag_id or 'Pipeline'} · "
                    f"{snapshot.success_rate_pct:.1f}% de sucesso · {snapshot.rows_processed or 0} linha(s) processada(s)."
                ),
                source_module="ingestion",
                source_label="Ingestão",
                actor_name=None,
                actor_email=None,
                mode="automatic",
                severity=severity,
                priority=priority,
                entity_type="table",
                entity_id=str(profile.table_id),
                table_id=profile.table_id,
                column_id=None,
                table_name=profile.table_name,
                column_name=None,
                schema_name=profile.schema_name,
                database_name=profile.database_name,
                datasource_name=profile.datasource_name,
                table_fqn=profile.table_fqn,
                owner_name=profile.owner_name,
                certification_status=profile.certification_status,
                certification_status_label=certification_status_label(profile.certification_status),
                readiness_score=profile.readiness_score,
                active_dq_violation=bool(profile.active_dq_violation),
                active_dq_rule_names=list(profile.active_dq_rule_names or []),
                href=_asset_href(profile.table_id),
                metadata_json={
                    "pipeline_name": snapshot.pipeline_name,
                    "dag_id": snapshot.dag_id,
                    "task_name": snapshot.task_name,
                    "latest_status_label": snapshot.latest_status_label,
                    "last_success_at": snapshot.last_success_at,
                    "last_execution_finished_at": snapshot.last_execution_finished_at,
                    "window_runs": snapshot.window_runs,
                    "success_rate_pct": snapshot.success_rate_pct,
                    "failed_runs": snapshot.failed_runs,
                    "recurrent_degradation": snapshot.recurrent_degradation,
                    "currently_stale": snapshot.currently_stale,
                },
            )
        )
    return events


def _rows_to_events_from_trust_snapshots(
    rows: list[GovernanceTrustSnapshot],
    *,
    profile_by_table_id: dict[int, object],
    filters: TimelineQuery,
) -> list[TimelineEventOut]:
    events: list[TimelineEventOut] = []
    previous_score_by_table: dict[int, int] = {}
    for snapshot in rows:
        profile = profile_by_table_id.get(int(snapshot.table_id))
        if profile is None:
            continue
        if filters.table_id is not None and profile.table_id != filters.table_id:
            continue
        trust_score = int(snapshot.score or 0)
        previous_score = previous_score_by_table.get(profile.table_id)
        delta = trust_score - previous_score if previous_score is not None else None
        if delta is None:
            title = "Trust score calculado"
            event_type = "trust_snapshot"
        elif delta > 0:
            title = "Trust score em alta"
            event_type = "trust_improved"
        elif delta < 0:
            title = "Trust score em queda"
            event_type = "trust_degraded"
        else:
            title = "Trust score estável"
            event_type = "trust_stable"
        severity = _trust_severity(trust_score)
        priority = _priority_from_event(
            category="governance",
            severity=severity,
            confidence_score=trust_score,
            recurrent=bool(delta is not None and abs(delta) >= 10),
        )
        trust_context = dict(snapshot.trust_context_json or {})
        adjustments = trust_context.get("adjustments") if isinstance(trust_context.get("adjustments"), list) else []
        penalties = trust_context.get("penalties") if isinstance(trust_context.get("penalties"), list) else []
        detail_parts = [
            f"Trust {trust_score} ({snapshot.label}).",
            f"Prontidão {int(snapshot.readiness_score or 0)} · Governança {int(snapshot.governance_score or 0)} · Operação {int(snapshot.operational_score or 0)}.",
        ]
        if delta is not None:
            delta_label = f"+{delta}" if delta > 0 else str(delta)
            detail_parts.append(f"Variação {delta_label} ponto(s) desde a última leitura.")
        if adjustments:
            detail_parts.append(
                f"Ajustes: {', '.join(str(item.get('label') or item.get('key') or 'ajuste') for item in adjustments[:3])}."
            )
        elif penalties:
            detail_parts.append(
                f"Penalidades: {', '.join(str(item.get('label') or item.get('key') or 'penalidade') for item in penalties[:3])}."
            )
        events.append(
            _timeline_item(
                id=f"trust:{snapshot.id}",
                occurred_at=snapshot.bucket_date,
                category="governance",
                event_type=event_type,
                title=title,
                detail=" ".join(detail_parts),
                source_module="governance",
                source_label="Confiança",
                actor_name=None,
                actor_email=None,
                mode="automatic",
                severity=severity,
                priority=priority,
                entity_type="table",
                entity_id=str(profile.table_id),
                table_id=profile.table_id,
                column_id=None,
                table_name=profile.table_name,
                column_name=None,
                schema_name=profile.schema_name,
                database_name=profile.database_name,
                datasource_name=profile.datasource_name,
                table_fqn=profile.table_fqn,
                owner_name=profile.owner_name,
                certification_status=profile.certification_status,
                certification_status_label=certification_status_label(profile.certification_status),
                readiness_score=profile.readiness_score,
                trust_score=trust_score,
                trust_label=snapshot.label,
                trust_tone=snapshot.tone,
                trust_delta=delta,
                trust_summary=snapshot.label,
                active_dq_violation=bool(profile.active_dq_violation),
                active_dq_rule_names=list(profile.active_dq_rule_names or []),
                href=_asset_href(profile.table_id),
                metadata_json={
                    "bucket_date": snapshot.bucket_date.isoformat(),
                    "trust_context": trust_context,
                    "previous_score": previous_score,
                    "delta": delta,
                    "readiness_score": int(snapshot.readiness_score or 0),
                    "governance_score": int(snapshot.governance_score or 0),
                    "operational_score": int(snapshot.operational_score or 0),
                },
            )
        )
        previous_score_by_table[profile.table_id] = trust_score
    return events


def _rows_to_events_from_ingestion_history(
    history: list[dict[str, Any]],
    *,
    profile,
) -> list[TimelineEventOut]:
    events: list[TimelineEventOut] = []
    if profile is None:
        return events
    for point in history:
        status_label = str(point.get("latest_status_label") or "Pendente")
        current_status = status_label.lower()
        if "fal" in current_status:
            title = "Pipeline com falha"
            severity = "critical"
            event_type = "pipeline_failure"
        elif point.get("currently_stale"):
            title = "Pipeline sem atualização recente"
            severity = "high"
            event_type = "pipeline_stale"
        elif point.get("recurrent_degradation"):
            title = "Pipeline degradado"
            severity = "high"
            event_type = "pipeline_degraded"
        else:
            title = "Pipeline executado"
            severity = "low"
            event_type = "pipeline_success"
        occurred_at = point.get("bucket_start_at")
        if isinstance(occurred_at, str):
            try:
                occurred_at = datetime.fromisoformat(occurred_at)
            except ValueError:
                occurred_at = None
        if occurred_at is None:
            continue
        priority = _priority_from_event(category="operation", severity=severity, recurrent=bool(point.get("recurrent_degradation")))
        events.append(
            _timeline_item(
                id=f"ingestion:{profile.table_id}:{point.get('bucket_start_at')}",
                occurred_at=occurred_at,
                category="operation",
                event_type=event_type,
                title=title,
                detail=(
                    f"{point.get('pipeline_name') or point.get('dag_id') or 'Pipeline'} · "
                    f"{float(point.get('success_rate_pct') or 0.0):.1f}% de sucesso · {int(point.get('rows_processed') or 0)} linha(s)."
                ),
                source_module="ingestion",
                source_label="Ingestão",
                actor_name=None,
                actor_email=None,
                mode="automatic",
                severity=severity,
                priority=priority,
                entity_type="table",
                entity_id=str(profile.table_id),
                table_id=profile.table_id,
                column_id=None,
                table_name=profile.table_name,
                column_name=None,
                schema_name=profile.schema_name,
                database_name=profile.database_name,
                datasource_name=profile.datasource_name,
                table_fqn=profile.table_fqn,
                owner_name=profile.owner_name,
                certification_status=profile.certification_status,
                certification_status_label=certification_status_label(profile.certification_status),
                readiness_score=profile.readiness_score,
                active_dq_violation=bool(profile.active_dq_violation),
                active_dq_rule_names=list(profile.active_dq_rule_names or []),
                href=_asset_href(profile.table_id),
                metadata_json=point,
            )
        )
    return events


def _build_asset_context_events(
    session: Session,
    *,
    profile,
    column_id: int | None,
    date_from: datetime,
    date_to: datetime,
) -> list[TimelineEventOut]:
    column_lookup = _column_lookup(session, [profile.table_id])
    table = session.scalar(
        select(TableEntity)
        .options(selectinload(TableEntity.columns), selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
        .where(TableEntity.id == profile.table_id)
    )
    if table is None:
        return []

    events: list[TimelineEventOut] = []
    audit_stmt = (
        select(AuditLog)
        .where(
            AuditLog.created_at >= date_from,
            AuditLog.created_at <= date_to,
            or_(
                and_(AuditLog.entity_type == "table", AuditLog.entity_id == str(profile.table_id)),
                and_(AuditLog.parent_entity_type == "table", AuditLog.parent_entity_id == str(profile.table_id)),
            ),
        )
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
        .limit(60)
    )
    audit_rows = session.execute(audit_stmt).all()
    _extend_events_safely(
        session,
        events,
        source_name="audit",
        scope="asset",
        table_id=profile.table_id,
        build_events=lambda: _rows_to_events_from_audit(
            audit_rows,
            profile_by_table_id={profile.table_id: profile},
            column_lookup=column_lookup,
            filters=TimelineQuery(table_id=profile.table_id, column_id=column_id),
        ),
    )

    tag_rows = session.scalars(
        select(TagIntelligenceEvent)
        .options(selectinload(TagIntelligenceEvent.tag))
        .where(
            TagIntelligenceEvent.created_at >= date_from,
            TagIntelligenceEvent.created_at <= date_to,
            TagIntelligenceEvent.entity_type.in_(["table", "column"]),
            TagIntelligenceEvent.entity_id.in_([profile.table_id, *column_lookup.keys()]),
        )
        .order_by(TagIntelligenceEvent.created_at.desc(), TagIntelligenceEvent.id.desc())
        .limit(80)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="tags",
        scope="asset",
        table_id=profile.table_id,
        build_events=lambda: _rows_to_events_from_tag_intelligence(
            tag_rows,
            profile_by_table_id={profile.table_id: profile},
            column_lookup=column_lookup,
            filters=TimelineQuery(table_id=profile.table_id, column_id=column_id),
        ),
    )

    incident_fqns = sorted(_table_variant_keys(profile))
    incidents = session.scalars(
        select(Incident)
        .where(
            Incident.entity_type == "table",
            Incident.table_fqn.in_(incident_fqns),
            Incident.detected_at <= date_to,
            or_(Incident.last_seen_at.is_(None), Incident.last_seen_at >= date_from),
        )
        .order_by(Incident.last_seen_at.desc().nullslast(), Incident.detected_at.desc(), Incident.id.desc())
        .limit(40)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="incidents",
        scope="asset",
        table_id=profile.table_id,
        build_events=lambda: _rows_to_events_from_incidents(
            incidents,
            profile_by_fqn={key: profile for key in incident_fqns},
            filters=TimelineQuery(table_id=profile.table_id, column_id=column_id),
        ),
    )

    dq_rows = session.execute(
        select(DQRun, DQTableMetric)
        .join(DQTableMetric, DQTableMetric.run_id == DQRun.id)
        .where(
            DQTableMetric.table_id == profile.table_id,
            DQRun.created_at >= date_from,
            DQRun.created_at <= date_to,
            DQRun.status == "success",
        )
        .order_by(DQRun.created_at.desc(), DQRun.id.desc())
        .limit(20)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="dq",
        scope="asset",
        table_id=profile.table_id,
        build_events=lambda: _rows_to_events_from_dq_runs(
            dq_rows,
            profile_by_table_id={profile.table_id: profile},
            filters=TimelineQuery(table_id=profile.table_id, column_id=column_id),
        ),
    )

    snapshots = session.scalars(
        select(OperationalStabilitySnapshot)
        .where(
            OperationalStabilitySnapshot.table_id == profile.table_id,
            OperationalStabilitySnapshot.bucket_start_at >= date_from,
            OperationalStabilitySnapshot.bucket_start_at <= date_to,
        )
        .order_by(OperationalStabilitySnapshot.bucket_start_at.desc(), OperationalStabilitySnapshot.id.desc())
        .limit(40)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="operation",
        scope="asset",
        table_id=profile.table_id,
        build_events=lambda: _rows_to_events_from_pipeline_snapshots(
            snapshots,
            profile_by_table_id={profile.table_id: profile},
            filters=TimelineQuery(table_id=profile.table_id, column_id=column_id),
        ),
    )

    trust_snapshots = session.scalars(
        select(GovernanceTrustSnapshot)
        .where(
            GovernanceTrustSnapshot.table_id == profile.table_id,
            GovernanceTrustSnapshot.bucket_date >= date_from,
            GovernanceTrustSnapshot.bucket_date <= date_to,
        )
        .order_by(GovernanceTrustSnapshot.bucket_date.asc(), GovernanceTrustSnapshot.id.asc())
        .limit(40)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="governance",
        scope="asset",
        table_id=profile.table_id,
        build_events=lambda: _rows_to_events_from_trust_snapshots(
            trust_snapshots,
            profile_by_table_id={profile.table_id: profile},
            filters=TimelineQuery(table_id=profile.table_id, column_id=column_id),
        ),
    )

    try:
        detail = load_table_ingestion_detail(
            session,
            schema_name=profile.schema_name,
            table_name=profile.table_name,
            page=1,
            page_size=8,
            airflow_ui_base_url=None,
        )
        history = detail.get("history") or []
        events.extend(_rows_to_events_from_ingestion_history(history, profile=profile))
    except IngestionIntegrationUnavailable:
        pass
    except Exception:
        pass

    return events


def _build_global_context_events(
    session: Session,
    *,
    profiles: list,
    filters: TimelineQuery,
    date_from: datetime,
    date_to: datetime,
) -> list[TimelineEventOut]:
    profile_by_table_id = _profile_by_table_id(profiles)
    profile_by_fqn = _profile_by_fqn(profiles)
    table_ids = [profile.table_id for profile in profiles]
    column_lookup = _column_lookup(session, table_ids)

    events: list[TimelineEventOut] = []
    if not table_ids:
        return events

    audit_rows = session.execute(
        select(AuditLog)
        .where(
            AuditLog.created_at >= date_from,
            AuditLog.created_at <= date_to,
            or_(
                and_(AuditLog.entity_type == "table", AuditLog.entity_id.in_([str(table_id) for table_id in table_ids])),
                and_(AuditLog.entity_type == "column", AuditLog.entity_id.in_([str(column_id) for column_id in column_lookup])),
                and_(AuditLog.parent_entity_type == "table", AuditLog.parent_entity_id.in_([str(table_id) for table_id in table_ids])),
            ),
        )
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
        .limit(250)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="audit",
        scope="global",
        table_id=None,
        build_events=lambda: _rows_to_events_from_audit(
            audit_rows,
            profile_by_table_id=profile_by_table_id,
            column_lookup=column_lookup,
            filters=filters,
        ),
    )

    tag_rows = session.scalars(
        select(TagIntelligenceEvent)
        .options(selectinload(TagIntelligenceEvent.tag))
        .where(
            TagIntelligenceEvent.created_at >= date_from,
            TagIntelligenceEvent.created_at <= date_to,
            TagIntelligenceEvent.entity_type.in_(["table", "column"]),
            TagIntelligenceEvent.entity_id.in_([*table_ids, *column_lookup.keys()]),
        )
        .order_by(TagIntelligenceEvent.created_at.desc(), TagIntelligenceEvent.id.desc())
        .limit(250)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="tags",
        scope="global",
        table_id=None,
        build_events=lambda: _rows_to_events_from_tag_intelligence(
            tag_rows,
            profile_by_table_id=profile_by_table_id,
            column_lookup=column_lookup,
            filters=filters,
        ),
    )

    incident_fqns = sorted({key for profile in profiles for key in _table_variant_keys(profile)})
    incidents = session.scalars(
        select(Incident)
        .where(
            Incident.entity_type == "table",
            Incident.table_fqn.in_(incident_fqns),
            Incident.detected_at <= date_to,
            or_(Incident.last_seen_at.is_(None), Incident.last_seen_at >= date_from),
        )
        .order_by(Incident.last_seen_at.desc().nullslast(), Incident.detected_at.desc(), Incident.id.desc())
        .limit(250)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="incidents",
        scope="global",
        table_id=None,
        build_events=lambda: _rows_to_events_from_incidents(
            incidents,
            profile_by_fqn=profile_by_fqn,
            filters=filters,
        ),
    )

    dq_rows = session.execute(
        select(DQRun, DQTableMetric)
        .join(DQTableMetric, DQTableMetric.run_id == DQRun.id)
        .where(
            DQTableMetric.table_id.in_(table_ids),
            DQRun.created_at >= date_from,
            DQRun.created_at <= date_to,
            DQRun.status == "success",
        )
        .order_by(DQRun.created_at.desc(), DQRun.id.desc())
        .limit(250)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="dq",
        scope="global",
        table_id=None,
        build_events=lambda: _rows_to_events_from_dq_runs(
            dq_rows,
            profile_by_table_id=profile_by_table_id,
            filters=filters,
        ),
    )

    snapshots = session.scalars(
        select(OperationalStabilitySnapshot)
        .where(
            OperationalStabilitySnapshot.table_id.in_(table_ids),
            OperationalStabilitySnapshot.bucket_start_at >= date_from,
            OperationalStabilitySnapshot.bucket_start_at <= date_to,
        )
        .order_by(OperationalStabilitySnapshot.bucket_start_at.desc(), OperationalStabilitySnapshot.id.desc())
        .limit(250)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="operation",
        scope="global",
        table_id=None,
        build_events=lambda: _rows_to_events_from_pipeline_snapshots(
            snapshots,
            profile_by_table_id=profile_by_table_id,
            filters=filters,
        ),
    )

    trust_snapshots = session.scalars(
        select(GovernanceTrustSnapshot)
        .where(
            GovernanceTrustSnapshot.table_id.in_(table_ids),
            GovernanceTrustSnapshot.bucket_date >= date_from,
            GovernanceTrustSnapshot.bucket_date <= date_to,
        )
        .order_by(GovernanceTrustSnapshot.bucket_date.asc(), GovernanceTrustSnapshot.id.asc())
        .limit(250)
    ).all()
    _extend_events_safely(
        session,
        events,
        source_name="governance",
        scope="global",
        table_id=None,
        build_events=lambda: _rows_to_events_from_trust_snapshots(
            trust_snapshots,
            profile_by_table_id=profile_by_table_id,
            filters=filters,
        ),
    )

    return events


def _apply_event_filters(events: list[TimelineEventOut], filters: TimelineQuery) -> list[TimelineEventOut]:
    filtered: list[TimelineEventOut] = []
    q = filters.q.strip().lower() if filters.q else None
    source = filters.source.strip().lower() if filters.source else None
    event_type = filters.event_type.strip().lower() if filters.event_type else None
    category = filters.category.strip().lower() if filters.category else None
    severity = filters.severity.strip().lower() if filters.severity else None
    for event in events:
        if source and source not in (event.source_module or "").lower() and source not in (event.source_label or "").lower():
            continue
        if event_type and event.event_type.lower() != event_type:
            continue
        if category and event.category.lower() != category:
            continue
        if severity and event.severity.lower() != severity:
            continue
        if filters.manual_only and event.mode != "manual":
            continue
        if filters.automatic_only and event.mode != "automatic":
            continue
        if q:
            haystack = " ".join(
                str(value)
                for value in (
                    event.title,
                    event.detail or "",
                    event.source_label or "",
                    event.table_fqn or "",
                    event.table_name or "",
                    event.column_name or "",
                    event.owner_name or "",
                    event.actor_name or "",
                    event.actor_email or "",
                )
            ).lower()
            if q not in haystack:
                continue
        filtered.append(event)
    return filtered


def _severity_rank(severity: str | None) -> int:
    return {"low": 1, "medium": 2, "high": 3, "critical": 4}.get((severity or "").lower(), 1)


def _episode_axis(event: TimelineEventOut) -> str:
    if event.category in {"operation", "quality", "incident"}:
        return "operational"
    if event.category in {"governance", "audit"}:
        return "governance"
    return "other"


def _episode_family(event: TimelineEventOut) -> str:
    event_type = event.event_type.lower()
    source = (event.source_module or "").lower()
    if event_type.startswith("pipeline_") or source in {"ingestion", "ops", "system"}:
        return "ingestion"
    if event_type.startswith("dq_") or source == "dq" or event.active_dq_violation:
        return "quality"
    if event_type.startswith("incident_") or source == "incidents":
        return "incident"
    if event_type.startswith("trust_"):
        return "trust"
    if event_type in {"owner_changed", "classification_changed", "certification_changed", "metadata_updated", "tag_changed", "tag_applied", "tag_removed", "tag_blocked", "tag_suggestion", "term_changed", "recommendation_change"}:
        return "governance_change"
    return f"{event.category}:{source or 'system'}"


def _episode_bucket_minutes(event: TimelineEventOut) -> int:
    axis = _episode_axis(event)
    if axis == "governance":
        return 24 * 60
    if event.category == "incident":
        return 6 * 60
    if event.category == "quality":
        return 4 * 60
    return 3 * 60


def _episode_group_key(event: TimelineEventOut) -> str:
    anchor = f"table:{event.table_id}" if event.table_id is not None else f"source:{event.source_module or event.category}"
    bucket_minutes = _episode_bucket_minutes(event)
    bucket = int(event.occurred_at.timestamp() // (bucket_minutes * 60))
    axis = _episode_axis(event)
    family = _episode_family(event)
    if axis == "operational":
        return f"{axis}:{anchor}:{bucket}"
    return f"{axis}:{family}:{anchor}:{bucket}"


def _episode_identifier(episode_key: str, events: list[TimelineEventOut]) -> str:
    """
    Keep the public episode_key stable for action persistence, but make the rendered
    id unique per deterministic episode content so React keys do not collide.
    """
    event_ids = sorted(event.id for event in events)
    signature_source = "|".join(event_ids)
    signature = hashlib.sha1(signature_source.encode("utf-8")).hexdigest()[:12]
    return f"episode:{episode_key}:{signature}"


def _episode_title(events: list[TimelineEventOut]) -> str:
    event_types = {event.event_type for event in events}
    categories = {event.category for event in events}
    if "incident_opened" in event_types or "pipeline_failure" in event_types or "pipeline_stale" in event_types:
        return "Episódio de ingestão degradada"
    if "dq_run_failure" in event_types or "dq_run_degraded" in event_types:
        return "Episódio de qualidade degradada"
    if event_types.intersection({"owner_changed", "classification_changed", "certification_changed", "metadata_updated", "tag_changed", "tag_applied", "tag_removed", "tag_blocked", "tag_suggestion", "term_changed", "recommendation_change"}):
        return "Episódio de governança"
    if "trust_degraded" in event_types or "trust_improved" in event_types or "trust_snapshot" in event_types:
        return "Episódio de confiança do ativo"
    if "incident_closed" in event_types and categories == {"incident"}:
        return "Episódio de tratamento de incidente"
    if len(events) > 1:
        return "Episódio operacional correlacionado"
    return events[0].title if events else "Episódio"


def _episode_status(events: list[TimelineEventOut]) -> str:
    if any(event.event_type in {"incident_opened", "pipeline_failure", "dq_run_failure", "pipeline_stale"} for event in events):
        return "open"
    if any(event.event_type in {"incident_closed", "trust_improved"} for event in events):
        return "resolved"
    if any(event.event_type in {"owner_changed", "classification_changed", "certification_changed"} for event in events):
        return "acknowledged"
    return "watching"


def _episode_next_action(events: list[TimelineEventOut]) -> tuple[str, str]:
    top = events[0]
    event_types = {event.event_type for event in events}
    if "incident_opened" in event_types or "pipeline_failure" in event_types:
        return "Abrir incidente ou verificar o pipeline", top.href or "/governance/timeline"
    if "pipeline_stale" in event_types:
        return "Ver DAG, watermark e janela de reprocessamento", top.href or "/governance/timeline"
    if "dq_run_failure" in event_types or "dq_run_degraded" in event_types:
        return "Reexecutar profiling e validar freshness", top.href or "/governance/timeline"
    if event_types.intersection({"owner_changed", "classification_changed", "certification_changed"}):
        return "Revisar owner, contrato e certificação", top.href or "/governance/timeline"
    if "trust_degraded" in event_types:
        return "Rever sinais de confiança e impacto downstream", top.href or "/governance/timeline"
    if any(event.active_dq_violation for event in events):
        return "Investigar as regras DQ ativas", top.href or "/governance/timeline"
    return "Abrir contexto e seguir a investigação", top.href or "/governance/timeline"


def _episode_summary(events: list[TimelineEventOut]) -> str:
    top = events[0]
    changed = []
    if top.table_name:
        changed.append(top.table_fqn or top.table_name)
    if top.column_name:
        changed.append(top.column_name)
    if len(events) > 1:
        changed.append(f"{len(events)} sinal(is) correlacionado(s)")
    if top.detail:
        changed.append(top.detail)
    if not changed:
        changed.append(top.title)
    return " · ".join(part for part in changed if part)


def _episode_impact_summary(events: list[TimelineEventOut]) -> tuple[str, list[int], list[str], list[str], int]:
    table_ids: list[int] = []
    fqns: list[str] = []
    owners: list[str] = []
    column_ids: set[int] = set()
    for event in events:
        if event.table_id is not None and event.table_id not in table_ids:
            table_ids.append(event.table_id)
        if event.table_fqn and event.table_fqn not in fqns:
            fqns.append(event.table_fqn)
        if event.owner_name and event.owner_name not in owners:
            owners.append(event.owner_name)
        if event.column_id is not None:
            column_ids.add(event.column_id)
    impact = f"{len(table_ids)} ativo(s) afetado(s)"
    if column_ids:
        impact += f", {len(column_ids)} coluna(s) envolvida(s)"
    critical_count = sum(1 for event in events if event.severity == "critical")
    if critical_count:
        impact += f", {critical_count} sinal(is) crítico(s)"
    return impact, table_ids, fqns, owners, len(column_ids)


def _episode_why_it_matters(events: list[TimelineEventOut]) -> str:
    event_types = {event.event_type for event in events}
    if "pipeline_failure" in event_types or "pipeline_stale" in event_types:
        return "A cadeia operacional indica atraso ou falha que pode reduzir a confiança do ativo e dos consumidores downstream."
    if "dq_run_failure" in event_types or "dq_run_degraded" in event_types:
        return "A leitura de qualidade perdeu aderência e pode afetar o uso seguro do ativo."
    if event_types.intersection({"owner_changed", "classification_changed", "certification_changed"}):
        return "A mudança de governança altera responsabilidade, confiança e elegibilidade do ativo."
    if "incident_opened" in event_types:
        return "Existe um incidente em aberto com potencial de impacto operacional e rastreabilidade pendente."
    if "trust_degraded" in event_types:
        return "A confiança do ativo caiu e merece verificação antes de ampliar o uso."
    return "Os sinais agrupados apontam uma mudança operacional relevante e merecem leitura consolidada."


def _episode_importance_score(events: list[TimelineEventOut], affected_assets: int, affected_columns: int) -> int:
    top_priority = max((event.priority for event in events), default=0)
    severity_bonus = max((_severity_rank(event.severity) for event in events), default=1) * 4
    category_bonus = 8 if any(event.category == "incident" for event in events) else 0
    recurrence_bonus = 10 if len(events) > 1 else 0
    impact_bonus = min(18, affected_assets * 3 + affected_columns * 2)
    dq_bonus = 8 if any(event.active_dq_violation for event in events) else 0
    return min(100, max(top_priority, 25) + severity_bonus + category_bonus + recurrence_bonus + impact_bonus + dq_bonus)


def _event_to_episode_member(event: TimelineEventOut) -> TimelineEpisodeMemberOut:
    return TimelineEpisodeMemberOut.model_validate(
        {
            "id": event.id,
            "occurred_at": event.occurred_at,
            "title": event.title,
            "detail": event.detail,
            "category": event.category,
            "event_type": event.event_type,
            "mode": event.mode,
            "severity": event.severity,
            "priority": event.priority,
            "table_id": event.table_id,
            "column_id": event.column_id,
            "table_name": event.table_name,
            "column_name": event.column_name,
            "table_fqn": event.table_fqn,
            "owner_name": event.owner_name,
            "trust_score": event.trust_score,
            "trust_delta": event.trust_delta,
            "active_dq_violation": event.active_dq_violation,
            "href": event.href,
            "metadata_json": event.metadata_json,
        }
    )


def _dedupe_timeline_episodes(episodes: list[TimelineEpisodeOut]) -> list[TimelineEpisodeOut]:
    deduped: list[TimelineEpisodeOut] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    for episode in episodes:
        if episode.id in seen:
            duplicates.append(episode.id)
            continue
        seen.add(episode.id)
        deduped.append(episode)
    if duplicates:
        _LOGGER.warning("Duplicate timeline episode ids detected and deduplicated: %s", ", ".join(sorted(set(duplicates))))
    return deduped


def _dedupe_episode_keys(episodes: list[TimelineEpisodeOut]) -> list[str]:
    return list(dict.fromkeys(episode.episode_key for episode in episodes))


def _group_timeline_episodes(events: list[TimelineEventOut]) -> list[TimelineEpisodeOut]:
    if not events:
        return []
    buckets: list[list[TimelineEventOut]] = []
    grouped: dict[str, list[TimelineEventOut]] = {}
    ordered_keys: list[str] = []
    for event in events:
        key = _episode_group_key(event)
        if key not in grouped:
            grouped[key] = []
            ordered_keys.append(key)
        grouped[key].append(event)
    for key in ordered_keys:
        bucket = grouped[key]
        bucket.sort(key=lambda item: (item.occurred_at, item.priority, item.id), reverse=True)
        buckets.append(bucket)

    episodes: list[TimelineEpisodeOut] = []
    for bucket in buckets:
        if not bucket:
            continue
        impact_summary, affected_table_ids, affected_table_fqns, impacted_owner_names, affected_columns_count = _episode_impact_summary(bucket)
        next_action, href = _episode_next_action(bucket)
        title = _episode_title(bucket)
        summary = _episode_summary(bucket)
        episode_type = _episode_family(bucket[0])
        primary = bucket[0]
        priority = max(event.priority for event in bucket)
        severity_rank = max((_severity_rank(event.severity) for event in bucket), default=1)
        severity = {1: "low", 2: "medium", 3: "high", 4: "critical"}.get(severity_rank, "medium")
        importance_score = _episode_importance_score(bucket, len(affected_table_ids), affected_columns_count)
        episode_events = [
            _event_to_episode_member(event)
            for event in bucket
        ]
        related_labels = []
        for value in [primary.source_label, primary.category.title(), primary.source_module]:
            if value and value not in related_labels:
                related_labels.append(value)
        correlation_chain = []
        for event in bucket:
            label = event.title
            if label not in correlation_chain:
                correlation_chain.append(label)
            if len(correlation_chain) >= 4:
                break
        episodes.append(
            TimelineEpisodeOut(
                episode_key=key,
                id=_episode_identifier(key, bucket),
                episode_type=episode_type,
                title=title,
                summary=summary,
                impact_summary=impact_summary,
                why_it_matters=_episode_why_it_matters(bucket),
                next_action=next_action,
                status=_episode_status(bucket),
                category=primary.category,
                source_module=primary.source_module,
                source_label=primary.source_label,
                mode=primary.mode,
                severity=severity,
                priority=priority,
                importance_score=importance_score,
                occurred_at=bucket[0].occurred_at,
                updated_at=bucket[0].occurred_at,
                window_start=min(event.occurred_at for event in bucket),
                window_end=max(event.occurred_at for event in bucket),
                event_count=len(bucket),
                affected_assets_count=len(affected_table_ids),
                affected_columns_count=affected_columns_count,
                impacted_table_ids=affected_table_ids,
                impacted_table_fqns=affected_table_fqns,
                impacted_owner_names=impacted_owner_names,
                related_labels=related_labels,
                child_events=episode_events,
                action_count=0,
                href=href,
                metadata_json={
                    "correlated_event_ids": [event.id for event in bucket],
                    "categories": sorted({event.category for event in bucket}),
                    "event_types": sorted({event.event_type for event in bucket}),
                },
                correlation_label="Sinais correlacionados" if len(bucket) > 1 else None,
                correlation_chain=correlation_chain,
            )
        )
    episodes.sort(key=lambda item: (item.window_end, item.importance_score, item.id), reverse=True)
    return _dedupe_timeline_episodes(episodes)


def _load_episode_actions(session: Session, episode_keys: list[str]) -> dict[str, list[TimelineEpisodeAction]]:
    if not episode_keys:
        return {}
    rows = session.scalars(
        select(TimelineEpisodeAction)
        .where(TimelineEpisodeAction.episode_key.in_(episode_keys))
        .order_by(TimelineEpisodeAction.episode_key.asc(), TimelineEpisodeAction.created_at.desc(), TimelineEpisodeAction.id.desc())
    ).all()
    grouped: dict[str, list[TimelineEpisodeAction]] = {}
    for row in rows:
        grouped.setdefault(row.episode_key, []).append(row)
    return grouped


def _episode_status_from_action(
    episode: TimelineEpisodeOut,
    action: TimelineEpisodeAction | None,
    *,
    now: datetime,
) -> tuple[str, datetime | None, str | None, str | None, str | None]:
    action_created_at = _normalize_dt(action.created_at) if action is not None else None
    if episode.status == "resolved":
        if action is None:
            return episode.status, None, None, None, None
        return (
            episode.status,
            action_created_at,
            action.actor_name or action.actor_email,
            action.silent_until,
            action.reason,
        )
    if action is None:
        return episode.status, None, None, None, None

    silent_until = _normalize_dt(action.silent_until)
    is_silence_active = action.action_type == "silence" and (silent_until is None or silent_until >= now)
    if is_silence_active:
        return (
            "silenced",
            action_created_at,
            action.actor_name or action.actor_email,
            silent_until,
            action.reason,
        )
    if action.action_type == "acknowledge":
        return (
            "acknowledged",
            action_created_at,
            action.actor_name or action.actor_email,
            None,
            action.reason,
        )
    return episode.status, action_created_at, action.actor_name or action.actor_email, silent_until, action.reason


def _apply_episode_actions(
    episodes: list[TimelineEpisodeOut],
    action_map: dict[str, list[TimelineEpisodeAction]],
) -> list[TimelineEpisodeOut]:
    now = datetime.now(timezone.utc)
    enriched: list[TimelineEpisodeOut] = []
    for episode in episodes:
        latest_action = action_map.get(episode.episode_key, [None])[0]
        episode.action_count = len(action_map.get(episode.episode_key, []))
        status, acknowledged_at, acknowledged_by_name, silenced_until, silence_reason = _episode_status_from_action(
            episode,
            latest_action,
            now=now,
        )
        episode.status = status
        episode.acknowledged_at = acknowledged_at
        episode.acknowledged_by_name = acknowledged_by_name
        episode.silenced_until = silenced_until
        episode.silence_reason = silence_reason
        episode.last_action_type = latest_action.action_type if latest_action is not None else None
        enriched.append(episode)
    return enriched


def _apply_episode_filters(episodes: list[TimelineEpisodeOut], filters: TimelineQuery) -> list[TimelineEpisodeOut]:
    filtered = episodes
    episode_status = filters.episode_status.strip().lower() if filters.episode_status else None
    episode_type = filters.episode_type.strip().lower() if filters.episode_type else None
    min_importance_score = filters.min_importance_score
    if episode_status:
        filtered = [episode for episode in filtered if episode.status == episode_status]
    if episode_type:
        filtered = [episode for episode in filtered if episode.episode_type == episode_type]
    if min_importance_score is not None:
        filtered = [episode for episode in filtered if episode.importance_score >= min_importance_score]
    return filtered


def _analytics_for_episodes(episodes: list[TimelineEpisodeOut]) -> TimelineAnalyticsOut:
    if not episodes:
        return TimelineAnalyticsOut()
    status_counts = Counter(episode.status for episode in episodes)
    type_counts = Counter(episode.episode_type for episode in episodes)
    source_counts = Counter((episode.source_label or episode.source_module or "Sistema") for episode in episodes)
    avg_importance = round(sum(episode.importance_score for episode in episodes) / len(episodes), 1)
    avg_events = round(sum(episode.event_count for episode in episodes) / len(episodes), 1)
    impacted_assets = len({table_id for episode in episodes for table_id in episode.impacted_table_ids})
    impacted_columns = sum(episode.affected_columns_count for episode in episodes)
    return TimelineAnalyticsOut(
        total_episodes=len(episodes),
        open_episodes=int(status_counts.get("open", 0) + status_counts.get("watching", 0)),
        acknowledged_episodes=int(status_counts.get("acknowledged", 0)),
        silenced_episodes=int(status_counts.get("silenced", 0)),
        resolved_episodes=int(status_counts.get("resolved", 0)),
        critical_episodes=sum(1 for episode in episodes if episode.severity == "critical"),
        recurrent_episodes=sum(1 for episode in episodes if episode.event_count > 1),
        impacted_assets=impacted_assets,
        impacted_columns=impacted_columns,
        average_importance_score=avg_importance,
        average_event_count=avg_events,
        top_episode_types=[
            TimelineAnalyticsBucketOut(label=label, count=count)
            for label, count in type_counts.most_common(5)
        ],
        top_sources=[
            TimelineAnalyticsBucketOut(label=label, count=count)
            for label, count in source_counts.most_common(5)
        ],
        top_statuses=[
            TimelineAnalyticsBucketOut(label=label, count=count)
            for label, count in status_counts.most_common(5)
        ],
    )


def record_timeline_episode_action(
    session: Session,
    *,
    payload: TimelineEpisodeActionIn,
    current_user: User | None = None,
    audit_kwargs: dict[str, Any] | None = None,
) -> TimelineEpisodeActionOut:
    now = datetime.now(timezone.utc)
    silent_until = payload.silent_until
    if payload.action_type == "silence" and silent_until is None:
        silent_until = now + timedelta(hours=2)
    status = "acknowledged" if payload.action_type == "acknowledge" else "silenced"
    actor_user_id = getattr(current_user, "id", None)
    actor_name = getattr(current_user, "name", None) or getattr(current_user, "full_name", None)
    actor_email = getattr(current_user, "email", None)
    action = TimelineEpisodeAction(
        episode_key=payload.episode_key,
        table_id=payload.table_id,
        column_id=payload.column_id,
        action_type=payload.action_type,
        status=status,
        reason=payload.reason,
        silent_until=silent_until,
        actor_user_id=actor_user_id,
        actor_name=actor_name,
        actor_email=actor_email,
        metadata_json={
            "action_type": payload.action_type,
            "episode_key": payload.episode_key,
            "table_id": payload.table_id,
            "column_id": payload.column_id,
            "reason": payload.reason,
            "silent_until": silent_until.isoformat() if silent_until else None,
        },
        created_at=now,
        updated_at=now,
    )
    session.add(action)
    session.flush()

    from t2c_data.services.audit import write_audit_log_sync

    write_audit_log_sync(
        session,
        action=f"timeline.episode.{payload.action_type}",
        entity_type="timeline_episode",
        entity_id=payload.episode_key,
        parent_entity_type="table" if payload.table_id is not None else None,
        parent_entity_id=payload.table_id,
        source_module="timeline",
        after={
            "episode_key": payload.episode_key,
            "action_type": payload.action_type,
            "status": status,
            "table_id": payload.table_id,
            "column_id": payload.column_id,
            "reason": payload.reason,
            "silent_until": silent_until.isoformat() if silent_until else None,
        },
        metadata={"message": f"Episode {payload.action_type} recorded"},
        **(audit_kwargs or {}),
    )

    return TimelineEpisodeActionOut.model_validate(
        {
            "id": action.id,
            "episode_key": action.episode_key,
            "table_id": action.table_id,
            "column_id": action.column_id,
            "action_type": action.action_type,
            "status": action.status,
            "reason": action.reason,
            "silent_until": action.silent_until,
            "actor_user_id": action.actor_user_id,
            "actor_name": action.actor_name,
            "actor_email": action.actor_email,
            "metadata_json": action.metadata_json,
            "created_at": action.created_at,
            "updated_at": action.updated_at,
        }
    )


def _summary_for(events: list[TimelineEventOut]) -> TimelineSummaryOut:
    summary = TimelineSummaryOut(total=len(events))
    for event in events:
        if event.category == "governance":
            summary.governance += 1
        elif event.category == "operation":
            summary.operation += 1
        elif event.category == "quality":
            summary.quality += 1
        elif event.category == "incident":
            summary.incident += 1
        elif event.category == "audit":
            summary.audit += 1
        if event.mode == "manual":
            summary.manual += 1
        elif event.mode == "automatic":
            summary.automatic += 1
        if event.severity == "critical":
            summary.critical += 1
    return summary


def _paginate(events: list[TimelineEventOut], *, page: int, page_size: int) -> tuple[list[TimelineEventOut], int]:
    total = len(events)
    start = max((page - 1) * page_size, 0)
    end = start + page_size
    return events[start:end], total


def build_timeline_payload(
    session: Session,
    *,
    current_user=None,
    filters: TimelineQuery,
    episode_page: int = 1,
    episode_page_size: int = 25,
) -> dict[str, Any]:
    date_from, date_to = _ensure_window(filters)
    profiles = _profiles_map(session, filters=filters, current_user=current_user)
    _LOGGER.info(
        "timeline request scope=%s table_id=%s column_id=%s profiles=%s window=%s..%s",
        "asset" if filters.table_id is not None else "global",
        filters.table_id,
        filters.column_id,
        len(profiles),
        date_from.isoformat(),
        date_to.isoformat(),
    )
    if not profiles:
        return {
            "generated_at": datetime.now(timezone.utc),
            "scope": "asset" if filters.table_id is not None else "global",
            "table_id": filters.table_id,
            "column_id": filters.column_id,
            "table_fqn": None,
            "page": 1,
            "page_size": 0,
            "total": 0,
            "summary": TimelineSummaryOut(),
            "items": [],
            "episode_page": episode_page,
            "episode_page_size": episode_page_size,
            "episode_total": 0,
            "episodes": [],
            "analytics": TimelineAnalyticsOut(),
        }

    if filters.table_id is not None:
        profile = profiles[0]
        events = _build_asset_context_events(
            session,
            profile=profile,
            column_id=filters.column_id,
            date_from=date_from,
            date_to=date_to,
        )
        table_fqn = profile.table_fqn
    else:
        events = _build_global_context_events(
            session,
            profiles=profiles,
            filters=filters,
            date_from=date_from,
            date_to=date_to,
        )
        table_fqn = None

    filtered_events = _apply_event_filters(events, filters)
    filtered_events.sort(key=lambda item: (item.occurred_at, item.priority, item.id), reverse=True)
    episodes_all = _group_timeline_episodes(filtered_events)
    action_map = _load_episode_actions(session, _dedupe_episode_keys(episodes_all))
    episodes_all = _apply_episode_actions(episodes_all, action_map)
    episodes_all = _apply_episode_filters(episodes_all, filters)
    episodes, episode_total = _paginate(episodes_all, page=episode_page, page_size=episode_page_size)
    _LOGGER.info(
        "timeline resolved scope=%s table_id=%s events=%s episodes=%s",
        "asset" if filters.table_id is not None else "global",
        filters.table_id,
        len(filtered_events),
        len(episodes_all),
    )
    summary = _summary_for(filtered_events)
    analytics = _analytics_for_episodes(episodes_all)
    return {
        "generated_at": datetime.now(timezone.utc),
        "scope": "asset" if filters.table_id is not None else "global",
        "table_id": filters.table_id,
        "column_id": filters.column_id,
        "table_fqn": table_fqn,
        "page": 1,
        "page_size": 0,
        "total": len(filtered_events),
        "summary": summary,
        "items": filtered_events,
        "episode_page": episode_page,
        "episode_page_size": episode_page_size,
        "episode_total": episode_total,
        "episodes": episodes,
        "analytics": analytics,
    }


def get_asset_timeline(
    session: Session,
    *,
    table_id: int,
    column_id: int | None = None,
    page: int = 1,
    page_size: int = 25,
    episode_page: int = 1,
    episode_page_size: int = 10,
    current_user=None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> TimelinePageOut:
    payload = build_timeline_payload(
        session,
        current_user=current_user,
        filters=TimelineQuery(
            table_id=table_id,
            column_id=column_id,
            date_from=date_from,
            date_to=date_to,
        ),
        episode_page=episode_page,
        episode_page_size=episode_page_size,
    )
    items, total = _paginate(payload["items"], page=page, page_size=page_size)
    return TimelinePageOut(
        generated_at=payload["generated_at"],
        scope=payload["scope"],
        table_id=payload["table_id"],
        column_id=payload["column_id"],
        table_fqn=payload["table_fqn"],
        page=page,
        page_size=page_size,
        total=total,
        summary=payload["summary"],
        items=items,
        episode_page=payload["episode_page"],
        episode_page_size=payload["episode_page_size"],
        episode_total=payload["episode_total"],
        episodes=payload["episodes"],
        analytics=payload["analytics"],
    )


def get_governance_timeline(
    session: Session,
    *,
    current_user=None,
    page: int = 1,
    page_size: int = 50,
    episode_page: int = 1,
    episode_page_size: int = 12,
    q: str | None = None,
    source: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    owner: str | None = None,
    certification_status: str | None = None,
    event_type: str | None = None,
    category: str | None = None,
    severity: str | None = None,
    manual_only: bool = False,
    automatic_only: bool = False,
    contains_pii: bool | None = None,
    contains_sensitive: bool | None = None,
    contains_critical: bool | None = None,
    open_incidents: bool | None = None,
    dq_recent: bool | None = None,
    table_id: int | None = None,
    column_id: int | None = None,
    episode_status: str | None = None,
    episode_type: str | None = None,
    min_importance_score: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> TimelinePageOut:
    payload = build_timeline_payload(
        session,
        current_user=current_user,
        filters=TimelineQuery(
            q=q,
            source=source,
            datasource=datasource,
            schema_name=schema_name,
            owner=owner,
            certification_status=certification_status,
            event_type=event_type,
            category=category,
            severity=severity,
            manual_only=manual_only,
            automatic_only=automatic_only,
            contains_pii=contains_pii,
            contains_sensitive=contains_sensitive,
            contains_critical=contains_critical,
            open_incidents=open_incidents,
            dq_recent=dq_recent,
            table_id=table_id,
            column_id=column_id,
            episode_status=episode_status,
            episode_type=episode_type,
            min_importance_score=min_importance_score,
            date_from=date_from,
            date_to=date_to,
        ),
        episode_page=episode_page,
        episode_page_size=episode_page_size,
    )
    items, total = _paginate(payload["items"], page=page, page_size=page_size)
    return TimelinePageOut(
        generated_at=payload["generated_at"],
        scope=payload["scope"],
        table_id=payload["table_id"],
        column_id=payload["column_id"],
        table_fqn=payload["table_fqn"],
        page=page,
        page_size=page_size,
        total=total,
        summary=payload["summary"],
        items=items,
        episode_page=payload["episode_page"],
        episode_page_size=payload["episode_page_size"],
        episode_total=payload["episode_total"],
        episodes=payload["episodes"],
        analytics=payload["analytics"],
    )
