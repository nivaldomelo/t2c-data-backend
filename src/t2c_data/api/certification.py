from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
import logging
from io import StringIO
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import asc, case, desc, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.access_control.abac import record_abac_denial
from t2c_data.features.export_jobs import ExportArtifactResult, enqueue_export_job, register_export_request_audit, serialize_export_job
from t2c_data.features.export_security import safe_csv_writer, DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, redact_export_value, resolve_export_limit
from t2c_data.features.audit import certification_changes
from t2c_data.features.certification.api_support import (
    build_certification_summary_out,
    build_table_certification_query,
    certification_order_clause,
    get_table_certification_or_404,
    validate_certification_patch,
)
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.platform.sensitive_data import can_view_sensitive_data, mask_payload_by_policy
from t2c_data.features.platform.visibility import mask_certification_summary_payload, table_visibility_decision_from_entity
from t2c_data.features.privacy_access import can_view_table
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import CertificationDecisionEvent, CertificationGoal
from t2c_data.schemas.catalog import (
    CertificationGoalCreate,
    CertificationDecisionEventOut,
    CertificationDecisionEventPageOut,
    CertificationGoalOut,
    CertificationGoalProgressOut,
    CertificationGoalProgressMetricsOut,
    CertificationGoalDailyProgressOut,
    CertificationGoalUpdate,
    CertificationRecommendationOut,
    TableCertificationDecisionIn,
    TableCertificationFiltersOut,
    TableCertificationSummaryMetricsOut,
    TableCertificationPageOut,
    TableCertificationPatch,
    TableCertificationSubmitIn,
    TableCertificationSummaryOut,
)
from t2c_data.schemas.platform import IntegrationSyncJobOut
from t2c_data.services.audit import log_field_changes, request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter(prefix="/certification", tags=["certification"])
logger = logging.getLogger(__name__)

CERTIFICATION_GOAL_SCOPE_TYPES = {"global", "datasource", "database", "schema", "owner", "criticality"}
CERTIFICATION_GOAL_STATUSES = {"active", "completed", "paused", "archived"}
CERTIFICATION_DECISION_TYPES = {
    "status_change",
    "certification",
    "refusal",
    "review",
    "revalidation",
    "expiration",
    "automatic_eligibility",
    "manual_override",
}
CERTIFICATION_DECISION_SOURCES = {"manual", "automatic", "system", "migration"}

CERTIFICATION_BLOCKER_DEFINITIONS = {
    "owner_defined": {
        "label": "Sem owner",
        "description": "Ativos sem responsável não podem ser certificados com segurança.",
        "action": "Definir owner no Explorer.",
    },
    "table_description_complete": {
        "label": "Sem descrição",
        "description": "Sem descrição, consumidores não entendem finalidade, escopo ou uso correto da tabela.",
        "action": "Completar documentação do ativo.",
    },
    "documentation_coverage": {
        "label": "Colunas pouco documentadas",
        "description": "A certificação exige documentação mínima das colunas para reduzir ambiguidade.",
        "action": "Documentar colunas principais.",
    },
    "tags_applied": {
        "label": "Sem tags",
        "description": "Tags ajudam busca, classificação e organização dos ativos.",
        "action": "Adicionar tags de domínio, sensibilidade ou uso.",
    },
    "terms_associated": {
        "label": "Sem termos",
        "description": "Termos conectam o ativo ao glossário de negócio.",
        "action": "Associar termos de negócio.",
    },
    "privacy_reviewed": {
        "label": "Privacidade sem revisão",
        "description": "Ativos com dado pessoal ou sensível precisam de revisão formal de privacidade antes de avançar na certificação.",
        "action": "Registrar revisão de privacidade.",
    },
    "privacy_context_complete": {
        "label": "Sem base legal ou finalidade",
        "description": "Ativos com dado pessoal ou sensível precisam de base legal e finalidade estruturadas para sustentar a decisão de certificação.",
        "action": "Completar base legal e finalidade.",
    },
    "dq_score": {
        "label": "Sem DQ",
        "description": "Sem score de Data Quality, a confiança mínima não está comprovada.",
        "action": "Executar ou configurar Data Quality.",
    },
    "no_critical_incidents": {
        "label": "Com incidente crítico",
        "description": "Incidentes críticos abertos bloqueiam certificação até análise ou resolução.",
        "action": "Abrir incidentes.",
    },
    "review_recent": {
        "label": "Sem revisão recente",
        "description": "A certificação precisa de revisão periódica para seguir válida.",
        "action": "Registrar revisão.",
    },
}

CERTIFICATION_BLOCKER_ROUTE_MAP = {
    "owner_defined": "/explorer",
    "table_description_complete": "/explorer",
    "documentation_coverage": "/governance/dictionary",
    "tags_applied": "/tags",
    "terms_associated": "/glossary",
    "privacy_reviewed": "/privacy-access",
    "privacy_context_complete": "/privacy-access",
    "dq_score": "/data-quality",
    "no_critical_incidents": "/incidents/tickets",
    "review_recent": "/certification",
}


def build_certification_queue_export_artifact(
    db: Session,
    *,
    current_user: User,
    q: str | None = None,
    certification_status: str | None = None,
    certification_criticality: str | None = None,
    owner_id: int | None = None,
    schema_name: str | None = None,
    database_name: str | None = None,
    datasource_name: str | None = None,
    sort_by: str = "updated_at",
    sort_dir: str = "desc",
    **_: Any,
) -> ExportArtifactResult:
    summaries = _build_visible_certification_summaries(
        db=db,
        current_user=current_user,
        q=q,
        certification_status=certification_status,
        certification_criticality=certification_criticality,
        owner_id=owner_id,
        schema_name=schema_name,
        database_name=database_name,
        datasource_name=datasource_name,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    export_limit = resolve_export_limit(source_module="certification", entity_type="certification_queue")
    summaries, truncated = enforce_export_limit(summaries, limit=export_limit)
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "banco",
            "schema",
            "tabela",
            "status",
            "prontidao",
            "owner",
            "criticidade",
            "criterios_atendidos",
            "criterios_totais",
            "criterios_pendentes",
            "principal_bloqueio",
            "proximo_passo",
            "ultima_revisao",
            "observacao",
        ]
    )
    for item in summaries:
        pending = _pending_criteria(item)
        writer.writerow(
            [
                item.database_name,
                item.schema_name,
                item.name,
                item.certification_status_label,
                item.readiness_score,
                redact_export_value(item.data_owner.name if item.data_owner else (item.owner or ""), field_name="owner"),
                item.certification_criticality or "",
                item.readiness_completed,
                item.readiness_total,
                len(pending),
                str(pending[0].get("label")) if pending else "",
                item.certification_next_step or "",
                item.certification_decided_at.isoformat() if item.certification_decided_at else "",
                redact_export_value(item.certification_notes, field_name="certification_notes"),
            ]
        )
    return ExportArtifactResult(
        payload=buffer.getvalue().encode("utf-8-sig"),
        filename="certification_queue.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(summaries),
        truncated=truncated,
        export_format="csv",
    )


def build_certification_events_export_artifact(
    db: Session,
    *,
    current_user: User,
    certification_status: str | None = None,
    decision_type: str | None = None,
    decision_source: str | None = None,
    reviewer: str | None = None,
    database_name: str | None = None,
    schema_name: str | None = None,
    table_name: str | None = None,
    goal_id: int | None = None,
    owner_id: int | None = None,
    certification_criticality: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    **_: Any,
) -> ExportArtifactResult:
    export_limit = resolve_export_limit(source_module="certification", entity_type="certification_event")
    page = _resolve_event_page(
        db=db,
        current_user=current_user,
        certification_status=certification_status,
        decision_type=decision_type,
        decision_source=decision_source,
        reviewer=reviewer,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        goal_id=goal_id,
        owner_id=owner_id,
        certification_criticality=certification_criticality,
        date_from=date_from,
        date_to=date_to,
        page=1,
        page_size=export_limit,
    )
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "data",
            "ativo",
            "banco",
            "schema",
            "tabela",
            "tipo_decisao",
            "status_anterior",
            "novo_status",
            "prontidao_anterior",
            "prontidao_atual",
            "responsavel",
            "observacao",
            "validade",
            "origem",
        ]
    )
    for item in page.items:
        writer.writerow(
            [
                item.created_at.isoformat(),
                item.asset_name,
                item.database_name,
                item.schema_name,
                item.table_name,
                item.decision_type,
                item.previous_status or "",
                item.new_status,
                item.previous_readiness if item.previous_readiness is not None else "",
                item.new_readiness if item.new_readiness is not None else "",
                item.reviewer or item.reviewer_email or "",
                redact_export_value(item.observation, field_name="observation"),
                item.valid_until.isoformat() if item.valid_until else "",
                item.decision_source,
            ]
        )
    return ExportArtifactResult(
        payload=buffer.getvalue().encode("utf-8-sig"),
        filename="certification_events.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(page.items),
        truncated=page.total > len(page.items),
        export_format="csv",
    )


def _validate_goal_dates(period_start: date, period_end: date) -> None:
    if period_end < period_start:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="O período final da meta não pode ser anterior ao período inicial.",
        )


def _validate_goal_scope(scope_type: str, scope_value: str | None) -> tuple[str, str | None]:
    normalized_scope = (scope_type or "global").strip().lower()
    if normalized_scope not in CERTIFICATION_GOAL_SCOPE_TYPES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Escopo da meta de certificação inválido.")
    normalized_value = scope_value.strip() if scope_value and scope_value.strip() else None
    if normalized_scope == "global":
        return normalized_scope, None
    if not normalized_value:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Este escopo exige um valor associado.")
    if normalized_scope == "owner" and not normalized_value.isdigit():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Para escopo por owner, informe o identificador numérico do owner.",
        )
    return normalized_scope, normalized_value


def _goal_scope_filters(goal: CertificationGoal) -> dict[str, str | int | None]:
    if goal.scope_type == "global":
        return {}
    if goal.scope_type == "datasource":
        return {"datasource_name": goal.scope_value}
    if goal.scope_type == "database":
        return {"database_name": goal.scope_value}
    if goal.scope_type == "schema":
        return {"schema_name": goal.scope_value}
    if goal.scope_type == "criticality":
        return {"certification_criticality": goal.scope_value}
    if goal.scope_type == "owner":
        return {"owner_id": int(goal.scope_value or "0")}
    return {}


def _goal_status_label(status: str) -> str:
    return {
        "on_track": "No prazo",
        "attention": "Atenção",
        "delayed": "Atrasado",
        "no_data": "Sem dados suficientes",
    }.get(status, "Sem dados suficientes")


def _decision_type_for_transition(previous_status: str | None, new_status: str, *, status_changed: bool) -> str:
    if new_status == "certified":
        return "certification"
    if new_status == "rejected":
        return "refusal"
    if new_status == "revalidation_pending":
        return "revalidation"
    if new_status == "expired":
        return "expiration"
    if status_changed:
        return "status_change"
    return "review"


def _build_event_query():
    return (
        select(CertificationDecisionEvent)
        .join(TableEntity, TableEntity.id == CertificationDecisionEvent.asset_id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    )


def _serialize_event(event: CertificationDecisionEvent) -> CertificationDecisionEventOut:
    return CertificationDecisionEventOut.model_validate(event)


def _resolve_event_page(
    *,
    db: Session,
    current_user: User,
    asset_id: int | None = None,
    decision_type: str | None = None,
    decision_source: str | None = None,
    certification_status: str | None = None,
    reviewer: str | None = None,
    database_name: str | None = None,
    schema_name: str | None = None,
    table_name: str | None = None,
    owner_id: int | None = None,
    certification_criticality: str | None = None,
    goal_id: int | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: int = 1,
    page_size: int = 25,
) -> CertificationDecisionEventPageOut:
    query = _build_event_query()
    if asset_id is not None:
        query = query.where(CertificationDecisionEvent.asset_id == asset_id)
    if decision_type:
        query = query.where(CertificationDecisionEvent.decision_type == decision_type.strip().lower())
    if decision_source:
        query = query.where(CertificationDecisionEvent.decision_source == decision_source.strip().lower())
    if certification_status:
        query = query.where(CertificationDecisionEvent.new_status == certification_status.strip().lower())
    if reviewer and reviewer.strip():
        token = f"%{reviewer.strip()}%"
        query = query.where(or_(CertificationDecisionEvent.reviewer.ilike(token), CertificationDecisionEvent.reviewer_email.ilike(token)))
    if database_name and database_name.strip():
        query = query.where(func.lower(CertificationDecisionEvent.database_name) == database_name.strip().lower())
    if schema_name and schema_name.strip():
        query = query.where(func.lower(CertificationDecisionEvent.schema_name) == schema_name.strip().lower())
    if table_name and table_name.strip():
        query = query.where(func.lower(CertificationDecisionEvent.table_name) == table_name.strip().lower())
    if owner_id is not None:
        query = query.where(TableEntity.data_owner_id == owner_id)
    if certification_criticality and certification_criticality.strip():
        query = query.where(TableEntity.certification_criticality == certification_criticality.strip().lower())
    if goal_id is not None:
        query = query.where(CertificationDecisionEvent.goal_id == goal_id)
    if date_from is not None:
        query = query.where(CertificationDecisionEvent.created_at >= datetime.combine(date_from, datetime.min.time(), tzinfo=timezone.utc))
    if date_to is not None:
        query = query.where(CertificationDecisionEvent.created_at <= datetime.combine(date_to, datetime.max.time(), tzinfo=timezone.utc))

    rows = db.scalars(query.order_by(desc(CertificationDecisionEvent.created_at), desc(CertificationDecisionEvent.id))).unique().all()
    visible_rows: list[CertificationDecisionEvent] = []
    for event in rows:
        table = db.get(TableEntity, event.asset_id)
        if table and can_view_table(current_user, table):
            visible_rows.append(event)
    total = len(visible_rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    effective_page = min(page, total_pages) if total else 1
    start = (effective_page - 1) * page_size
    end = start + page_size
    return CertificationDecisionEventPageOut(
        items=[_serialize_event(item) for item in visible_rows[start:end]],
        total=total,
        page=effective_page,
        page_size=page_size,
    )


def _scope_label(goal: CertificationGoal) -> str:
    if goal.scope_type == "global":
        return "catálogo completo"
    if goal.scope_type == "owner":
        return f"owner {goal.scope_value}"
    return f"{goal.scope_type} {goal.scope_value}"


def _build_goal_recommendations(
    *,
    goal: CertificationGoal,
    metrics: TableCertificationSummaryMetricsOut,
    certified_in_period: int,
    current_daily_rate: float,
    required_daily_rate: float,
) -> list[CertificationRecommendationOut]:
    recommendations: list[CertificationRecommendationOut] = []
    if metrics.blockers:
        top_blocker = metrics.blockers[0]
        recommendations.append(
            CertificationRecommendationOut(
                title=f"Atacar bloqueio: {top_blocker.label}",
                description=(
                    f"{top_blocker.count} ativos do escopo {_scope_label(goal)} ainda estão bloqueados por {top_blocker.label.lower()}. "
                    f"{top_blocker.action}"
                ),
                priority="high",
                action_label="Abrir módulo sugerido",
                action_href=CERTIFICATION_BLOCKER_ROUTE_MAP.get(top_blocker.key),
            )
        )
    if goal.target_certified_assets > 0 and certified_in_period < goal.target_certified_assets and required_daily_rate > current_daily_rate:
        recommendations.append(
            CertificationRecommendationOut(
                title="Acelerar certificações do período",
                description=(
                    f"O ritmo atual está abaixo do necessário para atingir a meta. "
                    f"Priorize ativos já elegíveis e próximos da certificação."
                ),
                priority="high",
                action_label="Revisar ativos elegíveis",
                action_href="/certification",
            )
        )
    if metrics.near_certification:
        recommendations.append(
            CertificationRecommendationOut(
                title="Fechar pendências dos ativos mais próximos",
                description="Há ativos quase elegíveis que podem aumentar rapidamente o número de certificações ou revisões concluídas.",
                priority="medium",
                action_label="Ver ativos prioritários",
                action_href="/certification",
            )
        )
    if not recommendations:
        recommendations.append(
            CertificationRecommendationOut(
                title="Meta acompanhada sem bloqueios dominantes",
                description="O escopo atual não mostrou um bloqueio predominante. Continue revisando ativos elegíveis e pendentes de revalidação.",
                priority="low",
                action_label="Abrir fila de certificação",
                action_href="/certification",
            )
        )
    return recommendations


def _mask_certification_summary(item: TableCertificationSummaryOut, *, current_user: User) -> TableCertificationSummaryOut:
    payload = mask_certification_summary_payload(item.model_dump())
    if not can_view_sensitive_data(current_user, table=table):
        payload = mask_payload_by_policy(payload, can_view_sensitive=False)
        payload["owner"] = "[masked]" if payload.get("owner") is not None else None
        payload["owner_email"] = "[masked]" if payload.get("owner_email") is not None else None
        if isinstance(payload.get("data_owner"), dict):
            payload["data_owner"]["name"] = "[masked]"
            payload["data_owner"]["email"] = "[masked]"
    return TableCertificationSummaryOut(**payload)


def _build_visible_certification_summaries(
    *,
    db: Session,
    current_user: User,
    q: str | None = None,
    certification_status: str | None = None,
    certification_criticality: str | None = None,
    owner_id: int | None = None,
    schema_name: str | None = None,
    database_name: str | None = None,
    datasource_name: str | None = None,
    quick_filter: str | None = None,
    sort_by: str = "updated_at",
    sort_dir: str = "desc",
) -> list[TableCertificationSummaryOut]:
    query = build_table_certification_query()

    if q and q.strip():
        term = f"%{q.strip()}%"
        query = query.where(
            or_(
                TableEntity.name.ilike(term),
                Schema.name.ilike(term),
                Database.name.ilike(term),
                DataSource.name.ilike(term),
                TableEntity.owner.ilike(term),
                TableEntity.certification_notes.ilike(term),
            )
        )
    if certification_criticality:
        query = query.where(TableEntity.certification_criticality == certification_criticality)
    if owner_id is not None:
        query = query.where(TableEntity.data_owner_id == owner_id)
    if schema_name and schema_name.strip():
        query = query.where(func.lower(Schema.name) == schema_name.strip().lower())
    if database_name and database_name.strip():
        query = query.where(func.lower(Database.name) == database_name.strip().lower())
    if datasource_name and datasource_name.strip():
        query = query.where(func.lower(DataSource.name) == datasource_name.strip().lower())

    query = query.order_by(certification_order_clause(sort_by, sort_dir), asc(Schema.name), asc(TableEntity.name))
    tables = db.scalars(query).unique().all()
    settings_snapshot = get_governance_settings_snapshot(db)
    summaries: list[TableCertificationSummaryOut] = []
    for table in tables:
        if not can_view_table(current_user, table):
            continue
        try:
            summary = build_certification_summary_out(db, table, settings_snapshot=settings_snapshot)
        except Exception as exc:
            logger.warning(
                "certification_table_summary_skipped table_id=%s schema=%s table=%s error=%s",
                table.id,
                table.schema.name,
                table.name,
                exc,
                exc_info=True,
            )
            continue
        if table_visibility_decision_from_entity(table, user=current_user).masked or not can_view_sensitive_data(current_user, table=table):
            summary = _mask_certification_summary(summary, current_user=current_user)
        summaries.append(summary)
    if certification_status:
        summaries = [item for item in summaries if item.certification_status == certification_status]
    summaries = _apply_certification_quick_filter(summaries, quick_filter)
    return summaries


def _apply_certification_quick_filter(
    summaries: list[TableCertificationSummaryOut],
    quick_filter: str | None,
) -> list[TableCertificationSummaryOut]:
    """Filter the full (computed) summary set server-side so pagination is consistent.

    Mirrors the options the UI used to apply only to the current page:
    - 'near_certification': not certified, readiness >= 70, no failing critical-incident check;
    - 'low_readiness': readiness < 50;
    - any other value: a blocker checklist key that is currently failing (not passed).
    """
    normalized = (quick_filter or "").strip()
    if not normalized:
        return summaries
    if normalized == "near_certification":
        return [
            item
            for item in summaries
            if item.certification_status != "certified"
            and item.readiness_score >= 70
            and not any(
                str(check.get("key")) == "no_critical_incidents" and not bool(check.get("passed"))
                for check in item.checklist
            )
        ]
    if normalized == "low_readiness":
        return [item for item in summaries if item.readiness_score < 50]
    return [
        item
        for item in summaries
        if any(str(check.get("key")) == normalized and not bool(check.get("passed")) for check in item.checklist)
    ]


def _pending_criteria(item: TableCertificationSummaryOut) -> list[dict[str, str | bool]]:
    return [check for check in item.checklist if not bool(check.get("passed"))]


def _priority_item(item: TableCertificationSummaryOut) -> dict[str, object]:
    pending = _pending_criteria(item)
    primary = pending[0] if pending else None
    return {
        "id": item.id,
        "name": item.name,
        "schema_name": item.schema_name,
        "database_name": item.database_name,
        "datasource_name": item.datasource_name,
        "certification_status": item.certification_status,
        "certification_status_label": item.certification_status_label,
        "readiness_score": item.readiness_score,
        "readiness_completed": item.readiness_completed,
        "readiness_total": item.readiness_total,
        "pending_criteria": len(pending),
        "primary_blocker": str(primary.get("label")) if primary else None,
        "primary_blocker_detail": str(primary.get("detail")) if primary else None,
        "next_step": item.certification_next_step,
    }


def _build_certification_metrics(summaries: list[TableCertificationSummaryOut]) -> TableCertificationSummaryMetricsOut:
    total = len(summaries)
    blocker_items = []
    for key, definition in CERTIFICATION_BLOCKER_DEFINITIONS.items():
        count = sum(1 for item in summaries if any(check.get("key") == key and not bool(check.get("passed")) for check in item.checklist))
        blocker_items.append(
            {
                "key": key,
                "label": definition["label"],
                "count": count,
                "percent": round((count / total) * 100) if total else 0,
                "description": definition["description"],
                "action": definition["action"],
            }
        )
    near_certification = sorted(
        [item for item in summaries if item.certification_status != "certified"],
        key=lambda item: (len(_pending_criteria(item)), -item.readiness_score),
    )[:10]
    most_blocked = sorted(
        [item for item in summaries if _pending_criteria(item)],
        key=lambda item: (len(_pending_criteria(item)), -item.readiness_score),
        reverse=True,
    )[:10]
    distribution_map: dict[str, dict[str, object]] = {}
    for item in summaries:
        key = f"{item.database_name}.{item.schema_name}"
        bucket = distribution_map.setdefault(
            key,
            {
                "key": key,
                "database_name": item.database_name,
                "schema_name": item.schema_name,
                "total": 0,
                "certified": 0,
                "eligible": 0,
                "not_eligible": 0,
                "readiness": [],
                "blockers": {},
            },
        )
        bucket["total"] = int(bucket["total"]) + 1
        bucket["certified"] = int(bucket["certified"]) + (1 if item.certification_status == "certified" else 0)
        bucket["eligible"] = int(bucket["eligible"]) + (1 if item.certification_status == "eligible" else 0)
        bucket["not_eligible"] = int(bucket["not_eligible"]) + (1 if item.certification_status == "not_eligible" else 0)
        bucket["readiness"].append(item.readiness_score)  # type: ignore[index, union-attr]
        for pending in _pending_criteria(item):
            label = str(pending.get("label") or pending.get("key"))
            bucket["blockers"][label] = int(bucket["blockers"].get(label, 0)) + 1  # type: ignore[union-attr]
    distribution = []
    for bucket in distribution_map.values():
        readiness_values = list(bucket["readiness"])  # type: ignore[arg-type]
        blocker_entries = sorted(dict(bucket["blockers"]).items(), key=lambda entry: entry[1], reverse=True)  # type: ignore[arg-type]
        distribution.append(
            {
                "key": str(bucket["key"]),
                "database_name": str(bucket["database_name"]),
                "schema_name": str(bucket["schema_name"]),
                "total": int(bucket["total"]),
                "certified": int(bucket["certified"]),
                "eligible": int(bucket["eligible"]),
                "not_eligible": int(bucket["not_eligible"]),
                "avg_readiness": round(sum(readiness_values) / max(len(readiness_values), 1)),
                "primary_blocker": blocker_entries[0][0] if blocker_entries else None,
                "primary_blocker_count": int(blocker_entries[0][1]) if blocker_entries else 0,
            }
        )
    return TableCertificationSummaryMetricsOut(
        total=total,
        certified=sum(1 for item in summaries if item.certification_status == "certified"),
        eligible=sum(1 for item in summaries if item.certification_status == "eligible"),
        in_review=sum(1 for item in summaries if item.certification_status == "in_review"),
        rejected=sum(1 for item in summaries if item.certification_status == "rejected"),
        revalidation_pending=sum(1 for item in summaries if item.certification_status == "revalidation_pending"),
        not_eligible=sum(1 for item in summaries if item.certification_status == "not_eligible"),
        avg_readiness=round(sum(item.readiness_score for item in summaries) / max(total, 1)),
        blockers=[item for item in blocker_items if item["count"] > 0],
        near_certification=[_priority_item(item) for item in near_certification],
        most_blocked=[_priority_item(item) for item in most_blocked],
        distribution=sorted(distribution, key=lambda item: (-int(item["total"]), str(item["key"]))),
    )


def _daterange(start: date, end: date) -> list[date]:
    current = start
    days: list[date] = []
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _build_goal_progress(
    *,
    goal: CertificationGoal,
    summaries: list[TableCertificationSummaryOut],
) -> CertificationGoalProgressOut:
    today = datetime.now(timezone.utc).date()
    metrics = _build_certification_metrics(summaries)
    effective_end = min(goal.period_end, today)
    has_started = today >= goal.period_start
    period_days = _daterange(goal.period_start, effective_end) if has_started and effective_end >= goal.period_start else []

    certified_daily: dict[date, int] = {}
    reviewed_daily: dict[date, set[int]] = {}
    revalidated_daily: dict[date, int] = {}
    certified_in_period = 0
    reviewed_in_period_ids: set[int] = set()
    revalidated_in_period = 0

    for item in summaries:
        decided_at = item.certification_decided_at.date() if item.certification_decided_at else None
        review_at = item.certification_review_at.date() if item.certification_review_at else None
        if decided_at and goal.period_start <= decided_at <= goal.period_end:
            reviewed_daily.setdefault(decided_at, set()).add(item.id)
            reviewed_in_period_ids.add(item.id)
            if item.certification_status == "certified":
                certified_daily[decided_at] = certified_daily.get(decided_at, 0) + 1
                certified_in_period += 1
            if item.certification_status == "revalidation_pending":
                revalidated_daily[decided_at] = revalidated_daily.get(decided_at, 0) + 1
                revalidated_in_period += 1
        if review_at and goal.period_start <= review_at <= goal.period_end:
            reviewed_daily.setdefault(review_at, set()).add(item.id)
            reviewed_in_period_ids.add(item.id)

    daily_items: list[CertificationGoalDailyProgressOut] = []
    accumulated_certified = 0
    for day in period_days:
        certified_count = certified_daily.get(day, 0)
        accumulated_certified += certified_count
        daily_items.append(
            CertificationGoalDailyProgressOut(
                date=day,
                certified=certified_count,
                eligible=0,
                reviewed=len(reviewed_daily.get(day, set())),
                revalidated=revalidated_daily.get(day, 0),
                accumulated_certified=accumulated_certified,
            )
        )

    days_elapsed = len(period_days)
    if today < goal.period_start:
        days_remaining = (goal.period_end - goal.period_start).days + 1
    else:
        days_remaining = max((goal.period_end - today).days + 1, 0)
    current_certified_assets = metrics.certified
    current_eligible_assets = metrics.eligible
    remaining_certified_assets = max(goal.target_certified_assets - certified_in_period, 0)
    completion_percent = (
        round((certified_in_period / goal.target_certified_assets) * 100)
        if goal.target_certified_assets > 0
        else 0
    )
    required_daily_rate = round((remaining_certified_assets / days_remaining), 2) if days_remaining > 0 else 0.0
    current_daily_rate = round((certified_in_period / max(days_elapsed, 1)), 2) if days_elapsed > 0 else 0.0
    projected_total = (
        int(round(certified_in_period + (current_daily_rate * days_remaining)))
        if goal.target_certified_assets > 0 and days_elapsed > 0
        else certified_in_period
    )
    if goal.target_certified_assets <= 0:
        progress_status = "no_data"
    elif certified_in_period >= goal.target_certified_assets:
        progress_status = "on_track"
    elif days_remaining == 0:
        progress_status = "delayed"
    elif current_daily_rate >= required_daily_rate:
        progress_status = "on_track"
    elif current_daily_rate >= round(required_daily_rate * 0.75, 2):
        progress_status = "attention"
    else:
        progress_status = "delayed"

    history_note = (
        "A série diária usa decisões e revisões já registradas. Elegibilidade diária histórica ainda não está consolidada em eventos dedicados."
        if daily_items
        else "Ainda não há histórico suficiente de decisões ou revisões dentro do período desta meta."
    )

    return CertificationGoalProgressOut(
        goal=CertificationGoalOut.model_validate(goal),
        progress=CertificationGoalProgressMetricsOut(
            certified_assets=certified_in_period,
            eligible_assets=current_eligible_assets,
            reviewed_assets=len(reviewed_in_period_ids),
            revalidated_assets=revalidated_in_period,
            decisions_assets=len(reviewed_in_period_ids),
            refusal_assets=0,
            current_certified_assets=current_certified_assets,
            current_eligible_assets=current_eligible_assets,
            remaining_certified_assets=remaining_certified_assets,
            completion_percent=completion_percent,
            days_elapsed=days_elapsed,
            days_remaining=days_remaining,
            required_daily_rate=required_daily_rate,
            current_daily_rate=current_daily_rate,
            projected_total=projected_total,
            status=progress_status,
            status_label=_goal_status_label(progress_status),
            history_source="legacy_dates_fallback",
            history_note=history_note,
        ),
        daily=daily_items,
        blockers=metrics.blockers[:5],
        recommendations=_build_goal_recommendations(
            goal=goal,
            metrics=metrics,
            certified_in_period=certified_in_period,
            current_daily_rate=current_daily_rate,
            required_daily_rate=required_daily_rate,
        ),
    )


def _build_goal_progress_from_events(
    *,
    goal: CertificationGoal,
    summaries: list[TableCertificationSummaryOut],
    events: list[CertificationDecisionEvent],
) -> CertificationGoalProgressOut:
    today = datetime.now(timezone.utc).date()
    metrics = _build_certification_metrics(summaries)
    effective_end = min(goal.period_end, today)
    has_started = today >= goal.period_start
    period_days = _daterange(goal.period_start, effective_end) if has_started and effective_end >= goal.period_start else []
    event_days = {day: {"certified": 0, "reviewed": 0, "revalidated": 0, "eligible": 0, "refusal": 0} for day in period_days}
    certified_ids: set[int] = set()
    reviewed_ids: set[int] = set()
    revalidated_ids: set[int] = set()
    refusal_ids: set[int] = set()
    decision_ids: set[int] = set()

    for event in events:
        day = event.created_at.astimezone(timezone.utc).date()
        if day not in event_days:
            continue
        if event.decision_type in {"review", "status_change", "certification", "refusal", "revalidation", "expiration", "manual_override"}:
            event_days[day]["reviewed"] += 1
            reviewed_ids.add(event.asset_id)
        if event.decision_type == "certification":
            event_days[day]["certified"] += 1
            certified_ids.add(event.asset_id)
            decision_ids.add(event.asset_id)
        if event.decision_type == "refusal":
            event_days[day]["refusal"] += 1
            refusal_ids.add(event.asset_id)
            decision_ids.add(event.asset_id)
        if event.decision_type == "revalidation":
            event_days[day]["revalidated"] += 1
            revalidated_ids.add(event.asset_id)
            decision_ids.add(event.asset_id)
        if event.new_status in {"in_review", "eligible"}:
            event_days[day]["eligible"] += 1
        if event.decision_type in {"status_change", "manual_override", "expiration"}:
            decision_ids.add(event.asset_id)

    accumulated_certified = 0
    daily_items: list[CertificationGoalDailyProgressOut] = []
    for day in period_days:
        certified_count = event_days[day]["certified"]
        accumulated_certified += certified_count
        daily_items.append(
            CertificationGoalDailyProgressOut(
                date=day,
                certified=certified_count,
                eligible=event_days[day]["eligible"],
                reviewed=event_days[day]["reviewed"],
                revalidated=event_days[day]["revalidated"],
                accumulated_certified=accumulated_certified,
            )
        )

    days_elapsed = len(period_days)
    days_remaining = max((goal.period_end - today).days + 1, 0) if today >= goal.period_start else (goal.period_end - goal.period_start).days + 1
    certified_in_period = sum(item["certified"] for item in event_days.values())
    refusal_in_period = sum(item["refusal"] for item in event_days.values())
    revalidated_in_period = sum(item["revalidated"] for item in event_days.values())
    reviewed_in_period = sum(item["reviewed"] for item in event_days.values())
    current_certified_assets = metrics.certified
    current_eligible_assets = metrics.eligible
    remaining_certified_assets = max(goal.target_certified_assets - certified_in_period, 0)
    completion_percent = round((certified_in_period / goal.target_certified_assets) * 100) if goal.target_certified_assets > 0 else 0
    required_daily_rate = round((remaining_certified_assets / days_remaining), 2) if days_remaining > 0 else 0.0
    current_daily_rate = round((certified_in_period / max(days_elapsed, 1)), 2) if days_elapsed > 0 else 0.0
    projected_total = int(round(certified_in_period + (current_daily_rate * days_remaining))) if goal.target_certified_assets > 0 and days_elapsed > 0 else certified_in_period
    if goal.target_certified_assets <= 0:
        progress_status = "no_data"
    elif certified_in_period >= goal.target_certified_assets:
        progress_status = "on_track"
    elif days_remaining == 0:
        progress_status = "delayed"
    elif current_daily_rate >= required_daily_rate:
        progress_status = "on_track"
    elif current_daily_rate >= round(required_daily_rate * 0.75, 2):
        progress_status = "attention"
    else:
        progress_status = "delayed"

    return CertificationGoalProgressOut(
        goal=CertificationGoalOut.model_validate(goal),
        progress=CertificationGoalProgressMetricsOut(
            certified_assets=certified_in_period,
            eligible_assets=current_eligible_assets,
            reviewed_assets=reviewed_in_period,
            revalidated_assets=revalidated_in_period,
            decisions_assets=len(decision_ids),
            refusal_assets=refusal_in_period,
            current_certified_assets=current_certified_assets,
            current_eligible_assets=current_eligible_assets,
            remaining_certified_assets=remaining_certified_assets,
            completion_percent=completion_percent,
            days_elapsed=days_elapsed,
            days_remaining=days_remaining,
            required_daily_rate=required_daily_rate,
            current_daily_rate=current_daily_rate,
            projected_total=projected_total,
            status=progress_status,
            status_label=_goal_status_label(progress_status),
            history_source="events",
            history_note="Evolução baseada em eventos auditáveis de decisão registrados no período.",
        ),
        daily=daily_items,
        blockers=metrics.blockers[:5],
        recommendations=_build_goal_recommendations(
            goal=goal,
            metrics=metrics,
            certified_in_period=certified_in_period,
            current_daily_rate=current_daily_rate,
            required_daily_rate=required_daily_rate,
        ),
    )


def _record_certification_event(
    *,
    db: Session,
    table: TableEntity,
    user: User,
    before_summary: TableCertificationSummaryOut,
    after_summary: TableCertificationSummaryOut,
    previous_status: str,
    new_status: str,
    notes: str | None,
    decision_source: str = "manual",
    explicit_reason: str | None = None,
) -> CertificationDecisionEvent:
    status_changed = previous_status != new_status
    decision_type = _decision_type_for_transition(previous_status, new_status, status_changed=status_changed)
    pending_checklist = [check for check in after_summary.checklist if not bool(check.get("passed"))]
    passed_checklist = [check for check in after_summary.checklist if bool(check.get("passed"))]
    event = CertificationDecisionEvent(
        asset_id=table.id,
        asset_name=f"{table.schema.name}.{table.name}",
        database_name=table.schema.database.name,
        schema_name=table.schema.name,
        table_name=table.name,
        previous_status=previous_status,
        new_status=new_status,
        previous_readiness=before_summary.readiness_score,
        new_readiness=after_summary.readiness_score,
        decision_type=decision_type,
        decision_source=decision_source if decision_source in CERTIFICATION_DECISION_SOURCES else "manual",
        reviewer_user_id=user.id,
        reviewer=user.name or user.full_name,
        reviewer_email=user.email,
        observation=notes,
        reason=explicit_reason or after_summary.certification_status_reason,
        valid_until=table.certification_expires_at,
        revalidation_due_at=table.certification_review_at,
        metadata_json={
            "status_changed": status_changed,
            "submitted_at": table.certification_submitted_at.isoformat() if table.certification_submitted_at else None,
            "decided_at": table.certification_decided_at.isoformat() if table.certification_decided_at else None,
            "source_rule": after_summary.certification_status_rule,
            "source_label": after_summary.certification_status_source,
            "effective_status": after_summary.certification_status,
            "effective_status_label": after_summary.certification_status_label,
            "workflow_stage": after_summary.certification_status,
            "workflow_gates_total": len(after_summary.checklist or []),
            "workflow_gates_passed": len(passed_checklist),
            "workflow_gates_pending": len(pending_checklist),
            "workflow_gates_pending_labels": [item["label"] for item in pending_checklist],
            "readiness_completed": after_summary.readiness_completed,
            "readiness_total": after_summary.readiness_total,
            "eligible_for_certification": after_summary.eligible_for_certification,
            "active_dq_violation_count": after_summary.active_dq_violation_count,
            "active_dq_rule_names": list(after_summary.active_dq_rule_names or []),
            "checklist": list(after_summary.checklist or []),
            "pending_checklist": pending_checklist,
            "primary_pending_check": pending_checklist[0] if pending_checklist else None,
        },
    )
    db.add(event)
    db.flush()
    return event


@router.get("/tables", response_model=TableCertificationPageOut)
def list_table_certifications(
    q: str | None = Query(default=None),
    certification_status: str | None = Query(default=None),
    certification_criticality: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    datasource_name: str | None = Query(default=None),
    quick_filter: str | None = Query(default=None),
    sort_by: str = Query(default="updated_at"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=6, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableCertificationPageOut:
    try:
        summaries = _build_visible_certification_summaries(
            db=db,
            current_user=current_user,
            q=q,
            certification_status=certification_status,
            certification_criticality=certification_criticality,
            owner_id=owner_id,
            schema_name=schema_name,
            database_name=database_name,
            datasource_name=datasource_name,
            quick_filter=quick_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        total = len(summaries)
        total_pages = max(1, (total + page_size - 1) // page_size)
        effective_page = min(page, total_pages) if total > 0 else 1
        start = (effective_page - 1) * page_size
        end = start + page_size
        owner_map: dict[int, str] = {}
        schema_names: set[str] = set()
        database_names: set[str] = set()
        for summary in summaries:
            schema_names.add(summary.schema_name)
            database_names.add(summary.database_name)
            if summary.data_owner_id and summary.data_owner and summary.data_owner.name:
                owner_map.setdefault(summary.data_owner_id, summary.data_owner.name)
        return TableCertificationPageOut(
            total=total,
            page=effective_page,
            page_size=page_size,
            items=summaries[start:end],
            filters=TableCertificationFiltersOut(
                owners=[
                    {"id": owner_id, "name": owner_name}
                    for owner_id, owner_name in sorted(owner_map.items(), key=lambda item: item[1].lower())
                ],
                schemas=sorted(schema_names),
                databases=sorted(database_names),
            ),
        )
    except SQLAlchemyError as exc:
        logger.exception(
            "certification_tables_query_failed sort_by=%s sort_dir=%s page=%s page_size=%s error=%s",
            sort_by,
            sort_dir,
            page,
            page_size,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Falha interna ao carregar a certificação. Verifique se as migrations do banco foram aplicadas.",
        ) from exc


@router.get("/summary", response_model=TableCertificationSummaryMetricsOut)
def get_certification_summary(
    q: str | None = Query(default=None),
    certification_status: str | None = Query(default=None),
    certification_criticality: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    datasource_name: str | None = Query(default=None),
    sort_by: str = Query(default="updated_at"),
    sort_dir: str = Query(default="desc"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableCertificationSummaryMetricsOut:
    try:
        summaries = _build_visible_certification_summaries(
            db=db,
            current_user=current_user,
            q=q,
            certification_status=certification_status,
            certification_criticality=certification_criticality,
            owner_id=owner_id,
            schema_name=schema_name,
            database_name=database_name,
            datasource_name=datasource_name,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        return _build_certification_metrics(summaries)
    except SQLAlchemyError as exc:
        logger.exception("certification_summary_query_failed error=%s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Falha interna ao carregar o resumo de certificação.",
        ) from exc


@router.get("/goals", response_model=list[CertificationGoalOut])
def list_certification_goals(
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[CertificationGoalOut]:
    query = db.query(CertificationGoal)
    if status_filter and status_filter.strip():
        query = query.filter(CertificationGoal.status == status_filter.strip().lower())
    goals = query.order_by(
        case((CertificationGoal.status == "active", 0), else_=1),
        CertificationGoal.period_start.desc(),
        CertificationGoal.id.desc(),
    ).all()
    return [CertificationGoalOut.model_validate(goal) for goal in goals]


@router.post("/goals", response_model=CertificationGoalOut, status_code=status.HTTP_201_CREATED)
def create_certification_goal(
    payload: CertificationGoalCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> CertificationGoalOut:
    _validate_goal_dates(payload.period_start, payload.period_end)
    scope_type, scope_value = _validate_goal_scope(payload.scope_type, payload.scope_value)
    status_value = (payload.status or "active").strip().lower()
    if status_value not in CERTIFICATION_GOAL_STATUSES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Status da meta de certificação inválido.")
    goal = CertificationGoal(
        name=payload.name.strip(),
        period_start=payload.period_start,
        period_end=payload.period_end,
        target_certified_assets=payload.target_certified_assets,
        target_eligible_assets=payload.target_eligible_assets,
        target_reviewed_assets=payload.target_reviewed_assets,
        target_revalidated_assets=payload.target_revalidated_assets,
        scope_type=scope_type,
        scope_value=scope_value,
        owner=payload.owner.strip() if payload.owner else None,
        status=status_value,
        notes=payload.notes.strip() if payload.notes else None,
    )
    db.add(goal)
    db.commit()
    db.refresh(goal)
    write_audit_log_sync(
        db,
        action="certification.goal.create",
        entity_type="certification_goal",
        entity_id=goal.id,
        after=serialize_model(goal),
        metadata={"message": "Certification goal created"},
        user_id=current_user.id,
    )
    db.commit()
    return CertificationGoalOut.model_validate(goal)


@router.get("/goals/{goal_id}", response_model=CertificationGoalOut)
def get_certification_goal(
    goal_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> CertificationGoalOut:
    goal = db.get(CertificationGoal, goal_id)
    if not goal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Meta de certificação não encontrada.")
    return CertificationGoalOut.model_validate(goal)


@router.patch("/goals/{goal_id}", response_model=CertificationGoalOut)
def patch_certification_goal(
    goal_id: int,
    payload: CertificationGoalUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> CertificationGoalOut:
    goal = db.get(CertificationGoal, goal_id)
    if not goal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Meta de certificação não encontrada.")
    before = serialize_model(goal)
    next_period_start = payload.period_start or goal.period_start
    next_period_end = payload.period_end or goal.period_end
    _validate_goal_dates(next_period_start, next_period_end)
    next_scope_type, next_scope_value = _validate_goal_scope(payload.scope_type or goal.scope_type, payload.scope_value if payload.scope_type or payload.scope_value is not None else goal.scope_value)
    if payload.name is not None:
        goal.name = payload.name.strip()
    goal.period_start = next_period_start
    goal.period_end = next_period_end
    if payload.target_certified_assets is not None:
        goal.target_certified_assets = payload.target_certified_assets
    if payload.target_eligible_assets is not None:
        goal.target_eligible_assets = payload.target_eligible_assets
    if payload.target_reviewed_assets is not None:
        goal.target_reviewed_assets = payload.target_reviewed_assets
    if payload.target_revalidated_assets is not None:
        goal.target_revalidated_assets = payload.target_revalidated_assets
    goal.scope_type = next_scope_type
    goal.scope_value = next_scope_value
    if payload.owner is not None:
        goal.owner = payload.owner.strip() or None
    if payload.status is not None:
        status_value = payload.status.strip().lower()
        if status_value not in CERTIFICATION_GOAL_STATUSES:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Status da meta de certificação inválido.")
        goal.status = status_value
    if payload.notes is not None:
        goal.notes = payload.notes.strip() or None
    db.commit()
    db.refresh(goal)
    write_audit_log_sync(
        db,
        action="certification.goal.update",
        entity_type="certification_goal",
        entity_id=goal.id,
        before=before,
        after=serialize_model(goal),
        metadata={"message": "Certification goal updated"},
        user_id=current_user.id,
    )
    db.commit()
    return CertificationGoalOut.model_validate(goal)


@router.delete("/goals/{goal_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_certification_goal(
    goal_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> None:
    goal = db.get(CertificationGoal, goal_id)
    if not goal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Meta de certificação não encontrada.")
    before = serialize_model(goal)
    db.delete(goal)
    db.commit()
    write_audit_log_sync(
        db,
        action="certification.goal.delete",
        entity_type="certification_goal",
        entity_id=goal_id,
        before=before,
        metadata={"message": "Certification goal deleted"},
        user_id=current_user.id,
    )
    db.commit()
    return None


@router.get("/goals/{goal_id}/progress", response_model=CertificationGoalProgressOut)
def get_certification_goal_progress(
    goal_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> CertificationGoalProgressOut:
    goal = db.get(CertificationGoal, goal_id)
    if not goal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Meta de certificação não encontrada.")
    scope_filters = _goal_scope_filters(goal)
    summaries = _build_visible_certification_summaries(
        db=db,
        current_user=current_user,
        certification_criticality=scope_filters.get("certification_criticality"),  # type: ignore[arg-type]
        owner_id=scope_filters.get("owner_id"),  # type: ignore[arg-type]
        schema_name=scope_filters.get("schema_name"),  # type: ignore[arg-type]
        database_name=scope_filters.get("database_name"),  # type: ignore[arg-type]
        datasource_name=scope_filters.get("datasource_name"),  # type: ignore[arg-type]
        sort_by="updated_at",
        sort_dir="desc",
    )
    event_query = select(CertificationDecisionEvent).where(
        CertificationDecisionEvent.asset_id.in_([item.id for item in summaries] or [-1]),
        CertificationDecisionEvent.created_at >= datetime.combine(goal.period_start, datetime.min.time(), tzinfo=timezone.utc),
        CertificationDecisionEvent.created_at <= datetime.combine(goal.period_end, datetime.max.time(), tzinfo=timezone.utc),
    )
    scoped_events = db.scalars(event_query.order_by(CertificationDecisionEvent.created_at.asc(), CertificationDecisionEvent.id.asc())).all()
    if scoped_events:
        return _build_goal_progress_from_events(goal=goal, summaries=summaries, events=scoped_events)
    return _build_goal_progress(goal=goal, summaries=summaries)


@router.get("/assets/{asset_id}/events", response_model=CertificationDecisionEventPageOut)
def list_certification_asset_events(
    asset_id: int,
    decision_type: str | None = Query(default=None),
    reviewer: str | None = Query(default=None),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> CertificationDecisionEventPageOut:
    return _resolve_event_page(
        db=db,
        current_user=current_user,
        asset_id=asset_id,
        decision_type=decision_type,
        reviewer=reviewer,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )


@router.get("/events", response_model=CertificationDecisionEventPageOut)
def list_certification_events(
    certification_status: str | None = Query(default=None),
    decision_type: str | None = Query(default=None),
    decision_source: str | None = Query(default=None),
    reviewer: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    table_name: str | None = Query(default=None),
    goal_id: int | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    certification_criticality: str | None = Query(default=None),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> CertificationDecisionEventPageOut:
    return _resolve_event_page(
        db=db,
        current_user=current_user,
        certification_status=certification_status,
        decision_type=decision_type,
        decision_source=decision_source,
        reviewer=reviewer,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        goal_id=goal_id,
        owner_id=owner_id,
        certification_criticality=certification_criticality,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )


@router.get("/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_certification_csv(
    request: Request,
    q: str | None = Query(default=None),
    certification_status: str | None = Query(default=None),
    certification_criticality: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    datasource_name: str | None = Query(default=None),
    sort_by: str = Query(default="updated_at"),
    sort_dir: str = Query(default="desc"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("certification:export")),
) -> StreamingResponse:
    job = enqueue_export_job(
        db,
        job_type="certification.queue.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "q": q,
            "certification_status": certification_status,
            "certification_criticality": certification_criticality,
            "owner_id": owner_id,
            "schema_name": schema_name,
            "database_name": database_name,
            "datasource_name": datasource_name,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "export_format": "csv",
        },
        context_json={
            "filters": {
                "q": q,
                "certification_status": certification_status,
                "certification_criticality": certification_criticality,
                "owner_id": owner_id,
                "schema_name": schema_name,
                "database_name": database_name,
                "datasource_name": datasource_name,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            }
        },
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="certification.export_requested",
        entity_type="certification_queue",
        source_module="certification",
        export_format="csv",
        filters={
            "q": q,
            "certification_status": certification_status,
            "certification_criticality": certification_criticality,
            "owner_id": owner_id,
            "schema_name": schema_name,
            "database_name": database_name,
            "datasource_name": datasource_name,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        },
    )
    return serialize_export_job(job, request=request)
    summaries = _build_visible_certification_summaries(
        db=db,
        current_user=current_user,
        q=q,
        certification_status=certification_status,
        certification_criticality=certification_criticality,
        owner_id=owner_id,
        schema_name=schema_name,
        database_name=database_name,
        datasource_name=datasource_name,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    export_limit = resolve_export_limit(source_module="certification", entity_type="certification_queue")
    summaries, truncated = enforce_export_limit(summaries, limit=export_limit)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="certification.export_csv",
        entity_type="certification_queue",
        source_module="certification",
        row_count=len(summaries),
        truncated=truncated,
        limit=export_limit,
        filters={
            "q": q,
            "certification_status": certification_status,
            "certification_criticality": certification_criticality,
            "owner_id": owner_id,
            "schema_name": schema_name,
            "database_name": database_name,
            "datasource_name": datasource_name,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        },
    )
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "banco",
            "schema",
            "tabela",
            "status",
            "prontidao",
            "owner",
            "criticidade",
            "criterios_atendidos",
            "criterios_totais",
            "criterios_pendentes",
            "principal_bloqueio",
            "proximo_passo",
            "ultima_revisao",
            "observacao",
        ]
    )
    for item in summaries:
        pending = _pending_criteria(item)
        writer.writerow(
            [
                item.database_name,
                item.schema_name,
                item.name,
                item.certification_status_label,
                item.readiness_score,
                redact_export_value(item.data_owner.name if item.data_owner else (item.owner or ""), field_name="owner"),
                item.certification_criticality or "",
                item.readiness_completed,
                item.readiness_total,
                len(pending),
                str(pending[0].get("label")) if pending else "",
                item.certification_next_step or "",
                item.certification_decided_at.isoformat() if item.certification_decided_at else "",
                redact_export_value(item.certification_notes, field_name="certification_notes"),
            ]
        )
    return StreamingResponse(
        iter([buffer.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="certification_queue.csv"'},
    )


@router.get("/events/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_certification_events_csv(
    request: Request,
    certification_status: str | None = Query(default=None),
    decision_type: str | None = Query(default=None),
    decision_source: str | None = Query(default=None),
    reviewer: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    table_name: str | None = Query(default=None),
    goal_id: int | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    certification_criticality: str | None = Query(default=None),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("certification:export")),
) -> StreamingResponse:
    job = enqueue_export_job(
        db,
        job_type="certification.events.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "certification_status": certification_status,
            "decision_type": decision_type,
            "decision_source": decision_source,
            "reviewer": reviewer,
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "goal_id": goal_id,
            "owner_id": owner_id,
            "certification_criticality": certification_criticality,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "export_format": "csv",
        },
        context_json={
            "filters": {
                "certification_status": certification_status,
                "decision_type": decision_type,
                "decision_source": decision_source,
                "reviewer": reviewer,
                "database_name": database_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "goal_id": goal_id,
                "owner_id": owner_id,
                "certification_criticality": certification_criticality,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None,
            }
        },
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="certification.events.export_requested",
        entity_type="certification_event",
        source_module="certification",
        export_format="csv",
        filters={
            "certification_status": certification_status,
            "decision_type": decision_type,
            "decision_source": decision_source,
            "reviewer": reviewer,
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "goal_id": goal_id,
            "owner_id": owner_id,
            "certification_criticality": certification_criticality,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
        },
    )
    return serialize_export_job(job, request=request)
    export_limit = resolve_export_limit(source_module="certification", entity_type="certification_event")
    page = _resolve_event_page(
        db=db,
        current_user=current_user,
        certification_status=certification_status,
        decision_type=decision_type,
        decision_source=decision_source,
        reviewer=reviewer,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        goal_id=goal_id,
        owner_id=owner_id,
        certification_criticality=certification_criticality,
        date_from=date_from,
        date_to=date_to,
        page=1,
        page_size=export_limit,
    )
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="certification.events.export_csv",
        entity_type="certification_event",
        source_module="certification",
        row_count=len(page.items),
        truncated=page.total > len(page.items),
        limit=export_limit,
        filters={
            "certification_status": certification_status,
            "decision_type": decision_type,
            "decision_source": decision_source,
            "reviewer": reviewer,
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "goal_id": goal_id,
            "owner_id": owner_id,
            "certification_criticality": certification_criticality,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
        },
    )
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "data",
            "ativo",
            "banco",
            "schema",
            "tabela",
            "tipo_decisao",
            "status_anterior",
            "novo_status",
            "prontidao_anterior",
            "prontidao_atual",
            "responsavel",
            "observacao",
            "validade",
            "origem",
        ]
    )
    for item in page.items:
        writer.writerow(
            [
                item.created_at.isoformat(),
                item.asset_name,
                item.database_name,
                item.schema_name,
                item.table_name,
                item.decision_type,
                item.previous_status or "",
                item.new_status,
                item.previous_readiness if item.previous_readiness is not None else "",
                item.new_readiness if item.new_readiness is not None else "",
                item.reviewer or item.reviewer_email or "",
                redact_export_value(item.observation, field_name="observation"),
                item.valid_until.isoformat() if item.valid_until else "",
                item.decision_source,
            ]
        )
    return StreamingResponse(
        iter([buffer.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="certification_events.csv"'},
    )


@router.patch("/tables/{table_id}", response_model=TableCertificationSummaryOut)
def patch_table_certification(
    table_id: int,
    payload: TableCertificationPatch,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> TableCertificationSummaryOut:
    table = get_table_certification_or_404(db, table_id)
    if not can_view_table(user, table):
        record_abac_denial(
            db,
            request=request,
            current_user=user,
            action="update",
            table=table,
            reason="certification_table_visibility_denied",
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    settings_snapshot = get_governance_settings_snapshot(db)
    validate_certification_patch(db, table=table, payload=payload)
    badges = payload.certification_badges or []

    before = {
        "certification_status": table.certification_status,
        "certification_criticality": table.certification_criticality,
        "certification_badges": table.certification_badges,
        "certification_notes": table.certification_notes,
        "certification_submitted_by_user_id": table.certification_submitted_by_user_id,
        "certification_submitted_at": table.certification_submitted_at.isoformat() if table.certification_submitted_at else None,
        "certification_decided_by_user_id": table.certification_decided_by_user_id,
        "certification_decided_at": table.certification_decided_at.isoformat() if table.certification_decided_at else None,
        "certification_review_at": table.certification_review_at.isoformat() if table.certification_review_at else None,
        "certification_expires_at": table.certification_expires_at.isoformat() if table.certification_expires_at else None,
    }

    before_summary = build_certification_summary_out(db, table, settings_snapshot=settings_snapshot)
    previous_status = before_summary.certification_status
    target_status = payload.certification_status
    if target_status == "in_review" and not payload.certification_review_at:
        payload = TableCertificationPatch(
            certification_status=payload.certification_status,
            certification_criticality=payload.certification_criticality,
            certification_badges=payload.certification_badges,
            certification_notes=payload.certification_notes,
            certification_review_at=datetime.now(timezone.utc) + timedelta(days=settings_snapshot.certification_review_sla_days),
            certification_expires_at=payload.certification_expires_at,
        )
    table.certification_status = target_status
    table.certification_criticality = payload.certification_criticality
    table.certification_badges = badges or None
    table.certification_notes = payload.certification_notes.strip() if payload.certification_notes else None
    table.certification_review_at = payload.certification_review_at
    table.certification_expires_at = payload.certification_expires_at
    now = datetime.now(timezone.utc)
    if target_status == "certified":
        if table.certification_review_at is None:
            table.certification_review_at = now + timedelta(days=settings_snapshot.certification_review_interval_days)
        if table.certification_expires_at is None:
            table.certification_expires_at = table.certification_review_at + timedelta(
                days=settings_snapshot.certification_revalidation_window_days
            )
    if target_status == "in_review":
        table.certification_submitted_by_user_id = user.id
        table.certification_submitted_at = now
    if target_status in {"certified", "rejected", "expired", "revalidation_pending"}:
        table.certification_decided_by_user_id = user.id
        table.certification_decided_at = now
    elif previous_status != target_status:
        table.certification_decided_by_user_id = None
        table.certification_decided_at = None

    db.flush()

    after = {
        "certification_status": table.certification_status,
        "certification_criticality": table.certification_criticality,
        "certification_badges": table.certification_badges,
        "certification_notes": table.certification_notes,
        "certification_submitted_by_user_id": table.certification_submitted_by_user_id,
        "certification_submitted_at": table.certification_submitted_at.isoformat() if table.certification_submitted_at else None,
        "certification_decided_by_user_id": table.certification_decided_by_user_id,
        "certification_decided_at": table.certification_decided_at.isoformat() if table.certification_decided_at else None,
        "certification_review_at": table.certification_review_at.isoformat() if table.certification_review_at else None,
        "certification_expires_at": table.certification_expires_at.isoformat() if table.certification_expires_at else None,
    }

    changes = certification_changes(before=before, after=after)
    if changes:
        log_field_changes(
            db,
            action="table.certification.patch",
            entity_type="table",
            entity_id=table.id,
            changes=changes,
            source_module="certification",
            metadata={"message": "Table certification updated"},
            audit_kwargs=request_audit_kwargs(request, user),
            actor_user_id=user.id,
        )

    summary = build_certification_summary_out(db, table, settings_snapshot=settings_snapshot)
    _record_certification_event(
        db=db,
        table=table,
        user=user,
        before_summary=before_summary,
        after_summary=summary,
        previous_status=previous_status,
        new_status=summary.certification_status,
        notes=table.certification_notes,
    )
    db.commit()
    db.refresh(table)
    summary = build_certification_summary_out(db, table, settings_snapshot=settings_snapshot)
    if table_visibility_decision_from_entity(table, user=user).masked or not can_view_sensitive_data(user, table=table):
        summary = _mask_certification_summary(summary, current_user=user)
    return summary


@router.post("/tables/{table_id}/submit", response_model=TableCertificationSummaryOut)
def submit_table_certification(
    table_id: int,
    payload: TableCertificationSubmitIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> TableCertificationSummaryOut:
    patch = TableCertificationPatch(
        certification_status="in_review",
        certification_criticality=None,
        certification_badges=None,
        certification_notes=payload.certification_notes,
        certification_review_at=payload.certification_review_at,
        certification_expires_at=payload.certification_expires_at,
    )
    return patch_table_certification(table_id, patch, request, db, user)


@router.post("/tables/{table_id}/decision", response_model=TableCertificationSummaryOut)
def decide_table_certification(
    table_id: int,
    payload: TableCertificationDecisionIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> TableCertificationSummaryOut:
    decision = (payload.decision or "").strip().lower()
    if decision not in {"certified", "rejected", "expired", "revalidation_pending"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Decisão de certificação inválida.")
    patch = TableCertificationPatch(
        certification_status=decision,
        certification_criticality=payload.certification_criticality,
        certification_badges=payload.certification_badges,
        certification_notes=payload.certification_notes,
        certification_review_at=payload.certification_review_at,
        certification_expires_at=payload.certification_expires_at,
    )
    return patch_table_certification(table_id, patch, request, db, user)
