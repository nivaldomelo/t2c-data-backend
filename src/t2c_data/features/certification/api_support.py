from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging

from fastapi import HTTPException, status
from sqlalchemy import asc, desc, func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRule, DQRuleRun, DQRun, DQTableMetric
from t2c_data.models.glossary import GlossaryAssignment
from t2c_data.models.incident import Incident
from t2c_data.models.tag import TagAssignment
from t2c_data.features.governance.rules import owner_review_due, privacy_review_due
from t2c_data.features.governance.settings import GovernanceSettingsSnapshot, get_governance_settings_snapshot
from t2c_data.features.ingestion import IngestionIntegrationUnavailable, load_table_ingestion_detail, operational_session_for_datasource
from t2c_data.schemas.catalog import TableCertificationPatch, TableCertificationSummaryOut

CERTIFICATION_STATUSES = {
    "not_eligible",
    "eligible",
    "in_review",
    "certified",
    "rejected",
    "expired",
    "revalidation_pending",
}
CERTIFICATION_CRITICALITIES = {"low", "medium", "high", "critical"}
CERTIFICATION_BADGES = {"internal_use", "official_use", "restricted_sensitive"}
CERTIFICATION_STATUS_LABELS = {
    "not_assessed": "Não elegível",
    "not_eligible": "Não elegível",
    "eligible": "Elegível",
    "in_review": "Em revisão",
    "certified": "Certificado",
    "rejected": "Recusado",
    "expired": "Vencido",
    "revalidation_pending": "Pendente de revalidação",
}


@dataclass(frozen=True)
class CertificationEvaluation:
    readiness_score: int
    readiness_completed: int
    readiness_total: int
    eligible_for_certification: bool
    certified_for_certification: bool
    active_dq_violation: bool
    active_dq_violation_count: int
    active_dq_rule_names: list[str]
    certification_status: str
    certification_status_label: str
    certification_status_source: str
    certification_status_rule: str
    certification_status_reason: str
    certification_revalidation_required: bool
    certification_next_step: str
WORKFLOW_TRANSITIONS = {
    "not_eligible": {"eligible", "in_review", "rejected"},
    "eligible": {"not_eligible", "in_review", "certified", "rejected"},
    "in_review": {"eligible", "certified", "rejected", "revalidation_pending"},
    "certified": {"in_review", "revalidation_pending", "expired", "rejected"},
    "rejected": {"not_eligible", "eligible", "in_review"},
    "expired": {"in_review", "certified", "rejected"},
    "revalidation_pending": {"in_review", "certified", "rejected", "expired"},
}

logger = logging.getLogger(__name__)


def build_table_certification_query():
    return (
        select(TableEntity)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .options(
            selectinload(TableEntity.columns),
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.certification_submitted_by_user),
            selectinload(TableEntity.certification_decided_by_user),
            selectinload(TableEntity.owner_reviewed_by_user),
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
    )


def get_table_certification_or_404(db: Session, table_id: int) -> TableEntity:
    table = db.scalar(build_table_certification_query().where(TableEntity.id == table_id))
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return table


def _table_fqn(table: TableEntity) -> str:
    return f"{table.schema.name}.{table.name}"


def _user_display_name(user) -> str | None:
    if not user:
        return None
    return user.name or user.full_name


def _user_display_email(user) -> str | None:
    if not user:
        return None
    return user.email


def _latest_dq_score(db: Session, table_id: int) -> float | None:
    row = db.execute(
        select(DQTableMetric.dq_score)
        .join(DQRun, DQTableMetric.run_id == DQRun.id)
        .where(DQTableMetric.table_id == table_id, DQRun.status == "success")
        .order_by(DQRun.finished_at.desc().nullslast(), DQRun.id.desc())
        .limit(1)
    ).first()
    if not row:
        return None
    return float(row[0]) if row[0] is not None else None


def _has_open_critical_incident(db: Session, table: TableEntity) -> bool:
    count = db.scalar(
        select(func.count(Incident.id)).where(
            Incident.entity_type == "table",
            Incident.table_fqn == _table_fqn(table),
            Incident.severity == "sev1",
            Incident.status.in_(["open", "investigating"]),
        )
    )
    return bool(count)


def _count_tags(db: Session, table_id: int) -> int:
    return int(
        db.scalar(
            select(func.count(TagAssignment.id)).where(
                TagAssignment.entity_type == "table",
                TagAssignment.entity_id == table_id,
            )
        )
        or 0
    )


def _count_terms(db: Session, table_id: int) -> int:
    return int(
        db.scalar(
            select(func.count(GlossaryAssignment.id)).where(
                GlossaryAssignment.entity_type == "table",
                GlossaryAssignment.entity_id == table_id,
            )
        )
        or 0
    )


def _table_description_complete(table: TableEntity) -> bool:
    return bool(((table.description_manual or table.description_source) or "").strip())


def _column_documentation_coverage(table: TableEntity) -> tuple[int, int, int]:
    if not table.columns:
        return 0, 0, 0
    documented = 0
    for column in table.columns:
        if (
            (column.dictionary_description or "").strip()
            or (column.description_manual or "").strip()
            or (column.description_source or "").strip()
        ):
            documented += 1
    total = len(table.columns)
    pct = int(round((documented / total) * 100)) if total else 0
    return documented, total, pct


def _minimum_documentation_complete(table: TableEntity, *, minimum_pct: int = 80) -> bool:
    _documented, _total, pct = _column_documentation_coverage(table)
    return pct >= minimum_pct


def _review_is_recent(table: TableEntity) -> bool:
    if not table.certification_decided_at and not table.certification_review_at:
        return False
    threshold = datetime.now(timezone.utc) - timedelta(days=90)
    candidates = [candidate for candidate in [table.certification_decided_at, table.certification_review_at] if candidate]
    if not candidates:
        return False
    latest = max(candidate.replace(tzinfo=timezone.utc) if candidate.tzinfo is None else candidate for candidate in candidates)
    return latest >= threshold


def _active_dq_violation_summary(db: Session, table: TableEntity) -> tuple[bool, int, list[str]]:
    rule_rows = db.execute(
        select(DQRule.id, DQRule.name)
        .where(DQRule.table_id == table.id, DQRule.is_active.is_(True))
    ).all()
    if not rule_rows:
        return False, 0, []

    latest_runs_sq = (
        select(
            DQRuleRun.rule_id.label("rule_id"),
            DQRuleRun.status.label("status"),
            DQRuleRun.violations_count.label("violations_count"),
            DQRule.name.label("rule_name"),
            func.row_number()
            .over(
                partition_by=DQRuleRun.rule_id,
                order_by=(DQRuleRun.created_at.desc(), DQRuleRun.id.desc()),
            )
            .label("rn"),
        )
        .join(DQRule, DQRuleRun.rule_id == DQRule.id)
        .where(DQRule.table_id == table.id, DQRule.is_active.is_(True))
        .subquery()
    )

    rows = db.execute(
        select(
            latest_runs_sq.c.rule_id,
            latest_runs_sq.c.status,
            latest_runs_sq.c.violations_count,
            latest_runs_sq.c.rule_name,
        ).where(latest_runs_sq.c.rn == 1)
    ).all()

    active_rule_names = [
        str(row.rule_name)
        for row in rows
        if str(row.status).lower() == "fail" and int(row.violations_count or 0) > 0
    ]
    open_dq_incidents = int(
        db.scalar(
            select(func.count(Incident.id)).where(
                Incident.entity_type == "table",
                Incident.table_fqn == f"{table.schema.name}.{table.name}",
                Incident.source_type == "dq_rule",
                Incident.status.in_(["open", "investigating"]),
            )
        )
        or 0
    )
    active = bool(active_rule_names or open_dq_incidents > 0)
    return active, open_dq_incidents, active_rule_names


def _certification_status_reason(
    *,
    status: str,
    readiness_score: int,
    active_dq_violation: bool,
    active_dq_rule_names: list[str] | None,
    critical_open_incidents: int,
    review_due: bool,
    operational_revalidation_required: bool,
) -> str:
    rule_names = active_dq_rule_names or []
    if status == "in_review":
        return "Decisão manual em revisão."
    if status == "rejected":
        return "Decisão manual recusada."
    if status == "expired":
        return "A certificação venceu e precisa ser revalidada."
    if status == "revalidation_pending":
        if active_dq_violation:
            if rule_names:
                return f"Violação ativa de Data Quality em {', '.join(rule_names[:3])}."
            return "Violação ativa de Data Quality."
        if critical_open_incidents > 0:
            return "Existe incidente crítico aberto e a certificação precisa ser revalidada."
        if operational_revalidation_required:
            return "A estabilidade operacional indica reavaliação obrigatória da certificação."
        if readiness_score >= 70:
            return "A certificação foi colocada em reavaliação preventiva por oscilação relevante de prontidão."
        return "A certificação requer revalidação."
    if status == "certified":
        return "Certificada automaticamente por prontidão >= 80% e sem sinais de degradação crítica."
    if status == "eligible":
        return "Elegível automaticamente por prontidão >= 50%."
    if active_dq_violation:
        if rule_names:
            return f"Há violação ativa de Data Quality em {', '.join(rule_names[:3])}."
        return "Há violação ativa de Data Quality."
    if readiness_score >= 50:
        return "Prontidão >= 50%, aguardando critérios para certificação."
    return "Prontidão abaixo do patamar mínimo para certificação."


def _certification_status_rule(
    *,
    status: str,
    readiness_score: int,
    active_dq_violation: bool,
    critical_open_incidents: int,
    review_due: bool,
    operational_revalidation_required: bool,
) -> str:
    if status == "in_review":
        return "manual_in_review"
    if status == "rejected":
        return "manual_rejected"
    if status == "expired":
        return "automatic_expired"
    if status == "revalidation_pending":
        if active_dq_violation:
            return "automatic_dq_revalidation"
        if critical_open_incidents > 0:
            return "automatic_incident_revalidation"
        if operational_revalidation_required:
            return "automatic_operational_revalidation"
        if readiness_score >= 70:
            return "automatic_certified_hysteresis_revalidation"
        return "automatic_revalidation"
    if readiness_score >= 80 and not active_dq_violation:
        return "automatic_readiness_certified"
    if readiness_score >= 50:
        return "automatic_readiness_eligible"
    return "automatic_readiness_not_eligible"


def _build_certification_evaluation(
    table: TableEntity,
    *,
    readiness_score: int,
    readiness_completed: int,
    readiness_total: int,
    active_dq_violation: bool = False,
    active_dq_violation_count: int = 0,
    active_dq_rule_names: list[str] | None = None,
    critical_open_incidents: int = 0,
    operational_revalidation_required: bool = False,
    now: datetime | None = None,
) -> CertificationEvaluation:
    now = now or datetime.now(timezone.utc)
    current = _normalized_workflow_status(table.certification_status)
    active_dq_rule_names = active_dq_rule_names or []
    eligible_for_certification = readiness_score >= 50
    certified_for_certification = readiness_score >= 80

    review_at = getattr(table, "certification_review_at", None)
    expires_at = getattr(table, "certification_expires_at", None)
    review_due = False
    expired = False
    if expires_at is not None:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        expired = expires_at <= now
    if review_at is not None:
        if review_at.tzinfo is None:
            review_at = review_at.replace(tzinfo=timezone.utc)
        review_due = review_at <= now

    status = current
    source = "automatic"
    if current == "in_review":
        status = "in_review"
        source = "manual"
    elif current == "rejected":
        status = "rejected"
        source = "manual"
    elif current == "certified":
        if expired:
            status = "expired"
        elif active_dq_violation or critical_open_incidents > 0 or operational_revalidation_required:
            status = "revalidation_pending"
        elif certified_for_certification and readiness_score >= 70:
            status = "certified"
        else:
            status = "revalidation_pending"
    elif current == "revalidation_pending":
        if certified_for_certification and not active_dq_violation and critical_open_incidents == 0 and not operational_revalidation_required:
            status = "certified"
        else:
            status = "revalidation_pending"
    else:
        if certified_for_certification and not active_dq_violation:
            status = "certified"
        elif eligible_for_certification:
            status = "eligible"
        else:
            status = "not_eligible"

    if status in {"revalidation_pending", "expired"}:
        source = "automatic"
    elif status in {"certified", "eligible", "not_eligible"} and current not in {"in_review", "rejected"}:
        source = "automatic"

    status_rule = _certification_status_rule(
        status=status,
        readiness_score=readiness_score,
        active_dq_violation=active_dq_violation,
        critical_open_incidents=critical_open_incidents,
        review_due=review_due,
        operational_revalidation_required=operational_revalidation_required,
    )
    status_reason = _certification_status_reason(
        status=status,
        readiness_score=readiness_score,
        active_dq_violation=active_dq_violation,
        active_dq_rule_names=active_dq_rule_names,
        critical_open_incidents=critical_open_incidents,
        review_due=review_due,
        operational_revalidation_required=operational_revalidation_required,
    )
    revalidation_required = status in {"revalidation_pending", "expired"}
    next_step: str
    if status == "in_review":
        next_step = "Concluir decisão de certificação"
    elif status == "eligible":
        next_step = "Decisão automática aplicada"
    elif status == "not_eligible":
        next_step = "Completar critérios de prontidão"
    elif status == "rejected":
        next_step = "Corrigir gaps e reenviar para revisão"
    elif status in {"revalidation_pending", "expired"}:
        next_step = "Corrigir bloqueios e iniciar revalidação"
    else:
        next_step = "Monitorar validade e sinais operacionais"

    return CertificationEvaluation(
        readiness_score=readiness_score,
        readiness_completed=readiness_completed,
        readiness_total=readiness_total,
        eligible_for_certification=eligible_for_certification,
        certified_for_certification=certified_for_certification,
        active_dq_violation=active_dq_violation,
        active_dq_violation_count=active_dq_violation_count,
        active_dq_rule_names=active_dq_rule_names,
        certification_status=status,
        certification_status_label=certification_status_label(status),
        certification_status_source=source,
        certification_status_rule=status_rule,
        certification_status_reason=status_reason,
        certification_revalidation_required=revalidation_required,
        certification_next_step=next_step,
    )


def build_certification_evaluation(
    db: Session,
    table: TableEntity,
    *,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
    now: datetime | None = None,
) -> CertificationEvaluation:
    checklist, completed, _eligible = build_certification_checklist(db, table)
    total = len(checklist)
    score = int(round((completed / total) * 100)) if total else 0
    settings_snapshot = settings_snapshot or get_governance_settings_snapshot(db)
    active_dq_violation, active_dq_violation_count, active_dq_rule_names = _active_dq_violation_summary(db, table)
    operational_revalidation_required = _operational_revalidation_required(table, settings_snapshot=settings_snapshot)
    return _build_certification_evaluation(
        table,
        readiness_score=score,
        readiness_completed=completed,
        readiness_total=total,
        active_dq_violation=active_dq_violation,
        active_dq_violation_count=active_dq_violation_count,
        active_dq_rule_names=active_dq_rule_names,
        critical_open_incidents=int(getattr(table, "critical_open_incidents", 0) or 0),
        operational_revalidation_required=operational_revalidation_required,
        now=now,
    )


def certification_status_label(status: str | None) -> str:
    normalized = (status or "not_eligible").strip() or "not_eligible"
    return CERTIFICATION_STATUS_LABELS.get(normalized, normalized)


def _normalized_workflow_status(status: str | None) -> str:
    normalized = (status or "not_eligible").strip() or "not_eligible"
    if normalized == "not_assessed":
        return "not_eligible"
    return normalized


def resolve_certification_status(
    table: TableEntity,
    *,
    readiness_score: int | None = None,
    eligible: bool | None = None,
    active_dq_violation: bool = False,
    active_dq_violation_count: int = 0,
    active_dq_rule_names: list[str] | None = None,
    critical_open_incidents: int = 0,
    now: datetime | None = None,
    operational_revalidation_required: bool = False,
) -> str:
    current = _normalized_workflow_status(getattr(table, "certification_status", None))
    if readiness_score is None:
        readiness_score = 100 if bool(eligible) else 0
    eligible_flag = readiness_score >= 50
    certified_flag = readiness_score >= 80
    now = now or datetime.now(timezone.utc)
    review_at = getattr(table, "certification_review_at", None)
    expires_at = getattr(table, "certification_expires_at", None)

    if expires_at is not None:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now and current in {"certified", "revalidation_pending", "expired"}:
            return "expired"
    if current == "certified":
        if active_dq_violation or critical_open_incidents > 0 or operational_revalidation_required:
            return "revalidation_pending"
        if certified_flag and readiness_score >= 70:
            return "certified"
        return "revalidation_pending"
    if current == "revalidation_pending":
        if certified_flag and not active_dq_violation and critical_open_incidents == 0 and not operational_revalidation_required:
            return "certified"
        return "revalidation_pending"
    if active_dq_violation and current in {"certified", "revalidation_pending"}:
        return "revalidation_pending"

    if current in {"in_review", "rejected"}:
        return current

    if current == "certified":
        if certified_flag and not active_dq_violation:
            return "certified"
        if eligible_flag:
            return "eligible"
        return "not_eligible"

    if certified_flag and not active_dq_violation:
        return "certified"
    if eligible_flag:
        return "eligible"
    return "not_eligible"


def resolve_certification_status_for_profile(
    table,
    *,
    now: datetime | None = None,
) -> str:
    return resolve_certification_status(
        table,
        readiness_score=getattr(table, "readiness_score", None),
        eligible=getattr(table, "eligible_for_certification", None),
        active_dq_violation=bool(getattr(table, "active_dq_violation", False)),
        active_dq_violation_count=int(getattr(table, "active_dq_violation_count", 0) or 0),
        active_dq_rule_names=list(getattr(table, "active_dq_rule_names", []) or []),
        critical_open_incidents=int(getattr(table, "critical_open_incidents", 0) or 0),
        now=now,
        operational_revalidation_required=bool(getattr(table, "operational_revalidation_required", False)),
    )


def certification_review_due(
    table: TableEntity,
    *,
    readiness_score: int | None = None,
    eligible: bool | None = None,
    now: datetime | None = None,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> bool:
    return resolve_certification_status(table, readiness_score=readiness_score, eligible=eligible, now=now) in {
        "expired",
        "revalidation_pending",
    }


def _operational_revalidation_required(
    table: TableEntity,
    *,
    settings_snapshot: GovernanceSettingsSnapshot,
) -> bool:
    try:
        datasource = table.schema.database.datasource
        with operational_session_for_datasource(datasource) as operational_db:
            detail = load_table_ingestion_detail(
                operational_db,
                schema_name=table.schema.name,
                table_name=table.name,
                page=1,
                page_size=8,
                airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
            )
    except IngestionIntegrationUnavailable:
        return False
    except Exception as exc:
        logger.warning(
            "certification_operational_revalidation_unavailable table_id=%s schema=%s table=%s error=%s",
            table.id,
            table.schema.name,
            table.name,
            exc,
            exc_info=True,
        )
        return False

    stability = detail.get("stability")
    return bool(isinstance(stability, dict) and stability.get("recurrent_degradation"))


def _sla_status_label(value: str) -> str:
    return {
        "on_track": "Dentro do SLA",
        "due_soon": "SLA próximo do vencimento",
        "overdue": "SLA vencido",
        "not_applicable": "Sem SLA ativo",
    }.get(value, "Sem SLA ativo")


def certification_workflow_guidance(
    table: TableEntity,
    *,
    eligible: bool,
    readiness_score: int | None = None,
    now: datetime | None = None,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
    active_dq_violation: bool = False,
    active_dq_rule_names: list[str] | None = None,
    critical_open_incidents: int = 0,
    operational_revalidation_required: bool = False,
) -> dict[str, object]:
    now = now or datetime.now(timezone.utc)
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    status = resolve_certification_status(
        table,
        readiness_score=readiness_score,
        eligible=eligible,
        now=now,
        active_dq_violation=active_dq_violation,
        active_dq_rule_names=active_dq_rule_names,
        critical_open_incidents=critical_open_incidents,
        operational_revalidation_required=operational_revalidation_required,
    )
    sla_due_at = None
    sla_status = "not_applicable"
    revalidation_required = status in {"revalidation_pending", "expired"}
    next_step: str | None

    if status == "in_review":
        anchor = table.certification_submitted_at or table.updated_at or now
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)
        sla_due_at = anchor + timedelta(days=settings_snapshot.certification_review_sla_days)
        if sla_due_at <= now:
            sla_status = "overdue"
        elif sla_due_at <= now + timedelta(days=2):
            sla_status = "due_soon"
        else:
            sla_status = "on_track"
        next_step = "Concluir decisão de certificação"
    elif status == "eligible":
        next_step = "Decisão automática aplicada"
    elif status == "not_eligible":
        next_step = "Completar critérios de elegibilidade"
    elif status == "rejected":
        next_step = "Corrigir gaps e reenviar para revisão"
    elif status in {"revalidation_pending", "expired"}:
        base_due = table.certification_review_at or table.certification_expires_at or table.certification_decided_at
        if base_due is not None:
            if base_due.tzinfo is None:
                base_due = base_due.replace(tzinfo=timezone.utc)
            sla_due_at = base_due
            if sla_due_at <= now:
                sla_status = "overdue"
            elif sla_due_at <= now + timedelta(days=7):
                sla_status = "due_soon"
            else:
                sla_status = "on_track"
        next_step = "Corrigir violação ativa de Data Quality"
        revalidation_required = True
    else:
        upcoming_revalidation = table.certification_review_at or table.certification_expires_at
        if upcoming_revalidation is not None:
            if upcoming_revalidation.tzinfo is None:
                upcoming_revalidation = upcoming_revalidation.replace(tzinfo=timezone.utc)
            if upcoming_revalidation <= now + timedelta(days=settings_snapshot.certification_revalidation_window_days):
                sla_due_at = upcoming_revalidation
                if upcoming_revalidation <= now:
                    sla_status = "overdue"
                elif upcoming_revalidation <= now + timedelta(days=7):
                    sla_status = "due_soon"
                else:
                    sla_status = "on_track"
        next_step = "Monitorar validade da certificação"

    return {
        "certification_sla_due_at": sla_due_at,
        "certification_sla_status": sla_status,
        "certification_sla_label": _sla_status_label(sla_status),
        "certification_revalidation_required": revalidation_required,
        "certification_next_step": next_step,
    }


def validate_workflow_transition(
    *,
    current_status: str,
    target_status: str,
    eligible: bool,
    certifiable: bool,
) -> None:
    if target_status not in CERTIFICATION_STATUSES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid certification status")
    if current_status == target_status:
        return
    allowed = WORKFLOW_TRANSITIONS.get(current_status, set())
    if target_status not in allowed:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Transição inválida de {certification_status_label(current_status)} para {certification_status_label(target_status)}",
        )
    if target_status in {"eligible", "in_review", "certified"} and not eligible:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="O ativo ainda não atende aos critérios mínimos para avançar no workflow de certificação.",
        )
    if target_status == "certified" and not certifiable:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="O ativo ainda não atende à prontidão mínima de 80% para certificação.",
        )


def build_certification_checklist(db: Session, table: TableEntity) -> tuple[list[dict[str, str | bool]], int, bool]:
    dq_score = _latest_dq_score(db, table.id)
    has_critical_incident = _has_open_critical_incident(db, table)
    has_tags = _count_tags(db, table.id) > 0
    has_terms = _count_terms(db, table.id) > 0
    documentated_columns, total_columns, documentation_pct = _column_documentation_coverage(table)
    privacy_review_required = bool(table.has_personal_data or table.has_sensitive_personal_data)
    privacy_reviewed = bool(table.privacy_reviewed_at) if privacy_review_required else True
    privacy_context_complete = (
        bool((table.legal_basis or "").strip()) and bool((table.privacy_purpose or "").strip())
        if privacy_review_required
        else True
    )

    checks = [
        {
            "key": "owner_defined",
            "label": "Owner definido",
            "passed": bool(table.data_owner_id or (table.owner or "").strip()),
            "detail": table.data_owner.name if table.data_owner else (table.owner or "Owner pendente"),
        },
        {
            "key": "table_description_complete",
            "label": "Descrição da tabela",
            "passed": _table_description_complete(table),
            "detail": "Descrição pronta" if _table_description_complete(table) else "Descrição da tabela ausente ou insuficiente",
        },
        {
            "key": "documentation_coverage",
            "label": "Colunas documentadas >= 80%",
            "passed": total_columns > 0 and documentation_pct >= 80,
            "detail": f"{documentated_columns}/{total_columns} colunas documentadas" if total_columns else "Sem colunas para avaliar",
        },
        {
            "key": "tags_applied",
            "label": "Tags aplicadas",
            "passed": has_tags,
            "detail": "Tags vinculadas" if has_tags else "Sem tags associadas",
        },
        {
            "key": "terms_associated",
            "label": "Termos associados",
            "passed": has_terms,
            "detail": "Glossário conectado" if has_terms else "Sem termos associados",
        },
        {
            "key": "privacy_reviewed",
            "label": "Privacidade revisada quando aplicável",
            "passed": privacy_reviewed,
            "detail": (
                "Revisão de privacidade registrada"
                if privacy_reviewed
                else "Ativo com dado pessoal ou sensível ainda sem revisão formal de privacidade"
            ),
        },
        {
            "key": "privacy_context_complete",
            "label": "Base legal e finalidade registradas quando aplicável",
            "passed": privacy_context_complete,
            "detail": (
                "Base legal e finalidade estruturadas"
                if privacy_context_complete
                else "Ativo com dado pessoal ou sensível sem base legal ou finalidade registradas"
            ),
        },
        {
            "key": "dq_score",
            "label": "DQ score >= 90",
            "passed": dq_score is not None and dq_score >= 90,
            "detail": f"DQ {dq_score:.1f}" if dq_score is not None else "Sem métricas DQ",
        },
        {
            "key": "no_critical_incidents",
            "label": "Sem incidente crítico aberto",
            "passed": not has_critical_incident,
            "detail": "Sem incidentes críticos abertos" if not has_critical_incident else "Existe incidente crítico aberto",
        },
        {
            "key": "review_recent",
            "label": "Revisão realizada nos últimos 90 dias",
            "passed": _review_is_recent(table),
            "detail": table.certification_decided_at.isoformat() if table.certification_decided_at else "Sem revisão registrada",
        },
    ]
    completed = sum(1 for item in checks if item["passed"])
    eligible = (completed / len(checks) * 100 if checks else 0) >= 50
    return checks, completed, eligible


def _missing_certification_gates(checklist: list[dict[str, str | bool]]) -> list[dict[str, str | bool]]:
    return [item for item in checklist if not bool(item.get("passed"))]


def build_certification_summary_out(
    db: Session,
    table: TableEntity,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> TableCertificationSummaryOut:
    checklist, completed, eligible = build_certification_checklist(db, table)
    total = len(checklist)
    score = int(round((completed / total) * 100)) if total else 0
    now = datetime.now(timezone.utc)
    settings_snapshot = settings_snapshot or get_governance_settings_snapshot(db)
    from t2c_data.features.dashboard.profile_loader import load_table_profiles
    from t2c_data.features.governance.trust_score import build_trust_score_for_profile

    trust_profile = load_table_profiles(db, now, table_ids=[table.id])[0]
    trust_payload = build_trust_score_for_profile(trust_profile, settings_snapshot=settings_snapshot)
    active_dq_violation, active_dq_violation_count, active_dq_rule_names = _active_dq_violation_summary(db, table)
    operational_revalidation_required = _operational_revalidation_required(table, settings_snapshot=settings_snapshot)
    owner_review_due_flag = owner_review_due(table, settings_snapshot=settings_snapshot)
    privacy_review_due_flag = privacy_review_due(table, settings_snapshot=settings_snapshot)
    certification_review_due_flag = certification_review_due(table, settings_snapshot=settings_snapshot)
    critical_open_incidents = int(_has_open_critical_incident(db, table))
    privacy_review_interval_days = (
        settings_snapshot.sensitive_privacy_review_interval_days
        if table.sensitivity_level or table.has_personal_data or table.has_sensitive_personal_data
        else settings_snapshot.privacy_review_interval_days
    )
    privacy_review_next_at = (
        table.privacy_reviewed_at + timedelta(days=privacy_review_interval_days)
        if table.privacy_reviewed_at
        else None
    )
    evaluation = _build_certification_evaluation(
        table,
        readiness_score=score,
        readiness_completed=completed,
        readiness_total=total,
        active_dq_violation=active_dq_violation,
        active_dq_violation_count=active_dq_violation_count,
        active_dq_rule_names=active_dq_rule_names,
        critical_open_incidents=critical_open_incidents,
        operational_revalidation_required=operational_revalidation_required,
        now=now,
    )
    guidance = certification_workflow_guidance(
        table,
        eligible=eligible,
        readiness_score=score,
        now=now,
        settings_snapshot=settings_snapshot,
        active_dq_violation=active_dq_violation,
        active_dq_rule_names=active_dq_rule_names,
        critical_open_incidents=critical_open_incidents,
        operational_revalidation_required=operational_revalidation_required,
    )
    return TableCertificationSummaryOut(
        id=table.id,
        name=table.name,
        schema_name=table.schema.name,
        database_name=table.schema.database.name,
        datasource_name=table.schema.database.datasource.name,
        owner=table.owner,
        owner_email=table.owner_email,
        data_owner_id=table.data_owner_id,
        data_owner=table.data_owner,
        data_owner_is_active=table.data_owner.is_active if table.data_owner is not None else None,
        certification_status=evaluation.certification_status,
        certification_status_label=evaluation.certification_status_label,
        certification_status_source=evaluation.certification_status_source,
        certification_status_rule=evaluation.certification_status_rule,
        certification_status_reason=evaluation.certification_status_reason,
        certification_criticality=table.certification_criticality,
        certification_badges=table.certification_badges or [],
        certification_notes=table.certification_notes,
        certification_submitted_by_user_id=table.certification_submitted_by_user_id,
        certification_submitted_by_user_name=_user_display_name(table.certification_submitted_by_user),
        certification_submitted_by_user_email=_user_display_email(table.certification_submitted_by_user),
        certification_submitted_at=table.certification_submitted_at,
        certification_decided_by_user_id=table.certification_decided_by_user_id,
        certification_decided_by_user_name=_user_display_name(table.certification_decided_by_user),
        certification_decided_by_user_email=_user_display_email(table.certification_decided_by_user),
        certification_decided_at=table.certification_decided_at,
        certification_review_at=table.certification_review_at,
        certification_expires_at=table.certification_expires_at,
        certification_review_due=certification_review_due_flag,
        certification_next_review_at=table.certification_expires_at or table.certification_review_at,
        certification_sla_due_at=guidance["certification_sla_due_at"],
        certification_sla_status=guidance["certification_sla_status"],
        certification_sla_label=guidance["certification_sla_label"],
        certification_revalidation_required=guidance["certification_revalidation_required"],
        certification_next_step=guidance["certification_next_step"],
        active_dq_violation=evaluation.active_dq_violation,
        active_dq_violation_count=evaluation.active_dq_violation_count,
        active_dq_rule_names=evaluation.active_dq_rule_names,
        owner_reviewed_by_user_id=table.owner_reviewed_by_user_id,
        owner_reviewed_by_user_name=_user_display_name(table.owner_reviewed_by_user),
        owner_reviewed_by_user_email=_user_display_email(table.owner_reviewed_by_user),
        owner_reviewed_at=table.owner_reviewed_at,
        owner_review_due=owner_review_due_flag,
        owner_review_next_at=(
            table.owner_reviewed_at + timedelta(days=settings_snapshot.owner_review_interval_days)
            if table.owner_reviewed_at
            else None
        ),
        privacy_review_due=privacy_review_due_flag,
        privacy_review_next_at=privacy_review_next_at,
        trust_score=int(trust_payload.score),
        trust_label=trust_payload.label,
        trust_tone=trust_payload.tone,
        readiness_score=evaluation.readiness_score,
        readiness_completed=evaluation.readiness_completed,
        readiness_total=evaluation.readiness_total,
        eligible_for_certification=evaluation.eligible_for_certification,
        checklist=checklist,
        created_at=table.created_at,
        updated_at=table.updated_at,
    )


def certification_order_clause(sort_by: str, sort_dir: str):
    sort_map = {
        "name": TableEntity.name,
        "schema": Schema.name,
        "database": Database.name,
        "datasource": DataSource.name,
        "status": TableEntity.certification_status,
        "criticality": TableEntity.certification_criticality,
        "updated_at": TableEntity.updated_at,
        "decision_at": TableEntity.certification_decided_at,
    }
    sort_column = sort_map.get(sort_by, TableEntity.updated_at)
    order_clause = desc(sort_column) if sort_dir.lower() == "desc" else asc(sort_column)
    return order_clause


def validate_certification_patch(
    db: Session,
    *,
    table: TableEntity,
    payload: TableCertificationPatch,
) -> tuple[list[dict[str, str | bool]], bool]:
    if payload.certification_criticality and payload.certification_criticality not in CERTIFICATION_CRITICALITIES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid certification criticality")

    badges = payload.certification_badges or []
    invalid_badges = [badge for badge in badges if badge not in CERTIFICATION_BADGES]
    if invalid_badges:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid certification badges: {', '.join(invalid_badges)}")

    target_status = (payload.certification_status or "").strip().lower()
    notes = (payload.certification_notes or "").strip()
    if target_status in {"in_review", "certified", "rejected", "revalidation_pending", "expired"} and not notes:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe um motivo ou observação formal para registrar esta decisão de certificação.",
        )
    if target_status == "certified" and payload.certification_review_at is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe a próxima revisão formal antes de concluir a certificação.",
        )
    if target_status == "certified" and payload.certification_expires_at is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe a validade da certificação antes de concluir a aprovação.",
        )
    if target_status in {"in_review", "certified"} and (table.has_personal_data or table.has_sensitive_personal_data):
        if not table.privacy_reviewed_at:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Ativos com dado pessoal ou sensível exigem revisão formal de privacidade antes de avançar na certificação.",
            )
        if not (table.legal_basis or "").strip() or not (table.privacy_purpose or "").strip():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Ativos com dado pessoal ou sensível exigem base legal e finalidade registradas antes de avançar na certificação.",
            )

    checklist, completed, eligible = build_certification_checklist(db, table)
    score = int(round((completed / len(checklist)) * 100)) if checklist else 0
    active_dq_violation, active_dq_violation_count, active_dq_rule_names = _active_dq_violation_summary(db, table)
    critical_open_incidents = int(_has_open_critical_incident(db, table))
    missing_gates = _missing_certification_gates(checklist)
    current_status = resolve_certification_status(
        table,
        readiness_score=score,
        eligible=eligible,
        active_dq_violation=active_dq_violation,
        active_dq_violation_count=active_dq_violation_count,
        active_dq_rule_names=active_dq_rule_names,
        critical_open_incidents=critical_open_incidents,
    )
    certifiable = score >= 80 and not active_dq_violation and critical_open_incidents == 0
    if target_status in {"in_review", "certified"} and missing_gates:
        pending_labels = [item["label"] for item in missing_gates]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Complete os gates obrigatórios antes de avançar a certificação: {', '.join(pending_labels)}.",
        )
    if payload.certification_status == "certified" and not certifiable:
        pending = [item["label"] for item in checklist if not item["passed"]]
        if active_dq_violation and active_dq_rule_names:
            detail = f"Tabela bloqueada por violação ativa de Data Quality: {', '.join(active_dq_rule_names[:3])}."
        elif active_dq_violation:
            detail = "Tabela bloqueada por violação ativa de Data Quality."
        elif critical_open_incidents > 0:
            detail = "Tabela bloqueada por incidente crítico aberto."
        elif score < 80:
            detail = "Tabela ainda não alcançou a prontidão mínima de 80% para certificação."
        else:
            detail = f"Tabela ainda não elegível para certificação. Pendências: {', '.join(pending)}"
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=detail,
        )
    validate_workflow_transition(
        current_status=current_status,
        target_status=payload.certification_status,
        eligible=eligible,
        certifiable=certifiable,
    )
    if payload.certification_expires_at and payload.certification_review_at and payload.certification_expires_at < payload.certification_review_at:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="A validade da certificação não pode ser anterior à data de revisão.",
        )
    return checklist, eligible
