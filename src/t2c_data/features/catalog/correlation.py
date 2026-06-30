from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, status
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from t2c_data.features.catalog.operational_context import load_table_operational_context
from t2c_data.features.data_quality.application import get_latest_metrics_by_table_id
from t2c_data.features.data_quality.incident_signals import evaluate_table_dq_incident_signals
from t2c_data.features.data_quality.rule_management import list_rules_with_filters
from t2c_data.features.governance.rules import certification_review_due, owner_review_due, privacy_review_due
from t2c_data.features.governance.scoring import build_governance_score
from t2c_data.features.governance.score_history import summarize_table_governance_score_trend
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.ingestion import IngestionIntegrationUnavailable, load_table_ingestion_summary, operational_session_for_datasource
from t2c_data.features.ingestion.service import STALE_SUCCESS_THRESHOLD_HOURS
from t2c_data.features.privacy_access import can_view_table
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.catalog import ColumnEntity
from t2c_data.models.glossary import GlossaryAssignment
from t2c_data.models.incident import Incident
from t2c_data.models.search import SearchResultClick
from t2c_data.models.tag import TagAssignment
from t2c_data.schemas.catalog import (
    TableCorrelationDQRuleOut,
    TableCorrelationDQSummaryOut,
    TableCorrelationIncidentItemOut,
    TableCorrelationIncidentSummaryOut,
    TableCorrelationOperationalSLAOut,
    TableCorrelationSignalsOut,
    TableCorrelationSummaryOut,
    TableLocatorOut,
    TableOperationalIncidentPrefillOut,
)

OPEN_INCIDENT_STATUSES = ("open", "investigating", "mitigated")
_SEVERITY_LABELS = {
    "sev1": "Crítico",
    "sev2": "Alto",
    "sev3": "Médio",
    "sev4": "Baixo",
}


def _correlation_type_and_summary(*, has_operational_failure: bool, has_dq_degradation: bool, has_open_incident: bool) -> tuple[str, str]:
    if has_operational_failure and has_dq_degradation and has_open_incident:
        return "Falha operacional + DQ degradada + incidente aberto", "Falha operacional, DQ degradada e incidente aberto aparecem ao mesmo tempo neste ativo."
    if has_operational_failure and has_dq_degradation:
        return "Falha operacional + DQ degradada", "Falha operacional e DQ degradada aparecem ao mesmo tempo neste ativo."
    if has_operational_failure and has_open_incident:
        return "Falha operacional + incidente aberto", "Falha operacional recente e incidente aberto aparecem ao mesmo tempo neste ativo."
    if has_dq_degradation and has_open_incident:
        return "DQ degradada + incidente aberto", "DQ degradada e incidente aberto aparecem ao mesmo tempo neste ativo."
    if has_operational_failure:
        return "Falha operacional", "Há falha operacional recente associada ao ativo."
    if has_dq_degradation:
        return "DQ degradada", "A qualidade do ativo merece atenção mesmo sem falha operacional explícita."
    if has_open_incident:
        return "Incidente aberto", "O ativo possui incidente aberto e merece atenção."
    return "Sem correlação crítica relevante", "Nenhum sinal crítico relevante foi identificado neste momento."


def build_correlation_priority_payload(
    *,
    table_id: int,
    asset_name: str,
    qualified_name: str,
    schema_name: str,
    source_name: str,
    has_operational_failure: bool,
    has_dq_degradation: bool,
    has_open_incident: bool,
    access_clicks: int = 0,
    total_clicks: int | None = None,
    table_fqn: str | None = None,
) -> dict[str, object]:
    access_bonus = 1 if access_clicks > 0 else 0
    priority_score = (
        (4 if has_operational_failure else 0)
        + (3 if has_dq_degradation else 0)
        + (3 if has_open_incident else 0)
        + access_bonus
    )
    correlation_type, summary = _correlation_type_and_summary(
        has_operational_failure=has_operational_failure,
        has_dq_degradation=has_dq_degradation,
        has_open_incident=has_open_incident,
    )
    return {
        "asset_id": table_id,
        "table_id": table_id,
        "asset_name": asset_name,
        "qualified_name": qualified_name,
        "schema_name": schema_name,
        "source_name": source_name,
        "has_operational_failure": has_operational_failure,
        "has_dq_degradation": has_dq_degradation,
        "has_open_incident": has_open_incident,
        "priority_score": priority_score,
        "correlation_type": correlation_type,
        "summary": summary,
        "table_fqn": table_fqn,
        "total_clicks": total_clicks,
        "target_url": f"/explorer?tableId={table_id}",
    }


def _resolve_visible_table_row(db: Session, table_id: int, current_user: User):
    row = db.execute(
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.id == table_id)
    ).first()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    table, schema, database, datasource = row
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    return table, schema, database, datasource


def _safe_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return None


def _is_stale(ingestion_summary: dict[str, object] | None, *, now: datetime) -> bool:
    if not ingestion_summary or not ingestion_summary.get("linked"):
        return False
    primary = ingestion_summary.get("primary_pipeline")
    if not isinstance(primary, dict):
        return False
    status_label = str(primary.get("latest_status_label") or "").strip()
    if status_label == "Em execução":
        return False
    last_success_at = _safe_dt(primary.get("last_success_at"))
    if last_success_at is None:
        return True
    return last_success_at <= now - timedelta(hours=STALE_SUCCESS_THRESHOLD_HOURS)


def _operational_sla_payload(
    *,
    ingestion_summary: dict[str, object] | None,
    stability_summary: dict[str, object] | None,
    open_incident_count: int,
    now: datetime,
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    if not ingestion_summary or not ingestion_summary.get("linked"):
        return None, None
    primary = ingestion_summary.get("primary_pipeline")
    if not isinstance(primary, dict):
        return None, None

    status_label = str(primary.get("latest_status_label") or "").strip()
    last_error = str(primary.get("last_error") or "").strip() or None
    is_failure = status_label == "Falha" or last_error is not None
    is_stale = _is_stale(ingestion_summary, now=now)
    if not is_failure and not is_stale:
        return None, None

    issue_type = "failure" if is_failure else "stale"
    issue_label = "Falha operacional" if is_failure else "Sem sucesso recente"
    detected_at = _safe_dt(primary.get("last_failure_at")) if is_failure else None
    if detected_at is None:
        detected_at = _safe_dt(primary.get("last_execution_finished_at")) or _safe_dt(primary.get("last_execution_started_at"))
    if detected_at is None and is_stale:
        last_success = _safe_dt(primary.get("last_success_at"))
        if last_success is not None:
            detected_at = last_success + timedelta(hours=STALE_SUCCESS_THRESHOLD_HOURS)
    if detected_at is None:
        detected_at = now

    sla_hours = 24
    due_at = detected_at + timedelta(hours=sla_hours)
    aging_hours = max(int((now - detected_at).total_seconds() // 3600), 0)
    remaining_seconds = (due_at - now).total_seconds()
    if remaining_seconds <= 0:
        status_value = "overdue"
        status_label_value = "Fora do SLA"
    elif remaining_seconds <= 4 * 3600:
        status_value = "due_soon"
        status_label_value = "SLA próximo do vencimento"
    else:
        status_value = "within_sla"
        status_label_value = "Dentro do SLA"

    recurrent = bool(stability_summary and stability_summary.get("recurrent_degradation"))
    payload = {
        "active": True,
        "issue_type": issue_type,
        "issue_label": issue_label,
        "detected_at": detected_at,
        "due_at": due_at,
        "aging_hours": aging_hours,
        "sla_hours": sla_hours,
        "status": status_value,
        "status_label": status_label_value,
        "recurrent_degradation": recurrent,
    }
    prefill_evidence = {
        "origin": "explorer_ingestion",
        "operational_issue_type": issue_type,
        "operational_issue_label": issue_label,
        "operational_status_label": status_label,
        "pipeline_name": primary.get("pipeline_name"),
        "dag_id": primary.get("dag_id"),
        "task_name": primary.get("task_name"),
        "last_error": primary.get("last_error"),
        "last_success_at": primary.get("last_success_at").isoformat() if isinstance(primary.get("last_success_at"), datetime) else primary.get("last_success_at"),
        "last_failure_at": primary.get("last_failure_at").isoformat() if isinstance(primary.get("last_failure_at"), datetime) else primary.get("last_failure_at"),
        "watermark_value": primary.get("watermark_value"),
        "rows_processed": primary.get("rows_processed"),
        "operational_sla_due_at": due_at.isoformat(),
        "operational_sla_hours": sla_hours,
        "recurrent_degradation": recurrent,
    }
    return payload, prefill_evidence


def build_table_correlation_summary(*, db: Session, table_id: int, current_user: User) -> TableCorrelationSummaryOut:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(db)
    table, schema, database, datasource = _resolve_visible_table_row(db, table_id, current_user)
    locator = TableLocatorOut(
        table_id=table.id,
        datasource_id=datasource.id,
        datasource_name=datasource.name,
        database_id=database.id,
        database_name=database.name,
        schema_id=schema.id,
        schema_name=schema.name,
        table_name=table.name,
        kind=table.table_type,
        db_type=datasource.db_type,
    )

    operational_context_payload = load_table_operational_context(
        db,
        table_id=table.id,
        datasource_id=datasource.id,
        database_id=database.id,
        schema_id=schema.id,
    )

    dq_payload: dict[str, object] | None
    try:
      dq_payload = get_latest_metrics_by_table_id(db=db, table_id=table.id, history_runs=14, current_user=current_user)
    except HTTPException as exc:
      dq_payload = None if exc.status_code == status.HTTP_404_NOT_FOUND else None

    incident_signals_payload = evaluate_table_dq_incident_signals(db, table_id=table.id)

    try:
        with operational_session_for_datasource(datasource) as operational_db:
            ingestion_summary_payload = load_table_ingestion_summary(
                operational_db,
                schema_name=schema.name,
                table_name=table.name,
                airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
            )
            ingestion_stability_payload = None
    except IngestionIntegrationUnavailable as exc:
        ingestion_summary_payload = {
            "linked": False,
            "state": "unavailable",
            "message": str(exc),
            "table_schema": schema.name,
            "table_name": table.name,
            "pipeline_count": 0,
            "primary_pipeline": None,
            "pipelines": [],
        }
        ingestion_stability_payload = None

    open_incidents = db.scalars(
        select(Incident)
        .where(
            Incident.entity_type == "table",
            Incident.table_fqn == f"{schema.name}.{table.name}",
            Incident.status.in_(OPEN_INCIDENT_STATUSES),
        )
        .order_by(desc(Incident.updated_at), desc(Incident.id))
        .limit(5)
    ).all()
    incidents_summary = TableCorrelationIncidentSummaryOut(
        open_count=len(open_incidents),
        critical_open_count=sum(1 for incident in open_incidents if incident.severity == "sev1"),
        latest_open_incident_id=(open_incidents[0].id if open_incidents else None),
        latest_open_incident_title=(open_incidents[0].title if open_incidents else None),
        items=[
            TableCorrelationIncidentItemOut(
                id=incident.id,
                title=incident.title,
                status=incident.status,
                severity=incident.severity,
                severity_label=_SEVERITY_LABELS.get(incident.severity, incident.severity.upper()),
                source_type=incident.source_type,
                detected_at=incident.detected_at,
                last_seen_at=incident.last_seen_at,
                target_url=f"/incidents/tickets?tableId={table.id}",
            )
            for incident in open_incidents
        ],
    )

    operational_sla_payload, prefill_evidence = _operational_sla_payload(
        ingestion_summary=ingestion_summary_payload,
        stability_summary=ingestion_stability_payload,
        open_incident_count=incidents_summary.open_count,
        now=now,
    )
    operational_failure = bool(
        ingestion_summary_payload.get("primary_pipeline")
        and isinstance(ingestion_summary_payload["primary_pipeline"], dict)
        and (
            str(ingestion_summary_payload["primary_pipeline"].get("latest_status_label") or "").strip() == "Falha"
            or ingestion_summary_payload["primary_pipeline"].get("last_error")
        )
    )
    stale_pipeline = _is_stale(ingestion_summary_payload, now=now)

    correlated_rules: list[TableCorrelationDQRuleOut] = []
    if operational_failure or stale_pipeline:
        for rule in list_rules_with_filters(
            db=db,
            rule_id=None,
            q=None,
            table_id=table.id,
            table_fqn=f"{schema.name}.{table.name}",
            is_active=True,
            severity=None,
            last_status="failed",
            current_user=current_user,
        )[:5]:
            correlated_rules.append(
                TableCorrelationDQRuleOut(
                    id=rule.id,
                    name=rule.name,
                    severity=rule.severity,
                    last_run_status=rule.last_run_status,
                    last_violations_count=rule.last_violations_count,
                    open_incident_id=rule.open_incident_id,
                    target_url=f"/data-quality/rules?rule_id={rule.id}",
                )
            )

    dq_summary = TableCorrelationDQSummaryOut(
        dq_score=(
            float(dq_payload.get("effective_dq_score") or dq_payload["dq_score"])
            if dq_payload and (dq_payload.get("effective_dq_score") is not None or dq_payload.get("dq_score") is not None)
            else None
        ),
        failed_rules=(int(dq_payload.get("failed_rules") or 0) if dq_payload else 0),
        freshness_seconds=(int(dq_payload.get("freshness_seconds") or 0) if dq_payload and dq_payload.get("freshness_seconds") is not None else None),
        run_at=dq_payload.get("run_at") if dq_payload else None,
        correlated_rules=correlated_rules,
    )

    has_dq_degradation = bool(dq_summary.dq_score is not None and dq_summary.dq_score < 90)
    has_open_incident = incidents_summary.open_count > 0
    has_operational_failure = bool(operational_failure or stale_pipeline)
    access_clicks = int(
        db.scalar(
            select(func.count(SearchResultClick.id)).where(
                SearchResultClick.created_at >= now - timedelta(days=30),
                SearchResultClick.entity_type == "table",
                SearchResultClick.entity_id == table.id,
            )
        )
        or 0
    )
    priority_payload = build_correlation_priority_payload(
        table_id=table.id,
        asset_name=table.name,
        qualified_name=f"{datasource.name}.{schema.name}.{table.name}",
        schema_name=schema.name,
        source_name=datasource.name,
        has_operational_failure=has_operational_failure,
        has_dq_degradation=has_dq_degradation,
        has_open_incident=has_open_incident,
        access_clicks=access_clicks,
        total_clicks=access_clicks,
        table_fqn=f"{schema.name}.{table.name}",
    )
    signal_summary = str(priority_payload["summary"])

    incident_prefill = None
    if prefill_evidence is not None:
        if dq_summary.dq_score is not None:
            prefill_evidence["dq_score"] = round(dq_summary.dq_score, 1)
            prefill_evidence["dq_failed_rules"] = dq_summary.failed_rules
        incident_prefill = TableOperationalIncidentPrefillOut(
            title=f"Falha operacional em {schema.name}.{table.name}",
            description="Chamado aberto a partir do contexto operacional do ativo para investigar impacto no pipeline, na qualidade e no consumo.",
            source_type="platform_ops",
            source_ref_id=table.id,
            evidence_json=prefill_evidence,
        )

    columns = db.scalars(select(ColumnEntity).where(ColumnEntity.table_id == table.id)).all()
    tags_count = int(
        db.scalar(
            select(func.count(TagAssignment.id)).where(
                TagAssignment.entity_type == "table",
                TagAssignment.entity_id == table.id,
            )
        )
        or 0
    )
    terms_count = int(
        db.scalar(
            select(func.count(GlossaryAssignment.id)).where(
                GlossaryAssignment.entity_type == "table",
                GlossaryAssignment.entity_id == table.id,
            )
        )
        or 0
    )
    table_description_complete = bool((table.description_manual or table.description_source or "").strip())
    column_description_complete = bool(columns) and all(
        bool(
            (
                column.description_manual
                or column.description_source
                or column.dictionary_description
                or column.dictionary_comment
                or column.existing_comment
                or ""
            ).strip()
        )
        for column in columns
    )
    eligible_for_certification = bool(
        table.data_owner_id
        and column_description_complete
        and tags_count > 0
        and terms_count > 0
        and dq_summary.dq_score is not None
        and dq_summary.dq_score >= 90
        and incidents_summary.critical_open_count == 0
        and not owner_review_due(table, settings_snapshot=settings_snapshot)
        and not privacy_review_due(table, settings_snapshot=settings_snapshot)
    )
    governance_score = build_governance_score(
        owner_defined=bool(table.data_owner_id or table.owner or table.owner_email),
        table_description_complete=table_description_complete,
        column_description_complete=column_description_complete,
        tags_count=tags_count,
        terms_count=terms_count,
        dq_score=dq_summary.dq_score,
        certification_status=table.certification_status,
        eligible_for_certification=eligible_for_certification,
        open_incidents=incidents_summary.open_count,
        critical_open_incidents=incidents_summary.critical_open_count,
        owner_review_current=not owner_review_due(table, settings_snapshot=settings_snapshot),
        privacy_review_current=not privacy_review_due(table, settings_snapshot=settings_snapshot),
        certification_review_current=not certification_review_due(table, settings_snapshot=settings_snapshot),
    )
    governance_trend = summarize_table_governance_score_trend(db, table_id=table.id)

    return TableCorrelationSummaryOut(
        table_id=table.id,
        locator=locator,
        operational_context=operational_context_payload,
        ingestion=ingestion_summary_payload,
        stability=ingestion_stability_payload,
        governance_score=governance_score,
        governance_trend=governance_trend,
        dq=dq_summary,
        incident_signals=incident_signals_payload,
        incidents=incidents_summary,
        operational_sla=(TableCorrelationOperationalSLAOut(**operational_sla_payload) if operational_sla_payload else None),
        incident_prefill=incident_prefill,
        asset_id=table.id,
        asset_name=table.name,
        qualified_name=f"{datasource.name}.{schema.name}.{table.name}",
        schema_name=schema.name,
        source_name=datasource.name,
        has_operational_failure=has_operational_failure,
        has_dq_degradation=has_dq_degradation,
        has_open_incident=has_open_incident,
        priority_score=int(priority_payload["priority_score"]),
        correlation_type=str(priority_payload["correlation_type"]),
        summary=signal_summary,
        signals=TableCorrelationSignalsOut(
            combined_attention=bool(has_operational_failure and has_dq_degradation and has_open_incident),
            operational_failure=has_operational_failure,
            stale_pipeline=stale_pipeline,
            open_incident=has_open_incident,
            dq_below_threshold=has_dq_degradation,
            summary=signal_summary,
        ),
    )
