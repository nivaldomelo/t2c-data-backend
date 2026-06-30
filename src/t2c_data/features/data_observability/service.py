from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from math import isfinite
from statistics import mean
from typing import Any

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from t2c_data.features.catalog.operational_context import load_table_operational_context
from t2c_data.features.data_quality.application import get_latest_metrics_by_table_id
from t2c_data.features.data_quality.incident_signals import evaluate_table_dq_incident_signals
from t2c_data.features.data_quality.observability_store import load_filtered_observability_artifacts
from t2c_data.features.dashboard.support import TableProfile, normalize_dt
from t2c_data.features.ingestion import load_table_ingestion_detail_from_source, load_table_ingestion_summary_from_source
from t2c_data.features.metabase import get_table_metabase_consumption
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.incident import Incident
from t2c_data.models.platform import DashboardAssetReadModel
from t2c_data.schemas.observability import (
    ObservabilityAssetDetailOut,
    ObservabilityAssetOut,
    ObservabilityContextOut,
    ObservabilityDiagnosticsOut,
    ObservabilityFilterOptionsOut,
    ObservabilityHistoryPointOut,
    ObservabilityLayerErrorOut,
    ObservabilityOverviewOut,
    ObservabilityRelatedSignalsOut,
    ObservabilityStageDurationOut,
    ObservabilitySummaryOut,
    ObservabilityTimelineEventOut,
)


def _iso(value: datetime | None) -> datetime | None:
    normalized = normalize_dt(value)
    return normalized


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        number = int(value)
        return number
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        number = float(value)
        return number if isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _status_from_freshness(freshness_seconds: int | None) -> str:
    if freshness_seconds is None:
        return "unreadable"
    if freshness_seconds <= 24 * 3600:
        return "healthy"
    if freshness_seconds <= 72 * 3600:
        return "late"
    return "critical"


def _status_from_delta(delta_pct: float | None) -> str:
    if delta_pct is None:
        return "unreadable"
    abs_delta = abs(delta_pct)
    if abs_delta < 5:
        return "healthy"
    if abs_delta < 20:
        return "attention"
    return "critical"


def _status_from_schema_artifacts(artifacts: dict[str, Any]) -> tuple[str, bool, str, str, list[str], list[str], list[str], list[str], list[str], list[str]]:
    events = artifacts.get("events") if isinstance(artifacts, dict) else []
    schema_events = [event for event in events if isinstance(event, dict) and event.get("metric_key") in {"schema_drift", "schema"}]
    if not schema_events:
        return "healthy", False, "Sem drift", "Sem impacto downstream registrado.", [], [], [], [], [], []
    latest = schema_events[0]
    severity = str(latest.get("severity") or "warning").strip().lower()
    if severity in {"critical", "sev1"}:
        status = "drift"
    elif severity in {"warning", "sev2"}:
        status = "attention"
    else:
        status = "attention"
    details = latest.get("details_json") if isinstance(latest.get("details_json"), dict) else {}
    impact = str(details.get("downstream_impact") or details.get("impact") or "Mudança estrutural registrada.")
    drift_severity = str(details.get("severity_label") or details.get("drift_severity") or "Média")
    return (
        status,
        True,
        drift_severity,
        impact,
        [str(item) for item in details.get("new_columns", []) if item],
        [str(item) for item in details.get("removed_columns", []) if item],
        [str(item) for item in details.get("altered_columns", []) if item],
        [str(item) for item in details.get("nulled_columns", []) if item],
        [str(item) for item in details.get("parquet_changes", []) if item],
        [str(item) for item in details.get("relational_changes", []) if item],
    )


def _status_from_volume(current: int | None, expected: int | None, artifacts: dict[str, Any]) -> tuple[str, float | None]:
    events = artifacts.get("events") if isinstance(artifacts, dict) else []
    volume_events = [event for event in events if isinstance(event, dict) and event.get("metric_key") == "volume"]
    if volume_events:
        latest = volume_events[0]
        severity = str(latest.get("severity") or "").strip().lower()
        if severity in {"critical", "sev1"}:
            return "critical", _to_float(latest.get("delta_pct"))
        if severity in {"warning", "sev2"}:
            return "attention", _to_float(latest.get("delta_pct"))
        return "attention", _to_float(latest.get("delta_pct"))
    if current is None or expected in (None, 0):
        return "unreadable", None
    delta_pct = ((current - expected) / expected) * 100.0
    return _status_from_delta(delta_pct), round(delta_pct, 1)


def _reliability_from_statuses(
    *,
    freshness: str,
    volume: str,
    schema: str,
    pipeline: str,
    open_incidents: int,
    blocking_incidents: int,
    dq_score: float | None,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if blocking_incidents > 0:
        reasons.append("Incidentes bloqueantes em aberto")
    if pipeline in {"blocked", "critical"}:
        reasons.append("Pipeline bloqueado ou com falha")
    if schema == "drift":
        reasons.append("Schema drift detectado")
    if volume in {"critical", "attention"}:
        reasons.append("Volume fora do padrão")
    if freshness in {"late", "critical"}:
        reasons.append("Freshness fora do SLA")
    if dq_score is not None and dq_score < 70:
        reasons.append("DQ abaixo do patamar mínimo")
    if open_incidents > 0:
        reasons.append("Incidentes abertos")
    if not reasons:
        return "reliable", ["Sem sinais críticos relevantes"]
    if blocking_incidents > 0 or pipeline in {"blocked", "critical"}:
        return "blocked", reasons
    if len(reasons) >= 3 or (dq_score is not None and dq_score < 60):
        return "unreliable", reasons
    return "reliable_with_reservations", reasons


def _observability_score(dq_score: float | None, trust_score: int | None, freshness: str, volume: str, schema: str, pipeline: str) -> int:
    values = [value for value in [dq_score, float(trust_score) if trust_score is not None else None] if value is not None]
    base = round(mean(values)) if values else 0
    penalty = 0
    for status in (freshness, volume, schema, pipeline):
        if status == "critical":
            penalty += 20
        elif status in {"attention", "late", "drift"}:
            penalty += 8
        elif status == "blocked":
            penalty += 30
    return max(0, min(100, int(base) - penalty))


def _history_points(history: list[dict[str, Any]]) -> list[ObservabilityHistoryPointOut]:
    points: list[ObservabilityHistoryPointOut] = []
    for item in history:
        label = item.get("run_at")
        value = item.get("row_count")
        if isinstance(value, (int, float)):
            points.append(ObservabilityHistoryPointOut(label=str(label or "run"), value=int(value)))
    return points[-8:]


def _timeline_events(
    *,
    profile: TableProfile,
    dq_latest: dict[str, Any] | None,
    ingestion_summary: dict[str, Any] | None,
    ingestion_detail: dict[str, Any] | None,
    artifacts: dict[str, Any] | None,
) -> list[ObservabilityTimelineEventOut]:
    events: list[ObservabilityTimelineEventOut] = []
    if ingestion_summary and ingestion_summary.get("linked"):
        primary = ingestion_summary.get("primary_pipeline")
        if isinstance(primary, dict):
            events.append(
                ObservabilityTimelineEventOut(
                    id=f"{profile.table_id}-arrival",
                    type="arrival",
                    at=primary.get("last_success_at") or primary.get("last_execution_started_at"),
                    label="Chegada de arquivo",
                    description="Último processamento operacional encontrado na fonte de ingestão.",
                )
            )
            events.append(
                ObservabilityTimelineEventOut(
                    id=f"{profile.table_id}-pipeline",
                    type="pipeline",
                    at=primary.get("last_execution_finished_at") or primary.get("last_execution_started_at"),
                    label="Execução da DAG",
                    description=str(primary.get("pipeline_name") or primary.get("dag_id") or "Pipeline operacional vinculado."),
                )
            )
    if dq_latest:
        current = dq_latest.get("current")
        if isinstance(current, dict):
            events.append(
                ObservabilityTimelineEventOut(
                    id=f"{profile.table_id}-profiling",
                    type="profiling",
                    at=current.get("run_at"),
                    label="Profiling",
                    description="Último perfilamento consolidado pela camada de DQ.",
                )
            )
            events.append(
                ObservabilityTimelineEventOut(
                    id=f"{profile.table_id}-validation",
                    type="validation",
                    at=current.get("run_at"),
                    label="Validação de regras",
                    description="As regras críticas do ativo foram avaliadas no último run.",
                )
            )
    if profile.open_incidents > 0:
        events.append(
            ObservabilityTimelineEventOut(
                id=f"{profile.table_id}-incident",
                type="incident",
                at=_iso(profile.last_updated_at),
                label="Incidente",
                description="Há incidente(s) aberto(s) relacionado(s) ao ativo.",
            )
        )
    if profile.certification_review_at:
        events.append(
            ObservabilityTimelineEventOut(
                id=f"{profile.table_id}-certification",
                type="certification",
                at=_iso(profile.certification_review_at),
                label="Certificação",
                description="Revisão ou decisão de certificação registrada no catálogo.",
            )
        )
    if artifacts:
        for event in artifacts.get("events", [])[:2]:
            if not isinstance(event, dict):
                continue
            events.append(
                ObservabilityTimelineEventOut(
                    id=f"{profile.table_id}-dq-{event.get('id')}",
                    type="alert" if str(event.get("event_type")) != "drift" else "validation",
                    at=event.get("detected_at"),
                    label=str(event.get("event_type") or "Evento DQ").title(),
                    description="Evento de observabilidade persistido pela camada de DQ.",
                )
            )
    events = [event for event in events if event.at is not None]
    events.sort(key=lambda item: item.at or datetime.min.replace(tzinfo=timezone.utc))
    return events[-8:]


def _signal_copy(asset: ObservabilityAssetOut, **updates: Any) -> ObservabilityAssetOut:
    return asset.model_copy(update=updates)


def _stage_durations(ingestion_detail: dict[str, Any] | None) -> list[ObservabilityStageDurationOut]:
    if not ingestion_detail:
        return []
    summary = ingestion_detail.get("summary")
    primary = summary.get("primary_pipeline") if isinstance(summary, dict) else None
    if not isinstance(primary, dict):
        return []
    started = primary.get("last_execution_started_at")
    finished = primary.get("last_execution_finished_at")
    if not isinstance(started, datetime) or not isinstance(finished, datetime):
        return []
    total_ms = max(int((finished - started).total_seconds() * 1000), 0)
    bronze = max(total_ms // 4, 0)
    silver = max(total_ms // 2, bronze)
    gold = max(total_ms - bronze - silver, 0)
    return [
        ObservabilityStageDurationOut(stage="Bronze", duration_ms=bronze),
        ObservabilityStageDurationOut(stage="Silver", duration_ms=silver),
        ObservabilityStageDurationOut(stage="Gold", duration_ms=gold),
    ]


def _layer_errors(ingestion_detail: dict[str, Any] | None) -> list[ObservabilityLayerErrorOut]:
    if not ingestion_detail:
        return []
    executions = ingestion_detail.get("executions")
    items = executions.get("items") if isinstance(executions, dict) else []
    errors: list[ObservabilityLayerErrorOut] = []
    if isinstance(items, list):
        for item in items[:3]:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status_label") or item.get("latest_status_label") or "").strip()
            if status and status not in {"Sucesso", "Em execução"}:
                errors.append(
                    ObservabilityLayerErrorOut(
                        layer=str(item.get("layer") or item.get("pipeline_name") or "Pipeline"),
                        message=str(item.get("error_message") or item.get("last_error") or status),
                    )
                )
    return errors


def _build_asset_record(
    *,
    profile: TableProfile,
    dq_latest: dict[str, Any] | None,
    dq_artifacts: dict[str, Any] | None,
    ingestion_summary: dict[str, Any] | None,
    ingestion_detail: dict[str, Any] | None,
    observability_summary: dict[str, Any] | None,
    metabase_consumption: dict[str, Any] | None,
    selected: bool,
) -> ObservabilityAssetOut:
    current = dq_latest.get("current") if isinstance(dq_latest, dict) else None
    previous = dq_latest.get("previous") if isinstance(dq_latest, dict) else None
    history = dq_latest.get("history") if isinstance(dq_latest, dict) else []
    current_row_count = _to_int(current.get("row_count")) if isinstance(current, dict) else None
    previous_row_count = _to_int(previous.get("row_count")) if isinstance(previous, dict) else None
    expected_row_count = None
    if isinstance(dq_artifacts, dict):
        baselines = dq_artifacts.get("baselines") or []
        if baselines and isinstance(baselines[0], dict):
            expected_row_count = _to_int(baselines[0].get("baseline_value"), default=0) or _to_int(baselines[0].get("current_value"), default=0)
    historical_values = [int(point.get("row_count") or 0) for point in history if isinstance(point, dict) and point.get("row_count") is not None]
    historical_avg = int(round(mean(historical_values))) if historical_values else current_row_count
    same_weekday_avg = historical_avg
    volume_history = [
        ObservabilityHistoryPointOut(label=str(point.get("run_at") or point.get("run_id") or f"run-{index}"), value=_to_int(point.get("row_count")))
        for index, point in enumerate(history)
        if isinstance(point, dict)
    ]
    freshness_seconds = profile.freshness_seconds
    freshness_status = _status_from_freshness(freshness_seconds)
    (
        schema_status,
        schema_drift_detected,
        drift_severity,
        downstream_impact,
        new_columns,
        removed_columns,
        altered_columns,
        nulled_columns,
        parquet_changes,
        relational_changes,
    ) = _status_from_schema_artifacts(dq_artifacts or {})
    volume_status, volume_change_pct = _status_from_volume(current_row_count, expected_row_count or previous_row_count, dq_artifacts or {})
    if current_row_count is None and expected_row_count is None:
        volume_change_pct = None
    pipeline_payload = None
    if isinstance(ingestion_summary, dict):
        pipeline_payload = ingestion_summary.get("primary_pipeline") if isinstance(ingestion_summary.get("primary_pipeline"), dict) else None
    pipeline_status = "unreadable"
    last_pipeline_run_at = None
    dag_name = None
    last_pipeline_status = None
    pipeline_duration_ms = None
    pipeline_attempts = 0
    last_error_message = None
    reprocess_count = 0
    backfill_count = 0
    slow_spark_jobs_count = 0
    gold_write_failures_count = 0
    if pipeline_payload:
        last_pipeline_status = str(pipeline_payload.get("latest_status_label") or pipeline_payload.get("last_status") or "").strip() or None
        pipeline_status = "healthy"
        if last_pipeline_status == "Falha":
            pipeline_status = "blocked"
        elif last_pipeline_status == "Em execução":
            pipeline_status = "attention"
        elif ingestion_summary.get("state") == "not_linked":
            pipeline_status = "unreadable"
        last_pipeline_run_at = pipeline_payload.get("last_execution_finished_at") or pipeline_payload.get("last_execution_started_at")
        dag_name = str(pipeline_payload.get("dag_id") or pipeline_payload.get("pipeline_name") or "").strip() or None
        started = pipeline_payload.get("last_execution_started_at")
        finished = pipeline_payload.get("last_execution_finished_at")
        if isinstance(started, datetime) and isinstance(finished, datetime):
            pipeline_duration_ms = max(int((finished - started).total_seconds() * 1000), 0)
        last_error_message = str(pipeline_payload.get("last_error") or "").strip() or None
        pipeline_attempts = _to_int(ingestion_detail.get("executions", {}).get("total"), 0) if isinstance(ingestion_detail, dict) else 0
        if ingestion_detail and isinstance(ingestion_detail.get("stability"), dict):
            stability = ingestion_detail["stability"]
            reprocess_count = _to_int(stability.get("failed_runs"), 0)
            backfill_count = 1 if stability.get("recurrent_degradation") else 0
            slow_spark_jobs_count = 1 if stability.get("currently_stale") else 0
    if pipeline_status == "unreadable" and freshness_status == "late":
        pipeline_status = "late"
    open_incidents = profile.open_incidents
    blocking_incidents = profile.critical_open_incidents
    dq_score = _to_float(current.get("dq_score") if isinstance(current, dict) else profile.dq_score, None)
    failed_rules = _to_int(current.get("failed_rules"), 0) if isinstance(current, dict) else 0
    critical_rules_total = _to_int(profile.active_dq_rules_count, 0) + failed_rules
    critical_rules_passed = max(critical_rules_total - failed_rules, 0)
    reliability_status, reliability_reasons = _reliability_from_statuses(
        freshness=freshness_status,
        volume=volume_status,
        schema=schema_status,
        pipeline=pipeline_status,
        open_incidents=open_incidents,
        blocking_incidents=blocking_incidents,
        dq_score=dq_score,
    )
    observability_score = _observability_score(profile.dq_score, getattr(profile, "trust_score", None), freshness_status, volume_status, schema_status, pipeline_status)
    summary_bits = [profile.summary if hasattr(profile, "summary") else None]
    if not summary_bits[0]:
        summary_bits = []
    if not summary_bits:
        if reliability_status == "blocked":
            summary = "O ativo está bloqueado para consumo até reprocesso ou correção dos sinais abertos."
        elif reliability_status == "unreliable":
            summary = "O ativo apresenta sinais críticos que exigem tratamento antes do consumo regular."
        elif reliability_status == "reliable_with_reservations":
            summary = "O ativo é consumível, mas ainda possui sinais que merecem acompanhamento."
        else:
            summary = "O ativo está saudável para consumo no contexto atual."
    else:
        summary = summary_bits[0]
    recommendation = " | ".join(reliability_reasons[:3]) if reliability_reasons else "Manter acompanhamento do ativo."
    related_context_state = "selected" if selected else "related"
    return ObservabilityAssetOut(
        table_id=profile.table_id,
        table_name=profile.table_name,
        datasource_id=profile.datasource_id,
        data_source=profile.datasource_name,
        domain=profile.domain_name or "Sem domínio",
        layer=profile.schema_name,
        criticality=(profile.certification_criticality or "medium").lower(),
        source_origin="datasource_scan" if profile.last_sync_at else "catalog",
        linked_by="table_id",
        linked_confidence=100,
        confidence=100,
        scan_run_id=None,
        last_seen_at=_iso(profile.last_sync_at or profile.last_updated_at),
        is_demo=False,
        context_state="selected" if selected else related_context_state,
        freshness_status=freshness_status,
        volume_status=volume_status,
        schema_status=schema_status,
        pipeline_status=pipeline_status,
        reliability_status=reliability_status,
        observability_score=observability_score,
        quality_score=profile.dq_score,
        last_arrival_at=_iso(pipeline_payload.get("last_success_at")) if pipeline_payload else _iso(profile.last_sync_at),
        last_partition=None,
        last_file_path=None,
        last_source_row_at=_iso(pipeline_payload.get("last_success_at")) if pipeline_payload else _iso(profile.last_sync_at),
        last_silver_load_at=_iso(pipeline_payload.get("last_execution_started_at")) if pipeline_payload else None,
        last_gold_load_at=_iso(pipeline_payload.get("last_execution_finished_at")) if pipeline_payload else None,
        last_dw_load_at=_iso(profile.last_updated_at),
        last_updated_at=_iso(profile.last_updated_at or profile.last_sync_at),
        current_row_count=current_row_count,
        expected_row_count=expected_row_count or previous_row_count,
        historical_avg_row_count=historical_avg,
        same_weekday_avg_row_count=same_weekday_avg,
        volume_change_pct=volume_change_pct,
        schema_drift_detected=schema_drift_detected,
        pipeline_failed=pipeline_status in {"blocked", "critical"},
        partial_failure_detected=bool(ingestion_summary and ingestion_summary.get("state") == "available" and ingestion_summary.get("linked") and pipeline_status == "attention"),
        critical_rules_total=critical_rules_total,
        critical_rules_passed=critical_rules_passed,
        open_incidents_total=open_incidents,
        blocking_incidents_total=blocking_incidents,
        summary=summary,
        recommendation=recommendation,
        timeline_events=_timeline_events(
            profile=profile,
            dq_latest=dq_latest,
            ingestion_summary=ingestion_summary,
            ingestion_detail=ingestion_detail,
            artifacts=dq_artifacts,
        ),
        last_pipeline_run_at=_iso(last_pipeline_run_at),
        dag_name=dag_name,
        last_pipeline_status=last_pipeline_status,
        pipeline_duration_ms=pipeline_duration_ms,
        pipeline_attempts=pipeline_attempts,
        stage_durations=_stage_durations(ingestion_detail),
        layer_errors=_layer_errors(ingestion_detail),
        reprocess_count=reprocess_count,
        backfill_count=backfill_count,
        slow_spark_jobs_count=slow_spark_jobs_count,
        gold_write_failures_count=gold_write_failures_count,
        last_error_message=last_error_message,
        certification_valid=profile.certification_status == "certified",
        gold_newer_than_silver=not schema_drift_detected or pipeline_status != "blocked",
        silver_validated_before_gold=not schema_drift_detected or pipeline_status != "blocked",
        reliability_reasons=reliability_reasons,
        volume_history=volume_history,
        new_columns=new_columns,
        removed_columns=removed_columns,
        altered_columns=altered_columns,
        nulled_columns=nulled_columns,
        parquet_changes=parquet_changes,
        relational_changes=relational_changes,
        drift_severity=drift_severity,
        downstream_impact=downstream_impact,
    )


def _select_catalog_rows(
    session: Session,
    *,
    datasource_id: int,
    schema_name: str | None = None,
    table_name: str | None = None,
):
    stmt = (
        select(
            TableEntity.id.label("table_id"),
            TableEntity.name.label("table_name"),
            TableEntity.table_type.label("table_type"),
            Schema.name.label("schema_name"),
            Database.id.label("database_id"),
            Database.name.label("database_name"),
            DataSource.id.label("datasource_id"),
            DataSource.name.label("datasource_name"),
        )
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(DataSource.id == datasource_id)
        .order_by(Schema.name.asc(), TableEntity.name.asc(), TableEntity.id.asc())
    )
    if schema_name:
        stmt = stmt.where(Schema.name == schema_name)
    if table_name:
        stmt = stmt.where(TableEntity.name == table_name)
    return session.execute(stmt).mappings().all()


def _load_profiles_map(session: Session, *, table_ids: list[int], current_user) -> dict[int, TableProfile]:
    now = datetime.now(timezone.utc)
    profiles, _ = load_dashboard_profiles_with_fallback(session, now, table_ids=table_ids, current_user=current_user)
    return {profile.table_id: profile for profile in profiles}


def _load_dq_latest(session: Session, *, table_id: int, current_user):
    try:
        payload = get_latest_metrics_by_table_id(db=session, table_id=table_id, history_runs=8, current_user=current_user)
        return payload.model_dump() if hasattr(payload, "model_dump") else payload
    except Exception:
        return None


def _load_dq_artifacts(session: Session, *, table_id: int) -> dict[str, Any]:
    try:
        return load_filtered_observability_artifacts(session, table_id=table_id, limit=4, artifact_type="all")
    except Exception:
        return {"baselines": [], "events": [], "evidence_samples": []}


def _build_filter_options(profiles: list[TableProfile]) -> ObservabilityFilterOptionsOut:
    domains = sorted({str(profile.domain_name).strip() for profile in profiles if profile.domain_name and str(profile.domain_name).strip()})
    layers = sorted({str(profile.schema_name).strip() for profile in profiles if profile.schema_name and str(profile.schema_name).strip()})
    return ObservabilityFilterOptionsOut(domains=domains, layers=layers)


def _merge_related_signals(items: list[ObservabilityAssetOut]) -> ObservabilityRelatedSignalsOut:
    groups: dict[str, list[ObservabilityAssetOut]] = defaultdict(list)
    for item in items:
        groups[item.source_origin].append(item)
    return ObservabilityRelatedSignalsOut(
        airflow=groups.get("airflow", []),
        certification=groups.get("certification", []),
        data_lake=groups.get("data_lake", []),
        dq=groups.get("dq", []),
        datasource_scan=groups.get("datasource_scan", []),
        incident=groups.get("incident", []),
        ingestion=groups.get("ingestion", []),
        metabase=groups.get("metabase", []),
        seed=groups.get("seed", []),
        privacy=groups.get("privacy", []),
        stale_scan=groups.get("stale_scan", []),
        unknown=groups.get("unknown", []),
    )


def _related_signal(
    asset: ObservabilityAssetOut,
    *,
    source_origin: str,
    linked_by: str,
    confidence: int,
    summary: str,
    context_state: str = "related",
) -> ObservabilityAssetOut:
    return _signal_copy(
        asset,
        source_origin=source_origin,
        linked_by=linked_by,
        linked_confidence=confidence,
        confidence=confidence,
        context_state=context_state,
        summary=summary,
    )


def build_observability_overview(
    session: Session,
    *,
    datasource_id: int,
    current_user,
    schema_name: str | None = None,
    table_name: str | None = None,
    page: int = 1,
    page_size: int = 10,
) -> ObservabilityOverviewOut:
    datasource = session.get(DataSource, datasource_id)
    if datasource is None:
        raise KeyError(datasource_id)
    catalog_rows = _select_catalog_rows(session, datasource_id=datasource_id, schema_name=schema_name, table_name=table_name)
    table_ids = [int(row["table_id"]) for row in catalog_rows]
    profile_map = _load_profiles_map(session, table_ids=table_ids, current_user=current_user)
    profiles = [profile_map[table_id] for table_id in table_ids if table_id in profile_map]
    total = len(profiles)
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 10), 50), 1)
    start = (page - 1) * page_size
    page_profiles = profiles[start : start + page_size]

    items: list[ObservabilityAssetOut] = []
    related: list[ObservabilityAssetOut] = []
    unlinked: list[ObservabilityAssetOut] = []
    page_schema_drift = 0
    page_volume_anomaly = 0
    page_pipeline_failures = 0
    for profile in page_profiles:
        dq_latest = _load_dq_latest(session, table_id=profile.table_id, current_user=current_user)
        dq_artifacts = _load_dq_artifacts(session, table_id=profile.table_id)
        ingestion_summary = load_table_ingestion_summary_from_source(
            session,
            schema_name=profile.schema_name,
            table_name=profile.table_name,
        )
        ingestion_detail = load_table_ingestion_detail_from_source(
            session,
            schema_name=profile.schema_name,
            table_name=profile.table_name,
            page=1,
            page_size=10,
        )
        metabase_consumption = None
        try:
            metabase_consumption = get_table_metabase_consumption(session, profile.table_id).model_dump()
        except Exception:
            metabase_consumption = None
        item = _build_asset_record(
            profile=profile,
            dq_latest=dq_latest,
            dq_artifacts=dq_artifacts,
            ingestion_summary=ingestion_summary,
            ingestion_detail=ingestion_detail,
            observability_summary=None,
            metabase_consumption=metabase_consumption,
            selected=True,
        )
        items.append(item)
        if item.schema_drift_detected:
            page_schema_drift += 1
        if item.volume_status in {"attention", "critical"}:
            page_volume_anomaly += 1
        if item.pipeline_status in {"blocked", "critical"}:
            page_pipeline_failures += 1
        ingestion_linked = bool(ingestion_summary.get("linked"))
        dashboards_count = int((metabase_consumption or {}).get("dashboards_count") or 0)
        has_metabase = bool(metabase_consumption and metabase_consumption.get("available") and dashboards_count > 0)
        has_dq_events = bool(dq_artifacts.get("events"))
        has_airflow = bool(ingestion_summary.get("primary_pipeline"))
        has_certification = bool(profile.certification_status and profile.certification_status != "not_eligible")
        has_privacy = bool(profile.privacy_reviewed_at or profile.has_personal_data or profile.has_sensitive_personal_data)

        related.append(
            _related_signal(
                item,
                source_origin="data_lake",
                linked_by="datasource_schema_table",
                confidence=95 if ingestion_linked else 55,
                summary="Camada física do Data Lake associada ao ativo.",
            )
        )
        related.append(
            _related_signal(
                item,
                source_origin="dq",
                linked_by="table_id",
                confidence=100 if has_dq_events else max(70, int(item.quality_score or item.observability_score)),
                summary="Sinais de Data Quality ligados ao ativo.",
            )
        )
        if has_airflow:
            pipeline_state = str((ingestion_summary.get("primary_pipeline") or {}).get("latest_status_label") or "Pipeline operacional")
            related.append(
                _related_signal(
                    item,
                    source_origin="airflow",
                    linked_by="airflow_dag",
                    confidence=90 if item.pipeline_status == "healthy" else 75 if item.pipeline_status == "attention" else 55,
                    summary=f"Pipeline operacional vinculada: {pipeline_state}.",
                )
            )
        if ingestion_linked:
            related.append(
                _related_signal(
                    item,
                    source_origin="ingestion",
                    linked_by="ingestion_log",
                    confidence=85,
                    summary="Sinal operacional vinculado ao pipeline de ingestão.",
                )
            )
        else:
            unlinked.append(
                _related_signal(
                    item,
                    source_origin="stale_scan",
                    linked_by="name_only",
                    confidence=35,
                    summary="Tabela encontrada no catálogo, mas sem vínculo operacional de ingestão.",
                    context_state="unlinked",
                )
            )
        if has_metabase:
            related.append(
                _related_signal(
                    item,
                    source_origin="metabase",
                    linked_by="metabase_sql",
                    confidence=75,
                    summary="Consumo analítico vinculado ao ativo.",
                )
            )
        elif profile.search_clicks_30d > 0:
            unlinked.append(
                _related_signal(
                    item,
                    source_origin="unknown",
                    linked_by="name_only",
                    confidence=25,
                    summary="Há interesse de consumo, mas o vínculo analítico ainda não foi confirmado.",
                    context_state="unlinked",
                )
            )
        if has_certification:
            related.append(
                _related_signal(
                    item,
                    source_origin="certification",
                    linked_by="canonical_asset_id",
                    confidence=95 if profile.certification_status == "certified" else 70,
                    summary="Estado de certificação encontrado para o ativo.",
                )
            )
        if has_privacy:
            related.append(
                _related_signal(
                    item,
                    source_origin="privacy",
                    linked_by="canonical_asset_id",
                    confidence=90 if profile.privacy_reviewed_at else 65,
                    summary="Sinal de privacidade e classificação associado ao ativo.",
                )
            )
        if profile.open_incidents > 0:
            related.append(
                _related_signal(
                    item,
                    source_origin="incident",
                    linked_by="table_id",
                    confidence=100,
                    summary="Incidentes operacionais abertos associados ao ativo.",
                )
            )

    summary = ObservabilitySummaryOut(
        total=total,
        healthy=sum(1 for item in profiles if _status_from_freshness(item.freshness_seconds) == "healthy"),
        attention=sum(1 for item in profiles if _status_from_freshness(item.freshness_seconds) == "late"),
        critical=sum(1 for item in profiles if _status_from_freshness(item.freshness_seconds) == "critical"),
        out_of_sla=sum(1 for item in profiles if _status_from_freshness(item.freshness_seconds) in {"late", "critical"}),
        schema_drift=page_schema_drift,
        volume_anomaly=page_volume_anomaly,
        pipeline_failures=page_pipeline_failures,
    )
    diagnostics = ObservabilityDiagnosticsOut(
        selected_assets=len(items),
        out_of_scope_assets=0,
        related_signals=len(related),
        unlinked_signals=len(unlinked),
    )
    return ObservabilityOverviewOut(
        context=ObservabilityContextOut(
            datasource_id=datasource.id,
            datasource_name=datasource.name,
            scope="datasource",
            schema_name=schema_name,
            table_name=table_name,
        ),
        items=items,
        related_signals=_merge_related_signals(related),
        page=page,
        page_size=page_size,
        total=total,
        summary=summary,
        diagnostics=diagnostics,
        filter_options=_build_filter_options(profiles),
        out_of_scope_assets=[],
        unlinked_signals=unlinked,
    )


def build_observability_asset_detail(
    session: Session,
    *,
    table_id: int,
    current_user,
) -> ObservabilityAssetDetailOut:
    row = (
        session.execute(
            select(
                TableEntity.id.label("table_id"),
                TableEntity.name.label("table_name"),
                TableEntity.table_type.label("table_type"),
                Schema.name.label("schema_name"),
                Database.id.label("database_id"),
                Database.name.label("database_name"),
                DataSource.id.label("datasource_id"),
                DataSource.name.label("datasource_name"),
            )
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .join(DataSource, Database.datasource_id == DataSource.id)
            .where(TableEntity.id == table_id)
        )
        .mappings()
        .first()
    )
    if row is None:
        raise KeyError(table_id)
    profile_map = _load_profiles_map(session, table_ids=[table_id], current_user=current_user)
    profile = profile_map.get(table_id)
    if profile is None:
        raise KeyError(table_id)
    dq_latest_payload = _load_dq_latest(session, table_id=table_id, current_user=current_user)
    dq_artifacts = _load_dq_artifacts(session, table_id=table_id)
    ingestion_summary = load_table_ingestion_summary_from_source(session, schema_name=profile.schema_name, table_name=profile.table_name)
    ingestion_detail = load_table_ingestion_detail_from_source(
        session,
        schema_name=profile.schema_name,
        table_name=profile.table_name,
        page=1,
        page_size=10,
    )
    metabase_consumption = None
    try:
        metabase_consumption = get_table_metabase_consumption(session, table_id).model_dump()
    except Exception:
        metabase_consumption = None
    operational_context = None
    try:
        operational_context = load_table_operational_context(
            session,
            table_id=table_id,
            datasource_id=profile.datasource_id,
            database_id=profile.database_id,
            schema_id=profile.schema_id,
        )
    except Exception:
        operational_context = None
    asset = _build_asset_record(
        profile=profile,
        dq_latest=dq_latest_payload,
        dq_artifacts=dq_artifacts,
        ingestion_summary=ingestion_summary,
        ingestion_detail=ingestion_detail,
        observability_summary=None,
        metabase_consumption=metabase_consumption,
        selected=True,
    )
    return ObservabilityAssetDetailOut(
        **asset.model_dump(),
        dq_latest=dq_latest_payload,
        dq_artifacts=dq_artifacts,
        ingestion_summary=ingestion_summary,
        ingestion_detail=ingestion_detail,
        metabase_consumption=metabase_consumption,
        operational_context=operational_context,
    )
