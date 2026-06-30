from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.ingestion import IngestionIntegrationUnavailable, load_table_ingestion_detail, operational_session_for_datasource
from t2c_data.models.catalog import TableEntity

FAILURE_PENALTY_POINTS = 15
STALE_PENALTY_POINTS = 8
RECURRENT_DEGRADATION_EXTRA_POINTS = 5


def apply_operational_dq_policy(
    db: Session,
    *,
    table: TableEntity,
    payload: dict[str, Any],
) -> dict[str, Any]:
    raw_score = payload.get("dq_score")
    current = payload.get("current")
    if isinstance(current, dict):
        row_count = int(current.get("row_count") or 0)
        if row_count <= 0:
            no_data_state = {
                "code": "no_data",
                "label": "Sem dados",
                "tone": "neutral",
                "score": None,
                "reason": "A última execução não encontrou linhas para avaliar.",
            }
            payload["effective_dq_score"] = None
            payload["operational_penalty_points"] = 0
            payload["operational_penalty_label"] = "Sem dados no último profiling"
            payload["operational_penalty_applied"] = False
            payload["operational_recurrent_degradation"] = False
            payload["assessment_state"] = no_data_state
            current["effective_dq_score"] = None
            current["assessment_state"] = no_data_state
            if isinstance(payload.get("observability"), dict):
                payload["observability"]["assessment_state"] = no_data_state
                table_section = payload["observability"].get("table")
                if isinstance(table_section, dict):
                    table_section["status"] = "no_data"
                    table_section["status_label"] = "Sem dados"
            return payload
    if raw_score is None:
        payload["effective_dq_score"] = None
        payload["operational_penalty_points"] = 0
        payload["operational_penalty_label"] = None
        payload["operational_penalty_applied"] = False
        payload["operational_recurrent_degradation"] = False
        return payload

    settings_snapshot = get_governance_settings_snapshot(db)
    operational_detail: dict[str, Any] | None = None
    try:
        datasource = table.schema.database.datasource
        with operational_session_for_datasource(datasource) as operational_db:
            operational_detail = load_table_ingestion_detail(
                operational_db,
                schema_name=table.schema.name,
                table_name=table.name,
                page=1,
                page_size=10,
                airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
            )
    except IngestionIntegrationUnavailable:
        operational_detail = None

    primary_pipeline = (operational_detail or {}).get("summary", {}).get("primary_pipeline") if operational_detail else None
    stability = (operational_detail or {}).get("stability") if operational_detail else None

    penalty_points = 0
    reasons: list[str] = []
    has_failure = bool(
        isinstance(primary_pipeline, dict)
        and (
            str(primary_pipeline.get("latest_status_label") or "").strip() == "Falha"
            or primary_pipeline.get("last_error")
        )
    )
    is_stale = bool(isinstance(stability, dict) and stability.get("currently_stale"))
    recurrent = bool(isinstance(stability, dict) and stability.get("recurrent_degradation"))

    if has_failure:
        penalty_points += settings_snapshot.dq_operational_failure_penalty_points
        reasons.append(f"-{settings_snapshot.dq_operational_failure_penalty_points} por falha operacional ativa")
    elif is_stale:
        penalty_points += settings_snapshot.dq_operational_stale_penalty_points
        reasons.append(f"-{settings_snapshot.dq_operational_stale_penalty_points} por pipeline sem sucesso recente")

    if recurrent:
        penalty_points += settings_snapshot.dq_operational_recurrent_penalty_points
        reasons.append(f"-{settings_snapshot.dq_operational_recurrent_penalty_points} por degradação recorrente")

    effective_score = max(round(float(raw_score) - penalty_points, 1), 0.0)
    penalty_label = " + ".join(reasons) if reasons else None

    payload["effective_dq_score"] = effective_score
    payload["operational_penalty_points"] = penalty_points
    payload["operational_penalty_label"] = penalty_label
    payload["operational_penalty_applied"] = penalty_points > 0
    payload["operational_recurrent_degradation"] = recurrent

    if isinstance(current, dict):
        current["effective_dq_score"] = effective_score
        current["operational_penalty_points"] = penalty_points
        current["operational_penalty_label"] = penalty_label
        current["operational_penalty_applied"] = penalty_points > 0
        current["operational_recurrent_degradation"] = recurrent
        if effective_score is None:
            current["assessment_state"] = {
                "code": "not_calculable",
                "label": "Não calculável",
                "tone": "neutral",
                "score": None,
                "reason": "A leitura operacional não permitiu calcular a pontuação efetiva.",
            }

    previous = payload.get("previous")
    if isinstance(previous, dict):
        previous.setdefault("effective_dq_score", previous.get("dq_score"))
        previous.setdefault("operational_penalty_points", 0)
        previous.setdefault("operational_penalty_label", None)
        previous.setdefault("operational_penalty_applied", False)
        previous.setdefault("operational_recurrent_degradation", False)

    observability = payload.get("observability")
    if isinstance(observability, dict):
        table_section = observability.get("table")
        if isinstance(table_section, dict):
            table_section["status"] = "healthy" if penalty_points <= 0 else "degraded"
            table_section["status_label"] = "Saudável" if penalty_points <= 0 else "Degradado"
            table_section["operational_penalty_points"] = penalty_points
            table_section["operational_penalty_label"] = penalty_label
        assessment_state = observability.get("assessment_state")
        if isinstance(assessment_state, dict) and penalty_points > 0:
            assessment_state["code"] = "degraded"
            assessment_state["label"] = "Degradado"
            assessment_state["tone"] = "warning"
            assessment_state["score"] = effective_score
            assessment_state["reason"] = (
                "A leitura operacional reduziu temporariamente a confiança do score."
                if penalty_points > 0
                else assessment_state.get("reason")
            )

    return payload


__all__ = [
    "FAILURE_PENALTY_POINTS",
    "STALE_PENALTY_POINTS",
    "RECURRENT_DEGRADATION_EXTRA_POINTS",
    "apply_operational_dq_policy",
]
