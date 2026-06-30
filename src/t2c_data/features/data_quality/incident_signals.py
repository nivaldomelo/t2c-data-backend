from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.data_quality.notifications import notify_dq_profile_issue
from t2c_data.models.catalog import Schema, TableEntity
from t2c_data.models.dq import DQRun, DQTableMetric
from t2c_data.models.incident import Incident

_SEVERITY_LABELS = {
    "sev1": "Crítico",
    "sev2": "Alto",
    "sev3": "Médio",
    "sev4": "Baixo",
}


def _severity_from_score(dq_score: float, failed_rules: int, sensitive: bool) -> str:
    if dq_score < 55 or failed_rules >= 3 or (sensitive and dq_score < 70):
        return "sev1"
    if dq_score < 70 or failed_rules >= 1:
        return "sev2"
    return "sev3"


def _trigger_suggestions(
    *,
    dq_score: float,
    failed_rules: int,
    duplicates_count: int,
    previous_score: float | None,
    sensitive: bool,
) -> list[dict[str, object]]:
    suggestions: list[dict[str, object]] = []
    drop = (previous_score - dq_score) if previous_score is not None else None

    if dq_score < 60:
        suggestions.append(
            {
                "key": "dq_score_critical",
                "mode": "automatic",
                "title": "Score de DQ crítico",
                "detail": f"O score caiu para {round(dq_score, 1)} pontos, abaixo do limiar crítico.",
                "severity": "sev1",
                "severity_label": _SEVERITY_LABELS["sev1"],
                "trigger_code": "dq_score_below_60",
            }
        )
    elif dq_score < 70:
        suggestions.append(
            {
                "key": "dq_score_warning",
                "mode": "suggested",
                "title": "Score de DQ abaixo do esperado",
                "detail": f"O score atual é {round(dq_score, 1)} pontos e merece avaliação operacional.",
                "severity": "sev2",
                "severity_label": _SEVERITY_LABELS["sev2"],
                "trigger_code": "dq_score_below_70",
            }
        )

    if failed_rules >= 3:
        suggestions.append(
            {
                "key": "failed_rules_critical",
                "mode": "automatic",
                "title": "Falhas graves e recorrentes em regras",
                "detail": f"Foram registradas {failed_rules} falhas de regra no último profiling.",
                "severity": "sev1",
                "severity_label": _SEVERITY_LABELS["sev1"],
                "trigger_code": "failed_rules_high",
            }
        )
    elif failed_rules > 0:
        suggestions.append(
            {
                "key": "failed_rules_warning",
                "mode": "suggested",
                "title": "Falhas de regra detectadas",
                "detail": f"O ativo apresentou {failed_rules} regra(s) com falha no último ciclo.",
                "severity": "sev2",
                "severity_label": _SEVERITY_LABELS["sev2"],
                "trigger_code": "failed_rules_present",
            }
        )

    if drop is not None and drop >= 20:
        suggestions.append(
            {
                "key": "abrupt_drop",
                "mode": "automatic",
                "title": "Queda abrupta de score",
                "detail": f"O score caiu {round(drop, 1)} pontos em relação ao run anterior.",
                "severity": "sev1",
                "severity_label": _SEVERITY_LABELS["sev1"],
                "trigger_code": "dq_abrupt_drop",
            }
        )
    elif drop is not None and drop >= 10:
        suggestions.append(
            {
                "key": "moderate_drop",
                "mode": "suggested",
                "title": "Queda relevante de qualidade",
                "detail": f"O score caiu {round(drop, 1)} pontos em relação ao run anterior.",
                "severity": "sev2",
                "severity_label": _SEVERITY_LABELS["sev2"],
                "trigger_code": "dq_relevant_drop",
            }
        )

    if sensitive and dq_score < 70:
        suggestions.append(
            {
                "key": "sensitive_asset",
                "mode": "automatic",
                "title": "Ativo sensível com falha relevante",
                "detail": "O ativo possui sensibilidade elevada e precisa de tratamento imediato quando a qualidade degrada.",
                "severity": "sev1",
                "severity_label": _SEVERITY_LABELS["sev1"],
                "trigger_code": "sensitive_asset_dq_issue",
            }
        )

    if duplicates_count > 0 and dq_score < 85:
        suggestions.append(
            {
                "key": "duplicates_detected",
                "mode": "suggested",
                "title": "Duplicidade detectada com impacto potencial",
                "detail": f"Foram identificados {duplicates_count} registros duplicados no recorte atual.",
                "severity": "sev3",
                "severity_label": _SEVERITY_LABELS["sev3"],
                "trigger_code": "duplicates_detected",
            }
        )

    return suggestions


def _current_table_fqn(session: Session, table_id: int) -> tuple[TableEntity, str]:
    row = session.execute(
        select(TableEntity, Schema.name.label("schema_name"))
        .join(Schema, TableEntity.schema_id == Schema.id)
        .where(TableEntity.id == table_id)
    ).first()
    if not row:
        raise ValueError("Table not found")
    table, schema_name = row
    return table, f"{schema_name}.{table.name}"


def _latest_metrics(session: Session, table_id: int) -> tuple[DQTableMetric | None, float | None]:
    rows = session.execute(
        select(DQTableMetric)
        .where(DQTableMetric.table_id == table_id)
        .order_by(desc(DQTableMetric.id))
        .limit(2)
    ).scalars().all()
    current = rows[0] if rows else None
    previous_score = float(rows[1].dq_score) if len(rows) > 1 and rows[1].dq_score is not None else None
    return current, previous_score


def _recent_failed_run_count(session: Session, table_id: int, *, days: int = 30) -> int:
    since = datetime.now(timezone.utc) - timedelta(days=max(days, 1))
    return int(
        session.scalar(
            select(func.count(DQRun.id)).where(
                DQRun.table_id == table_id,
                DQRun.status == "failed",
                DQRun.created_at >= since,
            )
        )
        or 0
    )


def upsert_incident_for_dq_profile(
    session: Session,
    *,
    table: TableEntity,
    table_fqn: str,
    dq_run: DQRun,
    table_metric: DQTableMetric,
    reporter_user_id: int | None,
    trigger_codes: list[str],
    previous_score: float | None,
) -> Incident:
    now = datetime.now(timezone.utc)
    sensitive = bool(table.sensitivity_level) or bool(table.has_personal_data) or bool(table.has_sensitive_personal_data)
    severity = _severity_from_score(float(table_metric.dq_score or 0.0), int(table_metric.failed_rules or 0), sensitive)
    description = (
        f"Data Quality do ativo {table_fqn} exige tratamento operacional. "
        f"Score atual: {round(float(table_metric.dq_score or 0.0), 1)}."
    )
    evidence_payload = {
        "origin": "dq",
        "origin_mode": "automatic",
        "table_id": table.id,
        "dq_run_id": dq_run.id,
        "dq_table_metric_id": table_metric.id,
        "dq_score": float(table_metric.dq_score or 0.0),
        "failed_rules": int(table_metric.failed_rules or 0),
        "duplicates_count": int(table_metric.duplicates_count or 0),
        "previous_dq_score": previous_score,
        "trigger_codes": trigger_codes,
        "evaluated_at": now.isoformat(),
    }
    existing = session.scalar(
        select(Incident)
        .where(
            Incident.entity_type == "table",
            Incident.table_fqn == table_fqn,
            Incident.source_type == "dq_profile",
            Incident.status.in_(["open", "investigating"]),
        )
        .order_by(desc(Incident.updated_at))
        .limit(1)
    )
    if existing:
        existing.last_seen_at = now
        existing.description = description
        existing.severity = severity
        existing.evidence_json = json.loads(json.dumps(evidence_payload, default=str))
        existing.occurrences = int(existing.occurrences or 0) + 1
        session.add(existing)
        try:
            notify_dq_profile_issue(
                session,
                table=table,
                dq_run=dq_run,
                table_metric=table_metric,
                reporter_user_id=reporter_user_id,
                trigger_codes=trigger_codes,
                previous_score=previous_score,
                incident_id=existing.id,
            )
        except Exception:
            pass
        return existing

    incident = Incident(
        title=f"DQ crítico em {table.name}",
        description=description,
        entity_type="table",
        table_fqn=table_fqn,
        airflow_dag_id=None,
        detected_at=now,
        last_seen_at=now,
        status="open",
        severity=severity,
        owner_user_id=None,
        reporter_user_id=reporter_user_id,
        tags=["dq", "automatic"],
        source_type="dq_profile",
        source_ref_id=table.id,
        evidence_json=json.loads(json.dumps(evidence_payload, default=str)),
        occurrences=1,
    )
    session.add(incident)
    session.flush()
    try:
        notify_dq_profile_issue(
            session,
            table=table,
            dq_run=dq_run,
            table_metric=table_metric,
            reporter_user_id=reporter_user_id,
            trigger_codes=trigger_codes,
            previous_score=previous_score,
            incident_id=incident.id,
        )
    except Exception:
        pass
    return incident


def evaluate_table_dq_incident_signals(session: Session, *, table_id: int) -> dict[str, object]:
    table, table_fqn = _current_table_fqn(session, table_id)
    current, previous_score = _latest_metrics(session, table_id)
    links = build_asset_links(
        table_id=table.id,
        datasource_id=table.schema.database.datasource_id,
        database_id=table.schema.database_id,
        schema_id=table.schema_id,
        data_owner_id=table.data_owner_id,
    )
    if current is None:
        return {
            "table_id": table.id,
            "generated_incident_id": None,
            "generated_mode": None,
            "open_incidents": 0,
            "suggestions": [],
            "links": links,
        }
    sensitive = bool(table.sensitivity_level) or bool(table.has_personal_data) or bool(table.has_sensitive_personal_data)
    recent_failed_runs = _recent_failed_run_count(session, table.id)
    suggestions = _trigger_suggestions(
        dq_score=float(current.dq_score or 0.0),
        failed_rules=int(current.failed_rules or 0),
        duplicates_count=int(current.duplicates_count or 0),
        previous_score=previous_score,
        sensitive=sensitive,
    )
    if (table.certification_criticality or "").strip().lower() in {"high", "critical"} and recent_failed_runs >= 2:
        suggestions.append(
            {
                "key": "recurring_dq_failure_critical",
                "mode": "automatic",
                "title": "Falha DQ recorrente em ativo crítico",
                "detail": f"Foram identificadas {recent_failed_runs} falhas de DQ recentes em um ativo crítico.",
                "severity": "sev1",
                "severity_label": _SEVERITY_LABELS["sev1"],
                "trigger_code": "dq_recurrent_failure_critical",
            }
        )
    existing_incident = session.scalar(
        select(Incident)
        .where(
            Incident.entity_type == "table",
            Incident.table_fqn == table_fqn,
            Incident.status.in_(["open", "investigating"]),
        )
        .order_by(desc(Incident.updated_at))
        .limit(1)
    )
    for suggestion in suggestions:
        suggestion["existing_incident_id"] = existing_incident.id if existing_incident else None
    return {
        "table_id": table.id,
        "generated_incident_id": existing_incident.id if existing_incident and existing_incident.source_type == "dq_profile" else None,
        "generated_mode": "automatic" if existing_incident and existing_incident.source_type == "dq_profile" else None,
        "open_incidents": 1 if existing_incident and existing_incident.status in {"open", "investigating"} else 0,
        "suggestions": suggestions,
        "links": links,
    }


def handle_profiling_incident_signals(
    session: Session,
    *,
    table: TableEntity,
    schema_name: str,
    dq_run: DQRun,
    table_metric: DQTableMetric,
    reporter_user_id: int | None,
) -> Incident | None:
    previous = session.execute(
        select(DQTableMetric)
        .where(DQTableMetric.table_id == table.id, DQTableMetric.id != table_metric.id)
        .order_by(desc(DQTableMetric.id))
        .limit(1)
    ).scalars().first()
    previous_score = float(previous.dq_score) if previous and previous.dq_score is not None else None
    sensitive = bool(table.sensitivity_level) or bool(table.has_personal_data) or bool(table.has_sensitive_personal_data)
    recent_failed_runs = _recent_failed_run_count(session, table.id)
    suggestions = _trigger_suggestions(
        dq_score=float(table_metric.dq_score or 0.0),
        failed_rules=int(table_metric.failed_rules or 0),
        duplicates_count=int(table_metric.duplicates_count or 0),
        previous_score=previous_score,
        sensitive=sensitive,
    )
    if (table.certification_criticality or "").strip().lower() in {"high", "critical"} and recent_failed_runs >= 2:
        suggestions.append(
            {
                "key": "recurring_dq_failure_critical",
                "mode": "automatic",
                "title": "Falha DQ recorrente em ativo crítico",
                "detail": f"Foram identificadas {recent_failed_runs} falhas de DQ recentes em um ativo crítico.",
                "severity": "sev1",
                "severity_label": _SEVERITY_LABELS["sev1"],
                "trigger_code": "dq_recurrent_failure_critical",
            }
        )
    automatic = [item for item in suggestions if item["mode"] == "automatic"]
    if not automatic:
        return None
    return upsert_incident_for_dq_profile(
        session,
        table=table,
        table_fqn=f"{schema_name}.{table.name}",
        dq_run=dq_run,
        table_metric=table_metric,
        reporter_user_id=reporter_user_id,
        trigger_codes=[str(item["trigger_code"]) for item in automatic],
        previous_score=previous_score,
    )


__all__ = [
    "evaluate_table_dq_incident_signals",
    "handle_profiling_incident_signals",
    "upsert_incident_for_dq_profile",
]
