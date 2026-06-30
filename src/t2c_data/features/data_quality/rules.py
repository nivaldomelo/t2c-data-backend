from __future__ import annotations

from datetime import datetime, timezone
import json

from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from t2c_data.features.data_quality.queries import resolve_table_context_by_fqn
from t2c_data.features.governance.column_classification import build_column_classification_map
from t2c_data.features.platform.sensitive_data import mask_row_by_classification
from t2c_data.models.dq import DQRule, DQRuleRun, DQRun
from t2c_data.models.incident import Incident
from t2c_data.services.data_quality import spark_only_execution_message


def _dimension_from_rule_type(rule_type: str) -> str:
    normalized = rule_type.strip().lower()
    return {
        "nullability": "completude",
        "column_validation": "validade",
        "domain": "validade",
        "uniqueness": "unicidade",
        "freshness": "tempestividade",
        "column_comparison": "consistencia",
        "reconciliation": "acuracia",
    }.get(normalized, "validade")


def _severity_to_incident(value: str) -> str:
    normalized = value.lower().strip()
    if normalized == "critical":
        return "sev1"
    if normalized == "high":
        return "sev2"
    if normalized == "medium":
        return "sev3"
    return "sev4"


def upsert_incident_for_dq_rule(
    session: Session,
    rule: DQRule,
    *,
    violations_count: int,
    preview_rows: list[dict],
    run_id: int | None,
    reporter_user_id: int | None = None,
) -> Incident | None:
    try:
        table, _schema, _database, _datasource = resolve_table_context_by_fqn(session, rule.table_fqn)
    except Exception:  # noqa: BLE001
        table = None
    try:
        columns_count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = 't2c_ops'
                  AND table_name = 'incidents'
                  AND column_name IN ('source_type', 'source_ref_id', 'evidence_json', 'occurrences', 'last_seen_at')
                """
            )
        ).scalar_one()
    except DBAPIError:
        return None
    if int(columns_count or 0) < 5:
        return None

    now = datetime.now(timezone.utc)
    rule_definition = getattr(rule, "rule_definition_json", None)
    definition = rule_definition if isinstance(rule_definition, dict) else {}
    rule_type = str(getattr(rule, "rule_type", "") or definition.get("type") or "").strip()
    dimension = str(definition.get("dimension") or "").strip() or _dimension_from_rule_type(rule_type)
    template_key = str(definition.get("template_key") or "").strip() or None
    column_classifications = build_column_classification_map(session, table_id=table.id, key_by="name") if table else None
    masked_preview_rows = [
        mask_row_by_classification(
            row,
            can_view_sensitive=False,
            sensitivity_level=getattr(table, "sensitivity_level", None) if table else None,
            has_personal_data=bool(getattr(table, "has_personal_data", False)) if table else False,
            has_sensitive_personal_data=bool(getattr(table, "has_sensitive_personal_data", False)) if table else False,
            column_classifications=column_classifications,
        )
        for row in preview_rows
        if isinstance(row, dict)
    ]
    evidence_payload = {
        "origin": "dq",
        "origin_mode": "automatic",
        "dq_rule_id": rule.id,
        "table_id": table.id if table else None,
        "quality_dimension": dimension,
        "template_key": template_key,
        "violations_count": violations_count,
        "sample_rows": json.loads(json.dumps(masked_preview_rows, default=str)) if masked_preview_rows else [],
        "dq_rule_run_id": run_id,
        "executed_at": now.isoformat(),
    }
    description = (
        f"Regra '{rule.name}' violada em {now.isoformat()} com "
        f"{violations_count} violações (run #{run_id}). Dimensão: {dimension}."
    )

    try:
        existing = session.scalar(
            select(Incident)
            .where(
                Incident.source_type == "dq_rule",
                Incident.source_ref_id == rule.id,
                Incident.status.in_(["open", "investigating"]),
            )
            .order_by(Incident.updated_at.desc())
            .limit(1)
        )
    except DBAPIError:
        return None
    if existing:
        existing.last_seen_at = now
        existing.description = description
        existing.evidence_json = evidence_payload
        existing.occurrences = int(existing.occurrences or 0) + 1
        session.add(existing)
        return existing

    incident = Incident(
        title=f"DQ Rule Violada: {rule.name}",
        description=description,
        entity_type="table",
        table_fqn=rule.table_fqn,
        airflow_dag_id=None,
        detected_at=now,
        last_seen_at=now,
        status="open",
        severity=_severity_to_incident(rule.severity),
        owner_user_id=None,
        reporter_user_id=reporter_user_id,
        tags=["dq"],
        source_type="dq_rule",
        source_ref_id=rule.id,
        evidence_json=evidence_payload,
        occurrences=1,
    )
    try:
        session.add(incident)
    except DBAPIError:
        return None
    return incident


def run_dq_rule(
    session: Session,
    rule: DQRule,
    *,
    reporter_user_id: int | None = None,
    save_run: bool = True,
    execution_engine: str = "python",
    dq_run: DQRun | None = None,
) -> tuple[DQRuleRun | None, dict]:
    raise RuntimeError(spark_only_execution_message())


__all__ = [
    "run_dq_rule",
    "upsert_incident_for_dq_rule",
]
