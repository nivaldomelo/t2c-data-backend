from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from sqlalchemy import delete, desc, func, select
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.features.governance.column_classification import build_column_classification_map
from t2c_data.features.platform.sensitive_data import mask_row_by_classification, mask_sensitive_value
from t2c_data.models.catalog import TableEntity
from t2c_data.models.dq import (
    DQColumnMetric,
    DQEvidenceSample,
    DQObservabilityBaseline,
    DQObservabilityEvent,
    DQRun,
    DQTableMetric,
)

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utcify(value: datetime | None) -> datetime:
    if value is None:
        return _now()
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _median_or_none(values: Iterable[float | int | None]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return float(median(cleaned))


def _mask_scalar(value: Any, *, field_name: str | None, table: TableEntity) -> dict[str, Any]:
    if value is None:
        return {"value": None, "redacted": False, "visibility": "unavailable", "reason": "no_value"}
    sensitive_context = bool(
        getattr(table, "is_masked", False)
        or getattr(table, "has_personal_data", False)
        or getattr(table, "has_sensitive_personal_data", False)
        or getattr(table, "sensitivity_level", None) in {"confidential", "restricted", "personal_data"}
    )
    if sensitive_context:
        return {"value": "[masked]", "redacted": True, "visibility": "masked", "reason": "sensitive_field"}
    masked_value = mask_sensitive_value(value, field_name=field_name, can_view_sensitive=False)
    if masked_value != value:
        return {"value": masked_value, "redacted": True, "visibility": "masked", "reason": "sensitive_field"}
    if isinstance(value, (str, int, float, bool)):
        json_safe_value: Any = value
    else:
        json_safe_value = str(value)
    return {"value": json_safe_value, "redacted": False, "visibility": "visible", "reason": None}


def mask_evidence_rows(
    rows: list[dict[str, Any]] | None,
    *,
    table: TableEntity,
    column_classifications: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    masked_rows: list[dict[str, Any]] = []
    masked_fields: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        base_row = mask_row_by_classification(
            row,
            can_view_sensitive=False,
            sensitivity_level=getattr(table, "sensitivity_level", None),
            has_personal_data=bool(getattr(table, "has_personal_data", False)),
            has_sensitive_personal_data=bool(getattr(table, "has_sensitive_personal_data", False)),
            column_classifications=column_classifications,
        )
        masked_row: dict[str, Any] = {}
        for key, value in row.items():
            redacted = _mask_scalar(value, field_name=str(key), table=table)
            if base_row.get(str(key)) == "[masked]":
                redacted = {"value": "[masked]", "redacted": True, "visibility": "masked", "reason": "sensitive_field"}
            masked_row[str(key)] = redacted
            if redacted["redacted"]:
                masked_fields.add(str(key))
        masked_rows.append(masked_row)
    return masked_rows, sorted(masked_fields)


def _table_metric_history(session: Session, table_id: int, *, limit: int = 14) -> list[tuple[DQTableMetric, DQRun]]:
    rows = session.execute(
        select(DQTableMetric, DQRun)
        .join(DQRun, DQTableMetric.run_id == DQRun.id)
        .where(DQTableMetric.table_id == table_id, DQRun.status == "success")
        .order_by(DQRun.created_at.desc())
        .limit(max(2, limit))
    ).all()
    return [(table_metric, run) for table_metric, run in rows]


def _column_history(session: Session, table_metric_ids: list[int]) -> dict[str, list[DQColumnMetric]]:
    if not table_metric_ids:
        return {}
    columns = session.scalars(select(DQColumnMetric).where(DQColumnMetric.table_metric_id.in_(table_metric_ids))).all()
    history: dict[str, list[DQColumnMetric]] = {}
    for column in columns:
        history.setdefault(column.column_name, []).append(column)
    return history


def _schema_changes(current_columns: list[DQColumnMetric], previous_columns: list[DQColumnMetric]) -> list[dict[str, Any]]:
    previous_map = {column.column_name: column for column in previous_columns}
    current_map = {column.column_name: column for column in current_columns}
    changes: list[dict[str, Any]] = []
    for name in current_map:
        if name not in previous_map:
            changes.append({"kind": "schema_addition", "column_name": name, "breaking": False})
    for name in previous_map:
        if name not in current_map:
            changes.append({"kind": "schema_removal", "column_name": name, "breaking": True})
    for name, current in current_map.items():
        previous = previous_map.get(name)
        if previous and (current.data_type or "").lower() != (previous.data_type or "").lower():
            changes.append(
                {
                    "kind": "schema_type_change",
                    "column_name": name,
                    "breaking": True,
                    "current_type": current.data_type,
                    "previous_type": previous.data_type,
                }
            )
    return changes


def _upsert_baseline(
    session: Session,
    *,
    dq_run: DQRun,
    table: TableEntity,
    metric_key: str,
    metric_scope: str,
    current_value: float | None,
    history_values: list[float | int | None],
    column_id: int | None = None,
    column_name: str | None = None,
    window_size: int = 14,
    details_json: dict[str, Any] | None = None,
) -> DQObservabilityBaseline:
    cleaned_history = [float(value) for value in history_values if value is not None]
    baseline_value = _median_or_none(cleaned_history)
    row = DQObservabilityBaseline(
        run_id=dq_run.id,
        table_id=table.id,
        column_id=column_id,
        column_name=column_name,
        metric_key=metric_key,
        metric_scope=metric_scope,
        current_value=current_value,
        baseline_value=baseline_value,
        mean_value=(sum(cleaned_history) / len(cleaned_history)) if cleaned_history else None,
        median_value=baseline_value,
        min_value=min(cleaned_history) if cleaned_history else None,
        max_value=max(cleaned_history) if cleaned_history else None,
        tolerance_abs=None,
        tolerance_pct=None,
        window_size=window_size,
        calculated_at=_now(),
        details_json=details_json,
    )
    session.add(row)
    return row


def _record_anomaly(
    session: Session,
    *,
    dq_run: DQRun,
    table: TableEntity,
    metric_key: str,
    event_type: str,
    severity: str,
    observed_value: float | None,
    expected_value: float | None,
    baseline_value: float | None,
    delta_value: float | None,
    delta_pct: float | None,
    dimension_key: str | None = None,
    column_id: int | None = None,
    column_name: str | None = None,
    details_json: dict[str, Any] | None = None,
) -> DQObservabilityEvent:
    event = DQObservabilityEvent(
        run_id=dq_run.id,
        table_id=table.id,
        column_id=column_id,
        column_name=column_name,
        metric_key=metric_key,
        dimension_key=dimension_key,
        event_type=event_type,
        status="open",
        severity=severity,
        observed_value=observed_value,
        expected_value=expected_value,
        baseline_value=baseline_value,
        delta_value=delta_value,
        delta_pct=delta_pct,
        detected_at=_now(),
        details_json=details_json,
    )
    session.add(event)
    return event


def persist_observability_artifacts(
    session: Session,
    *,
    dq_run: DQRun,
    table: TableEntity,
    table_metric: DQTableMetric,
    current_columns: list[DQColumnMetric] | None = None,
    history_limit: int = 14,
) -> dict[str, Any]:
    history = _table_metric_history(session, table.id, limit=history_limit)
    previous_metric_rows = [entry for entry in history if entry[1].id != dq_run.id]
    current_columns = list(current_columns or [])
    previous_metric = previous_metric_rows[0][0] if previous_metric_rows else None
    try:
        previous_columns = list(previous_metric.column_metrics) if previous_metric is not None else []
    except Exception:
        previous_columns = []
    table_row_count_history = [metric.row_count for metric, _run in previous_metric_rows]
    table_score_history = [metric.dq_score for metric, _run in previous_metric_rows]
    completeness_history = [metric.completeness_pct_avg for metric, _run in previous_metric_rows]
    duplicates_history = [metric.duplicates_count for metric, _run in previous_metric_rows]
    failed_rules_history = [metric.failed_rules for metric, _run in previous_metric_rows]
    freshness_history = [max(0, int((_now() - _utcify(run.created_at)).total_seconds())) for _metric, run in previous_metric_rows]

    baselines = [
        _upsert_baseline(
            session,
            dq_run=dq_run,
            table=table,
            metric_key="volume",
            metric_scope="table",
            current_value=float(table_metric.row_count),
            history_values=table_row_count_history,
            details_json={"unit": "rows"},
        ),
        _upsert_baseline(
            session,
            dq_run=dq_run,
            table=table,
            metric_key="dq_score",
            metric_scope="table",
            current_value=float(table_metric.dq_score),
            history_values=table_score_history,
            details_json={"unit": "score"},
        ),
        _upsert_baseline(
            session,
            dq_run=dq_run,
            table=table,
            metric_key="completeness_pct_avg",
            metric_scope="table",
            current_value=float(table_metric.completeness_pct_avg),
            history_values=completeness_history,
            details_json={"unit": "%"},
        ),
        _upsert_baseline(
            session,
            dq_run=dq_run,
            table=table,
            metric_key="duplicates_count",
            metric_scope="table",
            current_value=float(table_metric.duplicates_count),
            history_values=duplicates_history,
            details_json={"unit": "rows"},
        ),
        _upsert_baseline(
            session,
            dq_run=dq_run,
            table=table,
            metric_key="failed_rules",
            metric_scope="table",
            current_value=float(table_metric.failed_rules),
            history_values=failed_rules_history,
            details_json={"unit": "rules"},
        ),
        _upsert_baseline(
            session,
            dq_run=dq_run,
            table=table,
            metric_key="freshness_seconds",
            metric_scope="table",
            current_value=float(max(0, int((_now() - _utcify(dq_run.created_at)).total_seconds()))),
            history_values=freshness_history,
            details_json={"unit": "seconds"},
        ),
    ]

    events: list[DQObservabilityEvent] = []
    baseline_map = {baseline.metric_key: baseline for baseline in baselines}
    current_row_count = float(table_metric.row_count)
    if (baseline_row_count := baseline_map["volume"].baseline_value) not in {None, 0}:
        delta_value = current_row_count - float(baseline_row_count)
        delta_pct = round((delta_value / float(baseline_row_count)) * 100.0, 2) if baseline_row_count else None
        if abs(delta_pct or 0.0) >= 35:
            events.append(
                _record_anomaly(
                    session,
                    dq_run=dq_run,
                    table=table,
                    metric_key="volume",
                    event_type="anomaly",
                    severity="critical" if abs(delta_pct or 0.0) >= 50 else "warning",
                    observed_value=current_row_count,
                    expected_value=float(baseline_row_count),
                    baseline_value=float(baseline_row_count),
                    delta_value=delta_value,
                    delta_pct=delta_pct,
                    dimension_key="volume",
                    details_json={"window_size": baseline_map["volume"].window_size},
                )
            )

    completeness_baseline = baseline_map["completeness_pct_avg"].baseline_value
    if completeness_baseline is not None:
        delta_value = float(table_metric.completeness_pct_avg) - float(completeness_baseline)
        if delta_value <= -10:
            events.append(
                _record_anomaly(
                    session,
                    dq_run=dq_run,
                    table=table,
                    metric_key="completeness_pct_avg",
                    event_type="anomaly",
                    severity="warning",
                    observed_value=float(table_metric.completeness_pct_avg),
                    expected_value=float(completeness_baseline),
                    baseline_value=float(completeness_baseline),
                    delta_value=delta_value,
                    delta_pct=delta_value,
                    dimension_key="completeness",
                )
            )

    freshness_baseline = baseline_map["freshness_seconds"].baseline_value
    current_freshness = float(max(0, int((_now() - _utcify(dq_run.created_at)).total_seconds())))
    if freshness_baseline is not None and current_freshness > float(freshness_baseline) * 1.5:
        delta_value = current_freshness - float(freshness_baseline)
        events.append(
            _record_anomaly(
                session,
                dq_run=dq_run,
                table=table,
                metric_key="freshness_seconds",
                event_type="anomaly",
                severity="warning",
                observed_value=current_freshness,
                expected_value=float(freshness_baseline),
                baseline_value=float(freshness_baseline),
                delta_value=delta_value,
                delta_pct=round((delta_value / float(freshness_baseline)) * 100.0, 2) if freshness_baseline else None,
                dimension_key="freshness",
            )
        )

    duplicates_baseline = baseline_map["duplicates_count"].baseline_value
    if duplicates_baseline is not None:
        delta_value = float(table_metric.duplicates_count) - float(duplicates_baseline)
        if delta_value >= max(2.0, float(duplicates_baseline) * 0.25):
            events.append(
                _record_anomaly(
                    session,
                    dq_run=dq_run,
                    table=table,
                    metric_key="duplicates_count",
                    event_type="anomaly",
                    severity="warning",
                    observed_value=float(table_metric.duplicates_count),
                    expected_value=float(duplicates_baseline),
                    baseline_value=float(duplicates_baseline),
                    delta_value=delta_value,
                    delta_pct=round((delta_value / float(duplicates_baseline)) * 100.0, 2) if duplicates_baseline else None,
                    dimension_key="uniqueness",
                )
            )

    previous_column_metrics = previous_columns
    schema_changes = _schema_changes(current_columns, previous_column_metrics)
    for change in schema_changes:
        events.append(
            _record_anomaly(
                session,
                dq_run=dq_run,
                table=table,
                metric_key="schema_drift",
                event_type="drift",
                severity="critical" if change.get("breaking") else "warning",
                observed_value=None,
                expected_value=None,
                baseline_value=None,
                delta_value=None,
                delta_pct=None,
                dimension_key="schema",
                column_name=change.get("column_name"),
                details_json=change,
            )
        )

    history_artifacts = {
        "baselines": [
            {
                "metric_key": baseline.metric_key,
                "metric_scope": baseline.metric_scope,
                "current_value": baseline.current_value,
                "baseline_value": baseline.baseline_value,
                "mean_value": baseline.mean_value,
                "median_value": baseline.median_value,
                "min_value": baseline.min_value,
                "max_value": baseline.max_value,
                "window_size": baseline.window_size,
            }
            for baseline in baselines
        ],
        "events": [
            {
                "event_type": event.event_type,
                "metric_key": event.metric_key,
                "dimension_key": event.dimension_key,
                "severity": event.severity,
                "observed_value": event.observed_value,
                "expected_value": event.expected_value,
                "baseline_value": event.baseline_value,
                "delta_value": event.delta_value,
                "delta_pct": event.delta_pct,
                "column_name": event.column_name,
                "details_json": event.details_json,
            }
            for event in events
        ],
        "schema_changes": schema_changes,
    }
    return history_artifacts


def persist_evidence_sample(
    session: Session,
    *,
    dq_run: DQRun | None,
    rule_run_id: int | None = None,
    table: TableEntity,
    sample_rows: list[dict[str, Any]] | None,
    evidence_type: str,
    origin: str = "dq",
    status: str = "masked",
    rule_id: int | None = None,
    column_id: int | None = None,
    column_name: str | None = None,
    affected_rows_count: int | None = None,
    details_json: dict[str, Any] | None = None,
) -> DQEvidenceSample | None:
    if not sample_rows:
        return None
    column_classifications = build_column_classification_map(session, table_id=table.id, key_by="name")
    masked_rows, masked_fields = mask_evidence_rows(sample_rows, table=table, column_classifications=column_classifications)
    evidence = DQEvidenceSample(
        dq_run_id=dq_run.id if dq_run is not None else None,
        rule_run_id=rule_run_id,
        table_id=table.id,
        column_id=column_id,
        rule_id=rule_id,
        evidence_type=evidence_type,
        origin=origin,
        status=status,
        sample_size=len(masked_rows),
        affected_rows_count=affected_rows_count if affected_rows_count is not None else len(masked_rows),
        column_name=column_name,
        sample_rows_json=masked_rows,
        masked_fields_json=masked_fields,
        evidence_json=details_json,
    )
    session.add(evidence)
    return evidence


def load_persisted_observability_artifacts(session: Session, *, table_id: int, limit: int = 10) -> dict[str, Any]:
    baselines = session.scalars(
        select(DQObservabilityBaseline)
        .where(DQObservabilityBaseline.table_id == table_id)
        .order_by(desc(DQObservabilityBaseline.calculated_at), desc(DQObservabilityBaseline.id))
        .limit(max(1, limit))
    ).all()
    events = session.scalars(
        select(DQObservabilityEvent)
        .where(DQObservabilityEvent.table_id == table_id)
        .order_by(desc(DQObservabilityEvent.detected_at), desc(DQObservabilityEvent.id))
        .limit(max(1, limit))
    ).all()
    evidence = session.scalars(
        select(DQEvidenceSample)
        .where(DQEvidenceSample.table_id == table_id)
        .order_by(desc(DQEvidenceSample.created_at), desc(DQEvidenceSample.id))
        .limit(max(1, limit))
    ).all()
    return {
        "baselines": [
            {
                "id": row.id,
                "run_id": row.run_id,
                "metric_key": row.metric_key,
                "metric_scope": row.metric_scope,
                "column_name": row.column_name,
                "current_value": row.current_value,
                "baseline_value": row.baseline_value,
                "mean_value": row.mean_value,
                "median_value": row.median_value,
                "min_value": row.min_value,
                "max_value": row.max_value,
                "tolerance_abs": row.tolerance_abs,
                "tolerance_pct": row.tolerance_pct,
                "window_size": row.window_size,
                "calculated_at": row.calculated_at,
                "details_json": row.details_json,
            }
            for row in baselines
        ],
        "events": [
            {
                "id": row.id,
                "run_id": row.run_id,
                "metric_key": row.metric_key,
                "dimension_key": row.dimension_key,
                "event_type": row.event_type,
                "status": row.status,
                "severity": row.severity,
                "observed_value": row.observed_value,
                "expected_value": row.expected_value,
                "baseline_value": row.baseline_value,
                "delta_value": row.delta_value,
                "delta_pct": row.delta_pct,
                "column_name": row.column_name,
                "detected_at": row.detected_at,
                "resolved_at": row.resolved_at,
                "details_json": row.details_json,
            }
            for row in events
        ],
        "evidence_samples": [
            {
                "id": row.id,
                "dq_run_id": row.dq_run_id,
                "rule_run_id": row.rule_run_id,
                "rule_id": row.rule_id,
                "column_name": row.column_name,
                "evidence_type": row.evidence_type,
                "origin": row.origin,
                "status": row.status,
                "sample_size": row.sample_size,
                "affected_rows_count": row.affected_rows_count,
                "masked_fields_json": row.masked_fields_json,
                "sample_rows_json": row.sample_rows_json,
                "evidence_json": row.evidence_json,
                "created_at": row.created_at,
            }
            for row in evidence
        ],
    }


def load_filtered_observability_artifacts(
    session: Session,
    *,
    table_id: int,
    limit: int = 10,
    artifact_type: str = "all",
    metric_key: str | None = None,
    column_name: str | None = None,
    dimension_key: str | None = None,
    event_type: str | None = None,
    severity: str | None = None,
    evidence_type: str | None = None,
    origin: str | None = None,
    status: str | None = None,
    dq_run_id: int | None = None,
    rule_run_id: int | None = None,
    rule_id: int | None = None,
) -> dict[str, Any]:
    artifact_type = (artifact_type or "all").strip().lower()
    include_baselines = artifact_type in {"all", "baseline", "baselines"}
    include_events = artifact_type in {"all", "event", "events", "drift", "anomaly"}
    include_evidence = artifact_type in {"all", "evidence", "evidences", "samples"}
    payload = {
        "baselines": [],
        "events": [],
        "evidence_samples": [],
    }
    if include_baselines:
        baseline_stmt = select(DQObservabilityBaseline).where(DQObservabilityBaseline.table_id == table_id)
        if metric_key:
            baseline_stmt = baseline_stmt.where(DQObservabilityBaseline.metric_key == metric_key)
        if column_name:
            baseline_stmt = baseline_stmt.where(DQObservabilityBaseline.column_name == column_name)
        baselines = session.scalars(
            baseline_stmt.order_by(desc(DQObservabilityBaseline.calculated_at), desc(DQObservabilityBaseline.id)).limit(
                max(1, limit)
            )
        ).all()
        payload["baselines"] = [
            {
                "id": row.id,
                "run_id": row.run_id,
                "metric_key": row.metric_key,
                "metric_scope": row.metric_scope,
                "column_name": row.column_name,
                "current_value": row.current_value,
                "baseline_value": row.baseline_value,
                "mean_value": row.mean_value,
                "median_value": row.median_value,
                "min_value": row.min_value,
                "max_value": row.max_value,
                "tolerance_abs": row.tolerance_abs,
                "tolerance_pct": row.tolerance_pct,
                "window_size": row.window_size,
                "calculated_at": row.calculated_at,
                "details_json": row.details_json,
            }
            for row in baselines
        ]
    if include_events:
        event_stmt = select(DQObservabilityEvent).where(DQObservabilityEvent.table_id == table_id)
        if metric_key:
            event_stmt = event_stmt.where(DQObservabilityEvent.metric_key == metric_key)
        if column_name:
            event_stmt = event_stmt.where(DQObservabilityEvent.column_name == column_name)
        if dimension_key:
            event_stmt = event_stmt.where(DQObservabilityEvent.dimension_key == dimension_key)
        if event_type:
            event_stmt = event_stmt.where(DQObservabilityEvent.event_type == event_type)
        if severity:
            event_stmt = event_stmt.where(DQObservabilityEvent.severity == severity)
        events = session.scalars(
            event_stmt.order_by(desc(DQObservabilityEvent.detected_at), desc(DQObservabilityEvent.id)).limit(max(1, limit))
        ).all()
        payload["events"] = [
            {
                "id": row.id,
                "run_id": row.run_id,
                "metric_key": row.metric_key,
                "dimension_key": row.dimension_key,
                "event_type": row.event_type,
                "status": row.status,
                "severity": row.severity,
                "observed_value": row.observed_value,
                "expected_value": row.expected_value,
                "baseline_value": row.baseline_value,
                "delta_value": row.delta_value,
                "delta_pct": row.delta_pct,
                "column_name": row.column_name,
                "detected_at": row.detected_at,
                "resolved_at": row.resolved_at,
                "details_json": row.details_json,
            }
            for row in events
        ]
    if include_evidence:
        evidence_stmt = select(DQEvidenceSample).where(DQEvidenceSample.table_id == table_id)
        if column_name:
            evidence_stmt = evidence_stmt.where(DQEvidenceSample.column_name == column_name)
        if evidence_type:
            evidence_stmt = evidence_stmt.where(DQEvidenceSample.evidence_type == evidence_type)
        if origin:
            evidence_stmt = evidence_stmt.where(DQEvidenceSample.origin == origin)
        if status:
            evidence_stmt = evidence_stmt.where(DQEvidenceSample.status == status)
        if dq_run_id is not None:
            evidence_stmt = evidence_stmt.where(DQEvidenceSample.dq_run_id == dq_run_id)
        if rule_run_id is not None:
            evidence_stmt = evidence_stmt.where(DQEvidenceSample.rule_run_id == rule_run_id)
        if rule_id is not None:
            evidence_stmt = evidence_stmt.where(DQEvidenceSample.rule_id == rule_id)
        evidence_rows = session.scalars(
            evidence_stmt.order_by(desc(DQEvidenceSample.created_at), desc(DQEvidenceSample.id)).limit(max(1, limit))
        ).all()
        payload["evidence_samples"] = [
            {
                "id": row.id,
                "dq_run_id": row.dq_run_id,
                "rule_run_id": row.rule_run_id,
                "rule_id": row.rule_id,
                "column_name": row.column_name,
                "evidence_type": row.evidence_type,
                "origin": row.origin,
                "status": row.status,
                "sample_size": row.sample_size,
                "affected_rows_count": row.affected_rows_count,
                "masked_fields_json": row.masked_fields_json,
                "sample_rows_json": row.sample_rows_json,
                "evidence_json": row.evidence_json,
                "created_at": row.created_at,
            }
            for row in evidence_rows
        ]
    return payload


def purge_persisted_observability_artifacts(
    session: Session,
    *,
    evidence_retention_days: int | None = None,
) -> dict[str, int]:
    now = _now()
    baseline_cutoff = now - timedelta(days=max(int(settings.dq_observability_retention_days or 180), 30))
    event_cutoff = baseline_cutoff
    evidence_cutoff = now - timedelta(days=max(int(evidence_retention_days or settings.dq_evidence_sample_retention_days or 90), 1))
    baselines_deleted = session.execute(
        delete(DQObservabilityBaseline).where(DQObservabilityBaseline.created_at < baseline_cutoff)
    ).rowcount or 0
    events_deleted = session.execute(
        delete(DQObservabilityEvent).where(DQObservabilityEvent.created_at < event_cutoff)
    ).rowcount or 0
    evidence_deleted = session.execute(
        delete(DQEvidenceSample).where(DQEvidenceSample.created_at < evidence_cutoff)
    ).rowcount or 0
    session.flush()
    return {
        "dq_observability_baselines_deleted": int(baselines_deleted),
        "dq_observability_events_deleted": int(events_deleted),
        "dq_evidence_samples_deleted": int(evidence_deleted),
    }
