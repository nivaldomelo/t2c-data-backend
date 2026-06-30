from __future__ import annotations

import logging
from sqlalchemy.orm import Session

from t2c_data.features.catalog.row_count_metrics_repository import get_latest_row_count_snapshots
from t2c_data.schemas.catalog import TableRowCountMetricsOut

logger = logging.getLogger(__name__)

TRUSTED_MEASUREMENT_SOURCES = {
    "postgres_count",
    "mysql_count",
    "sql_count",
    "catalog_profile",
    "datalake_footer",
    "manual",
}

FAILURE_STATUSES = {"error", "failed"}


def _build_empty_row_count_metrics() -> TableRowCountMetricsOut:
    return TableRowCountMetricsOut(
        current_row_count=None,
        previous_row_count=None,
        snapshot_at=None,
        previous_snapshot_at=None,
        collection_method=None,
        collection_status=None,
        measured_at=None,
        measurement_type=None,
        measurement_source=None,
        status=None,
        error_message=None,
        duration_ms=None,
        growth_absolute=None,
        growth_percent=None,
        has_history=False,
    )


def _growth_percent(current_row_count: int, previous_row_count: int) -> float | None:
    if previous_row_count == 0:
        return None
    delta = current_row_count - previous_row_count
    return round((delta / previous_row_count) * 100.0, 4)


def build_row_count_metrics(*, db: Session, table_id: int) -> TableRowCountMetricsOut | None:
    try:
        snapshots = get_latest_row_count_snapshots(db=db, table_id=table_id, limit=5)
    except Exception:
        logger.exception("Failed to load row count snapshots table_id=%s", table_id)
        return None

    if not snapshots:
        return _build_empty_row_count_metrics()

    latest = snapshots[0]
    latest_status = (latest.status or "").strip().lower()

    if latest_status in FAILURE_STATUSES:
        return TableRowCountMetricsOut(
            current_row_count=None,
            previous_row_count=None,
            snapshot_at=None,
            previous_snapshot_at=None,
            collection_method=latest.measurement_type,
            collection_status=latest.status,
            measured_at=latest.measured_at,
            measurement_type=latest.measurement_type,
            measurement_source=latest.measurement_source,
            status=latest.status,
            error_message=latest.error_message,
            duration_ms=latest.duration_ms,
            growth_absolute=None,
            growth_percent=None,
            has_history=any((snapshot.status or "").strip().lower() == "success" for snapshot in snapshots[1:]),
        )

    trusted_snapshots = [
        snapshot
        for snapshot in snapshots
        if (snapshot.status or "").strip().lower() == "success"
        and snapshot.row_count is not None
        and (snapshot.measurement_source or "").strip().lower() in TRUSTED_MEASUREMENT_SOURCES
    ]

    if not trusted_snapshots:
        return _build_empty_row_count_metrics()

    current = trusted_snapshots[0]
    previous = trusted_snapshots[1] if len(trusted_snapshots) > 1 else None
    current_row_count = current.row_count
    previous_row_count = previous.row_count if previous is not None else None
    growth_absolute = None
    growth_percent = None
    if current_row_count is not None and previous_row_count is not None:
        growth_absolute = current_row_count - previous_row_count
        growth_percent = _growth_percent(current_row_count, previous_row_count)

    return TableRowCountMetricsOut(
        current_row_count=current_row_count,
        previous_row_count=previous_row_count,
        snapshot_at=current.measured_at,
        previous_snapshot_at=previous.measured_at if previous is not None else None,
        collection_method=current.measurement_type,
        collection_status=current.status,
        measured_at=current.measured_at,
        measurement_type=current.measurement_type,
        measurement_source=current.measurement_source,
        status=current.status,
        error_message=current.error_message,
        duration_ms=current.duration_ms,
        growth_absolute=growth_absolute,
        growth_percent=growth_percent,
        has_history=len(trusted_snapshots) > 1,
    )
