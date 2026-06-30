from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.ingestion.runtime import operational_session_for_datasource
from t2c_data.features.ingestion.service import IngestionIntegrationUnavailable, load_table_ingestion_detail
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import OperationalStabilitySnapshot

SNAPSHOT_RETENTION_DAYS = 90


def _bucket_start(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    return current.replace(minute=0, second=0, microsecond=0)


def refresh_operational_stability_snapshots(session: Session) -> dict[str, object]:
    settings_snapshot = get_governance_settings_snapshot(session)
    bucket_start_at = _bucket_start()
    retention_cutoff = bucket_start_at - timedelta(days=SNAPSHOT_RETENTION_DAYS)

    rows = session.execute(
        select(TableEntity, Schema.name, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(DataSource.is_active.is_(True))
    ).all()

    by_datasource: dict[int, list[tuple[TableEntity, str, DataSource]]] = defaultdict(list)
    for table, schema_name, datasource in rows:
        by_datasource[datasource.id].append((table, str(schema_name), datasource))

    processed = 0
    snapshotted = 0
    linked = 0
    errors = 0
    skipped = 0

    for datasource_id, items in by_datasource.items():
        datasource = items[0][2]
        try:
            with operational_session_for_datasource(datasource) as operational_db:
                for table, schema_name, datasource in items:
                    processed += 1
                    try:
                        detail = load_table_ingestion_detail(
                            operational_db,
                            schema_name=schema_name,
                            table_name=table.name,
                            page=1,
                            page_size=10,
                            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
                        )
                    except IngestionIntegrationUnavailable:
                        skipped += 1
                        continue
                    summary = detail.get("summary") or {}
                    stability = detail.get("stability") or {}
                    if not summary.get("linked"):
                        skipped += 1
                        continue
                    linked += 1
                    primary = summary.get("primary_pipeline") or {}
                    snapshot = session.scalar(
                        select(OperationalStabilitySnapshot).where(
                            OperationalStabilitySnapshot.table_id == table.id,
                            OperationalStabilitySnapshot.bucket_start_at == bucket_start_at,
                        )
                    )
                    if snapshot is None:
                        snapshot = OperationalStabilitySnapshot(
                            table_id=table.id,
                            datasource_id=datasource.id,
                            schema_name=schema_name,
                            table_name=table.name,
                            bucket_start_at=bucket_start_at,
                        )
                    snapshot.datasource_id = datasource.id
                    snapshot.schema_name = schema_name
                    snapshot.table_name = table.name
                    snapshot.pipeline_name = primary.get("pipeline_name")
                    snapshot.dag_id = primary.get("dag_id")
                    snapshot.task_name = primary.get("task_name")
                    snapshot.latest_status_label = primary.get("latest_status_label")
                    snapshot.last_success_at = primary.get("last_success_at")
                    snapshot.last_execution_finished_at = primary.get("last_execution_finished_at")
                    snapshot.rows_processed = primary.get("rows_processed")
                    snapshot.window_runs = int(stability.get("window_runs") or 0)
                    snapshot.success_rate_pct = float(stability.get("success_rate_pct") or 0.0)
                    snapshot.failed_runs = int(stability.get("failed_runs") or 0)
                    snapshot.recurrent_degradation = bool(stability.get("recurrent_degradation"))
                    snapshot.currently_stale = bool(stability.get("currently_stale"))
                    session.add(snapshot)
                    snapshotted += 1
        except Exception:
            errors += len(items)
            session.rollback()
            continue

    deleted = session.execute(
        delete(OperationalStabilitySnapshot).where(OperationalStabilitySnapshot.bucket_start_at < retention_cutoff)
    ).rowcount or 0
    session.commit()
    return {
        "bucket_start_at": bucket_start_at.isoformat(),
        "retention_days": SNAPSHOT_RETENTION_DAYS,
        "processed": processed,
        "linked": linked,
        "snapshotted": snapshotted,
        "skipped": skipped,
        "errors": errors,
        "deleted": int(deleted),
    }


def get_operational_stability_history(
    session: Session,
    *,
    table_id: int,
    limit: int = 24,
) -> list[OperationalStabilitySnapshot]:
    safe_limit = max(min(int(limit), 240), 1)
    return list(
        session.scalars(
            select(OperationalStabilitySnapshot)
            .where(OperationalStabilitySnapshot.table_id == table_id)
            .order_by(OperationalStabilitySnapshot.bucket_start_at.desc())
            .limit(safe_limit)
        ).all()
    )


__all__ = [
    "SNAPSHOT_RETENTION_DAYS",
    "get_operational_stability_history",
    "refresh_operational_stability_snapshots",
]
