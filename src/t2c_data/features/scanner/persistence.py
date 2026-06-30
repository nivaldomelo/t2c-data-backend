from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable

from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from t2c_data.core.db import SessionLocal
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.scan import ScanDiff, ScanRun, ScanSnapshot
from t2c_data.features.catalog.table_volume import load_table_volume_runtime_config, measure_table_volume
from t2c_data.features.tags.intelligence import reprocess_table_tag_intelligence
from t2c_data.features.scanner.types import ScanPayload

logger = logging.getLogger(__name__)


def _scan_status_from_row_counts(
    *,
    row_count_enabled: bool,
    row_count_stats: dict[str, int],
    skipped_due_limit: int,
) -> str:
    if not row_count_enabled:
        return "success"
    failed = int(row_count_stats.get("failed") or 0)
    skipped = int(row_count_stats.get("skipped") or 0)
    if failed > 0 or skipped > 0 or skipped_due_limit > 0:
        return "partial_success"
    return "success"


def hash_payload(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def create_running_scan_run(session: Session, *, datasource_id: int, started_by: int | None) -> ScanRun:
    scan_run = ScanRun(
        datasource_id=datasource_id,
        status="running",
        started_by=started_by,
        summary={
            "status": "running",
            "execution_engine": "spark",
        },
    )
    session.add(scan_run)
    session.flush()
    return scan_run


def create_queued_scan_run(session: Session, *, datasource_id: int, started_by: int | None) -> ScanRun:
    scan_run = ScanRun(
        datasource_id=datasource_id,
        status="queued",
        started_by=started_by,
        summary={
            "queued": True,
            "status": "queued",
            "execution_engine": "spark",
        },
    )
    session.add(scan_run)
    session.flush()
    return scan_run


def persist_failed_scan_run(
    session: Session,
    *,
    datasource: DataSource,
    started_by: int | None,
    message: str,
    detail: str,
    code: str,
) -> ScanRun:
    failed = ScanRun(
        datasource_id=datasource.id,
        status="failed",
        started_by=started_by,
        summary={
            "engine": datasource.db_type,
            "error": message,
            "error_code": code,
            "error_detail": detail,
        },
    )
    session.add(failed)
    session.commit()
    session.refresh(failed)
    return failed


def mark_scan_run_failed(
    session: Session,
    *,
    scan_run: ScanRun,
    datasource: DataSource,
    message: str,
    detail: str,
    code: str,
) -> ScanRun:
    scan_run.status = "failed"
    scan_run.summary = {
        "engine": datasource.db_type,
        "status": "failed",
        "error": message,
        "error_code": code,
        "error_detail": detail,
    }
    session.add(scan_run)
    session.commit()
    session.refresh(scan_run)
    return scan_run


def update_scan_run_summary(
    session: Session,
    *,
    scan_run: ScanRun,
    status: str | None = None,
    summary_updates: dict[str, object] | None = None,
) -> ScanRun:
    if status is not None:
        scan_run.status = status
    summary = dict(scan_run.summary or {})
    if summary_updates:
        summary.update(summary_updates)
    if status is not None:
        summary["status"] = status
    scan_run.summary = summary
    session.add(scan_run)
    session.commit()
    session.refresh(scan_run)
    return scan_run


def _upsert_database(session: Session, datasource_id: int, name: str) -> Database:
    db_obj = session.scalar(select(Database).where(and_(Database.datasource_id == datasource_id, Database.name == name)))
    if db_obj:
        return db_obj

    db_obj = Database(datasource_id=datasource_id, name=name)
    session.add(db_obj)
    session.flush()
    return db_obj


def _upsert_schema(session: Session, database_id: int, name: str) -> Schema:
    schema_obj = session.scalar(select(Schema).where(and_(Schema.database_id == database_id, Schema.name == name)))
    if schema_obj:
        return schema_obj

    schema_obj = Schema(database_id=database_id, name=name)
    session.add(schema_obj)
    session.flush()
    return schema_obj


def _upsert_table(
    session: Session,
    *,
    schema_id: int,
    name: str,
    table_type: str,
    description_source: str | None,
    schema_hash: str,
) -> TableEntity:
    table_obj = session.scalar(select(TableEntity).where(and_(TableEntity.schema_id == schema_id, TableEntity.name == name)))
    if table_obj:
        table_obj.table_type = table_type
        table_obj.description_source = description_source
        table_obj.schema_hash = schema_hash
        return table_obj

    table_obj = TableEntity(
        schema_id=schema_id,
        name=name,
        table_type=table_type,
        description_source=description_source,
        schema_hash=schema_hash,
    )
    session.add(table_obj)
    session.flush()
    return table_obj


def _upsert_columns(session: Session, table_id: int, columns: Iterable[dict]) -> None:
    existing = {
        c.name: c for c in session.scalars(select(ColumnEntity).where(ColumnEntity.table_id == table_id)).all()
    }
    for column in columns:
        current = existing.get(column["name"])
        if current:
            current.data_type = column["data_type"]
            current.is_primary_key = column["is_primary_key"]
            current.is_nullable = column["is_nullable"]
            current.ordinal_position = column["ordinal_position"]
            current.description_source = column["description_source"]
            continue

        session.add(
            ColumnEntity(
                table_id=table_id,
                name=column["name"],
                data_type=column["data_type"],
                is_primary_key=column["is_primary_key"],
                is_nullable=column["is_nullable"],
                ordinal_position=column["ordinal_position"],
                description_source=column["description_source"],
            )
        )


def build_snapshots(scan_payload: ScanPayload) -> list[tuple[str, str, str, dict]]:
    snapshots: list[tuple[str, str, str, dict]] = []

    for scanned_table in scan_payload.tables:
        table_key = f"{scanned_table.schema_name}.{scanned_table.table_name}"
        table_payload = {
            "schema": scanned_table.schema_name,
            "name": scanned_table.table_name,
            "table_type": scanned_table.table_type,
            "description_source": scanned_table.comment,
            "columns": [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "is_primary_key": c.is_primary_key,
                    "is_nullable": c.is_nullable,
                    "ordinal_position": c.ordinal_position,
                    "description_source": c.comment,
                }
                for c in scanned_table.columns
            ],
        }
        table_hash = hash_payload(table_payload)
        snapshots.append(("table", table_key, table_hash, table_payload))

        for column in scanned_table.columns:
            col_key = f"{table_key}.{column.name}"
            col_payload = {
                "schema": scanned_table.schema_name,
                "table": scanned_table.table_name,
                "name": column.name,
                "data_type": column.data_type,
                "is_primary_key": column.is_primary_key,
                "is_nullable": column.is_nullable,
                "ordinal_position": column.ordinal_position,
                "description_source": column.comment,
            }
            snapshots.append(("column", col_key, hash_payload(col_payload), col_payload))

    return snapshots


def save_diffs(session: Session, scan_run: ScanRun, current: list[tuple[str, str, str, dict]]) -> int:
    previous_run = session.scalar(
        select(ScanRun)
        .where(
            and_(
                ScanRun.datasource_id == scan_run.datasource_id,
                ScanRun.status.in_(("success", "partial_success", "succeeded")),
            )
        )
        .where(ScanRun.id != scan_run.id)
        .order_by(desc(ScanRun.id))
        .limit(1)
    )

    prev_map: dict[tuple[str, str], str] = {}
    if previous_run:
        previous_snapshots = session.scalars(select(ScanSnapshot).where(ScanSnapshot.scan_run_id == previous_run.id)).all()
        prev_map = {(s.entity_type, s.entity_key): s.entity_hash for s in previous_snapshots}

    curr_map = {(entity_type, entity_key): entity_hash for entity_type, entity_key, entity_hash, _ in current}
    diffs = 0

    for key, new_hash in curr_map.items():
        old_hash = prev_map.get(key)
        if old_hash is None:
            session.add(
                ScanDiff(
                    scan_run_id=scan_run.id,
                    entity_type=key[0],
                    entity_key=key[1],
                    diff_type="added",
                    old_hash=None,
                    new_hash=new_hash,
                    details="new entity detected",
                )
            )
            diffs += 1
        elif old_hash != new_hash:
            session.add(
                ScanDiff(
                    scan_run_id=scan_run.id,
                    entity_type=key[0],
                    entity_key=key[1],
                    diff_type="changed",
                    old_hash=old_hash,
                    new_hash=new_hash,
                    details="entity changed",
                )
            )
            diffs += 1

    for key, old_hash in prev_map.items():
        if key not in curr_map:
            session.add(
                ScanDiff(
                    scan_run_id=scan_run.id,
                    entity_type=key[0],
                    entity_key=key[1],
                    diff_type="removed",
                    old_hash=old_hash,
                    new_hash=None,
                    details="entity removed",
                )
            )
            diffs += 1

    return diffs


def measure_table_volume_isolated(
    *,
    table_id: int,
    measurement_context: str = "datasource_run",
):
    with SessionLocal() as volume_session:
        try:
            snapshot = measure_table_volume(db=volume_session, table_id=table_id, measurement_context=measurement_context)
            volume_session.commit()
            return snapshot
        except Exception:  # noqa: BLE001
            volume_session.rollback()
            logger.exception("datasource scan row count measurement failed table_id=%s", table_id)
            return None


def measure_scan_table_volumes(
    table_ids: Iterable[int],
    *,
    measurement_context: str = "datasource_run",
) -> dict[str, int]:
    row_count_success = 0
    row_count_failed = 0
    row_count_skipped = 0
    row_count_attempted = 0

    for table_id in table_ids:
        row_count_attempted += 1
        snapshot = measure_table_volume_isolated(table_id=table_id, measurement_context=measurement_context)
        if snapshot is None:
            row_count_skipped += 1
            continue

        status = (snapshot.status or "").strip().lower()
        if status == "success":
            row_count_success += 1
        elif status == "error":
            row_count_failed += 1
        else:
            row_count_skipped += 1

    return {
        "attempted": row_count_attempted,
        "success": row_count_success,
        "failed": row_count_failed,
        "skipped": row_count_skipped,
    }


def persist_scan_payload(
    session: Session,
    *,
    scan_run: ScanRun,
    datasource: DataSource,
    scanned: ScanPayload,
    progress_callback: Callable[[str], None] | None = None,
) -> ScanRun:
    if progress_callback is not None:
        progress_callback("catalog_persist")
    database = _upsert_database(session, datasource.id, scanned.database_name)
    touched_table_ids: set[int] = set()
    ordered_table_ids: list[int] = []
    row_count_runtime_config = load_table_volume_runtime_config(datasource)

    for scanned_table in scanned.tables:
        schema = _upsert_schema(session, database.id, scanned_table.schema_name)

        columns_payload = [
            {
                "name": c.name,
                "data_type": c.data_type,
                "is_primary_key": c.is_primary_key,
                "is_nullable": c.is_nullable,
                "ordinal_position": c.ordinal_position,
                "description_source": c.comment,
            }
            for c in scanned_table.columns
        ]
        table_payload = {
            "schema": scanned_table.schema_name,
            "name": scanned_table.table_name,
            "table_type": scanned_table.table_type,
            "description_source": scanned_table.comment,
            "columns": columns_payload,
        }
        schema_hash = hash_payload(table_payload)

        table = _upsert_table(
            session,
            schema_id=schema.id,
            name=scanned_table.table_name,
            table_type=scanned_table.table_type,
            description_source=scanned_table.comment,
            schema_hash=schema_hash,
        )
        _upsert_columns(session, table.id, columns_payload)
        if int(table.id) not in touched_table_ids:
            touched_table_ids.add(int(table.id))
            ordered_table_ids.append(int(table.id))

    table_ids_for_row_count = ordered_table_ids
    if row_count_runtime_config.max_tables_per_run is not None:
        table_ids_for_row_count = ordered_table_ids[: row_count_runtime_config.max_tables_per_run]
    skipped_due_limit = max(0, len(ordered_table_ids) - len(table_ids_for_row_count))

    session.commit()
    try:
        row_count_stats = measure_scan_table_volumes(table_ids_for_row_count, measurement_context="datasource_run")
    except Exception:  # noqa: BLE001
        logger.exception("datasource scan row count stage failed datasource_id=%s", datasource.id)
        row_count_stats = {
            "attempted": len(table_ids_for_row_count),
            "success": 0,
            "failed": 0,
            "skipped": len(table_ids_for_row_count),
        }
    final_status = _scan_status_from_row_counts(
        row_count_enabled=bool(row_count_runtime_config.enabled),
        row_count_stats=row_count_stats,
        skipped_due_limit=skipped_due_limit,
    )
    logger.info(
        "datasource_scan_row_count_summary datasource_id=%s attempted=%s success=%s failed=%s skipped=%s skipped_due_limit=%s final_status=%s",
        datasource.id,
        row_count_stats["attempted"],
        row_count_stats["success"],
        row_count_stats["failed"],
        row_count_stats["skipped"],
        skipped_due_limit,
        final_status,
    )

    for table_id in sorted(touched_table_ids):
        reprocess_table_tag_intelligence(
            session,
            table_id=table_id,
            source_module="scanner.persistence",
            metadata={"origin": "datasource_scan", "scan_run_id": scan_run.id},
        )

    if progress_callback is not None:
        progress_callback("diff_generation")
    snapshots = build_snapshots(scanned)
    for entity_type, entity_key, entity_hash, payload in snapshots:
        session.add(
            ScanSnapshot(
                scan_run_id=scan_run.id,
                entity_type=entity_type,
                entity_key=entity_key,
                entity_hash=entity_hash,
                payload=payload,
            )
        )

    diff_count = save_diffs(session, scan_run, snapshots)
    scan_run.status = final_status
    scan_run.summary = {
        "engine": datasource.db_type,
        "database": scanned.database_name,
        "tables": len(scanned.tables),
        "snapshots": len(snapshots),
        "diffs": diff_count,
        "row_counts": {
            "enabled": row_count_runtime_config.enabled,
            "strategy": row_count_runtime_config.strategy,
            "timeout_seconds": row_count_runtime_config.timeout_seconds,
            "max_tables_per_run": row_count_runtime_config.max_tables_per_run,
            "attempted": row_count_stats["attempted"],
            "success": row_count_stats["success"],
            "failed": row_count_stats["failed"],
            "skipped": row_count_stats["skipped"],
            "skipped_due_limit": skipped_due_limit,
            "row_count_measured_success": row_count_stats["success"],
            "row_count_measured_error": row_count_stats["failed"],
            "row_count_measured_skipped": row_count_stats["skipped"],
        },
    }
    session.commit()
    session.refresh(scan_run)
    return scan_run
