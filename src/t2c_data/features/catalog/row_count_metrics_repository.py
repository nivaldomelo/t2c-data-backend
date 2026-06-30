from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(slots=True)
class RowCountSnapshotRecord:
    table_id: int
    datasource_id: int | None
    schema_id: int | None
    connection_name: str | None
    database_name: str | None
    schema_name: str | None
    table_name: str | None
    fqn: str | None
    row_count: int | None
    measured_at: datetime | None
    measurement_type: str | None
    measurement_source: str | None
    status: str | None
    duration_ms: int | None
    error_message: str | None
    snapshot_date: date | None


def _normalize_datetime(value: object | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _normalize_date(value: object | None) -> date | None:
    if value is None:
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def get_latest_row_count_snapshots(*, db: Session, table_id: int, limit: int = 2) -> list[RowCountSnapshotRecord]:
    stmt = text(
        """
        select
            table_id,
            datasource_id,
            schema_id,
            connection_name,
            database_name,
            schema_name,
            table_name,
            fqn,
            row_count,
            coalesce(measured_at, snapshot_at) as measured_at,
            coalesce(measurement_type, collection_method) as measurement_type,
            measurement_source,
            coalesce(status, collection_status) as status,
            duration_ms,
            error_message,
            snapshot_date
        from controle.table_row_count_snapshots
        where table_id = :table_id
        order by coalesce(measured_at, snapshot_at) desc nulls last, id desc
        limit :limit
        """
    )
    rows = db.execute(stmt, {"table_id": table_id, "limit": max(int(limit or 2), 1)}).mappings().all()
    return [
        RowCountSnapshotRecord(
            table_id=int(row["table_id"]),
            datasource_id=int(row["datasource_id"]) if row.get("datasource_id") is not None else None,
            schema_id=int(row["schema_id"]) if row.get("schema_id") is not None else None,
            connection_name=row["connection_name"],
            database_name=row["database_name"],
            schema_name=row["schema_name"],
            table_name=row["table_name"],
            fqn=row["fqn"],
            row_count=int(row["row_count"]) if row["row_count"] is not None else None,
            measured_at=_normalize_datetime(row["measured_at"]),
            measurement_type=row["measurement_type"],
            measurement_source=row["measurement_source"],
            status=row["status"],
            duration_ms=int(row["duration_ms"]) if row.get("duration_ms") is not None else None,
            error_message=row["error_message"],
            snapshot_date=_normalize_date(row["snapshot_date"]),
        )
        for row in rows
    ]
