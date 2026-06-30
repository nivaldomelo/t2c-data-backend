from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import func, inspect, select, text
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.features.data_quality.schedule_utils import compute_next_run_at, describe_schedule, validate_schedule_payload
from t2c_data.models.auth import User
from t2c_data.models.platform import DataLakeConnection, DataLakeScanSchedule, DataLakeScanSchedulerStatus
from t2c_data.schemas.integrations import DataLakeScanScheduleIn, DataLakeScanScheduleOut
from t2c_data.services.audit import write_audit_log_sync

SCHEDULER_NAME = "data_lake_scan"


def _has_table(session: Session, table_name: str) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    inspector = inspect(bind)
    return inspector.has_table(table_name, schema=settings.db_schema)


def _schedule_support_ready(session: Session) -> bool:
    return _has_table(session, "data_lake_scan_schedules") and _has_table(session, "data_lake_scan_scheduler_status")


def _normalize_mode_value(value: str | None) -> str:
    normalized = (value or "manual").strip().lower()
    return normalized if normalized in {"manual", "interval", "daily", "weekly", "biweekly", "monthly"} else "manual"


def serialize_data_lake_scan_schedule(schedule: DataLakeScanSchedule, db: Session) -> DataLakeScanScheduleOut:
    connection = db.get(DataLakeConnection, schedule.connection_id)
    return DataLakeScanScheduleOut(
        id=schedule.id,
        connection_id=schedule.connection_id,
        schedule_mode=schedule.schedule_mode,
        schedule_enabled=bool(schedule.schedule_enabled),
        schedule_every_minutes=schedule.schedule_every_minutes,
        schedule_time=schedule.schedule_time,
        schedule_day_of_week=schedule.schedule_day_of_week,
        schedule_day_of_month=schedule.schedule_day_of_month,
        schedule_anchor_date=schedule.schedule_anchor_date,
        schedule_last_run_at=schedule.schedule_last_run_at,
        schedule_last_started_at=schedule.schedule_last_started_at,
        schedule_last_finished_at=schedule.schedule_last_finished_at,
        schedule_last_status=schedule.schedule_last_status,
        schedule_last_error=schedule.schedule_last_error,
        schedule_next_run_at=schedule.schedule_next_run_at or compute_next_run_at(schedule),
        schedule_summary=describe_schedule(schedule),
        created_by_user_id=schedule.created_by_user_id,
        created_at=schedule.created_at,
        updated_at=schedule.updated_at,
    )


def list_data_lake_scan_schedules(db: Session, connection_id: int | None = None) -> list[DataLakeScanScheduleOut]:
    if not _schedule_support_ready(db):
        return []
    stmt = select(DataLakeScanSchedule)
    if connection_id is not None:
        stmt = stmt.where(DataLakeScanSchedule.connection_id == connection_id)
    schedules = db.scalars(stmt.order_by(DataLakeScanSchedule.id.desc())).all()
    return [serialize_data_lake_scan_schedule(schedule, db) for schedule in schedules]


def get_data_lake_scan_schedule(db: Session, connection_id: int) -> DataLakeScanSchedule | None:
    if not _schedule_support_ready(db):
        return None
    stmt = (
        select(DataLakeScanSchedule)
        .where(DataLakeScanSchedule.connection_id == connection_id)
        .order_by(DataLakeScanSchedule.id.desc())
        .limit(1)
    )
    return db.scalar(stmt)


def upsert_data_lake_scan_schedule(
    db: Session,
    connection_id: int,
    payload: DataLakeScanScheduleIn,
    *,
    current_user: User,
    audit_kwargs: dict[str, object],
) -> DataLakeScanScheduleOut:
    if not _schedule_support_ready(db):
        raise ValueError("Data Lake scan schedule tables unavailable")
    connection = db.get(DataLakeConnection, connection_id)
    if connection is None:
        raise ValueError("Data Lake connection not found")

    existing = get_data_lake_scan_schedule(db, connection_id)
    existing_payload = (
        {
            "schedule_mode": existing.schedule_mode,
            "schedule_enabled": existing.schedule_enabled,
            "schedule_every_minutes": existing.schedule_every_minutes,
            "schedule_time": existing.schedule_time,
            "schedule_day_of_week": existing.schedule_day_of_week,
            "schedule_day_of_month": existing.schedule_day_of_month,
            "schedule_anchor_date": existing.schedule_anchor_date,
        }
        if existing is not None
        else {}
    )
    normalized = validate_schedule_payload(payload.model_dump(), existing=existing_payload)
    if existing is None:
        existing = DataLakeScanSchedule(connection_id=connection_id, created_by_user_id=current_user.id)
        db.add(existing)

    before = serialize_data_lake_scan_schedule(existing, db) if existing.id else None
    existing.connection_id = connection_id
    existing.schedule_mode = _normalize_mode_value(str(normalized["schedule_mode"]))
    existing.schedule_enabled = bool(normalized["schedule_enabled"])
    existing.schedule_every_minutes = normalized.get("schedule_every_minutes")
    existing.schedule_time = normalized.get("schedule_time")
    existing.schedule_day_of_week = normalized.get("schedule_day_of_week")
    existing.schedule_day_of_month = normalized.get("schedule_day_of_month")
    existing.schedule_anchor_date = normalized.get("schedule_anchor_date")
    existing.schedule_next_run_at = compute_next_run_at(existing)
    existing.schedule_summary = describe_schedule(existing)
    db.add(existing)
    db.commit()
    db.refresh(existing)

    write_audit_log_sync(
        db,
        action="integrations.data_lake.schedule_upsert",
        entity_type="data_lake_scan_schedule",
        entity_id=existing.id,
        before=before.model_dump() if before is not None else None,
        after=serialize_data_lake_scan_schedule(existing, db).model_dump(),
        metadata={
            "connection_id": connection.id,
            "connection_name": connection.name,
            "schedule_summary": existing.schedule_summary,
            "schedule_mode": existing.schedule_mode,
            "schedule_enabled": existing.schedule_enabled,
        },
        **audit_kwargs,
    )
    db.commit()
    return serialize_data_lake_scan_schedule(existing, db)


def delete_data_lake_scan_schedule(
    db: Session,
    connection_id: int,
    *,
    current_user: User,
    audit_kwargs: dict[str, object],
) -> None:
    if not _schedule_support_ready(db):
        return
    schedule = get_data_lake_scan_schedule(db, connection_id)
    if schedule is None:
        return
    before = serialize_data_lake_scan_schedule(schedule, db)
    db.delete(schedule)
    db.commit()
    write_audit_log_sync(
        db,
        action="integrations.data_lake.schedule_delete",
        entity_type="data_lake_scan_schedule",
        entity_id=schedule.id,
        before=before.model_dump(),
        metadata={"connection_id": connection_id},
        **audit_kwargs,
    )
    db.commit()


def mark_data_lake_scan_schedule_dispatched(db: Session, schedule_id: int, *, started_at: datetime | None = None) -> None:
    if not _schedule_support_ready(db):
        return
    schedule = db.get(DataLakeScanSchedule, schedule_id)
    if schedule is None:
        return
    now = datetime.now(timezone.utc)
    schedule.schedule_last_started_at = started_at or now
    schedule.schedule_last_status = "running"
    schedule.schedule_last_error = None
    db.add(schedule)
    db.flush()


def update_data_lake_scan_schedule_run_state(
    db: Session,
    *,
    schedule_id: int | None,
    status: str,
    error_message: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    if schedule_id is None or not _schedule_support_ready(db):
        return
    schedule = db.get(DataLakeScanSchedule, schedule_id)
    if schedule is None:
        return
    now = datetime.now(timezone.utc)
    schedule.schedule_last_run_at = finished_at or now
    if started_at is not None:
        schedule.schedule_last_started_at = started_at
    if finished_at is not None:
        schedule.schedule_last_finished_at = finished_at
    schedule.schedule_last_status = status
    schedule.schedule_last_error = error_message
    schedule.schedule_next_run_at = compute_next_run_at(schedule, reference=finished_at or now)
    schedule.schedule_summary = describe_schedule(schedule)
    db.add(schedule)
    db.flush()


def scheduler_status_snapshot(db: Session) -> dict[str, object]:
    if not _schedule_support_ready(db):
        return {
            "scheduler_name": SCHEDULER_NAME,
            "mode": "embedded",
            "is_enabled": True,
            "health": "idle",
            "last_started_at": None,
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_error": None,
            "last_run_summary": {},
            "scheduled_sources_total": 0,
            "next_expected_run_at": None,
    }
    status = db.get(DataLakeScanSchedulerStatus, 1)
    next_expected_run_at = db.scalar(
        select(DataLakeScanSchedule.schedule_next_run_at)
        .where(DataLakeScanSchedule.schedule_enabled.is_(True))
        .where(DataLakeScanSchedule.schedule_next_run_at.is_not(None))
        .order_by(DataLakeScanSchedule.schedule_next_run_at.asc())
        .limit(1)
    )
    scheduled_sources_total = int(db.scalar(select(func.count(DataLakeScanSchedule.id)).where(DataLakeScanSchedule.schedule_enabled.is_(True))) or 0)
    last_run_summary = dict(status.last_run_summary_json or {}) if status else {}
    health = "idle"
    if status and status.last_error:
        health = "degraded"
    elif not (bool(status.is_enabled) if status else True):
        health = "disabled"
    return {
        "scheduler_name": SCHEDULER_NAME,
        "mode": status.mode if status else settings.datasource_scan_scheduler_mode,
        "is_enabled": bool(status.is_enabled) if status else bool(settings.datasource_scan_scheduler_enabled),
        "health": health,
        "last_started_at": status.last_started_at if status else None,
        "last_heartbeat_at": status.last_heartbeat_at if status else None,
        "last_success_at": status.last_success_at if status else None,
        "last_failure_at": status.last_failure_at if status else None,
        "last_error": status.last_error if status else None,
        "last_run_summary": last_run_summary,
        "scheduled_sources_total": scheduled_sources_total,
        "next_expected_run_at": next_expected_run_at.isoformat() if next_expected_run_at else None,
    }


__all__ = [
    "delete_data_lake_scan_schedule",
    "get_data_lake_scan_schedule",
    "list_data_lake_scan_schedules",
    "mark_data_lake_scan_schedule_dispatched",
    "scheduler_status_snapshot",
    "serialize_data_lake_scan_schedule",
    "update_data_lake_scan_schedule_run_state",
    "upsert_data_lake_scan_schedule",
]
