from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import func, inspect, or_, select, text
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import settings
from t2c_data.features.data_quality.schedule_utils import compute_next_run_at, describe_schedule, validate_schedule_payload
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource
from t2c_data.models.datasource_scheduler import DataSourceScanSchedule, DataSourceScanSchedulerStatus
from t2c_data.schemas.datasource_schedules import (
    DataSourceScanScheduleCreate,
    DataSourceScanScheduleOut,
    DataSourceScanScheduleRecipientOut,
    DataSourceScanScheduleUpdate,
)


SCHEDULER_NAME = "datasource_scan"


def _has_table(session: Session, table_name: str) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    inspector = inspect(bind)
    return inspector.has_table(table_name, schema=settings.db_schema)


def _schedule_support_ready(session: Session) -> bool:
    if not _has_table(session, "datasource_scan_schedules") or not _has_table(session, "datasource_scan_scheduler_status"):
        return False
    bind = session.get_bind()
    if bind is None:
        return False
    try:
        columns = {column["name"] for column in inspect(bind).get_columns("datasource_scan_schedules", schema=settings.db_schema)}
    except Exception:  # noqa: BLE001
        return False
    required = {
        "datasource_id",
        "schedule_mode",
        "schedule_enabled",
        "schedule_every_minutes",
        "schedule_time",
        "schedule_day_of_week",
        "schedule_day_of_month",
        "schedule_anchor_date",
        "schedule_last_run_at",
        "schedule_last_started_at",
        "schedule_last_finished_at",
        "schedule_last_status",
        "schedule_last_error",
        "schedule_next_run_at",
    }
    return required.issubset(columns)


def _dedupe_users(users: Iterable[User]) -> list[User]:
    seen: set[int] = set()
    output: list[User] = []
    for user in users:
        if user.id in seen:
            continue
        seen.add(user.id)
        output.append(user)
    return output


def search_datasource_schedule_users(db: Session, q: str = "", limit: int = 20) -> list[dict[str, object]]:
    query = select(User).where(User.is_active.is_(True))
    if q.strip():
        pattern = f"%{q.strip()}%"
        query = query.where(or_(User.name.ilike(pattern), User.full_name.ilike(pattern), User.email.ilike(pattern)))
    users = db.scalars(query.order_by(User.full_name.nulls_last(), User.name.nulls_last(), User.email).limit(limit)).all()
    return [
        {
            "id": user.id,
            "display_name": user.name or user.full_name or user.email,
            "email": user.email,
        }
        for user in users
    ]


def _serialize_recipients(schedule: DataSourceScanSchedule) -> list[DataSourceScanScheduleRecipientOut]:
    return [
        DataSourceScanScheduleRecipientOut(
            id=user.id,
            display_name=user.name or user.full_name or user.email,
            email=user.email,
        )
        for user in _dedupe_users(schedule.notification_recipients or [])
    ]


def _serialize_schedule(db: Session, schedule: DataSourceScanSchedule) -> DataSourceScanScheduleOut:
    datasource = db.get(DataSource, schedule.datasource_id)
    datasource_name = datasource.name if datasource else f"Datasource #{schedule.datasource_id}"
    datasource_type = datasource.db_type if datasource else "postgres"
    return DataSourceScanScheduleOut(
        id=schedule.id,
        datasource_id=schedule.datasource_id,
        datasource_name=datasource_name,
        datasource_type=datasource_type,
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
        notification_recipients=_serialize_recipients(schedule),
        created_at=schedule.created_at,
        updated_at=schedule.updated_at,
    )


def list_scan_schedules(
    db: Session,
    *,
    datasource_id: int | None = None,
) -> list[DataSourceScanScheduleOut]:
    if not _schedule_support_ready(db):
        return []
    stmt = select(DataSourceScanSchedule).options(selectinload(DataSourceScanSchedule.notification_recipients))
    if datasource_id is not None:
        stmt = stmt.where(DataSourceScanSchedule.datasource_id == datasource_id)
    schedules = db.scalars(stmt.order_by(DataSourceScanSchedule.id.desc())).all()
    return [_serialize_schedule(db, schedule) for schedule in schedules]


def get_scan_schedule_for_datasource(db: Session, datasource_id: int) -> DataSourceScanSchedule | None:
    if not _schedule_support_ready(db):
        return None
    stmt = (
        select(DataSourceScanSchedule)
        .options(selectinload(DataSourceScanSchedule.notification_recipients))
        .where(DataSourceScanSchedule.datasource_id == datasource_id)
        .order_by(DataSourceScanSchedule.id.desc())
        .limit(1)
    )
    return db.scalar(stmt)


def upsert_scan_schedule(db: Session, payload: DataSourceScanScheduleCreate | DataSourceScanScheduleUpdate) -> DataSourceScanScheduleOut:
    if not _schedule_support_ready(db):
        raise ValueError("Datasource scan schedule table unavailable")
    normalized = validate_schedule_payload(payload.model_dump(), existing=None)
    datasource = db.get(DataSource, payload.datasource_id)
    if datasource is None:
        raise ValueError("Datasource not found")
    schedule = get_scan_schedule_for_datasource(db, payload.datasource_id)
    if schedule is None:
        schedule = DataSourceScanSchedule(datasource_id=payload.datasource_id)
        db.add(schedule)
    schedule.datasource_id = payload.datasource_id
    schedule.schedule_mode = str(normalized["schedule_mode"])
    schedule.schedule_enabled = bool(normalized["schedule_enabled"])
    schedule.schedule_every_minutes = normalized.get("schedule_every_minutes")
    schedule.schedule_time = normalized.get("schedule_time")
    schedule.schedule_day_of_week = normalized.get("schedule_day_of_week")
    schedule.schedule_day_of_month = normalized.get("schedule_day_of_month")
    schedule.schedule_anchor_date = normalized.get("schedule_anchor_date")
    schedule.schedule_next_run_at = compute_next_run_at(schedule)

    recipient_ids = [int(user_id) for user_id in payload.recipient_user_ids if int(user_id) > 0]
    recipients = db.scalars(select(User).where(User.id.in_(recipient_ids), User.is_active.is_(True))).all() if recipient_ids else []
    schedule.notification_recipients = _dedupe_users(recipients)
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_schedule(db, schedule)


def delete_scan_schedule(db: Session, schedule_id: int) -> None:
    if not _schedule_support_ready(db):
        return
    schedule = db.get(DataSourceScanSchedule, schedule_id)
    if schedule is None:
        return
    db.delete(schedule)
    db.commit()


def _get_or_create_scheduler_status(session: Session) -> DataSourceScanSchedulerStatus:
    status = session.get(DataSourceScanSchedulerStatus, 1)
    if status is None:
        status = DataSourceScanSchedulerStatus(id=1, scheduler_name=SCHEDULER_NAME)
        session.add(status)
        session.flush()
    return status


def update_scan_schedule_run_state(
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
    schedule = db.get(DataSourceScanSchedule, schedule_id)
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
    db.add(schedule)
    db.flush()


def mark_scan_schedule_dispatched(db: Session, schedule_id: int, *, started_at: datetime | None = None) -> None:
    if not _schedule_support_ready(db):
        return
    schedule = db.get(DataSourceScanSchedule, schedule_id)
    if schedule is None:
        return
    now = datetime.now(timezone.utc)
    schedule.schedule_last_started_at = started_at or now
    schedule.schedule_last_status = "running"
    schedule.schedule_last_error = None
    db.add(schedule)
    db.flush()


def mark_scan_schedule_queued(db: Session, schedule_id: int, *, queued_at: datetime | None = None) -> None:
    if not _schedule_support_ready(db):
        return
    schedule = db.get(DataSourceScanSchedule, schedule_id)
    if schedule is None:
        return
    schedule.schedule_last_status = "queued"
    schedule.schedule_last_error = None
    if queued_at is not None:
        schedule.schedule_last_started_at = queued_at
    db.add(schedule)
    db.flush()


def scheduler_status_snapshot(db: Session) -> dict[str, object]:
    if not _schedule_support_ready(db):
        return {
            "scheduler_name": SCHEDULER_NAME,
            "mode": settings.datasource_scan_scheduler_mode,
            "is_enabled": bool(settings.datasource_scan_scheduler_enabled),
            "health": "idle" if settings.datasource_scan_scheduler_enabled else "disabled",
            "last_started_at": None,
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_error": None,
            "last_run_summary": {},
            "scheduled_sources_total": 0,
            "next_expected_run_at": None,
    }
    status = db.get(DataSourceScanSchedulerStatus, 1)
    last_run_summary = dict(status.last_run_summary_json or {}) if status else {}
    scheduled_sources_total = int(
        db.scalar(select(func.count(DataSourceScanSchedule.id)).where(DataSourceScanSchedule.schedule_enabled.is_(True))) or 0
    )
    next_expected_run_at = db.scalar(
        select(DataSourceScanSchedule.schedule_next_run_at)
        .where(DataSourceScanSchedule.schedule_enabled.is_(True))
        .where(DataSourceScanSchedule.schedule_next_run_at.is_not(None))
        .order_by(DataSourceScanSchedule.schedule_next_run_at.asc())
        .limit(1)
    )
    health = "idle"
    if status and status.last_error:
        health = "degraded"
    elif not bool(settings.datasource_scan_scheduler_enabled):
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
    "delete_scan_schedule",
    "get_scan_schedule_for_datasource",
    "list_scan_schedules",
    "mark_scan_schedule_queued",
    "mark_scan_schedule_dispatched",
    "scheduler_status_snapshot",
    "search_datasource_schedule_users",
    "update_scan_schedule_run_state",
    "upsert_scan_schedule",
]
