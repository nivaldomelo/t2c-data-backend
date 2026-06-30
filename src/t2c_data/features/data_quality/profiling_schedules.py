from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import inspect, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import settings
from t2c_data.features.data_quality.rule_management import search_rule_notification_users
from t2c_data.features.data_quality.schedule_utils import compute_next_run_at, describe_schedule, validate_schedule_payload
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQProfilingSchedule
from t2c_data.schemas.dq import (
    DQProfilingScheduleCreate,
    DQProfilingScheduleOut,
    DQProfilingScheduleRecipientOut,
    DQProfilingScheduleUpdate,
)
from t2c_data.services.data_quality import configured_execution_engine


def _has_table(session: Session, table_name: str) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    inspector = inspect(bind)
    return inspector.has_table(table_name, schema=settings.db_schema)


def _schedule_support_ready(session: Session) -> bool:
    if not _has_table(session, "dq_profiling_schedules"):
        return False
    bind = session.get_bind()
    if bind is None:
        return False
    try:
        columns = {column["name"] for column in inspect(bind).get_columns("dq_profiling_schedules", schema=settings.db_schema)}
    except Exception:  # noqa: BLE001
        return False
    required = {
        "execution_engine",
        "scope",
        "name",
        "schedule_mode",
        "schedule_enabled",
        "schedule_every_minutes",
        "schedule_time",
        "schedule_timezone",
        "schedule_day_of_week",
        "schedule_day_of_month",
        "schedule_anchor_date",
        "schedule_last_run_at",
        "schedule_next_run_at",
        "table_ids_json",
    }
    return required.issubset(columns)


def search_profiling_schedule_users(db: Session, q: str = "", limit: int = 20) -> list[dict[str, object]]:
    return search_rule_notification_users(db=db, q=q, limit=limit)


def _dedupe_users(users: Iterable[User]) -> list[User]:
    seen: set[int] = set()
    output: list[User] = []
    for user in users:
        if user.id in seen:
            continue
        seen.add(user.id)
        output.append(user)
    return output


def _load_schedule_target(session: Session, schedule: DQProfilingSchedule) -> tuple[str, str | None]:
    if schedule.scope == "table" and schedule.table_id is not None:
        row = session.execute(
            select(TableEntity, Schema.name.label("schema_name"), Database.name.label("database_name"), DataSource.name.label("datasource_name"))
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .join(DataSource, Database.datasource_id == DataSource.id)
            .where(TableEntity.id == schedule.table_id)
            .limit(1)
        ).first()
        if row:
            table, schema_name, _database_name, datasource_name = row
            return f"Tabela {datasource_name}.{schema_name}.{table.name}", f"{schema_name}.{table.name}"
        return "Tabela não encontrada", None
    if schedule.scope == "schema" and schedule.datasource_id is not None and schedule.schema_name:
        datasource = session.get(DataSource, schedule.datasource_id)
        if datasource:
            return f"Schema {datasource.name}.{schedule.schema_name}", None
        return f"Schema {schedule.schema_name}", None
    if schedule.scope == "datasource" and schedule.datasource_id is not None:
        datasource = session.get(DataSource, schedule.datasource_id)
        if datasource:
            return f"Data Source {datasource.name}", None
        return "Data Source não encontrado", None
    if schedule.scope == "tables" and schedule.datasource_id is not None:
        datasource = session.get(DataSource, schedule.datasource_id)
        table_count = len(schedule.table_ids_json or [])
        if datasource and schedule.schema_name:
            return f"{table_count} tabela(s) em {datasource.name}.{schedule.schema_name}", None
        if datasource:
            return f"{table_count} tabela(s) em {datasource.name}", None
        return f"{table_count} tabela(s) selecionada(s)", None
    return "Escopo não definido", None


def _normalize_table_ids(values: Iterable[Any] | None) -> list[int]:
    table_ids: list[int] = []
    seen: set[int] = set()
    for value in values or []:
        try:
            table_id = int(value)
        except Exception:  # noqa: BLE001
            continue
        if table_id <= 0 or table_id in seen:
            continue
        seen.add(table_id)
        table_ids.append(table_id)
    return sorted(table_ids)


def _serialize_recipients(schedule: DQProfilingSchedule) -> list[DQProfilingScheduleRecipientOut]:
    return [
        DQProfilingScheduleRecipientOut(
            id=user.id,
            display_name=user.name or user.full_name or user.email,
            email=user.email,
        )
        for user in _dedupe_users(schedule.notification_recipients or [])
    ]


def _serialize_schedule(session: Session, schedule: DQProfilingSchedule) -> DQProfilingScheduleOut:
    target_label, table_fqn = _load_schedule_target(session, schedule)
    return DQProfilingScheduleOut(
        id=schedule.id,
        scope=schedule.scope,
        name=schedule.name,
        table_id=schedule.table_id,
        datasource_id=schedule.datasource_id,
        schema_name=schedule.schema_name,
        table_ids=list(schedule.table_ids_json or []),
        table_fqn=table_fqn,
        target_label=target_label,
        execution_engine=configured_execution_engine(getattr(schedule, "execution_engine", None)),
        schedule_mode=schedule.schedule_mode,
        schedule_enabled=bool(schedule.schedule_enabled),
        schedule_every_minutes=schedule.schedule_every_minutes,
        schedule_time=schedule.schedule_time,
        schedule_timezone=schedule.schedule_timezone,
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
        schema_limit=schedule.schema_limit,
        schema_concurrency=schedule.schema_concurrency,
        schema_sample_fraction=schedule.schema_sample_fraction,
        schema_include_tables_json=list(schedule.schema_include_tables_json or []),
        schema_exclude_tables_json=list(schedule.schema_exclude_tables_json or []),
        schema_columns_json=list(schedule.schema_columns_json or []),
        notification_recipients=_serialize_recipients(schedule),
        created_at=schedule.created_at,
        updated_at=schedule.updated_at,
    )


def get_profiling_schedule_for_target(
    db: Session,
    *,
    scope: str,
    table_id: int | None = None,
    datasource_id: int | None = None,
    schema_name: str | None = None,
    table_ids: list[int] | None = None,
) -> DQProfilingSchedule | None:
    if not _schedule_support_ready(db):
        return None
    stmt = select(DQProfilingSchedule).options(selectinload(DQProfilingSchedule.notification_recipients))
    if scope == "table":
        if table_id is None:
            return None
        stmt = stmt.where(DQProfilingSchedule.scope == "table", DQProfilingSchedule.table_id == table_id)
    elif scope == "schema":
        if datasource_id is None or not schema_name:
            return None
        stmt = stmt.where(
            DQProfilingSchedule.scope == "schema",
            DQProfilingSchedule.datasource_id == datasource_id,
            DQProfilingSchedule.schema_name == schema_name,
        )
    elif scope == "datasource":
        if datasource_id is None:
            return None
        stmt = stmt.where(
            DQProfilingSchedule.scope == "datasource",
            DQProfilingSchedule.datasource_id == datasource_id,
        )
    elif scope == "tables":
        if datasource_id is None:
            return None
        normalized_ids = _normalize_table_ids(table_ids)
        stmt = stmt.where(
            DQProfilingSchedule.scope == "tables",
            DQProfilingSchedule.datasource_id == datasource_id,
        )
        if schema_name:
            stmt = stmt.where(DQProfilingSchedule.schema_name == schema_name)
        if normalized_ids:
            stmt = stmt.where(DQProfilingSchedule.table_ids_json == normalized_ids)
    else:
        return None
    return db.scalar(stmt.order_by(DQProfilingSchedule.id.desc()).limit(1))


def list_profiling_schedules(
    db: Session,
    *,
    scope: str | None = None,
    table_id: int | None = None,
    datasource_id: int | None = None,
    schema_name: str | None = None,
) -> list[DQProfilingScheduleOut]:
    if not _schedule_support_ready(db):
        return []
    stmt = select(DQProfilingSchedule).options(selectinload(DQProfilingSchedule.notification_recipients))
    if scope:
        stmt = stmt.where(DQProfilingSchedule.scope == scope)
    if table_id is not None:
        stmt = stmt.where(DQProfilingSchedule.table_id == table_id)
    if datasource_id is not None:
        stmt = stmt.where(DQProfilingSchedule.datasource_id == datasource_id)
    if schema_name is not None:
        stmt = stmt.where(DQProfilingSchedule.schema_name == schema_name)
    schedules = db.scalars(stmt.order_by(DQProfilingSchedule.id.desc())).all()
    return [_serialize_schedule(db, schedule) for schedule in schedules]


def upsert_profiling_schedule(db: Session, payload: DQProfilingScheduleCreate | DQProfilingScheduleUpdate) -> DQProfilingScheduleOut:
    if not _schedule_support_ready(db):
        raise ValueError("Profiling schedule table unavailable")

    normalized = validate_schedule_payload(payload.model_dump(), existing=None)
    scope = normalized.get("scope") or payload.scope
    table_id = payload.table_id if scope == "table" else None
    datasource_id = payload.datasource_id
    schema_name = (payload.schema_name or "").strip() if payload.schema_name else None
    table_ids = _normalize_table_ids(getattr(payload, "table_ids", None))
    if scope == "table" and table_id is None:
        raise ValueError("table_id is required for scope=table")
    if scope == "schema" and (datasource_id is None or not schema_name):
        raise ValueError("datasource_id and schema_name are required for scope=schema")
    if scope == "datasource" and datasource_id is None:
        raise ValueError("datasource_id is required for scope=datasource")
    if scope == "tables" and (datasource_id is None or not table_ids):
        raise ValueError("datasource_id and table_ids are required for scope=tables")

    schedule = get_profiling_schedule_for_target(
        db,
        scope=scope,
        table_id=table_id,
        datasource_id=datasource_id,
        schema_name=schema_name,
        table_ids=table_ids,
    )
    if schedule is None:
        schedule = DQProfilingSchedule(scope=scope)
        db.add(schedule)
    schedule.scope = scope
    schedule.name = (payload.name or "").strip() or None
    schedule.table_id = table_id
    schedule.datasource_id = datasource_id
    schedule.schema_name = schema_name
    schedule.table_ids_json = table_ids
    schedule.execution_engine = configured_execution_engine(payload.execution_engine)
    schedule.schedule_mode = str(normalized["schedule_mode"])
    schedule.schedule_enabled = bool(normalized["schedule_enabled"])
    schedule.schedule_every_minutes = normalized.get("schedule_every_minutes")
    schedule.schedule_time = normalized.get("schedule_time")
    schedule.schedule_timezone = (payload.schedule_timezone or "").strip() or None
    schedule.schedule_day_of_week = normalized.get("schedule_day_of_week")
    schedule.schedule_day_of_month = normalized.get("schedule_day_of_month")
    schedule.schedule_anchor_date = normalized.get("schedule_anchor_date")
    schedule.schedule_next_run_at = compute_next_run_at(schedule)
    schedule.schema_limit = payload.schema_limit
    schedule.schema_concurrency = payload.schema_concurrency
    schedule.schema_sample_fraction = payload.schema_sample_fraction
    schedule.schema_include_tables_json = payload.schema_include_tables_json or []
    schedule.schema_exclude_tables_json = payload.schema_exclude_tables_json or []
    schedule.schema_columns_json = payload.schema_columns_json or []

    recipient_ids = [int(user_id) for user_id in payload.recipient_user_ids if int(user_id) > 0]
    recipients = db.scalars(select(User).where(User.id.in_(recipient_ids), User.is_active.is_(True))).all() if recipient_ids else []
    schedule.notification_recipients = _dedupe_users(recipients)
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_schedule(db, schedule)


def update_profiling_schedule(
    db: Session,
    schedule_id: int,
    payload: DQProfilingScheduleUpdate,
) -> DQProfilingScheduleOut:
    if not _schedule_support_ready(db):
        raise ValueError("Profiling schedule table unavailable")
    schedule = db.get(DQProfilingSchedule, schedule_id)
    if schedule is None:
        raise ValueError("Profiling schedule not found")

    normalized = validate_schedule_payload(payload.model_dump(), existing={"schedule_mode": schedule.schedule_mode, "schedule_enabled": schedule.schedule_enabled, "schedule_every_minutes": schedule.schedule_every_minutes, "schedule_time": schedule.schedule_time, "schedule_day_of_week": schedule.schedule_day_of_week, "schedule_day_of_month": schedule.schedule_day_of_month, "schedule_anchor_date": schedule.schedule_anchor_date})
    scope = normalized.get("scope") or payload.scope
    table_id = payload.table_id if scope == "table" else None
    datasource_id = payload.datasource_id
    schema_name = (payload.schema_name or "").strip() if payload.schema_name else None
    table_ids = _normalize_table_ids(getattr(payload, "table_ids", None))
    if scope == "table" and table_id is None:
        raise ValueError("table_id is required for scope=table")
    if scope == "schema" and (datasource_id is None or not schema_name):
        raise ValueError("datasource_id and schema_name are required for scope=schema")
    if scope == "datasource" and datasource_id is None:
        raise ValueError("datasource_id is required for scope=datasource")
    if scope == "tables" and (datasource_id is None or not table_ids):
        raise ValueError("datasource_id and table_ids are required for scope=tables")

    schedule.scope = scope
    schedule.name = (payload.name or "").strip() or None
    schedule.table_id = table_id
    schedule.datasource_id = datasource_id
    schedule.schema_name = schema_name
    schedule.table_ids_json = table_ids
    schedule.execution_engine = configured_execution_engine(payload.execution_engine)
    schedule.schedule_mode = str(normalized["schedule_mode"])
    schedule.schedule_enabled = bool(normalized["schedule_enabled"])
    schedule.schedule_every_minutes = normalized.get("schedule_every_minutes")
    schedule.schedule_time = normalized.get("schedule_time")
    schedule.schedule_timezone = (payload.schedule_timezone or "").strip() or None
    schedule.schedule_day_of_week = normalized.get("schedule_day_of_week")
    schedule.schedule_day_of_month = normalized.get("schedule_day_of_month")
    schedule.schedule_anchor_date = normalized.get("schedule_anchor_date")
    schedule.schedule_next_run_at = compute_next_run_at(schedule)
    schedule.schema_include_tables_json = payload.schema_include_tables_json or []
    schedule.schema_exclude_tables_json = payload.schema_exclude_tables_json or []
    schedule.schema_columns_json = payload.schema_columns_json or []

    recipient_ids = [int(user_id) for user_id in payload.recipient_user_ids if int(user_id) > 0]
    recipients = db.scalars(select(User).where(User.id.in_(recipient_ids), User.is_active.is_(True))).all() if recipient_ids else []
    schedule.notification_recipients = _dedupe_users(recipients)
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_schedule(db, schedule)


def set_profiling_schedule_enabled(db: Session, schedule_id: int, *, enabled: bool) -> DQProfilingScheduleOut:
    schedule = db.get(DQProfilingSchedule, schedule_id)
    if schedule is None:
        raise ValueError("Profiling schedule not found")
    schedule.schedule_enabled = bool(enabled)
    schedule.schedule_next_run_at = compute_next_run_at(schedule)
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_schedule(db, schedule)


def get_profiling_schedule(db: Session, schedule_id: int) -> DQProfilingScheduleOut | None:
    schedule = db.get(DQProfilingSchedule, schedule_id)
    if schedule is None:
        return None
    return _serialize_schedule(db, schedule)


def delete_profiling_schedule(db: Session, schedule_id: int) -> None:
    schedule = db.get(DQProfilingSchedule, schedule_id)
    if schedule is None:
        return
    db.delete(schedule)
    db.commit()


def update_profiling_schedule_run_state(
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
    schedule = db.get(DQProfilingSchedule, schedule_id)
    if schedule is None:
        return
    now = datetime.now(timezone.utc)
    if started_at is not None:
        schedule.schedule_last_started_at = started_at
    elif status == "running" and schedule.schedule_last_started_at is None:
        schedule.schedule_last_started_at = now
    if finished_at is not None:
        schedule.schedule_last_finished_at = finished_at
    elif status in {"success", "failed"}:
        schedule.schedule_last_finished_at = now
    if status in {"running", "success", "failed"}:
        schedule.schedule_last_run_at = schedule.schedule_last_started_at or schedule.schedule_last_run_at or now
    schedule.schedule_last_status = status
    schedule.schedule_last_error = error_message[:2000] if error_message else None
    schedule.schedule_next_run_at = compute_next_run_at(schedule)
    db.add(schedule)
    db.commit()


def mark_profiling_schedule_dispatched(db: Session, schedule_id: int | None) -> None:
    if schedule_id is None or not _schedule_support_ready(db):
        return
    schedule = db.get(DQProfilingSchedule, schedule_id)
    if schedule is None:
        return
    now = datetime.now(timezone.utc)
    schedule.schedule_last_started_at = now
    schedule.schedule_last_run_at = now
    schedule.schedule_last_status = "running"
    schedule.schedule_last_error = None
    schedule.schedule_next_run_at = compute_next_run_at(schedule)
    db.add(schedule)
    db.commit()


__all__ = [
    "delete_profiling_schedule",
    "get_profiling_schedule",
    "get_profiling_schedule_for_target",
    "list_profiling_schedules",
    "mark_profiling_schedule_dispatched",
    "search_profiling_schedule_users",
    "upsert_profiling_schedule",
    "set_profiling_schedule_enabled",
    "update_profiling_schedule",
    "update_profiling_schedule_run_state",
]
