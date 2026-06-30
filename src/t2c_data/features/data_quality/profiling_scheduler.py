from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import func, inspect, select, text
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import embedded_scheduler_allowed, normalize_scheduler_mode, settings
from t2c_data.core.db import SessionLocal
from t2c_data.core.json_utils import to_jsonable
from t2c_data.features.data_quality.contracts import DefaultDQExecutionGateway
from t2c_data.features.data_quality.notifications import notify_dq_profiling_failure
from t2c_data.features.data_quality.profiling_schedules import (
    mark_profiling_schedule_dispatched,
    update_profiling_schedule_run_state,
)
from t2c_data.features.data_quality.schedule_utils import compute_next_run_at
from t2c_data.features.platform.jobs import finish_integration_job, maybe_start_integration_job
from t2c_data.models.catalog import Database, Schema, TableEntity
from t2c_data.models.dq import DQProfilingSchedule, DQProfilingSchedulerStatus
from t2c_data.services.data_quality import configured_execution_engine
from t2c_data.schemas.dq import DQProfilingLaunchOut
from t2c_data.features.data_quality.spark_runs import update_dq_run_fields

logger = logging.getLogger(__name__)

SCHEDULER_NAME = "dq_profiling"
_DQ_PROFILING_SCHEDULER_LOCK_KEY = 791022342
_scheduler_task: asyncio.Task[None] | None = None
_scheduler_bootstrap_task: asyncio.Task[None] | None = None


@dataclass
class SchedulerRuntimeState:
    phase: str = "idle"
    mode: str = "worker"
    is_enabled: bool = True
    bootstrap_attempts: int = 0
    last_error: str | None = None
    last_error_at: str | None = None
    last_started_at: str | None = None
    last_heartbeat_at: str | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    last_run_summary: dict[str, object] | None = None


_runtime_state = SchedulerRuntimeState(
    phase="idle",
    mode=normalize_scheduler_mode(settings.dq_profiling_scheduler_mode),
    is_enabled=bool(settings.dq_profiling_scheduler_enabled),
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_runtime_state(
    *,
    phase: str | None = None,
    mode: str | None = None,
    is_enabled: bool | None = None,
    started: bool = False,
    heartbeat: bool = False,
    success: bool = False,
    failure: str | None = None,
    summary: dict[str, object] | None = None,
    bootstrap_attempt_increment: bool = False,
) -> None:
    now_iso = _utcnow_iso()
    if phase is not None:
        _runtime_state.phase = phase
    if mode is not None:
        _runtime_state.mode = mode
    if is_enabled is not None:
        _runtime_state.is_enabled = is_enabled
    if bootstrap_attempt_increment:
        _runtime_state.bootstrap_attempts += 1
    if started:
        _runtime_state.last_started_at = now_iso
    if heartbeat:
        _runtime_state.last_heartbeat_at = now_iso
    if success:
        _runtime_state.last_success_at = now_iso
        _runtime_state.last_error = None
    if failure:
        _runtime_state.last_failure_at = now_iso
        _runtime_state.last_error_at = now_iso
        _runtime_state.last_error = failure[:2000]
    if summary is not None:
        _runtime_state.last_run_summary = summary


def _empty_scheduler_snapshot() -> dict[str, object]:
    return {
        "scheduler_name": SCHEDULER_NAME,
        "mode": normalize_scheduler_mode(settings.dq_profiling_scheduler_mode),
        "is_enabled": bool(settings.dq_profiling_scheduler_enabled),
        "health": "disabled" if not settings.dq_profiling_scheduler_enabled else "idle",
        "last_started_at": None,
        "last_heartbeat_at": None,
        "last_success_at": None,
        "last_failure_at": None,
        "last_error": None,
        "last_run_summary": {},
        "scheduled_profiles_total": 0,
        "next_expected_run_at": None,
    }


def _status_table_exists(session: Session) -> bool:
    bind = session.get_bind()
    if bind is not None and getattr(bind.dialect, "name", None) != "postgresql":
        return True
    regclass = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.dq_profiling_scheduler_status"},
    ).scalar_one()
    return regclass is not None


def _support_is_ready(session: Session) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    inspector = inspect(bind)
    if not inspector.has_table("dq_profiling_scheduler_status", schema=settings.db_schema):
        return False
    if not inspector.has_table("dq_profiling_schedules", schema=settings.db_schema):
        return False
    column_names = {column["name"] for column in inspector.get_columns("dq_profiling_schedules", schema=settings.db_schema)}
    required = {
        "execution_engine",
        "scope",
        "schedule_mode",
        "schedule_enabled",
        "schedule_every_minutes",
        "schedule_time",
        "schedule_day_of_week",
        "schedule_day_of_month",
        "schedule_anchor_date",
        "schedule_last_run_at",
        "schedule_next_run_at",
    }
    return required.issubset(column_names)


def _get_or_create_scheduler_status(session: Session) -> DQProfilingSchedulerStatus:
    status = session.get(DQProfilingSchedulerStatus, 1)
    if status is None:
        status = DQProfilingSchedulerStatus(id=1, scheduler_name=SCHEDULER_NAME)
        session.add(status)
        session.flush()
    return status


def _update_scheduler_status(
    session: Session,
    *,
    mode: str,
    is_enabled: bool,
    started: bool = False,
    heartbeat: bool = False,
    success: bool = False,
    failure: str | None = None,
    summary: dict[str, object] | None = None,
) -> DQProfilingSchedulerStatus:
    status = _get_or_create_scheduler_status(session)
    now_iso = _utcnow_iso()
    status.mode = mode
    status.is_enabled = is_enabled
    if started or not status.last_started_at:
        status.last_started_at = now_iso
    if heartbeat:
        status.last_heartbeat_at = now_iso
    if success:
        status.last_success_at = now_iso
        status.last_error = None
    if failure:
        status.last_failure_at = now_iso
        status.last_error = failure[:2000]
    if summary is not None:
        status.last_run_summary_json = to_jsonable(summary)
    session.add(status)
    session.flush()
    return status


def _persist_scheduler_status(
    *,
    mode: str,
    is_enabled: bool,
    started: bool = False,
    heartbeat: bool = False,
    success: bool = False,
    failure: str | None = None,
    summary: dict[str, object] | None = None,
) -> bool:
    try:
        with SessionLocal() as session:
            if not _status_table_exists(session):
                logger.warning("dq profiling scheduler status table unavailable schema=%s table=dq_profiling_scheduler_status", settings.db_schema)
                return False
            _update_scheduler_status(
                session,
                mode=mode,
                is_enabled=is_enabled,
                started=started,
                heartbeat=heartbeat,
                success=success,
                failure=failure,
                summary=summary,
            )
            session.commit()
            return True
    except Exception:  # noqa: BLE001
        logger.exception(
            "dq profiling scheduler status persistence failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
            mode,
            is_enabled,
            started,
            heartbeat,
            success,
        )
        return False


def _try_update_scheduler_status_in_session(
    session: Session,
    *,
    mode: str,
    is_enabled: bool,
    started: bool = False,
    heartbeat: bool = False,
    success: bool = False,
    failure: str | None = None,
    summary: dict[str, object] | None = None,
) -> bool:
    try:
        if not _status_table_exists(session):
            session.rollback()
            return False
        _update_scheduler_status(
            session,
            mode=mode,
            is_enabled=is_enabled,
            started=started,
            heartbeat=heartbeat,
            success=success,
            failure=failure,
            summary=summary,
        )
        session.flush()
        return True
    except Exception:  # noqa: BLE001
        session.rollback()
        logger.exception(
            "dq profiling scheduler status update failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
            mode,
            is_enabled,
            started,
            heartbeat,
            success,
        )
        return False


def _scheduler_health() -> str:
    if not settings.dq_profiling_scheduler_enabled:
        return "disabled"
    if _runtime_state.phase in {"bootstrap_failed", "failed"}:
        return "degraded"
    if _runtime_state.phase == "running":
        return "healthy"
    return "idle"


def _next_expected_for_schedules(session: Session, schedules: list[DQProfilingSchedule]) -> str | None:
    next_candidates = []
    for schedule in schedules:
        next_candidate = schedule.schedule_next_run_at or compute_next_run_at(schedule)
        if next_candidate is not None:
            next_candidates.append(next_candidate)
    if not next_candidates:
        return None
    return min(next_candidates).astimezone(timezone.utc).isoformat()


def scheduler_status_snapshot(session: Session) -> dict[str, object]:
    try:
        if not _support_is_ready(session):
            return _empty_scheduler_snapshot()
        status = _get_or_create_scheduler_status(session)
        schedules = session.scalars(
            select(DQProfilingSchedule)
            .options(selectinload(DQProfilingSchedule.notification_recipients))
            .where(DQProfilingSchedule.schedule_enabled.is_(True), DQProfilingSchedule.schedule_mode != "manual")
        ).all()
        return {
            "scheduler_name": status.scheduler_name,
            "mode": status.mode,
            "is_enabled": bool(status.is_enabled),
            "health": _scheduler_health(),
            "last_started_at": status.last_started_at,
            "last_heartbeat_at": status.last_heartbeat_at,
            "last_success_at": status.last_success_at,
            "last_failure_at": status.last_failure_at,
            "last_error": status.last_error,
            "last_run_summary": status.last_run_summary_json or {},
            "scheduled_profiles_total": len(schedules),
            "next_expected_run_at": _next_expected_for_schedules(session, schedules),
        }
    except Exception:  # noqa: BLE001
        logger.exception("dq profiling scheduler snapshot fallback activated")
        return _empty_scheduler_snapshot()


def _acquire_scheduler_lock(session: Session) -> bool:
    try:
        return bool(session.execute(select(func.pg_try_advisory_lock(_DQ_PROFILING_SCHEDULER_LOCK_KEY))).scalar_one())
    except Exception:  # noqa: BLE001
        logger.exception("dq profiling scheduler advisory lock acquisition failed")
        return False


def _release_scheduler_lock(session: Session) -> None:
    try:
        session.execute(select(func.pg_advisory_unlock(_DQ_PROFILING_SCHEDULER_LOCK_KEY)))
    except Exception:  # noqa: BLE001
        logger.exception("dq profiling scheduler advisory lock release failed")


def _due_schedules(session: Session) -> list[DQProfilingSchedule]:
    now = datetime.now(timezone.utc)
    schedules = session.scalars(
        select(DQProfilingSchedule)
        .options(selectinload(DQProfilingSchedule.notification_recipients))
        .where(DQProfilingSchedule.schedule_enabled.is_(True))
        .where(DQProfilingSchedule.schedule_mode != "manual")
        .order_by(DQProfilingSchedule.id.asc())
    ).all()
    due: list[DQProfilingSchedule] = []
    for schedule in schedules:
        next_run = schedule.schedule_next_run_at or compute_next_run_at(schedule)
        if next_run is None:
            continue
        if next_run.astimezone(timezone.utc) <= now:
            due.append(schedule)
    return due


def _table_targets_for_schema(session: Session, schedule: DQProfilingSchedule) -> list[dict[str, object]]:
    if schedule.datasource_id is None or not schedule.schema_name:
        return []
    query = (
        select(TableEntity, Schema, Database)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .where(Database.datasource_id == schedule.datasource_id)
        .where(Schema.name == schedule.schema_name)
        .where(TableEntity.table_type == "table")
    )
    include = {name.strip() for name in (schedule.schema_include_tables_json or []) if str(name).strip()}
    exclude = {name.strip() for name in (schedule.schema_exclude_tables_json or []) if str(name).strip()}
    limit = max(int(schedule.schema_limit or 200), 1)
    table_targets: list[dict[str, object]] = []
    for table, schema, database in session.execute(query.order_by(TableEntity.name)).all():
        if include and table.name not in include:
            continue
        if table.name in exclude:
            continue
        table_targets.append(
            {
                "table_id": table.id,
                "table_fqn": f"{schema.name}.{table.name}",
                "schema_name": schema.name,
                "datasource_id": database.datasource_id,
            }
        )
        if len(table_targets) >= limit:
            break
    return table_targets


def _table_targets_for_datasource(session: Session, schedule: DQProfilingSchedule) -> list[dict[str, object]]:
    if schedule.datasource_id is None:
        return []
    query = (
        select(TableEntity, Schema, Database)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .where(Database.datasource_id == schedule.datasource_id)
        .where(TableEntity.table_type == "table")
    )
    table_targets: list[dict[str, object]] = []
    for table, schema, database in session.execute(query.order_by(Schema.name, TableEntity.name)).all():
        table_targets.append(
            {
                "table_id": table.id,
                "table_fqn": f"{schema.name}.{table.name}",
                "schema_name": schema.name,
                "datasource_id": database.datasource_id,
            }
        )
    return table_targets


def _table_targets_for_tables(session: Session, schedule: DQProfilingSchedule) -> list[dict[str, object]]:
    table_ids: list[int] = []
    for value in schedule.table_ids_json or []:
        try:
            table_id = int(value)
        except Exception:  # noqa: BLE001
            continue
        if table_id > 0:
            table_ids.append(table_id)
    if not table_ids:
        return []
    query = (
        select(TableEntity, Schema, Database)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .where(TableEntity.id.in_(table_ids))
        .where(TableEntity.table_type == "table")
    )
    if schedule.datasource_id is not None:
        query = query.where(Database.datasource_id == schedule.datasource_id)
    if schedule.schema_name:
        query = query.where(Schema.name == schedule.schema_name)
    rows = session.execute(query.order_by(Schema.name, TableEntity.name)).all()
    targets: list[dict[str, object]] = []
    for table, schema, database in rows:
        targets.append(
            {
                "table_id": table.id,
                "table_fqn": f"{schema.name}.{table.name}",
                "schema_name": schema.name,
                "datasource_id": database.datasource_id,
            }
        )
    return targets


def _run_schedule_targets(
    session: Session,
    *,
    schedule: DQProfilingSchedule,
    gateway: DefaultDQExecutionGateway,
    requested_by_user_id: int | None,
    audit_action: str | None = None,
) -> DQProfilingLaunchOut:
    mark_profiling_schedule_dispatched(session, schedule.id)
    execution_engine = configured_execution_engine(schedule.execution_engine)
    scope = schedule.scope
    if scope == "table":
        if schedule.table_id is None:
            raise ValueError("Scheduled profiling table target is missing")
        table = session.get(TableEntity, schedule.table_id)
        if table is None or table.schema is None or table.schema.database is None or table.schema.database.datasource is None:
            raise ValueError("Scheduled profiling table target is unavailable")
        dq_run = gateway.create_table_run(
            table_id=table.id,
            table_fqn=f"{table.schema.name}.{table.name}",
            profiling_schedule_id=schedule.id,
            execution_engine=execution_engine,
        )
        gateway.enqueue_profiling(
            table_id=table.id,
            table_fqn=f"{table.schema.name}.{table.name}",
            columns=[],
            sample_fraction=None,
            requested_by_user_id=requested_by_user_id,
            dq_run_id=dq_run.id,
            execution_engine=execution_engine,
        )
        return DQProfilingLaunchOut(
            run_id=dq_run.id,
            scope="table",
            table_fqn=f"{table.schema.name}.{table.name}",
            tables_total=1,
            status="queued",
            execution_engine=execution_engine,
            job_run_id=None,
        )

    if scope == "schema":
        table_targets = _table_targets_for_schema(session, schedule)
        if not table_targets:
            raise ValueError("No tables found for scheduled schema profiling")
        parent_run = gateway.create_schema_run(
            datasource_id=schedule.datasource_id,
            schema_name=schedule.schema_name or "",
            profiling_schedule_id=schedule.id,
            execution_engine=execution_engine,
        )
    else:
        if scope == "datasource":
            table_targets = _table_targets_for_datasource(session, schedule)
        elif scope == "tables":
            table_targets = _table_targets_for_tables(session, schedule)
        else:
            raise ValueError(f"Unsupported profiling schedule scope '{scope}'")
        if not table_targets:
            raise ValueError("No tables found for scheduled profiling")
        parent_run = gateway.create_batch_run(
            datasource_id=schedule.datasource_id,
            scope=scope,
            schema_name=schedule.schema_name,
            profiling_schedule_id=schedule.id,
            execution_engine=execution_engine,
        )

    update_dq_run_fields(
        parent_run.id,
        profile_payload_json={
            "trigger_source": "scheduled",
            "scope_type": scope,
            "datasource_id": schedule.datasource_id,
            "schema_name": schedule.schema_name,
            "table_ids": list(schedule.table_ids_json or []),
            "schedule_id": schedule.id,
        },
    )
    gateway.enqueue_schema_profiling(
        parent_run_id=parent_run.id,
        table_targets=table_targets,
        requested_by_user_id=requested_by_user_id,
        concurrency=max(int(schedule.schema_concurrency or 5), 1),
        sample_fraction=schedule.schema_sample_fraction,
        columns=list(schedule.schema_columns_json or []),
        execution_engine=execution_engine,
    )
    return DQProfilingLaunchOut(
        run_id=parent_run.id,
        scope=scope,
        schema=schedule.schema_name,
        tables_total=len(table_targets),
        status="queued",
        execution_engine=execution_engine,
        job_run_id=None,
    )


def run_dq_profiling_scheduler_cycle(*, trigger: str = "manual", scheduler_mode: str | None = None) -> dict[str, object]:
    mode = normalize_scheduler_mode(scheduler_mode or settings.dq_profiling_scheduler_mode)
    summary: dict[str, object] = {
        "scheduler_name": SCHEDULER_NAME,
        "mode": mode,
        "trigger": trigger,
        "queued": 0,
        "failed": 0,
        "skipped": None,
        "processed_schedule_ids": [],
    }
    if mode == "embedded_dev_only" and not embedded_scheduler_allowed(mode, settings.env):
        summary["skipped"] = "embedded_not_allowed"
        summary["error"] = "Embedded schedulers are not allowed outside dev/test. Use worker mode."
        _update_runtime_state(phase="disabled", mode=mode, is_enabled=False, summary=summary)
        return summary
    job_handle = None
    job_status = "success"
    job_error: str | None = None
    job_records: int | None = 0
    job_context: dict[str, object] | None = summary
    _update_runtime_state(phase="running", mode=mode, is_enabled=bool(settings.dq_profiling_scheduler_enabled), started=True, heartbeat=True)
    session = SessionLocal()
    try:
        try:
            job_handle = maybe_start_integration_job(
                session,
                source="dq",
                job_type="profiling_scheduler",
                trigger_mode=trigger,
            )
        except HTTPException as exc:
            if exc.status_code != status.HTTP_409_CONFLICT:
                raise
            summary["skipped"] = "job_already_running"
            job_status = "skipped"
            job_error = summary["skipped"]
            job_context = summary
            _try_update_scheduler_status_in_session(
                session,
                mode=mode,
                is_enabled=bool(settings.dq_profiling_scheduler_enabled),
                heartbeat=True,
                summary=summary,
            )
            _update_runtime_state(phase="idle", mode=mode, is_enabled=bool(settings.dq_profiling_scheduler_enabled), summary=summary)
            logger.info("dq profiling scheduler skipped: another execution is already running")
            return summary
        if not _support_is_ready(session):
            summary["skipped"] = "scheduler_support_unavailable"
            job_status = "skipped"
            job_error = summary["skipped"]
            job_context = summary
            _update_runtime_state(phase="idle", mode=mode, is_enabled=bool(settings.dq_profiling_scheduler_enabled), summary=summary)
            return summary
        if not _acquire_scheduler_lock(session):
            summary["skipped"] = "scheduler_lock_unavailable"
            job_status = "skipped"
            job_error = summary["skipped"]
            job_context = summary
            _try_update_scheduler_status_in_session(
                session,
                mode=mode,
                is_enabled=bool(settings.dq_profiling_scheduler_enabled),
                heartbeat=True,
                summary=summary,
            )
            _update_runtime_state(phase="idle", mode=mode, is_enabled=bool(settings.dq_profiling_scheduler_enabled), summary=summary)
            return summary
        _try_update_scheduler_status_in_session(
            session,
            mode=mode,
            is_enabled=bool(settings.dq_profiling_scheduler_enabled),
            started=True,
            heartbeat=True,
        )
        gateway = DefaultDQExecutionGateway()
        due_schedules = _due_schedules(session)
        summary["processed"] = len(due_schedules)
        for schedule in due_schedules:
            try:
                launch = _run_schedule_targets(
                    session,
                    schedule=schedule,
                    gateway=gateway,
                    requested_by_user_id=None,
                )
                summary["queued"] += 1
                summary["processed_schedule_ids"].append(schedule.id)
                summary.setdefault("launches", []).append(
                    {
                        "schedule_id": schedule.id,
                        "run_id": launch.run_id,
                        "scope": launch.scope,
                        "tables_total": launch.tables_total,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                summary["failed"] += 1
                logger.exception("dq profiling scheduler failed schedule_id=%s", schedule.id)
                update_profiling_schedule_run_state(
                    session,
                    schedule_id=schedule.id,
                    status="failed",
                    error_message=str(exc),
                )
                try:
                    schedule_table = session.get(TableEntity, schedule.table_id) if schedule.table_id is not None else None
                    notify_dq_profiling_failure(
                        session,
                        schedule=schedule,
                        table=schedule_table,
                        table_fqn=f"{schedule.schema_name}.*" if schedule.scope == "schema" and schedule.schema_name else None,
                        dq_run=None,
                        error_message=str(exc),
                        reporter_user_id=None,
                    )
                except Exception:
                    logger.exception("dq profiling scheduler failure notification failed schedule_id=%s", schedule.id)
        _try_update_scheduler_status_in_session(
            session,
            mode=mode,
            is_enabled=bool(settings.dq_profiling_scheduler_enabled),
            success=True,
            summary=summary,
        )
        job_records = int(summary.get("queued") or 0)
        job_status = "failed" if summary["failed"] > 0 else "success"
        job_context = summary
        _update_runtime_state(phase="idle", mode=mode, is_enabled=bool(settings.dq_profiling_scheduler_enabled), heartbeat=True, success=True, summary=summary)
        session.commit()
        return summary
    except Exception:  # noqa: BLE001
        logger.exception("dq profiling scheduler cycle failed mode=%s", mode)
        job_status = "failed"
        job_error = summary.get("error") if isinstance(summary.get("error"), str) else "scheduler cycle failed"
        job_records = int(summary.get("queued") or 0)
        job_context = summary
        _update_runtime_state(phase="failed", mode=mode, is_enabled=bool(settings.dq_profiling_scheduler_enabled), failure="scheduler cycle failed", summary=summary)
        return summary
    finally:
        try:
            finish_integration_job(
                session,
                job_handle,
                status=job_status,
                records_processed=job_records,
                error=job_error,
                context_json=job_context,
            )
            _release_scheduler_lock(session)
        except Exception:
            pass
        session.close()


def run_profiling_schedule_now(
    session: Session,
    *,
    schedule_id: int,
    requested_by_user_id: int | None = None,
    execution_gateway: DefaultDQExecutionGateway | None = None,
) -> DQProfilingLaunchOut:
    schedule = session.get(DQProfilingSchedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profiling schedule not found")
    gateway = execution_gateway or DefaultDQExecutionGateway()
    return _run_schedule_targets(
        session,
        schedule=schedule,
        gateway=gateway,
        requested_by_user_id=requested_by_user_id,
    )


async def _scheduler_loop() -> None:
    interval_minutes = max(int(settings.dq_profiling_scheduler_poll_interval_minutes or 1), 1)
    logger.info("dq profiling scheduler started interval_minutes=%s mode=embedded_dev_only", interval_minutes)
    while True:
        try:
            await asyncio.to_thread(run_dq_profiling_scheduler_cycle, trigger="scheduled", scheduler_mode="embedded_dev_only")
            _update_runtime_state(phase="running", mode="embedded_dev_only", is_enabled=bool(settings.dq_profiling_scheduler_enabled), heartbeat=True)
        except Exception as exc:  # noqa: BLE001
            logger.exception("dq profiling scheduler refresh failed mode=embedded_dev_only error=%s", exc)
            _update_runtime_state(phase="failed", mode="embedded_dev_only", failure=str(exc))
        await asyncio.sleep(max(int(settings.dq_profiling_scheduler_poll_interval_minutes or 1), 1) * 60)


async def run_dq_profiling_scheduler_forever() -> None:
    if not settings.dq_profiling_scheduler_enabled:
        logger.info("dq profiling scheduler disabled in dedicated worker")
        _persist_scheduler_status(mode=normalize_scheduler_mode(settings.dq_profiling_scheduler_mode), is_enabled=False, heartbeat=True)
        return
    _persist_scheduler_status(mode="dedicated", is_enabled=True, started=True, heartbeat=True)
    while True:
        try:
            await asyncio.to_thread(run_dq_profiling_scheduler_cycle, trigger="scheduled", scheduler_mode="dedicated")
        except Exception as exc:  # noqa: BLE001
            logger.exception("dq profiling scheduler dedicated worker failed error=%s", exc)
        await asyncio.sleep(max(int(settings.dq_profiling_scheduler_poll_interval_minutes or 1), 1) * 60)


async def _bootstrap_embedded_scheduler() -> None:
    global _scheduler_task
    configured_mode = normalize_scheduler_mode(settings.dq_profiling_scheduler_mode)
    try:
        _persist_scheduler_status(
            mode=configured_mode,
            is_enabled=bool(settings.dq_profiling_scheduler_enabled),
            started=True,
            heartbeat=True,
        )
        if not embedded_scheduler_allowed(configured_mode, settings.env):
            logger.info("dq profiling scheduler embedded skipped mode=%s", configured_mode)
            _persist_scheduler_status(mode=configured_mode, is_enabled=bool(settings.dq_profiling_scheduler_enabled), heartbeat=True)
            return
        if not settings.dq_profiling_scheduler_enabled:
            logger.info("dq profiling scheduler disabled")
            _persist_scheduler_status(mode=configured_mode, is_enabled=False)
            return
        if _scheduler_task is not None and not _scheduler_task.done():
            logger.info("dq profiling scheduler already running")
            return
        _scheduler_task = asyncio.create_task(_scheduler_loop(), name="dq-profiling-scheduler")
        logger.info("dq profiling scheduler bootstrap completed")
    except Exception as exc:  # noqa: BLE001
        logger.exception("dq profiling scheduler bootstrap failed retrying error=%s", exc)
        _update_runtime_state(phase="bootstrap_failed", mode=configured_mode, failure=str(exc), bootstrap_attempt_increment=True)


def start_dq_profiling_scheduler() -> None:
    global _scheduler_bootstrap_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.exception("dq profiling scheduler bootstrap could not be scheduled: no running event loop")
        return
    if _scheduler_bootstrap_task is not None and not _scheduler_bootstrap_task.done():
        logger.info("dq profiling scheduler bootstrap already in progress")
        return
    if _scheduler_task is not None and not _scheduler_task.done():
        logger.info("dq profiling scheduler already running")
        return
    _scheduler_bootstrap_task = loop.create_task(_bootstrap_embedded_scheduler(), name="dq-profiling-scheduler-bootstrap")
    logger.info(
        "dq profiling scheduler bootstrap scheduled mode=%s enabled=%s",
        normalize_scheduler_mode(settings.dq_profiling_scheduler_mode),
        settings.dq_profiling_scheduler_enabled,
    )


async def stop_dq_profiling_scheduler() -> None:
    global _scheduler_task, _scheduler_bootstrap_task
    bootstrap_task = _scheduler_bootstrap_task
    _scheduler_bootstrap_task = None
    if bootstrap_task is not None and not bootstrap_task.done():
        bootstrap_task.cancel()
        try:
            await bootstrap_task
        except asyncio.CancelledError:
            logger.info("dq profiling scheduler bootstrap stopped")
    task = _scheduler_task
    _scheduler_task = None
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info("dq profiling scheduler stopped")


__all__ = [
    "run_dq_profiling_scheduler_cycle",
    "run_dq_profiling_scheduler_forever",
    "run_profiling_schedule_now",
    "scheduler_status_snapshot",
    "start_dq_profiling_scheduler",
    "stop_dq_profiling_scheduler",
]
