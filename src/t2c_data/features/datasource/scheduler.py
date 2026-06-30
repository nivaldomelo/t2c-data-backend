from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import func, inspect, select, text
from sqlalchemy.orm import Session

from t2c_data.core.config import embedded_scheduler_allowed, normalize_scheduler_mode, settings
from t2c_data.core.db import SessionLocal
from t2c_data.core.json_utils import to_jsonable
from t2c_data.features.datasource.schedules import (
    _schedule_support_ready,
    mark_scan_schedule_queued,
    update_scan_schedule_run_state,
)
from t2c_data.features.scanner.application import enqueue_datasource_scan
from t2c_data.models.catalog import DataSource
from t2c_data.models.datasource_scheduler import DataSourceScanSchedule, DataSourceScanSchedulerStatus

logger = logging.getLogger(__name__)

SCHEDULER_NAME = "datasource_scan"
_DATASOURCE_SCAN_SCHEDULER_LOCK_KEY = 791022343
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
    mode=normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
    is_enabled=bool(settings.datasource_scan_scheduler_enabled),
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


def _scheduler_status_table_exists(session: Session) -> bool:
    bind = session.get_bind()
    if bind is not None and getattr(bind.dialect, "name", None) != "postgresql":
        return True
    regclass = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.datasource_scan_scheduler_status"},
    ).scalar_one()
    return regclass is not None


def _get_or_create_scheduler_status(session: Session) -> DataSourceScanSchedulerStatus:
    status = session.get(DataSourceScanSchedulerStatus, 1)
    if status is None:
        status = DataSourceScanSchedulerStatus(id=1, scheduler_name=SCHEDULER_NAME)
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
) -> DataSourceScanSchedulerStatus:
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
            if not _scheduler_status_table_exists(session):
                logger.warning(
                    "datasource scan scheduler status table unavailable schema=%s table=datasource_scan_scheduler_status",
                    settings.db_schema,
                )
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
            "datasource scan scheduler status persistence failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
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
        if not _scheduler_status_table_exists(session):
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
            "datasource scan scheduler status update failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
            mode,
            is_enabled,
            started,
            heartbeat,
            success,
        )
        return False


def _advisory_lock(session: Session) -> bool:
    return bool(session.execute(text("SELECT pg_try_advisory_lock(:lock_key)"), {"lock_key": _DATASOURCE_SCAN_SCHEDULER_LOCK_KEY}).scalar_one())


def _release_advisory_lock(session: Session) -> None:
    try:
        session.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": _DATASOURCE_SCAN_SCHEDULER_LOCK_KEY})
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()


def _select_due_schedules(session: Session) -> list[DataSourceScanSchedule]:
    now = datetime.now(timezone.utc)
    schedules = session.scalars(
        select(DataSourceScanSchedule)
        .where(DataSourceScanSchedule.schedule_enabled.is_(True))
        .order_by(DataSourceScanSchedule.schedule_next_run_at.asc().nulls_last(), DataSourceScanSchedule.id.asc())
    ).all()
    due: list[DataSourceScanSchedule] = []
    for schedule in schedules:
        if schedule.schedule_next_run_at is None:
            from t2c_data.features.data_quality.schedule_utils import compute_next_run_at

            schedule.schedule_next_run_at = compute_next_run_at(schedule)
            session.add(schedule)
            continue
        if schedule.schedule_next_run_at <= now:
            due.append(schedule)
    session.flush()
    return due


def _summary_from_cycle(
    due_count: int,
    success_count: int,
    failed_count: int,
    skipped_count: int,
    next_expected_run_at: datetime | None,
) -> dict[str, object]:
    return {
        "due_count": due_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "next_expected_run_at": next_expected_run_at.isoformat() if next_expected_run_at else None,
    }


def run_datasource_scan_scheduler_cycle(*, force: bool = False) -> dict[str, object]:
    if not settings.datasource_scan_scheduler_enabled and not force:
        summary = _summary_from_cycle(0, 0, 0, 0, None)
        _update_runtime_state(phase="idle", is_enabled=False, summary=summary)
        _persist_scheduler_status(mode=normalize_scheduler_mode(settings.datasource_scan_scheduler_mode), is_enabled=False, summary=summary)
        return summary

    with SessionLocal() as session:
        if not _schedule_support_ready(session):
            summary = _summary_from_cycle(0, 0, 0, 0, None)
            _update_runtime_state(phase="idle", summary=summary)
            _persist_scheduler_status(
                mode=normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
                is_enabled=bool(settings.datasource_scan_scheduler_enabled),
                summary=summary,
            )
            return summary

        if not _advisory_lock(session):
            summary = _summary_from_cycle(0, 0, 0, 0, None)
            _update_runtime_state(phase="idle", summary=summary)
            return summary

        try:
            _update_runtime_state(phase="running", started=True, heartbeat=True)
            _try_update_scheduler_status_in_session(
                session,
                mode=normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
                is_enabled=bool(settings.datasource_scan_scheduler_enabled),
                started=True,
                heartbeat=True,
            )
            due_schedules = _select_due_schedules(session)

            success_count = 0
            failed_count = 0
            skipped_count = 0
            for schedule in due_schedules:
                datasource = session.get(DataSource, schedule.datasource_id)
                if datasource is None:
                    update_scan_schedule_run_state(
                        session,
                        schedule_id=schedule.id,
                        status="failed",
                        error_message="Datasource not found",
                        finished_at=datetime.now(timezone.utc),
                    )
                    failed_count += 1
                    continue
                queued_at = datetime.now(timezone.utc)
                mark_scan_schedule_queued(session, schedule.id, queued_at=queued_at)
                try:
                    enqueue_datasource_scan(
                        session,
                        datasource=datasource,
                        started_by=None,
                        trigger_mode="scheduled",
                        schedule_id=schedule.id,
                    )
                    success_count += 1
                except HTTPException as exc:
                    if exc.status_code != status.HTTP_409_CONFLICT:
                        raise
                    skipped_count += 1
                    update_scan_schedule_run_state(
                        session,
                        schedule_id=schedule.id,
                        status="skipped",
                        error_message="job_already_running",
                        finished_at=queued_at,
                    )
                    logger.info(
                        "datasource scan scheduler skipped schedule_id=%s datasource_id=%s reason=job_already_running",
                        schedule.id,
                        schedule.datasource_id,
                    )
                    continue

            next_expected_run_at = session.scalar(
                select(DataSourceScanSchedule.schedule_next_run_at)
                .where(DataSourceScanSchedule.schedule_enabled.is_(True))
                .where(DataSourceScanSchedule.schedule_next_run_at.is_not(None))
                .order_by(DataSourceScanSchedule.schedule_next_run_at.asc())
                .limit(1)
            )
            summary = _summary_from_cycle(len(due_schedules), success_count, failed_count, skipped_count, next_expected_run_at)
            _update_runtime_state(phase="idle", success=failed_count == 0, summary=summary)
            _try_update_scheduler_status_in_session(
                session,
                mode=normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
                is_enabled=bool(settings.datasource_scan_scheduler_enabled),
                success=failed_count == 0,
                summary=summary,
            )
            session.commit()
            return summary
        except Exception as exc:  # noqa: BLE001
            session.rollback()
            summary = _summary_from_cycle(0, 0, 0, 0, None)
            _update_runtime_state(phase="idle", failure=str(exc), summary=summary)
            _persist_scheduler_status(
                mode=normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
                is_enabled=bool(settings.datasource_scan_scheduler_enabled),
                failure=str(exc),
                summary=summary,
            )
            logger.exception("datasource scan scheduler cycle failed error=%s", exc)
            return summary
        finally:
            _release_advisory_lock(session)


def scheduler_status_snapshot(db: Session) -> dict[str, object]:
    if not _schedule_support_ready(db):
        return {
            "scheduler_name": SCHEDULER_NAME,
            "mode": normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
            "is_enabled": bool(settings.datasource_scan_scheduler_enabled),
            "health": "disabled" if not settings.datasource_scan_scheduler_enabled else "idle",
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
        "mode": status.mode if status else normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
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


_stop_event: asyncio.Event | None = None


async def run_datasource_scan_scheduler_forever() -> None:
    global _stop_event
    _stop_event = asyncio.Event()
    poll_interval = max(1, int(settings.datasource_scan_scheduler_poll_interval_minutes))
    logger.info(
        "datasource scan scheduler started mode=%s interval_minutes=%s",
        normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
        poll_interval,
    )
    while not _stop_event.is_set():
        try:
            run_datasource_scan_scheduler_cycle()
        except Exception as exc:  # noqa: BLE001
            logger.exception("datasource scan scheduler cycle failed error=%s", exc)
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=poll_interval * 60)
        except TimeoutError:
            continue


async def stop_datasource_scan_scheduler() -> None:
    global _scheduler_task, _scheduler_bootstrap_task, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    tasks = [task for task in (_scheduler_task, _scheduler_bootstrap_task) if task is not None]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _scheduler_task = None
    _scheduler_bootstrap_task = None


def start_datasource_scan_scheduler() -> None:
    global _scheduler_task, _scheduler_bootstrap_task
    configured_mode = normalize_scheduler_mode(settings.datasource_scan_scheduler_mode)
    if not embedded_scheduler_allowed(configured_mode, settings.env):
        _update_runtime_state(phase="disabled", mode=configured_mode, is_enabled=bool(settings.datasource_scan_scheduler_enabled))
        _persist_scheduler_status(mode=configured_mode, is_enabled=bool(settings.datasource_scan_scheduler_enabled), heartbeat=True)
        logger.info("datasource scan embedded scheduler skipped mode=%s", configured_mode)
        return
    if _scheduler_task is not None and not _scheduler_task.done():
        return
    loop = asyncio.get_running_loop()
    _scheduler_task = loop.create_task(run_datasource_scan_scheduler_forever())
    _scheduler_bootstrap_task = loop.create_task(asyncio.sleep(0))


__all__ = [
    "run_datasource_scan_scheduler_cycle",
    "run_datasource_scan_scheduler_forever",
    "scheduler_status_snapshot",
    "start_datasource_scan_scheduler",
    "stop_datasource_scan_scheduler",
]
