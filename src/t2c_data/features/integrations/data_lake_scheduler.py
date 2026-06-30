from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from time import perf_counter

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from t2c_data.core.config import embedded_scheduler_allowed, normalize_scheduler_mode, settings
from t2c_data.core.db import SessionLocal
from t2c_data.core.json_utils import to_jsonable
from t2c_data.features.integrations.data_lake_inventory import enqueue_data_lake_inventory_scan
from t2c_data.features.integrations.data_lake_schedules import _schedule_support_ready, update_data_lake_scan_schedule_run_state
from t2c_data.models.platform import DataLakeScanSchedule, DataLakeScanSchedulerStatus

logger = logging.getLogger(__name__)

SCHEDULER_NAME = "data_lake_scan"
_DATA_LAKE_SCAN_SCHEDULER_LOCK_KEY = 791022344
_scheduler_task: asyncio.Task[None] | None = None
_scheduler_bootstrap_task: asyncio.Task[None] | None = None
_scheduler_lock = Lock()


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
    mode=normalize_scheduler_mode(settings.data_lake_scan_scheduler_mode),
    is_enabled=True,
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
        _runtime_state.last_run_summary = to_jsonable(summary)


def _scheduler_status_table_exists(session: Session) -> bool:
    regclass = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.data_lake_scan_scheduler_status"},
    ).scalar_one()
    return regclass is not None


def _get_or_create_scheduler_status(session: Session) -> DataLakeScanSchedulerStatus:
    status = session.get(DataLakeScanSchedulerStatus, 1)
    if status is None:
        status = DataLakeScanSchedulerStatus(id=1, scheduler_name=SCHEDULER_NAME)
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
) -> DataLakeScanSchedulerStatus:
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
        logger.exception("data lake scan scheduler status update failed mode=%s enabled=%s", mode, is_enabled)
        return False


def _advisory_lock(session: Session) -> bool:
    return bool(session.execute(text("SELECT pg_try_advisory_lock(:lock_key)"), {"lock_key": _DATA_LAKE_SCAN_SCHEDULER_LOCK_KEY}).scalar_one())


def _release_advisory_lock(session: Session) -> None:
    try:
        session.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": _DATA_LAKE_SCAN_SCHEDULER_LOCK_KEY})
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()


def _select_due_schedules(session: Session) -> list[DataLakeScanSchedule]:
    now = datetime.now(timezone.utc)
    schedules = session.scalars(
        select(DataLakeScanSchedule).where(DataLakeScanSchedule.schedule_enabled.is_(True)).order_by(DataLakeScanSchedule.schedule_next_run_at.asc().nulls_last(), DataLakeScanSchedule.id.asc())
    ).all()
    due: list[DataLakeScanSchedule] = []
    for schedule in schedules:
        if schedule.schedule_next_run_at is None:
            due.append(schedule)
        elif schedule.schedule_next_run_at <= now:
            due.append(schedule)
    return due


def run_data_lake_scan_scheduler_cycle(*, force: bool = False) -> dict[str, object]:
    if _runtime_state.mode == "embedded_dev_only" and not embedded_scheduler_allowed(_runtime_state.mode, settings.env):
        summary = {
            "scheduler_name": SCHEDULER_NAME,
            "trigger": "force" if force else "cycle",
            "skipped": "embedded_not_allowed",
            "error": "Embedded schedulers are not allowed outside dev/test. Use worker mode.",
        }
        _update_runtime_state(phase="disabled", is_enabled=False, summary=summary)
        return summary
    if not _scheduler_lock.acquire(blocking=False):
        return {"scheduler_name": SCHEDULER_NAME, "skipped": "scheduler_locked"}
    _update_runtime_state(phase="running", started=True, heartbeat=True)
    start = perf_counter()
    try:
        with SessionLocal() as session:
            if not _schedule_support_ready(session):
                summary = {
                    "scheduler_name": SCHEDULER_NAME,
                    "trigger": "force" if force else "cycle",
                    "skipped": "schedule_support_unavailable",
                }
                _update_runtime_state(phase="idle", summary=summary, success=True)
                return summary
            if not _advisory_lock(session):
                summary = {
                    "scheduler_name": SCHEDULER_NAME,
                    "trigger": "force" if force else "cycle",
                    "skipped": "advisory_lock_unavailable",
                }
                _update_runtime_state(phase="idle", summary=summary)
                return summary
            try:
                _try_update_scheduler_status_in_session(session, mode=_runtime_state.mode, is_enabled=True, started=True, heartbeat=True)
                due_schedules = _select_due_schedules(session)
                processed: list[dict[str, object]] = []
                for schedule in due_schedules:
                    started_at = datetime.now(timezone.utc)
                    try:
                        scan_out = enqueue_data_lake_inventory_scan(
                            session,
                            schedule.connection_id,
                            current_user=None,
                            audit_kwargs={"request_id": f"data-lake-schedule-{schedule.id}-{int(started_at.timestamp())}"},
                            trigger_mode="scheduled",
                            schedule_id=schedule.id,
                        )
                        update_data_lake_scan_schedule_run_state(
                            session,
                            schedule_id=schedule.id,
                            status="queued",
                            error_message=None,
                            started_at=started_at,
                            finished_at=None,
                        )
                        session.commit()
                        processed.append(
                            {
                                "schedule_id": schedule.id,
                                "connection_id": schedule.connection_id,
                                "status": scan_out.scan_run.status,
                                "job_id": scan_out.job_id,
                                "scan_run_id": scan_out.scan_run.id,
                            }
                        )
                    except Exception as exc:  # noqa: BLE001
                        session.rollback()
                        update_data_lake_scan_schedule_run_state(
                            session,
                            schedule_id=schedule.id,
                            status="error",
                            error_message=str(exc),
                            started_at=started_at,
                            finished_at=datetime.now(timezone.utc),
                        )
                        session.commit()
                        processed.append(
                            {
                                "schedule_id": schedule.id,
                                "connection_id": schedule.connection_id,
                                "status": "error",
                                "error": str(exc),
                            }
                        )
                summary = {
                    "scheduler_name": SCHEDULER_NAME,
                    "trigger": "force" if force else "cycle",
                    "processed_schedules": len(processed),
                    "processed": processed,
                    "elapsed_ms": round((perf_counter() - start) * 1000, 2),
                }
                _try_update_scheduler_status_in_session(session, mode=_runtime_state.mode, is_enabled=True, success=True, summary=summary)
                session.commit()
                _update_runtime_state(phase="idle", success=True, summary=summary)
                return summary
            finally:
                _release_advisory_lock(session)
    except Exception as exc:  # noqa: BLE001
        summary = {
            "scheduler_name": SCHEDULER_NAME,
            "trigger": "force" if force else "cycle",
            "error": str(exc),
        }
        _update_runtime_state(phase="failed", failure=str(exc), summary=summary)
        try:
            with SessionLocal() as session:
                if _scheduler_status_table_exists(session):
                    _try_update_scheduler_status_in_session(session, mode=_runtime_state.mode, is_enabled=True, failure=str(exc), summary=summary)
                    session.commit()
        except Exception:  # noqa: BLE001
            logger.exception("data lake scan scheduler status persistence failed during error handling")
        return summary
    finally:
        _scheduler_lock.release()


async def run_data_lake_scan_scheduler_forever() -> None:
    interval_seconds = 60
    while True:
        try:
            run_data_lake_scan_scheduler_cycle()
        except Exception:  # noqa: BLE001
            logger.exception("data lake scan scheduler cycle failed")
        await asyncio.sleep(interval_seconds)


def start_data_lake_scan_scheduler() -> None:
    global _scheduler_task, _scheduler_bootstrap_task
    configured_mode = normalize_scheduler_mode(settings.data_lake_scan_scheduler_mode)
    _update_runtime_state(mode=configured_mode)
    if not embedded_scheduler_allowed(configured_mode, settings.env):
        _update_runtime_state(phase="disabled", mode=configured_mode, is_enabled=True)
        logger.info("data lake scan embedded scheduler skipped mode=%s", configured_mode)
        return
    if _scheduler_task is not None and not _scheduler_task.done():
        return
    loop = asyncio.get_event_loop()
    _scheduler_task = loop.create_task(run_data_lake_scan_scheduler_forever())
    _scheduler_bootstrap_task = loop.create_task(asyncio.sleep(0))


async def stop_data_lake_scan_scheduler() -> None:
    global _scheduler_task, _scheduler_bootstrap_task
    for task in (_scheduler_bootstrap_task, _scheduler_task):
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    _scheduler_task = None
    _scheduler_bootstrap_task = None


__all__ = [
    "run_data_lake_scan_scheduler_cycle",
    "run_data_lake_scan_scheduler_forever",
    "start_data_lake_scan_scheduler",
    "stop_data_lake_scan_scheduler",
]
