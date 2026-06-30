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
from t2c_data.features.data_quality.contracts import DefaultDQExecutionGateway
from t2c_data.features.data_quality.schedule_utils import compute_next_run_at, infer_schedule_mode
from t2c_data.features.platform.jobs import finish_integration_job, maybe_start_integration_job
from t2c_data.models.dq import DQRule, DQSchedulerStatus
from t2c_data.services.data_quality import configured_execution_engine

logger = logging.getLogger(__name__)

SCHEDULER_NAME = "dq_rules"
_DQ_SCHEDULER_LOCK_KEY = 791022341
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
    mode=normalize_scheduler_mode(settings.dq_scheduler_mode),
    is_enabled=bool(settings.dq_scheduler_enabled),
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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
        "mode": normalize_scheduler_mode(settings.dq_scheduler_mode),
        "is_enabled": bool(settings.dq_scheduler_enabled),
        "health": "disabled" if not settings.dq_scheduler_enabled else "idle",
        "last_started_at": None,
        "last_heartbeat_at": None,
        "last_success_at": None,
        "last_failure_at": None,
        "last_error": None,
        "last_run_summary": {},
        "scheduled_rules_total": 0,
        "next_expected_run_at": None,
    }


def _scheduler_status_table_exists(session: Session) -> bool:
    bind = session.get_bind()
    if bind is not None and getattr(bind.dialect, "name", None) != "postgresql":
        return True
    regclass = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.dq_scheduler_status"},
    ).scalar_one()
    return regclass is not None


def _scheduler_support_is_ready(session: Session) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    inspector = inspect(bind)
    if not inspector.has_table("dq_scheduler_status", schema=settings.db_schema):
        return False
    if not inspector.has_table("dq_rules", schema=settings.db_schema):
        return False
    column_names = {column["name"] for column in inspector.get_columns("dq_rules", schema=settings.db_schema)}
    return {
        "execution_engine",
        "schedule_mode",
        "schedule_enabled",
        "schedule_every_minutes",
        "schedule_time",
        "schedule_day_of_week",
        "schedule_day_of_month",
        "schedule_anchor_date",
        "schedule_last_run_at",
        "rule_definition_json",
        "archived",
    }.issubset(column_names)


def _get_or_create_scheduler_status(session: Session) -> DQSchedulerStatus:
    status = session.get(DQSchedulerStatus, 1)
    if status is None:
        status = DQSchedulerStatus(id=1, scheduler_name=SCHEDULER_NAME)
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
) -> DQSchedulerStatus:
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
                    "dq scheduler status table unavailable schema=%s table=dq_scheduler_status",
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
            "dq scheduler status persistence failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
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
            logger.warning(
                "dq scheduler status update skipped schema=%s table=dq_scheduler_status",
                settings.db_schema,
            )
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
        session.commit()
        return True
    except Exception:  # noqa: BLE001
        session.rollback()
        logger.exception(
            "dq scheduler status update failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
            mode,
            is_enabled,
            started,
            heartbeat,
            success,
        )
        return False


def _runtime_snapshot() -> dict[str, object]:
    phase = _runtime_state.phase
    if not _runtime_state.is_enabled:
        health = "disabled"
    elif phase in {"starting", "bootstrapping"}:
        health = "starting"
    elif phase in {"bootstrap_failed", "failed"}:
        health = "unavailable"
    elif phase == "running":
        heartbeat_at = _coerce_iso(_runtime_state.last_heartbeat_at)
        heartbeat_grace_seconds = max(int(settings.platform_scheduler_heartbeat_grace_minutes or 10), 1) * 60
        now = datetime.now(timezone.utc)
        if heartbeat_at and (now - heartbeat_at).total_seconds() <= heartbeat_grace_seconds:
            health = "healthy"
        elif _runtime_state.mode in {"dedicated", "worker"}:
            health = "stale"
        else:
            health = "embedded"
    else:
        health = "idle"
    return {
        "scheduler_name": SCHEDULER_NAME,
        "mode": _runtime_state.mode,
        "is_enabled": _runtime_state.is_enabled,
        "health": health,
        "phase": phase,
        "last_started_at": _runtime_state.last_started_at,
        "last_heartbeat_at": _runtime_state.last_heartbeat_at,
        "last_success_at": _runtime_state.last_success_at,
        "last_failure_at": _runtime_state.last_failure_at,
        "last_error": _runtime_state.last_error,
        "last_run_summary": _runtime_state.last_run_summary or {},
    }


def scheduler_status_snapshot(session: Session) -> dict[str, object]:
    try:
        if not _scheduler_support_is_ready(session):
            return _empty_scheduler_snapshot()
        status = _get_or_create_scheduler_status(session)
        heartbeat_grace_seconds = max(int(settings.platform_scheduler_heartbeat_grace_minutes or 10), 1) * 60
        heartbeat_at = _coerce_iso(status.last_heartbeat_at)
        now = datetime.now(timezone.utc)
        if not status.is_enabled:
            health = "disabled"
        elif heartbeat_at and (now - heartbeat_at).total_seconds() <= heartbeat_grace_seconds:
            health = "healthy"
        elif status.mode in {"dedicated", "worker"}:
            health = "stale"
        elif status.last_started_at or status.last_success_at:
            health = "embedded"
        else:
            health = "idle"
        scheduled_rules = session.scalars(
            select(DQRule)
            .where(
                DQRule.is_active.is_(True),
                DQRule.schedule_enabled.is_(True),
                DQRule.archived.is_(False),
                DQRule.rule_definition_json.is_not(None),
            )
            .order_by(DQRule.id.asc())
        ).all()
        scheduled_rules_total = len(scheduled_rules)
        next_candidates: list[datetime] = []
        for rule in scheduled_rules:
            next_run = compute_next_run_at(rule, reference=now)
            if next_run is not None:
                next_candidates.append(next_run)
        next_expected_run_at = min(next_candidates).astimezone(timezone.utc).isoformat() if next_candidates else None
        return {
            "scheduler_name": status.scheduler_name,
            "mode": status.mode,
            "is_enabled": status.is_enabled,
            "health": health,
            "last_started_at": status.last_started_at,
            "last_heartbeat_at": status.last_heartbeat_at,
            "last_success_at": status.last_success_at,
            "last_failure_at": status.last_failure_at,
            "last_error": status.last_error,
            "last_run_summary": status.last_run_summary_json or {},
            "scheduled_rules_total": scheduled_rules_total,
            "next_expected_run_at": next_expected_run_at,
        }
    except Exception:  # noqa: BLE001
        logger.exception("dq scheduler snapshot fallback activated")
        return _empty_scheduler_snapshot()


def _lock_key_is_available(session: Session) -> bool:
    try:
        bind = session.get_bind()
        return bool(bind and getattr(bind.dialect, "name", None) == "postgresql")
    except Exception:
        return False


def _acquire_scheduler_lock(session: Session) -> bool:
    if not _lock_key_is_available(session):
        return True
    try:
        return bool(
            session.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": _DQ_SCHEDULER_LOCK_KEY},
            ).scalar_one()
        )
    except Exception:  # noqa: BLE001
        logger.exception("dq scheduler advisory lock acquisition failed")
        return True


def _release_scheduler_lock(session: Session) -> None:
    if not _lock_key_is_available(session):
        return
    try:
        session.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": _DQ_SCHEDULER_LOCK_KEY})
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()
        logger.exception("dq scheduler advisory lock release failed")


def _rule_due(rule: DQRule, *, now: datetime) -> bool:
    if infer_schedule_mode(
        schedule_mode=getattr(rule, "schedule_mode", None),
        schedule_enabled=getattr(rule, "schedule_enabled", None),
        schedule_every_minutes=getattr(rule, "schedule_every_minutes", None),
    ) == "interval" and getattr(rule, "schedule_last_run_at", None) is None:
        return True
    next_run_at = compute_next_run_at(rule, reference=now)
    return bool(next_run_at is not None and next_run_at <= now)


def run_dq_scheduler_cycle(*, trigger: str = "manual", scheduler_mode: str | None = None) -> dict[str, object]:
    mode = normalize_scheduler_mode(scheduler_mode or settings.dq_scheduler_mode)
    now = datetime.now(timezone.utc)
    summary: dict[str, object] = {
        "trigger": trigger,
        "scheduler_mode": mode,
        "rules_total": 0,
        "due_rules": [],
        "executed_rules": [],
        "failed_rules": [],
        "queued": 0,
        "violations_count_total": 0,
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
    _update_runtime_state(phase="running", mode=mode, is_enabled=bool(settings.dq_scheduler_enabled), started=True, heartbeat=True)
    with SessionLocal() as session:
        try:
            try:
                job_handle = maybe_start_integration_job(
                    session,
                    source="dq",
                    job_type="rules_scheduler",
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
                    is_enabled=bool(settings.dq_scheduler_enabled),
                    heartbeat=True,
                    summary=summary,
                )
                _update_runtime_state(phase="idle", mode=mode, is_enabled=bool(settings.dq_scheduler_enabled), summary=summary)
                logger.info("dq scheduler skipped: another execution is already running")
                return summary
            if not _scheduler_support_is_ready(session):
                summary["skipped"] = "scheduler_support_unavailable"
                job_status = "skipped"
                job_error = summary["skipped"]
                job_context = summary
                _update_runtime_state(phase="idle", mode=mode, is_enabled=bool(settings.dq_scheduler_enabled), summary=summary)
                return summary
            if not _acquire_scheduler_lock(session):
                summary["skipped"] = "lock_unavailable"
                job_status = "skipped"
                job_error = summary["skipped"]
                job_context = summary
                _try_update_scheduler_status_in_session(
                    session,
                    mode=mode,
                    is_enabled=bool(settings.dq_scheduler_enabled),
                    heartbeat=True,
                    summary=summary,
                )
                return summary
            _try_update_scheduler_status_in_session(
                session,
                mode=mode,
                is_enabled=bool(settings.dq_scheduler_enabled),
                started=True,
                heartbeat=True,
                summary=summary,
            )

            rules = session.scalars(
                select(DQRule)
                .where(
                    DQRule.is_active.is_(True),
                    DQRule.schedule_enabled.is_(True),
                    DQRule.archived.is_(False),
                    DQRule.rule_definition_json.is_not(None),
                )
                .order_by(DQRule.id.asc())
            ).all()
            summary["rules_total"] = len(rules)
            due_rules = [rule for rule in rules if _rule_due(rule, now=now)]
            summary["due_rules"] = [rule.id for rule in due_rules]
            gateway = DefaultDQExecutionGateway()

            for rule in due_rules:
                try:
                    rule.schedule_last_run_at = now
                    session.add(rule)
                    engine = configured_execution_engine(getattr(rule, "execution_engine", None))
                    dq_run = gateway.create_table_run(
                        table_id=rule.table_id,
                        table_fqn=rule.table_fqn,
                        execution_engine=engine,
                    )
                    job = gateway.enqueue_rules(
                        table_id=rule.table_id,
                        table_fqn=rule.table_fqn,
                        rule_ids=[rule.id],
                        requested_by_user_id=None,
                        dq_run_id=dq_run.id,
                        execution_engine=engine,
                    )
                    summary["queued"] = int(summary["queued"]) + 1
                    session.commit()
                    summary["executed_rules"].append(
                        {
                            "rule_id": rule.id,
                            "dq_run_id": dq_run.id,
                            "job_run_id": job.id,
                            "engine": engine,
                            "status": "queued",
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    session.rollback()
                    summary["failed_rules"].append({"rule_id": rule.id, "error": str(exc)[:500]})
                    logger.exception("dq scheduler rule execution failed rule_id=%s", rule.id)

            _try_update_scheduler_status_in_session(
                session,
                mode=mode,
                is_enabled=bool(settings.dq_scheduler_enabled),
                heartbeat=True,
                success=True,
                summary=summary,
            )
            job_records = len(due_rules)
            job_status = "failed" if summary["failed_rules"] else "success"
            job_context = summary
            _update_runtime_state(phase="running", mode=mode, is_enabled=bool(settings.dq_scheduler_enabled), heartbeat=True, success=True, summary=summary)
            return summary
        except Exception as exc:  # noqa: BLE001
            session.rollback()
            summary["error"] = str(exc)
            job_status = "failed"
            job_error = str(exc)
            job_records = len(summary.get("due_rules") or [])
            job_context = summary
            _try_update_scheduler_status_in_session(
                session,
                mode=mode,
                is_enabled=bool(settings.dq_scheduler_enabled),
                heartbeat=True,
                failure=str(exc),
                summary=summary,
            )
            _update_runtime_state(phase="failed", mode=mode, failure=str(exc), summary=summary)
            logger.exception("dq scheduler cycle failed mode=%s", mode)
            return summary
        finally:
            finish_integration_job(
                session,
                job_handle,
                status=job_status,
                records_processed=job_records,
                error=job_error,
                context_json=job_context,
            )
            _release_scheduler_lock(session)


async def _scheduler_loop() -> None:
    interval_minutes = max(int(settings.dq_scheduler_poll_interval_minutes or 1), 1)
    interval_seconds = interval_minutes * 60
    logger.info("dq scheduler started interval_minutes=%s mode=embedded_dev_only", interval_minutes)
    while True:
        try:
            await asyncio.to_thread(run_dq_scheduler_cycle, trigger="scheduled", scheduler_mode="embedded_dev_only")
            _update_runtime_state(phase="running", mode="embedded_dev_only", is_enabled=bool(settings.dq_scheduler_enabled), heartbeat=True)
        except Exception as exc:  # noqa: BLE001
            logger.exception("dq scheduler refresh failed mode=embedded_dev_only error=%s", exc)
            _update_runtime_state(phase="bootstrap_failed", mode="embedded_dev_only", failure=str(exc))
        await asyncio.sleep(interval_seconds)


async def run_dq_scheduler_forever() -> None:
    if not settings.dq_scheduler_enabled:
        logger.info("dq scheduler disabled in dedicated worker")
        _persist_scheduler_status(mode=normalize_scheduler_mode(settings.dq_scheduler_mode), is_enabled=False, heartbeat=True)
        return
    _persist_scheduler_status(mode="dedicated", is_enabled=True, started=True, heartbeat=True)
    _update_runtime_state(phase="running", mode="dedicated", is_enabled=True, started=True, heartbeat=True)
    while True:
        try:
            await asyncio.to_thread(run_dq_scheduler_cycle, trigger="scheduled", scheduler_mode="dedicated")
            _update_runtime_state(phase="running", mode="dedicated", is_enabled=True, heartbeat=True)
        except Exception as exc:  # noqa: BLE001
            logger.exception("dq scheduler dedicated worker failed error=%s", exc)
            _update_runtime_state(phase="failed", mode="dedicated", failure=str(exc))
        await asyncio.sleep(max(int(settings.dq_scheduler_poll_interval_minutes or 1), 1) * 60)


async def _bootstrap_embedded_scheduler() -> None:
    global _scheduler_task
    configured_mode = normalize_scheduler_mode(settings.dq_scheduler_mode)
    try:
        _persist_scheduler_status(
            mode=configured_mode,
            is_enabled=bool(settings.dq_scheduler_enabled),
            heartbeat=True,
        )
        if not embedded_scheduler_allowed(configured_mode, settings.env):
            logger.info("dq scheduler embedded skipped mode=%s", configured_mode)
            _persist_scheduler_status(mode=configured_mode, is_enabled=bool(settings.dq_scheduler_enabled), heartbeat=True)
            return
        if not settings.dq_scheduler_enabled:
            logger.info("dq scheduler disabled")
            _persist_scheduler_status(mode=configured_mode, is_enabled=False)
            return
        if _scheduler_task is not None and not _scheduler_task.done():
            logger.info("dq scheduler already running")
            return
        _scheduler_task = asyncio.create_task(_scheduler_loop(), name="dq-scheduler")
        logger.info("dq scheduler bootstrap completed")
    except Exception as exc:  # noqa: BLE001
        logger.exception("dq scheduler bootstrap failed retrying error=%s", exc)
        _update_runtime_state(phase="bootstrap_failed", mode=configured_mode, failure=str(exc), bootstrap_attempt_increment=True)


def start_dq_scheduler() -> None:
    global _scheduler_bootstrap_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.exception("dq scheduler bootstrap could not be scheduled: no running event loop")
        return
    if _scheduler_bootstrap_task is not None and not _scheduler_bootstrap_task.done():
        logger.info("dq scheduler bootstrap already in progress")
        return
    if _scheduler_task is not None and not _scheduler_task.done():
        logger.info("dq scheduler already running")
        return
    _scheduler_bootstrap_task = loop.create_task(_bootstrap_embedded_scheduler(), name="dq-scheduler-bootstrap")
    logger.info(
        "dq scheduler bootstrap scheduled mode=%s enabled=%s",
        normalize_scheduler_mode(settings.dq_scheduler_mode),
        settings.dq_scheduler_enabled,
    )


async def stop_dq_scheduler() -> None:
    global _scheduler_task, _scheduler_bootstrap_task
    bootstrap_task = _scheduler_bootstrap_task
    _scheduler_bootstrap_task = None
    if bootstrap_task is not None and not bootstrap_task.done():
        bootstrap_task.cancel()
        try:
            await bootstrap_task
        except asyncio.CancelledError:
            logger.info("dq scheduler bootstrap stopped")
    task = _scheduler_task
    _scheduler_task = None
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info("dq scheduler stopped")


__all__ = [
    "run_dq_scheduler_cycle",
    "run_dq_scheduler_forever",
    "scheduler_status_snapshot",
    "start_dq_scheduler",
    "stop_dq_scheduler",
]
