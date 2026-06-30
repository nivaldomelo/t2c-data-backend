from __future__ import annotations

import asyncio
import logging
from threading import Lock
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from t2c_data.core.config import embedded_scheduler_allowed, normalize_scheduler_mode, settings
from t2c_data.core.db import SessionLocal
from t2c_data.core.json_utils import to_jsonable
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.governance.notifications import refresh_governance_notifications
from t2c_data.features.governance.recommendations import refresh_governance_recommendations
from t2c_data.features.governance.score_history import refresh_governance_score_snapshots
from t2c_data.features.governance.trust_history import refresh_governance_trust_snapshots
from t2c_data.features.ingestion import refresh_operational_stability_snapshots
from t2c_data.features.notifications import (
    dispatch_daily_notification_digests,
    queue_governance_notifications_for_users,
)
from t2c_data.features.integrations.data_lake_scheduler import run_data_lake_scan_scheduler_cycle
from t2c_data.features.operations.backups import latest_backup, run_backup
from t2c_data.features.platform.automations import evaluate_automation_rules
from t2c_data.features.platform.retention import run_retention_cleanup_job
from t2c_data.features.platform.jobs import enqueue_integration_job, finish_integration_job_record
from t2c_data.features.platform.read_models import refresh_platform_read_models
from t2c_data.features.operations.failures import classify_operational_error, record_operational_failure
from t2c_data.models.platform import PlatformSchedulerStatus
from t2c_data.models.platform import IntegrationSyncJob
from t2c_data.services.audit import write_audit_log_sync

logger = logging.getLogger(__name__)

SCHEDULER_NAME = "platform_maintenance"
_scheduler_task: asyncio.Task[None] | None = None
_scheduler_bootstrap_task: asyncio.Task[None] | None = None
_maintenance_refresh_lock = Lock()


@dataclass
class SchedulerRuntimeState:
    phase: str = "idle"
    mode: str = "worker"
    is_enabled: bool = False
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
    mode=normalize_scheduler_mode(settings.platform_scheduler_mode),
    is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
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


def _normalize_summary_payload(summary: dict[str, object] | None) -> dict[str, object] | None:
    if summary is None:
        return None
    return to_jsonable(summary)


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
        _runtime_state.last_run_summary = _normalize_summary_payload(summary)


def _scheduler_status_table_exists(session: Session) -> bool:
    regclass = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.platform_scheduler_status"},
    ).scalar_one()
    return regclass is not None


def _get_or_create_scheduler_status(session: Session) -> PlatformSchedulerStatus:
    status = session.get(PlatformSchedulerStatus, 1)
    if status is None:
        status = PlatformSchedulerStatus(id=1, scheduler_name=SCHEDULER_NAME)
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
) -> PlatformSchedulerStatus:
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
        status.last_run_summary_json = _normalize_summary_payload(summary)
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
                    "platform scheduler status table unavailable schema=%s table=platform_scheduler_status",
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
            "platform scheduler status persistence failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
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
                "platform scheduler status update skipped schema=%s table=platform_scheduler_status",
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
            "platform scheduler status update failed mode=%s enabled=%s started=%s heartbeat=%s success=%s",
            mode,
            is_enabled,
            started,
            heartbeat,
            success,
        )
        return False


def _acquire_maintenance_refresh_lock() -> bool:
    return _maintenance_refresh_lock.acquire(blocking=False)


def _release_maintenance_refresh_lock() -> None:
    if _maintenance_refresh_lock.locked():
        try:
            _maintenance_refresh_lock.release()
        except RuntimeError:
            pass


def enqueue_platform_maintenance_job(
    session: Session,
    *,
    trigger: str = "manual",
    scheduler_mode: str | None = None,
    requested_by_user_id: int | None = None,
) -> IntegrationSyncJob | None:
    normalized_mode = normalize_scheduler_mode(scheduler_mode or settings.platform_scheduler_mode)
    job = enqueue_integration_job(
        session,
        source="platform",
        job_type="maintenance",
        target_type="platform_scheduler",
        target_id=1,
        target_name="platform maintenance",
        trigger_mode=trigger,
        requested_by_user_id=requested_by_user_id,
        payload_json={
            "trigger": trigger,
            "scheduler_mode": normalized_mode,
        },
        context_json={
            "trigger": trigger,
            "scheduler_mode": normalized_mode,
        },
    )
    return job


def _runtime_snapshot() -> dict[str, object]:
    phase = _runtime_state.phase
    applicable = bool(
        settings.platform_read_model_auto_refresh_enabled
        or _runtime_state.last_started_at
        or _runtime_state.last_heartbeat_at
        or _runtime_state.last_success_at
        or _runtime_state.last_failure_at
        or _runtime_state.last_run_summary
    )
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
        health = "unavailable"
    return {
        "scheduler_name": SCHEDULER_NAME,
        "mode": _runtime_state.mode,
        "is_enabled": bool(_runtime_state.is_enabled),
        "applicable": applicable,
        "health": health,
        "last_started_at": _runtime_state.last_started_at,
        "last_heartbeat_at": _runtime_state.last_heartbeat_at,
        "last_success_at": _runtime_state.last_success_at,
        "last_failure_at": _runtime_state.last_failure_at,
        "last_error": _runtime_state.last_error,
        "last_run_summary": _runtime_state.last_run_summary or {},
    }


def scheduler_status_snapshot(session: Session) -> dict[str, object]:
    try:
        if not _scheduler_status_table_exists(session):
            return _runtime_snapshot()
        status = _get_or_create_scheduler_status(session)
        heartbeat_at = _coerce_iso(status.last_heartbeat_at)
        heartbeat_grace_seconds = max(int(settings.platform_scheduler_heartbeat_grace_minutes or 10), 1) * 60
        now = datetime.now(timezone.utc)
        applicable = bool(
            status.is_enabled
            or status.last_started_at
            or status.last_heartbeat_at
            or status.last_success_at
            or status.last_failure_at
            or status.last_run_summary_json
        )
        if not status.is_enabled:
            health = "disabled"
        elif heartbeat_at and (now - heartbeat_at).total_seconds() <= heartbeat_grace_seconds:
            health = "healthy"
        elif status.mode in {"dedicated", "worker"}:
            health = "stale"
        else:
            health = "embedded"
        return {
            "scheduler_name": status.scheduler_name,
            "mode": status.mode,
            "is_enabled": bool(status.is_enabled),
            "applicable": applicable,
            "health": health,
            "last_started_at": status.last_started_at,
            "last_heartbeat_at": status.last_heartbeat_at,
            "last_success_at": status.last_success_at,
            "last_failure_at": status.last_failure_at,
            "last_error": status.last_error,
            "last_run_summary": status.last_run_summary_json or {},
        }
    except Exception:  # noqa: BLE001
        logger.exception("platform scheduler snapshot fallback activated")
        return _runtime_snapshot()


def _run_maintenance_once(*, trigger: str, scheduler_mode: str) -> dict[str, object]:
    if not _acquire_maintenance_refresh_lock():
        summary = {
            "trigger": trigger,
            "scheduler_mode": scheduler_mode,
            "skipped": "maintenance_already_running",
        }
        logger.info("platform maintenance cycle skipped trigger=%s mode=%s reason=lock_unavailable", trigger, scheduler_mode)
        return summary
    _update_runtime_state(
        phase="running",
        mode=scheduler_mode,
        is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
        started=True,
        heartbeat=True,
    )
    try:
        with SessionLocal() as session:
            _try_update_scheduler_status_in_session(
                session,
                mode=scheduler_mode,
                is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                started=True,
                heartbeat=True,
            )
            try:
                backup_payload = {"status": "not_started"}
                refresh_started = perf_counter()
                try:
                    refresh_payload = refresh_platform_read_models(session, mode="auto")
                except Exception:  # noqa: BLE001
                    runtime_metrics.job_finished(
                        job="platform_read_models_refresh",
                        duration_ms=round((perf_counter() - refresh_started) * 1000, 2),
                        success=False,
                    )
                    raise
                else:
                    runtime_metrics.job_finished(
                        job="platform_read_models_refresh",
                        duration_ms=round((perf_counter() - refresh_started) * 1000, 2),
                        success=True,
                    )
                backup_payload = {"status": "disabled"}
                if settings.platform_backup_enabled:
                    last_backup = latest_backup(session)
                    should_run = True
                    if last_backup and last_backup.started_at:
                        elapsed_hours = (datetime.now(timezone.utc) - last_backup.started_at).total_seconds() / 3600
                        should_run = elapsed_hours >= max(int(settings.platform_backup_min_interval_hours or 24), 1)
                    if should_run:
                        backup_record = run_backup(session, scope="platform", trigger_source="scheduler")
                        backup_payload = {
                            "status": backup_record.status,
                            "backup_id": backup_record.id,
                            "started_at": backup_record.started_at,
                            "finished_at": backup_record.finished_at,
                        }
                    else:
                        backup_payload = {"status": "skipped", "reason": "interval_not_reached"}
                try:
                    notification_payload = refresh_governance_notifications(session)
                except Exception as notification_exc:  # noqa: BLE001
                    session.rollback()
                    notification_payload = {
                        "enabled": bool(settings.platform_read_model_auto_refresh_enabled),
                        "error": str(notification_exc),
                    }
                    logger.exception(
                        "platform maintenance governance notifications refresh failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                try:
                    governance_inbox_payload = queue_governance_notifications_for_users(session)
                except Exception as governance_inbox_exc:  # noqa: BLE001
                    session.rollback()
                    governance_inbox_payload = {"error": str(governance_inbox_exc)}
                    logger.exception(
                        "platform maintenance governance inbox queue failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                try:
                    governance_score_history_payload = refresh_governance_score_snapshots(session)
                except Exception as governance_score_exc:  # noqa: BLE001
                    session.rollback()
                    governance_score_history_payload = {"error": str(governance_score_exc)}
                    logger.exception(
                        "platform maintenance governance score history refresh failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                try:
                    governance_trust_history_payload = refresh_governance_trust_snapshots(session)
                except Exception as governance_trust_exc:  # noqa: BLE001
                    session.rollback()
                    governance_trust_history_payload = {"error": str(governance_trust_exc)}
                    logger.exception(
                        "platform maintenance governance trust history refresh failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                try:
                    governance_recommendations_payload = refresh_governance_recommendations(session)
                except Exception as governance_recommendation_exc:  # noqa: BLE001
                    session.rollback()
                    governance_recommendations_payload = {"error": str(governance_recommendation_exc)}
                    logger.exception(
                        "platform maintenance governance recommendations refresh failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                try:
                    operational_stability_payload = refresh_operational_stability_snapshots(session)
                except Exception as stability_exc:  # noqa: BLE001
                    session.rollback()
                    operational_stability_payload = {
                        "error": str(stability_exc),
                    }
                    logger.exception(
                        "platform maintenance operational stability snapshot refresh failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                try:
                    daily_digest_payload = dispatch_daily_notification_digests(session)
                except Exception as digest_exc:  # noqa: BLE001
                    session.rollback()
                    daily_digest_payload = {"error": str(digest_exc)}
                    logger.exception(
                        "platform maintenance daily digest dispatch failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                try:
                    data_lake_payload = run_data_lake_scan_scheduler_cycle()
                except Exception as data_lake_exc:  # noqa: BLE001
                    session.rollback()
                    data_lake_payload = {"error": str(data_lake_exc)}
                    logger.exception(
                        "platform maintenance data lake scan scheduler failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                if settings.platform_automation_rules_enabled:
                    try:
                        automation_payload = evaluate_automation_rules(session)
                    except Exception as automation_exc:  # noqa: BLE001
                        session.rollback()
                        automation_payload = {"error": str(automation_exc)}
                        logger.exception(
                            "platform maintenance automation evaluation failed trigger=%s mode=%s",
                            trigger,
                            scheduler_mode,
                        )
                else:
                    automation_payload = {"skipped": "automation rules engine disabled"}
                purge_payload = run_retention_cleanup_job(session, trigger_source=trigger)
                summary = {
                    "trigger": trigger,
                    "scheduler_mode": scheduler_mode,
                    "read_models": refresh_payload,
                    "backup": backup_payload,
                    "governance_notifications": notification_payload,
                    "governance_inbox": governance_inbox_payload,
                    "governance_score_history": governance_score_history_payload,
                    "governance_trust_history": governance_trust_history_payload,
                    "governance_recommendations": governance_recommendations_payload,
                    "operational_stability": operational_stability_payload,
                    "daily_notification_digest": daily_digest_payload,
                    "data_lake_scan": data_lake_payload,
                    "automation": automation_payload,
                    "maintenance": purge_payload,
                }
                _update_runtime_state(
                    phase="running",
                    mode=scheduler_mode,
                    is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                    heartbeat=True,
                    success=True,
                    summary=summary,
                )
                _try_update_scheduler_status_in_session(
                    session,
                    mode=scheduler_mode,
                    is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                    heartbeat=True,
                    success=True,
                    summary=summary,
                )
                session.commit()
                try:
                    write_audit_log_sync(
                        session,
                        action=f"platform.scheduler.{trigger}",
                        entity_type="platform_scheduler",
                        entity_id=SCHEDULER_NAME,
                        actor_name="system",
                        source_module="platform.scheduler",
                        change_type="update",
                        after=summary,
                        metadata={"message": "Platform maintenance cycle completed"},
                    )
                    session.commit()
                except Exception:  # noqa: BLE001
                    session.rollback()
                    logger.exception(
                        "platform maintenance scheduler audit logging failed trigger=%s mode=%s",
                        trigger,
                        scheduler_mode,
                    )
                return summary
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                try:
                    category, severity, retryable = classify_operational_error(exc, source="platform.scheduler")
                    record_operational_failure(
                        session,
                        source="platform.scheduler",
                        message=str(exc),
                        category_code=category,
                        severity=severity,
                        retryable=retryable,
                        scheduler_name=SCHEDULER_NAME,
                        job_name=trigger,
                        context={"mode": scheduler_mode},
                    )
                    session.commit()
                except Exception:  # noqa: BLE001
                    session.rollback()
                _update_runtime_state(
                    phase="failed",
                    mode=scheduler_mode,
                    is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                    heartbeat=True,
                    failure=str(exc),
                    summary={"trigger": trigger},
                )
                _try_update_scheduler_status_in_session(
                    session,
                    mode=scheduler_mode,
                    is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                    heartbeat=True,
                    failure=str(exc),
                    summary={"trigger": trigger},
                )
                raise
    finally:
        _release_maintenance_refresh_lock()


def run_platform_maintenance_cycle(*, trigger: str = "manual", scheduler_mode: str | None = None) -> dict[str, object]:
    scheduler_mode = normalize_scheduler_mode(scheduler_mode or settings.platform_scheduler_mode)
    if scheduler_mode == "embedded_dev_only" and not embedded_scheduler_allowed(scheduler_mode, settings.env):
        summary = {
            "trigger": trigger,
            "scheduler_mode": scheduler_mode,
            "skipped": "embedded_not_allowed",
            "error": "Embedded schedulers are not allowed outside dev/test. Use worker mode.",
        }
        _update_runtime_state(phase="disabled", mode=scheduler_mode, is_enabled=False, summary=summary)
        return summary
    if not settings.platform_read_model_auto_refresh_enabled:
        summary = {
            "trigger": trigger,
            "scheduler_mode": scheduler_mode,
            "skipped": "maintenance_disabled",
        }
        _update_runtime_state(phase="idle", mode=scheduler_mode, is_enabled=False, summary=summary)
        _persist_scheduler_status(mode=scheduler_mode, is_enabled=False, summary=summary)
        return summary
    if not _acquire_maintenance_refresh_lock():
        summary = {
            "trigger": trigger,
            "scheduler_mode": scheduler_mode,
            "skipped": "maintenance_already_running",
        }
        logger.info("platform maintenance enqueue skipped trigger=%s mode=%s reason=lock_unavailable", trigger, scheduler_mode)
        _update_runtime_state(phase="idle", mode=scheduler_mode, is_enabled=True, summary=summary)
        return summary
    try:
        with SessionLocal() as session:
            _update_runtime_state(
                phase="running",
                mode=scheduler_mode,
                is_enabled=True,
                started=True,
                heartbeat=True,
            )
            _try_update_scheduler_status_in_session(
                session,
                mode=scheduler_mode,
                is_enabled=True,
                started=True,
                heartbeat=True,
            )
            try:
                job = enqueue_platform_maintenance_job(
                    session,
                    trigger=trigger,
                    scheduler_mode=scheduler_mode,
                )
            except HTTPException as exc:
                if exc.status_code != status.HTTP_409_CONFLICT:
                    session.rollback()
                    summary = {
                        "trigger": trigger,
                        "scheduler_mode": scheduler_mode,
                        "error": str(exc),
                    }
                    _update_runtime_state(phase="failed", mode=scheduler_mode, is_enabled=True, failure=str(exc), summary=summary)
                    _persist_scheduler_status(
                        mode=scheduler_mode,
                        is_enabled=True,
                        failure=str(exc),
                        summary=summary,
                    )
                    logger.exception("platform maintenance enqueue failed trigger=%s mode=%s", trigger, scheduler_mode)
                    return summary
                summary = {
                    "trigger": trigger,
                    "scheduler_mode": scheduler_mode,
                    "skipped": "job_already_active",
                }
                _update_runtime_state(phase="idle", mode=scheduler_mode, is_enabled=True, summary=summary)
                _try_update_scheduler_status_in_session(
                    session,
                    mode=scheduler_mode,
                    is_enabled=True,
                    summary=summary,
                )
                session.commit()
                return summary
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                summary = {
                    "trigger": trigger,
                    "scheduler_mode": scheduler_mode,
                    "error": str(exc),
                }
                _update_runtime_state(phase="failed", mode=scheduler_mode, is_enabled=True, failure=str(exc), summary=summary)
                _persist_scheduler_status(
                    mode=scheduler_mode,
                    is_enabled=True,
                    failure=str(exc),
                    summary=summary,
                )
                logger.exception("platform maintenance enqueue failed trigger=%s mode=%s", trigger, scheduler_mode)
                return summary
            if job is None:
                summary = {
                    "trigger": trigger,
                    "scheduler_mode": scheduler_mode,
                    "skipped": "queue_unavailable",
                }
                _update_runtime_state(phase="idle", mode=scheduler_mode, is_enabled=True, summary=summary)
                _try_update_scheduler_status_in_session(
                    session,
                    mode=scheduler_mode,
                    is_enabled=True,
                    summary=summary,
                )
                session.commit()
                return summary
            summary = {
                "trigger": trigger,
                "scheduler_mode": scheduler_mode,
                "job_id": job.id,
                "job_key": job.job_key,
                "job_status": job.status,
                "queued_at": job.queued_at.isoformat() if job.queued_at else None,
            }
            _update_runtime_state(phase="running", mode=scheduler_mode, is_enabled=True, heartbeat=True, success=True, summary=summary)
            _try_update_scheduler_status_in_session(
                session,
                mode=scheduler_mode,
                is_enabled=True,
                heartbeat=True,
                success=True,
                summary=summary,
            )
            session.commit()
            return summary
    finally:
        _release_maintenance_refresh_lock()


def process_platform_maintenance_job(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
    payload = dict(job.payload_json or {}) if isinstance(job.payload_json, dict) else {}
    trigger = str(payload.get("trigger") or job.trigger_mode or "scheduled")
    scheduler_mode = normalize_scheduler_mode(str(payload.get("scheduler_mode") or settings.platform_scheduler_mode))
    try:
        summary = _run_maintenance_once(trigger=trigger, scheduler_mode=scheduler_mode)
    except Exception as exc:  # noqa: BLE001
        context = {
            "trigger": trigger,
            "scheduler_mode": scheduler_mode,
            "job_key": job.job_key,
            "error": str(exc),
        }
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error=str(exc),
            context_json=context,
            result_summary_json={"error": str(exc), "trigger": trigger, "scheduler_mode": scheduler_mode},
            progress_pct=100.0,
        ) or job

    status = "success"
    error: str | None = None
    if isinstance(summary, dict) and summary.get("skipped"):
        status = "cancelled"
        error = str(summary.get("skipped"))

    context = {
        "trigger": trigger,
        "scheduler_mode": scheduler_mode,
        "job_key": job.job_key,
    }
    if isinstance(summary, dict):
        context["summary"] = summary
    return finish_integration_job_record(
        session,
        job,
        status=status,
        error=error,
        context_json=context,
        result_summary_json=summary,
        progress_pct=100.0,
    ) or job


async def _scheduler_loop() -> None:
    interval_seconds = max(int(settings.platform_read_model_refresh_interval_minutes or 30), 1) * 60
    logger.info("platform maintenance scheduler started interval_seconds=%s mode=embedded_dev_only", interval_seconds)
    consecutive_failures = 0
    while True:
        try:
            await asyncio.to_thread(run_platform_maintenance_cycle, trigger="scheduled", scheduler_mode="embedded_dev_only")
            consecutive_failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            consecutive_failures += 1
            retry_delay = min(interval_seconds, min(60, max(5, 2 ** min(consecutive_failures, 5))))
            logger.warning(
                "platform maintenance scheduler refresh failed mode=embedded_dev_only failure_count=%s retry_in_seconds=%s error=%s",
                consecutive_failures,
                retry_delay,
                exc,
            )
            await asyncio.sleep(retry_delay)
            continue
        await asyncio.sleep(interval_seconds)


async def run_platform_scheduler_forever() -> None:
    if not settings.platform_read_model_auto_refresh_enabled:
        logger.info("platform maintenance scheduler disabled in dedicated worker")
        _update_runtime_state(phase="disabled", mode="dedicated", is_enabled=False, heartbeat=True)
        _persist_scheduler_status(mode="dedicated", is_enabled=False, heartbeat=True)
        return
    interval_seconds = max(int(settings.platform_read_model_refresh_interval_minutes or 30), 1) * 60
    _update_runtime_state(phase="running", mode="dedicated", is_enabled=True, started=True, heartbeat=True)
    _persist_scheduler_status(mode="dedicated", is_enabled=True, started=True, heartbeat=True)
    logger.info("platform maintenance dedicated worker started interval_seconds=%s", interval_seconds)
    consecutive_failures = 0
    while True:
        try:
            await asyncio.to_thread(run_platform_maintenance_cycle, trigger="scheduled", scheduler_mode="dedicated")
            consecutive_failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            consecutive_failures += 1
            retry_delay = min(interval_seconds, min(60, max(5, 2 ** min(consecutive_failures, 5))))
            logger.warning(
                "platform maintenance dedicated worker cycle failed failure_count=%s retry_in_seconds=%s error=%s",
                consecutive_failures,
                retry_delay,
                exc,
            )
            await asyncio.sleep(retry_delay)
            continue
        await asyncio.sleep(interval_seconds)


async def _bootstrap_embedded_scheduler() -> None:
    global _scheduler_task
    backoff_seconds = 2
    configured_mode = normalize_scheduler_mode(settings.platform_scheduler_mode)
    while True:
        try:
            _update_runtime_state(
                phase="bootstrapping",
                mode=configured_mode,
                is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                bootstrap_attempt_increment=True,
            )
            if not embedded_scheduler_allowed(configured_mode, settings.env):
                logger.info("platform maintenance embedded scheduler skipped mode=%s", configured_mode)
                _update_runtime_state(
                    phase="disabled",
                    mode=configured_mode,
                    is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                )
                _persist_scheduler_status(
                    mode=configured_mode,
                    is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                )
                return
            if not settings.platform_read_model_auto_refresh_enabled:
                logger.info("platform maintenance scheduler disabled")
                _update_runtime_state(phase="disabled", mode=configured_mode, is_enabled=False)
                _persist_scheduler_status(mode=configured_mode, is_enabled=False)
                return
            if _scheduler_task is not None and not _scheduler_task.done():
                logger.info("platform maintenance scheduler already running")
                _update_runtime_state(phase="running", mode=configured_mode, is_enabled=True)
                return
            persisted = _persist_scheduler_status(mode=configured_mode, is_enabled=True, started=True, heartbeat=True)
            if not persisted:
                logger.warning(
                    "platform maintenance embedded scheduler bootstrap continuing without persistent status table"
                )
            _scheduler_task = asyncio.create_task(_scheduler_loop(), name="platform-maintenance-scheduler")
            _update_runtime_state(phase="running", mode=configured_mode, is_enabled=True, started=True, heartbeat=True)
            logger.info(
                "platform maintenance embedded scheduler bootstrap completed attempts=%s",
                _runtime_state.bootstrap_attempts,
            )
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            _update_runtime_state(
                phase="bootstrap_failed",
                mode=configured_mode,
                is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
                failure=str(exc),
            )
            logger.exception(
                "platform maintenance scheduler bootstrap failed mode=%s attempt=%s retry_in_seconds=%s",
                configured_mode,
                _runtime_state.bootstrap_attempts,
                backoff_seconds,
            )
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 60)


def start_platform_scheduler() -> None:
    global _scheduler_bootstrap_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _update_runtime_state(
            phase="bootstrap_failed",
            mode=normalize_scheduler_mode(settings.platform_scheduler_mode),
            is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
            failure="no running event loop available during startup",
        )
        logger.exception("platform maintenance scheduler bootstrap could not be scheduled: no running event loop")
        return
    if _scheduler_bootstrap_task is not None and not _scheduler_bootstrap_task.done():
        logger.info("platform maintenance scheduler bootstrap already in progress")
        return
    if _scheduler_task is not None and not _scheduler_task.done():
        logger.info("platform maintenance scheduler already running")
        return
    _update_runtime_state(
        phase="starting",
        mode=normalize_scheduler_mode(settings.platform_scheduler_mode),
        is_enabled=bool(settings.platform_read_model_auto_refresh_enabled),
    )
    _scheduler_bootstrap_task = loop.create_task(
        _bootstrap_embedded_scheduler(),
        name="platform-maintenance-bootstrap",
    )
    logger.info(
        "platform maintenance scheduler bootstrap scheduled mode=%s enabled=%s",
        normalize_scheduler_mode(settings.platform_scheduler_mode),
        settings.platform_read_model_auto_refresh_enabled,
    )


async def stop_platform_scheduler() -> None:
    global _scheduler_task, _scheduler_bootstrap_task
    bootstrap_task = _scheduler_bootstrap_task
    _scheduler_bootstrap_task = None
    if bootstrap_task is not None:
        bootstrap_task.cancel()
        try:
            await bootstrap_task
        except asyncio.CancelledError:
            logger.info("platform maintenance scheduler bootstrap stopped")
    task = _scheduler_task
    _scheduler_task = None
    if task is None:
        _update_runtime_state(phase="stopped")
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("platform maintenance scheduler stopped")
    _update_runtime_state(phase="stopped")
