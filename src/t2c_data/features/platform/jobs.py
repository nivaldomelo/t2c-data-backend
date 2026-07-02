from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import blake2b
from threading import Lock
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import func, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from t2c_data.core.config import normalize_scheduler_mode, settings
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.data_quality.schedule_utils import compute_next_run_at
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.pagination import DEFAULT_MAX_PAGE_SIZE, DEFAULT_PAGE_SIZE, normalize_page_params
from t2c_data.features.platform.alerting import emit_operational_alert_for_job
from t2c_data.features.platform.job_diagnostics import diagnose_integration_job
from t2c_data.models.datasource_scheduler import DataSourceScanSchedule
from t2c_data.models.dq import DQProfilingSchedule, DQRule
from t2c_data.models.platform import AssetRowCountSnapshot, DataLakeScanSchedule, IntegrationSyncJob
from t2c_data.schemas.pagination import PageOut
from t2c_data.schemas.platform import IntegrationSyncJobOut, IntegrationSyncJobRunIn

logger = logging.getLogger(__name__)
_ACTIVE_JOB_STATUSES = ("queued", "running")
_RECURRENCE_LOOKBACK_DAYS = 30

_JOB_LOCKS: dict[str, Lock] = {}
_JOB_LOCKS_GUARD = Lock()


@dataclass(slots=True)
class IntegrationJobHandle:
    job: IntegrationSyncJob
    job_key: str
    thread_lock: Lock
    advisory_lock_key: int | None
    advisory_locked: bool = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _job_log_context(
    *,
    job_key: str,
    source: str,
    job_type: str,
    target_type: str | None = None,
    target_id: int | None = None,
    job_id: int | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    return {
        "job_key": job_key,
        "source": source,
        "job_type": job_type,
        "target_type": target_type,
        "target_id": target_id,
        "job_id": job_id,
        "job_status": status,
    }


def _normalize_token(value: str | None) -> str:
    return (value or "").strip().lower()


def build_integration_job_key(
    source: str,
    job_type: str,
    *,
    target_type: str | None = None,
    target_id: int | None = None,
) -> str:
    parts = [_normalize_token(source), _normalize_token(job_type)]
    if target_type:
        parts.append(_normalize_token(target_type))
    if target_id is not None:
        parts.append(str(int(target_id)))
    return ":".join(part for part in parts if part)


def _job_lock_key(job_key: str) -> int:
    return int.from_bytes(blake2b(job_key.encode("utf-8"), digest_size=8).digest(), "big", signed=True)


def _is_stalled_integration_job(session: Session, job: IntegrationSyncJob) -> bool:
    try:
        settings_snapshot = get_governance_settings_snapshot(session)
    except SQLAlchemyError:
        settings_snapshot = None
    diagnostics = diagnose_integration_job(
        job,
        now=_now(),
        attention_minutes=getattr(settings_snapshot, "platform_job_running_attention_minutes", 120) if settings_snapshot is not None else 120,
        critical_hours=getattr(settings_snapshot, "platform_job_running_critical_hours", 24) if settings_snapshot is not None else 24,
        next_expected_delay_minutes=getattr(settings_snapshot, "platform_job_next_expected_delay_minutes", 60) if settings_snapshot is not None else 60,
    )
    return bool(diagnostics.get("is_stalled"))


def _job_table_ready(session: Session) -> bool:
    try:
        bind = session.connection()
    except Exception:  # noqa: BLE001
        bind = session.get_bind()
    if bind is None:
        return False
    inspector = inspect(bind)
    return inspector.has_table("integration_sync_jobs", schema=settings.db_schema)


def _row_count_table_ready(session: Session) -> bool:
    try:
        bind = session.connection()
    except Exception:  # noqa: BLE001
        bind = session.get_bind()
    if bind is None:
        return False
    inspector = inspect(bind)
    return inspector.has_table("asset_row_count_snapshots", schema=settings.db_schema)


def _acquire_thread_lock(job_key: str) -> Lock:
    with _JOB_LOCKS_GUARD:
        lock = _JOB_LOCKS.get(job_key)
        if lock is None:
            lock = Lock()
            _JOB_LOCKS[job_key] = lock
    return lock


def _acquire_advisory_lock(session: Session, job_key: str) -> bool:
    bind = session.get_bind()
    if bind is None or getattr(bind.dialect, "name", None) != "postgresql":
        return False
    try:
        return bool(
            session.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": _job_lock_key(job_key)},
            ).scalar_one()
        )
    except Exception:  # noqa: BLE001
        logger.exception("integration sync advisory lock acquisition failed job_key=%s", job_key)
        return False


def _release_advisory_lock(session: Session, job_key: str) -> None:
    bind = session.get_bind()
    if bind is None or getattr(bind.dialect, "name", None) != "postgresql":
        return
    try:
        session.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": _job_lock_key(job_key)})
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()
        logger.exception("integration sync advisory lock release failed job_key=%s", job_key)


def _serialize_job(
    session: Session,
    job: IntegrationSyncJob,
    *,
    settings_snapshot=None,
    now: datetime | None = None,
) -> dict[str, Any]:
    recurrence_count = _recent_job_recurrence_count(session, job, now=now)
    payload = IntegrationSyncJobOut.model_validate(job, from_attributes=True).model_dump()
    diagnostics = diagnose_integration_job(
        job,
        now=now,
        attention_minutes=getattr(settings_snapshot, "platform_job_running_attention_minutes", 120),
        critical_hours=getattr(settings_snapshot, "platform_job_running_critical_hours", 24),
        next_expected_delay_minutes=getattr(settings_snapshot, "platform_job_next_expected_delay_minutes", 60),
        recurrence_count=recurrence_count,
    )
    payload.update(diagnostics)
    return payload


def _recent_job_recurrence_count(session: Session, job: IntegrationSyncJob, *, now: datetime | None = None) -> int | None:
    if job.status not in {"failed", "partial_success"}:
        return None
    reference_time = now or _now()
    lookback_start = reference_time - timedelta(days=_RECURRENCE_LOOKBACK_DAYS)
    return int(
        session.scalar(
            select(func.count()).select_from(IntegrationSyncJob).where(
                IntegrationSyncJob.job_key == job.job_key,
                IntegrationSyncJob.status == job.status,
                IntegrationSyncJob.finished_at.is_not(None),
                IntegrationSyncJob.finished_at >= lookback_start,
            )
        )
        or 0
    )


def _latest_job_by_key(session: Session, job_key: str) -> IntegrationSyncJob | None:
    return session.scalar(
        select(IntegrationSyncJob)
        .where(IntegrationSyncJob.job_key == job_key)
        .order_by(IntegrationSyncJob.queued_at.desc().nulls_last(), IntegrationSyncJob.started_at.desc().nulls_last(), IntegrationSyncJob.id.desc())
        .limit(1)
    )


def enqueue_integration_job(
    session: Session,
    *,
    job_key: str | None = None,
    source: str,
    job_type: str,
    target_type: str | None = None,
    target_id: int | None = None,
    target_name: str | None = None,
    trigger_mode: str = "manual",
    requested_by_user_id: int | None = None,
    correlation_id: str | None = None,
    payload_json: dict[str, Any] | list | None = None,
    context_json: dict[str, Any] | list | None = None,
    artifact_public_id: str | None = None,
    replace_stalled_running_job: bool = False,
) -> IntegrationSyncJob | None:
    now = _now()
    if not _job_table_ready(session):
        logger.warning(
            "integration sync job table unavailable schema=%s table=integration_sync_jobs",
            settings.db_schema,
        )
        return None

    job_key = _normalize_token(job_key) or build_integration_job_key(source, job_type, target_type=target_type, target_id=target_id)
    active_job = session.scalar(
        select(IntegrationSyncJob)
        .where(IntegrationSyncJob.job_key == job_key)
        .where(IntegrationSyncJob.status.in_(_ACTIVE_JOB_STATUSES))
        .order_by(IntegrationSyncJob.queued_at.desc().nulls_last(), IntegrationSyncJob.started_at.desc().nulls_last(), IntegrationSyncJob.id.desc())
        .limit(1)
    )
    if active_job is not None:
        can_replace_stalled = (
            replace_stalled_running_job
            and active_job.status == "running"
            and _is_stalled_integration_job(session, active_job)
        )
        if not can_replace_stalled:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma execução ativa ou enfileirada para este job.")
        active_job.status = "failed"
        active_job.finished_at = now
        active_job.error = "Execução substituída por nova solicitação enfileirada após ultrapassar o limite esperado."
        active_job.context_json = {
            **(active_job.context_json if isinstance(active_job.context_json, dict) else {}),
            "force_replaced": True,
            "force_replaced_at": now.isoformat(),
            "replacement_reason": "stalled_running_job",
            "replacement_mode": "queued_job",
        }
        active_job.progress_pct = 100.0
        session.add(active_job)
        session.flush()

    job = IntegrationSyncJob(
        job_key=job_key,
        source=_normalize_token(source),
        job_type=_normalize_token(job_type),
        target_type=_normalize_token(target_type) or None,
        target_id=target_id,
        target_name=target_name,
        trigger_mode=_normalize_token(trigger_mode) or "manual",
        status="queued",
        queued_at=now,
        started_at=now,
        next_expected_run_at=_resolve_next_expected_run_at(
            session,
            source=source,
            job_type=job_type,
            target_type=target_type,
            target_id=target_id,
        ),
        requested_by_user_id=requested_by_user_id,
        correlation_id=(correlation_id or str(uuid4()))[:120],
        context_json=context_json,
        payload_json=payload_json,
        progress_pct=0.0,
        artifact_public_id=artifact_public_id,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    logger.info(
        "integration_job_queued",
        extra={
            **_job_log_context(
                job_key=job.job_key,
                source=job.source,
                job_type=job.job_type,
                target_type=job.target_type,
                target_id=job.target_id,
                job_id=job.id,
                status=job.status,
            ),
            "queued_at": job.queued_at.isoformat() if job.queued_at else None,
        },
    )
    return job


def reclaim_stalled_running_jobs(
    session: Session,
    *,
    source: str | None = None,
    job_type: str | None = None,
) -> int:
    """Auto-cura da fila: marca como 'failed' jobs presos em 'running' que já estão stalled
    (worker morto/queda de conexão antes de finalizar). Sem isso, o job_key fica bloqueado
    indefinidamente (enqueue de novo job levanta 409). Retorna quantos foram recuperados."""
    if not _job_table_ready(session):
        return 0
    stmt = select(IntegrationSyncJob).where(IntegrationSyncJob.status == "running")
    if source:
        stmt = stmt.where(IntegrationSyncJob.source == _normalize_token(source))
    if job_type:
        stmt = stmt.where(IntegrationSyncJob.job_type == _normalize_token(job_type))
    reclaimed = 0
    for job in session.scalars(stmt).all():
        if not _is_stalled_integration_job(session, job):
            continue
        job.status = "failed"
        job.finished_at = _now()
        job.error = "reclaimed: stalled running job (worker crash/connection drop)"
        job.progress_pct = 100.0
        session.add(job)
        reclaimed += 1
        logger.warning("integration_job_reclaimed job_id=%s source=%s job_type=%s", job.id, job.source, job.job_type)
    if reclaimed:
        session.commit()
    return reclaimed


def claim_queued_integration_job(
    session: Session,
    *,
    source: str | None = None,
    job_type: str | None = None,
) -> IntegrationSyncJob | None:
    if not _job_table_ready(session):
        return None
    stmt = (
        select(IntegrationSyncJob)
        .where(IntegrationSyncJob.status == "queued")
        .order_by(IntegrationSyncJob.queued_at.asc().nulls_last(), IntegrationSyncJob.id.asc())
    )
    if source:
        stmt = stmt.where(IntegrationSyncJob.source == _normalize_token(source))
    if job_type:
        stmt = stmt.where(IntegrationSyncJob.job_type == _normalize_token(job_type))
    bind = session.get_bind()
    if bind is not None and getattr(bind.dialect, "name", None) == "postgresql":
        stmt = stmt.with_for_update(skip_locked=True)
    job = session.scalar(stmt.limit(1))
    if job is None:
        return None
    job.status = "running"
    job.started_at = _now()
    job.finished_at = None
    job.error = None
    job.progress_pct = max(float(job.progress_pct or 0.0), 1.0)
    session.add(job)
    session.commit()
    session.refresh(job)
    logger.info(
        "integration_job_claimed",
        extra=_job_log_context(
            job_key=job.job_key,
            source=job.source,
            job_type=job.job_type,
            target_type=job.target_type,
            target_id=job.target_id,
            job_id=job.id,
            status=job.status,
        ),
    )
    return job


def _resolve_next_expected_run_at(
    session: Session,
    *,
    source: str,
    job_type: str,
    target_type: str | None = None,
    target_id: int | None = None,
) -> datetime | None:
    normalized_source = _normalize_token(source)
    normalized_job_type = _normalize_token(job_type)

    if normalized_source == "s3" and normalized_job_type in {"inventory_scan", "data_lake_scan", "scan"} and target_id is not None:
        schedule = session.scalar(
            select(DataLakeScanSchedule)
            .where(DataLakeScanSchedule.connection_id == int(target_id))
            .where(DataLakeScanSchedule.schedule_enabled.is_(True))
            .order_by(DataLakeScanSchedule.schedule_next_run_at.asc().nulls_last(), DataLakeScanSchedule.id.desc())
            .limit(1)
        )
        if schedule is None:
            return None
        return schedule.schedule_next_run_at or compute_next_run_at(schedule)

    if normalized_source == "datasource" and normalized_job_type in {"scan", "datasource_scan"} and target_id is not None:
        schedule = session.scalar(
            select(DataSourceScanSchedule)
            .where(DataSourceScanSchedule.datasource_id == int(target_id))
            .where(DataSourceScanSchedule.schedule_enabled.is_(True))
            .order_by(DataSourceScanSchedule.schedule_next_run_at.asc().nulls_last(), DataSourceScanSchedule.id.desc())
            .limit(1)
        )
        if schedule is None:
            return None
        return schedule.schedule_next_run_at or compute_next_run_at(schedule)

    if normalized_source == "dq" and normalized_job_type in {"rules_scheduler", "scheduler", "rules"}:
        rules = session.scalars(
            select(DQRule)
            .where(DQRule.is_active.is_(True), DQRule.schedule_enabled.is_(True))
            .order_by(DQRule.id.asc())
        ).all()
        next_candidates = [compute_next_run_at(rule) for rule in rules]
        next_candidates = [candidate for candidate in next_candidates if candidate is not None]
        return min(next_candidates).astimezone(timezone.utc) if next_candidates else None

    if normalized_source == "dq" and normalized_job_type in {"profiling_scheduler", "profiling"}:
        schedules = session.scalars(
            select(DQProfilingSchedule)
            .where(DQProfilingSchedule.schedule_enabled.is_(True))
            .order_by(DQProfilingSchedule.schedule_next_run_at.asc().nulls_last(), DQProfilingSchedule.id.asc())
        ).all()
        next_candidates = [schedule.schedule_next_run_at or compute_next_run_at(schedule) for schedule in schedules]
        next_candidates = [candidate for candidate in next_candidates if candidate is not None]
        return min(next_candidates).astimezone(timezone.utc) if next_candidates else None

    if normalized_source == "metabase":
        return None

    if normalized_source == "platform" and normalized_job_type in {"maintenance", "read_model_refresh", "read_models"}:
        return _now() + timedelta(minutes=max(int(settings.platform_read_model_refresh_interval_minutes or 30), 1))

    return None


def maybe_start_integration_job(
    session: Session,
    *,
    source: str,
    job_type: str,
    target_type: str | None = None,
    target_id: int | None = None,
    target_name: str | None = None,
    trigger_mode: str = "manual",
    force_stale_running_job: bool = False,
) -> IntegrationJobHandle | None:
    if not _job_table_ready(session):
        logger.warning(
            "integration sync job table unavailable schema=%s table=integration_sync_jobs",
            settings.db_schema,
        )
        return None

    job_key = build_integration_job_key(source, job_type, target_type=target_type, target_id=target_id)
    thread_lock = _acquire_thread_lock(job_key)
    if not thread_lock.acquire(blocking=False):
        if not force_stale_running_job:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma execução em andamento para este job.")

        running_job = session.scalar(
            select(IntegrationSyncJob)
            .where(IntegrationSyncJob.job_key == job_key)
            .where(IntegrationSyncJob.status == "running")
            .order_by(IntegrationSyncJob.started_at.desc(), IntegrationSyncJob.id.desc())
            .limit(1)
        )
        if running_job is None or not _is_stalled_integration_job(session, running_job):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma execução em andamento para este job.")
        try:
            thread_lock.release()
        except RuntimeError:
            pass
        if not thread_lock.acquire(blocking=False):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma execução em andamento para este job.")

    advisory_lock_key: int | None = None
    advisory_locked = False
    if _acquire_advisory_lock(session, job_key):
        advisory_lock_key = _job_lock_key(job_key)
        advisory_locked = True

    running_job = session.scalar(
        select(IntegrationSyncJob)
        .where(IntegrationSyncJob.job_key == job_key)
        .where(IntegrationSyncJob.status == "running")
        .order_by(IntegrationSyncJob.started_at.desc(), IntegrationSyncJob.id.desc())
        .limit(1)
    )
    if running_job is not None:
        if not force_stale_running_job or not _is_stalled_integration_job(session, running_job):
            if advisory_locked:
                _release_advisory_lock(session, job_key)
            thread_lock.release()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma execução em andamento para este job.")
        running_job.status = "failed"
        running_job.finished_at = _now()
        running_job.error = "Execução substituída por nova sync manual após ultrapassar o limite esperado."
        running_job.context_json = {
            **(running_job.context_json if isinstance(running_job.context_json, dict) else {}),
            "force_replaced": True,
            "force_replaced_at": _now().isoformat(),
            "replacement_reason": "stalled_running_job",
        }
        session.add(running_job)
        session.commit()

    job = IntegrationSyncJob(
        job_key=job_key,
        source=_normalize_token(source),
        job_type=_normalize_token(job_type),
        target_type=_normalize_token(target_type) or None,
        target_id=target_id,
        target_name=target_name,
        trigger_mode=_normalize_token(trigger_mode) or "manual",
        status="running",
        started_at=_now(),
        next_expected_run_at=_resolve_next_expected_run_at(
            session,
            source=source,
            job_type=job_type,
            target_type=target_type,
            target_id=target_id,
        ),
    )
    session.add(job)
    session.flush()
    session.commit()
    session.refresh(job)
    logger.info(
        "integration_job_started",
        extra=_job_log_context(
            job_key=job_key,
            source=job.source,
            job_type=job.job_type,
            target_type=job.target_type,
            target_id=job.target_id,
            job_id=job.id,
            status=job.status,
        ),
    )
    return IntegrationJobHandle(
        job=job,
        job_key=job_key,
        thread_lock=thread_lock,
        advisory_lock_key=advisory_lock_key,
        advisory_locked=advisory_locked,
    )


def finish_integration_job(
    session: Session,
    handle: IntegrationJobHandle | None,
    *,
    status: str,
    records_processed: int | None = None,
    error: str | None = None,
    context_json: dict[str, Any] | list | None = None,
    next_expected_run_at: datetime | None = None,
) -> IntegrationSyncJob | None:
    if handle is None:
        return None
    job = handle.job
    try:
        job.status = status
        job.finished_at = _now()
        job.records_processed = records_processed
        job.error = error[:4000] if error else None
        if context_json is not None:
            job.context_json = context_json
        if next_expected_run_at is not None:
            job.next_expected_run_at = next_expected_run_at
        session.add(job)
        session.commit()
        session.refresh(job)
        duration_ms = None
        if job.started_at and job.finished_at:
            duration_ms = max(0.0, (job.finished_at - job.started_at).total_seconds() * 1000.0)
            runtime_metrics.job_finished(
                job=f"{job.source}:{job.job_type}",
                duration_ms=duration_ms,
                success=str(status).strip().lower() == "success",
                status=status,
            )
        diagnostics = diagnose_integration_job(
            job,
            now=job.finished_at or _now(),
            recurrence_count=_recent_job_recurrence_count(session, job, now=job.finished_at or _now()),
        )
        runtime_metrics.diagnostic_emitted(
            module=str(diagnostics.get("diagnostic_module") or job.source or "platform"),
            severity=str(diagnostics.get("diagnostic_severity") or "info"),
            cause=str(diagnostics.get("diagnostic_probable_cause_code") or diagnostics.get("diagnostic_status") or "unknown"),
        )
        if emit_operational_alert_for_job(session, job=job, diagnostic=diagnostics):
            session.commit()
            session.refresh(job)
        logger.info(
            "integration_job_finished",
            extra={
                **_job_log_context(
                    job_key=handle.job_key,
                    source=job.source,
                    job_type=job.job_type,
                    target_type=job.target_type,
                    target_id=job.target_id,
                    job_id=job.id,
                    status=job.status,
                ),
                "records_processed": job.records_processed,
                "duration_ms": round(duration_ms, 2) if duration_ms is not None else None,
                "has_error": bool(job.error),
            },
        )
        return job
    finally:
        if handle.advisory_locked and handle.advisory_lock_key is not None:
            _release_advisory_lock(session, handle.job_key)
        handle.thread_lock.release()


def finish_integration_job_record(
    session: Session,
    job: IntegrationSyncJob | None,
    *,
    status: str,
    records_processed: int | None = None,
    error: str | None = None,
    context_json: dict[str, Any] | list | None = None,
    result_summary_json: dict[str, Any] | list | None = None,
    progress_pct: float | None = None,
    next_expected_run_at: datetime | None = None,
) -> IntegrationSyncJob | None:
    if job is None:
        return None
    job.status = status
    job.finished_at = _now()
    job.records_processed = records_processed
    job.error = error[:4000] if error else None
    if context_json is not None:
        job.context_json = context_json
    if result_summary_json is not None:
        job.result_summary_json = result_summary_json
    if progress_pct is not None:
        job.progress_pct = progress_pct
    else:
        job.progress_pct = 100.0 if status in {"success", "partial_success", "failed", "cancelled", "skipped"} else job.progress_pct
    if next_expected_run_at is not None:
        job.next_expected_run_at = next_expected_run_at
    session.add(job)
    session.commit()
    session.refresh(job)
    logger.info(
        "integration_job_finished",
        extra={
            **_job_log_context(
                job_key=job.job_key,
                source=job.source,
                job_type=job.job_type,
                target_type=job.target_type,
                target_id=job.target_id,
                job_id=job.id,
                status=job.status,
            ),
            "records_processed": job.records_processed,
            "has_error": bool(job.error),
        },
    )
    diagnostics = diagnose_integration_job(
        job,
        now=job.finished_at or _now(),
        recurrence_count=_recent_job_recurrence_count(session, job, now=job.finished_at or _now()),
    )
    runtime_metrics.diagnostic_emitted(
        module=str(diagnostics.get("diagnostic_module") or job.source or "platform"),
        severity=str(diagnostics.get("diagnostic_severity") or "info"),
        cause=str(diagnostics.get("diagnostic_probable_cause_code") or diagnostics.get("diagnostic_status") or "unknown"),
    )
    if emit_operational_alert_for_job(session, job=job, diagnostic=diagnostics):
        session.commit()
        session.refresh(job)
    return job


def record_asset_row_count_snapshot(
    session: Session,
    *,
    asset_type: str,
    asset_id: int,
    asset_name: str | None,
    asset_fqn: str | None,
    source: str = "s3",
    row_count: int | None,
    row_count_method: str | None,
    row_count_confidence: str | None,
    integration_sync_job_id: int | None = None,
    context_json: dict[str, Any] | list | None = None,
) -> AssetRowCountSnapshot | None:
    if not _row_count_table_ready(session):
        logger.warning(
            "asset row count snapshot table unavailable schema=%s table=asset_row_count_snapshots",
            settings.db_schema,
        )
        return None
    snapshot = AssetRowCountSnapshot(
        asset_type=_normalize_token(asset_type) or asset_type,
        asset_id=int(asset_id),
        asset_name=asset_name,
        asset_fqn=asset_fqn,
        source=_normalize_token(source) or "s3",
        observed_at=_now(),
        row_count=row_count,
        row_count_method=row_count_method,
        row_count_confidence=row_count_confidence,
        integration_sync_job_id=integration_sync_job_id,
        context_json=context_json,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def _jobs_base_stmt(
    *,
    source: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
):
    stmt = select(IntegrationSyncJob.id.label("job_id"))
    if source:
        stmt = stmt.where(IntegrationSyncJob.source == _normalize_token(source))
    if job_type:
        stmt = stmt.where(IntegrationSyncJob.job_type == _normalize_token(job_type))
    if status:
        stmt = stmt.where(IntegrationSyncJob.status == _normalize_token(status))
    rn = func.row_number().over(
        partition_by=IntegrationSyncJob.job_key,
        order_by=(IntegrationSyncJob.queued_at.desc().nulls_last(), IntegrationSyncJob.started_at.desc().nulls_last(), IntegrationSyncJob.id.desc()),
    ).label("rn")
    return stmt.add_columns(rn)


def _latest_jobs_stmt(
    *,
    source: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
):
    latest_jobs = _jobs_base_stmt(source=source, job_type=job_type, status=status).subquery()
    return (
        select(IntegrationSyncJob)
        .join(latest_jobs, IntegrationSyncJob.id == latest_jobs.c.job_id)
        .where(latest_jobs.c.rn == 1)
        .order_by(IntegrationSyncJob.queued_at.desc().nulls_last(), IntegrationSyncJob.started_at.desc().nulls_last(), IntegrationSyncJob.id.desc())
    )


def integration_jobs_status_snapshot(
    session: Session,
    *,
    limit: int = 12,
) -> dict[str, Any]:
    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    if not _job_table_ready(session):
        return {
            "generated_at": now,
            "total": 0,
            "queued": 0,
            "running": 0,
            "success": 0,
            "partial_success": 0,
            "failed": 0,
            "skipped": 0,
            "next_expected_run_at": None,
            "items": [],
        }
    latest_jobs = session.scalars(_latest_jobs_stmt().limit(max(int(limit or 12), 1))).all()
    counts = Counter(job.status for job in latest_jobs)
    next_candidates = [job.next_expected_run_at for job in latest_jobs if job.next_expected_run_at is not None]
    next_expected_run_at = min(next_candidates) if next_candidates else None
    return {
        "generated_at": now,
        "total": len(latest_jobs),
        "queued": int(counts.get("queued", 0)),
        "running": int(counts.get("running", 0)),
        "success": int(counts.get("success", 0)),
        "partial_success": int(counts.get("partial_success", 0)),
        "failed": int(counts.get("failed", 0)),
        "skipped": int(counts.get("skipped", 0)),
        "next_expected_run_at": next_expected_run_at,
        "items": [_serialize_job(session, job, settings_snapshot=settings_snapshot, now=now) for job in latest_jobs],
    }


def list_integration_jobs_history(
    session: Session,
    *,
    page: int = 1,
    page_size: int = 20,
    source: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
) -> PageOut[IntegrationSyncJobOut]:
    if not _job_table_ready(session):
        normalized_page, normalized_page_size = normalize_page_params(
            page=page,
            page_size=page_size,
            default_page_size=20,
            max_page_size=100,
        )
        return PageOut[IntegrationSyncJobOut](
            page=normalized_page,
            page_size=normalized_page_size,
            total=0,
            has_more=False,
            items=[],
        )
    normalized_page, normalized_page_size = normalize_page_params(
        page=page,
        page_size=page_size,
        default_page_size=20,
        max_page_size=100,
    )
    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    latest_jobs = _jobs_base_stmt(source=source, job_type=job_type, status=status).subquery()
    total = int(session.scalar(select(func.count()).select_from(latest_jobs).where(latest_jobs.c.rn == 1)) or 0)
    rows = session.scalars(
        _latest_jobs_stmt(source=source, job_type=job_type, status=status)
        .offset((normalized_page - 1) * normalized_page_size)
        .limit(normalized_page_size)
    )
    items = [
        IntegrationSyncJobOut.model_validate(_serialize_job(session, job, settings_snapshot=settings_snapshot, now=now))
        for job in rows
    ]
    total_pages = (total + normalized_page_size - 1) // normalized_page_size if total > 0 else 0
    return PageOut[IntegrationSyncJobOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=items,
    )


def run_platform_job(
    session: Session,
    *,
    payload: IntegrationSyncJobRunIn,
    current_user,
    audit_kwargs: dict[str, Any],
) -> IntegrationSyncJobOut:
    source = _normalize_token(payload.source)
    job_type = _normalize_token(payload.job_type)
    target_type = _normalize_token(getattr(payload, "target_type", None)) or None
    target_id = getattr(payload, "target_id", None)
    target_name = getattr(payload, "target_name", None)
    trigger_mode = _normalize_token(getattr(payload, "trigger_mode", None)) or "manual"

    if source == "s3" and job_type in {"inventory_scan", "data_lake_scan", "scan"}:
        from t2c_data.features.integrations.data_lake_inventory import enqueue_data_lake_inventory_scan

        if target_id is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="target_id é obrigatório para o scan do Data Lake.")
        queued = enqueue_data_lake_inventory_scan(
            session,
            int(target_id),
            current_user=current_user,
            audit_kwargs=audit_kwargs,
        )
        if queued.job_id is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Job do Data Lake não foi registrado.")
        job = session.get(IntegrationSyncJob, queued.job_id)
        if job is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Job do Data Lake não foi registrado.")
        return IntegrationSyncJobOut.model_validate(job, from_attributes=True)

    if source == "metabase" and job_type == "sync":
        from t2c_data.features.metabase.service import enqueue_metabase_instance_sync

        if target_id is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="target_id é obrigatório para o sync do Metabase.")
        enqueue_metabase_instance_sync(session, int(target_id), current_user=current_user)
        job_key = build_integration_job_key("metabase", "sync", target_type="metabase_instance", target_id=int(target_id))
        job = _latest_job_by_key(session, job_key)
        if job is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Job do Metabase não foi registrado.")
        return IntegrationSyncJobOut.model_validate(job, from_attributes=True)

    if source == "dq" and job_type in {"rules_scheduler", "scheduler", "rules"}:
        from t2c_data.features.data_quality.scheduler import run_dq_scheduler_cycle

        run_dq_scheduler_cycle(trigger=trigger_mode, scheduler_mode=normalize_scheduler_mode(settings.dq_scheduler_mode))
        job_key = build_integration_job_key("dq", "rules_scheduler")
        job = _latest_job_by_key(session, job_key)
        if job is None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A execução do scheduler de regras foi ignorada ou não pôde ser registrada.")
        return IntegrationSyncJobOut.model_validate(job, from_attributes=True)

    if source == "dq" and job_type in {"profiling_scheduler", "profiling"}:
        from t2c_data.features.data_quality.profiling_scheduler import run_dq_profiling_scheduler_cycle

        run_dq_profiling_scheduler_cycle(
            trigger=trigger_mode,
            scheduler_mode=normalize_scheduler_mode(settings.dq_profiling_scheduler_mode),
        )
        job_key = build_integration_job_key("dq", "profiling_scheduler")
        job = _latest_job_by_key(session, job_key)
        if job is None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A execução do scheduler de profiling foi ignorada ou não pôde ser registrada.")
        return IntegrationSyncJobOut.model_validate(job, from_attributes=True)

    if source == "platform" and job_type in {"maintenance", "read_model_refresh", "read_models"}:
        from t2c_data.features.platform.scheduler import enqueue_platform_maintenance_job

        job = enqueue_platform_maintenance_job(
            session,
            trigger=trigger_mode,
            scheduler_mode="worker",
            requested_by_user_id=getattr(current_user, "id", None),
        )
        if job is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Job de manutenção da plataforma não foi registrado.")
        return IntegrationSyncJobOut.model_validate(job, from_attributes=True)

    if source == "export" and job_type in {
        "privacy_access.csv",
        "privacy_access.events.csv",
        "governance.owners.csv",
        "certification.queue.csv",
        "certification.events.csv",
        "audit.history.csv",
        "audit.history.xlsx",
    }:
        from t2c_data.features.export_jobs import enqueue_export_job

        job = enqueue_export_job(
            session,
            job_type=job_type,
            target_name=target_name,
            requested_by_user_id=getattr(current_user, "id", None),
            payload_json={
                "source": source,
                "job_type": job_type,
                "target_type": target_type,
                "target_id": target_id,
                "target_name": target_name,
                "trigger_mode": trigger_mode,
            },
            context_json={"source": source, "job_type": job_type, "trigger_mode": trigger_mode},
        )
        return IntegrationSyncJobOut.model_validate(job, from_attributes=True)

    if source == "datasource" and job_type in {"scan", "datasource_scan"}:
        from t2c_data.features.scanner.application import enqueue_datasource_scan
        from t2c_data.models.catalog import DataSource

        if target_id is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="target_id é obrigatório para o scan de datasource.")
        datasource = session.get(DataSource, int(target_id))
        if datasource is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource não encontrada.")
        _, job = enqueue_datasource_scan(
            session,
            datasource=datasource,
            started_by=getattr(current_user, "id", None),
            trigger_mode=trigger_mode,
        )
        if job is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Job de datasource não foi registrado.")
        return IntegrationSyncJobOut.model_validate(job, from_attributes=True)

    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=f"Job não suportado para source={source} job_type={job_type}.",
    )


__all__ = [
    "IntegrationJobHandle",
    "build_integration_job_key",
    "claim_queued_integration_job",
    "enqueue_integration_job",
    "finish_integration_job",
    "finish_integration_job_record",
    "integration_jobs_status_snapshot",
    "list_integration_jobs_history",
    "maybe_start_integration_job",
    "record_asset_row_count_snapshot",
    "run_platform_job",
]
