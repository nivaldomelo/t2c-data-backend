from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
import socket
from typing import Any

from sqlalchemy import func, inspect, select
from sqlalchemy.orm import Session

from t2c_data.core.config import is_dev_environment, settings
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.platform.job_diagnostics import diagnose_integration_job
from t2c_data.models.platform import IntegrationSyncJob, PlatformWorkerHeartbeat

CRITICAL_WORKER_JOB_TYPES = [
    "datasource:scan",
    "s3:inventory_scan",
    "metabase:sync",
    "platform:maintenance",
]


@dataclass(slots=True)
class WorkerHeartbeatContext:
    worker_id: str
    hostname: str
    started_at: datetime
    supported_job_types: list[str]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def worker_heartbeat_table_ready(session: Session) -> bool:
    try:
        bind = session.get_bind()
    except Exception:  # noqa: BLE001
        bind = None
    if bind is None:
        return False
    inspector = inspect(bind)
    return inspector.has_table("platform_worker_heartbeats", schema=settings.db_schema)


def build_worker_heartbeat_context(*, source: str | None = None, job_type: str | None = None) -> WorkerHeartbeatContext:
    hostname = socket.gethostname()
    pid = os.getpid()
    normalized_source = (source or "").strip().lower()
    normalized_job_type = (job_type or "").strip().lower()
    supported_job_types = (
        [f"{normalized_source}:{normalized_job_type}"]
        if normalized_source and normalized_job_type
        else ["*"]
    )
    worker_id = f"{hostname}:{pid}:{normalized_source or 'all'}:{normalized_job_type or 'all'}"
    return WorkerHeartbeatContext(
        worker_id=worker_id[:160],
        hostname=hostname[:255] or "unknown",
        started_at=_now(),
        supported_job_types=supported_job_types,
    )


def heartbeat_worker(
    session: Session,
    context: WorkerHeartbeatContext,
    *,
    status: str,
    active_job: IntegrationSyncJob | None = None,
    last_job_status: str | None = None,
) -> PlatformWorkerHeartbeat | None:
    if not worker_heartbeat_table_ready(session):
        return None
    heartbeat = session.scalar(
        select(PlatformWorkerHeartbeat)
        .where(PlatformWorkerHeartbeat.worker_id == context.worker_id)
        .limit(1)
    )
    now = _now()
    if heartbeat is None:
        heartbeat = PlatformWorkerHeartbeat(
            worker_id=context.worker_id,
            hostname=context.hostname,
            started_at=context.started_at,
            last_seen_at=now,
            supported_job_types_json=context.supported_job_types,
            status=status,
        )
    else:
        heartbeat.hostname = context.hostname
        heartbeat.started_at = heartbeat.started_at or context.started_at
        heartbeat.supported_job_types_json = context.supported_job_types
        heartbeat.last_seen_at = now
        heartbeat.status = status
    if active_job is not None:
        heartbeat.active_job_source = active_job.source
        heartbeat.active_job_type = active_job.job_type
        heartbeat.active_job_id = active_job.id
    else:
        heartbeat.active_job_source = None
        heartbeat.active_job_type = None
        heartbeat.active_job_id = None
    if last_job_status is not None:
        heartbeat.last_job_finished_at = now
        heartbeat.last_job_status = last_job_status
    session.add(heartbeat)
    session.commit()
    session.refresh(heartbeat)
    return heartbeat


def _job_is_stalled(session: Session, job: IntegrationSyncJob) -> bool:
    try:
        settings_snapshot = get_governance_settings_snapshot(session)
    except Exception:  # noqa: BLE001
        settings_snapshot = None
    diagnostics = diagnose_integration_job(
        job,
        now=_now(),
        attention_minutes=getattr(settings_snapshot, "platform_job_running_attention_minutes", 120) if settings_snapshot is not None else 120,
        critical_hours=getattr(settings_snapshot, "platform_job_running_critical_hours", 24) if settings_snapshot is not None else 24,
        next_expected_delay_minutes=getattr(settings_snapshot, "platform_job_next_expected_delay_minutes", 60) if settings_snapshot is not None else 60,
    )
    return bool(diagnostics.get("is_stalled"))


def worker_health_snapshot(session: Session) -> dict[str, Any]:
    if not worker_heartbeat_table_ready(session):
        return {
            "worker_required": not is_dev_environment(settings.env),
            "status": "unavailable",
            "detail": "worker heartbeat table unavailable",
            "supported_critical_job_types": list(CRITICAL_WORKER_JOB_TYPES),
            "workers_total": 0,
            "recent_workers_total": 0,
            "worker_last_seen_at": None,
            "worker_age_seconds": None,
            "queued_jobs_count": 0,
            "stale_running_jobs_count": 0,
        }

    now = _now()
    grace_seconds = max(int(settings.platform_worker_heartbeat_grace_seconds or 90), 1)
    worker_required = not is_dev_environment(settings.env)
    heartbeats = session.scalars(
        select(PlatformWorkerHeartbeat).order_by(PlatformWorkerHeartbeat.last_seen_at.desc(), PlatformWorkerHeartbeat.id.desc())
    ).all()
    workers_total = len(heartbeats)
    latest = heartbeats[0] if heartbeats else None
    supported_map: dict[str, int] = {job_type: 0 for job_type in CRITICAL_WORKER_JOB_TYPES}
    recent_workers_total = 0
    for heartbeat in heartbeats:
        last_seen_at = _ensure_utc(heartbeat.last_seen_at)
        if last_seen_at is None:
            continue
        age_seconds = max(0, int((now - last_seen_at).total_seconds()))
        recent = age_seconds <= grace_seconds
        if recent:
            recent_workers_total += 1
        supported = heartbeat.supported_job_types_json or []
        supported_list = [str(item) for item in supported] if isinstance(supported, list) else []
        supports_all = "*" in supported_list
        for critical_job_type in CRITICAL_WORKER_JOB_TYPES:
            if supports_all or critical_job_type in supported_list:
                if recent:
                    supported_map[critical_job_type] += 1

    critical_sources = {item.split(":", 1)[0] for item in CRITICAL_WORKER_JOB_TYPES}
    queued_jobs_count = int(
        session.scalar(
            select(func.count(IntegrationSyncJob.id)).where(
                IntegrationSyncJob.status == "queued",
                IntegrationSyncJob.source.in_(sorted(critical_sources)),
            )
        )
        or 0
    )
    running_jobs = session.scalars(
        select(IntegrationSyncJob).where(
            IntegrationSyncJob.status == "running",
            IntegrationSyncJob.source.in_(sorted(critical_sources)),
        )
    ).all()
    stale_running_jobs_count = sum(1 for job in running_jobs if _job_is_stalled(session, job))

    latest_last_seen_at = _ensure_utc(latest.last_seen_at) if latest is not None else None
    latest_age_seconds = max(0, int((now - latest_last_seen_at).total_seconds())) if latest_last_seen_at is not None else None
    unsupported_critical = [job_type for job_type, count in supported_map.items() if count <= 0]

    status_name = "ok"
    detail = None
    if workers_total == 0:
        status_name = "error" if worker_required else "warning"
        detail = "No worker heartbeat registered."
    elif latest_age_seconds is not None and latest_age_seconds > grace_seconds:
        status_name = "error" if worker_required else "warning"
        detail = "Worker heartbeat is stale."
    elif unsupported_critical:
        status_name = "error" if worker_required else "warning"
        detail = "Critical worker job types are not covered by a recent worker."
    elif queued_jobs_count > 0 and recent_workers_total == 0:
        status_name = "error" if worker_required else "warning"
        detail = "Queued jobs exist but no recent worker heartbeat is available."
    elif stale_running_jobs_count > 0:
        status_name = "warning"
        detail = "Stale running jobs detected."

    return {
        "worker_required": worker_required,
        "status": status_name,
        "detail": detail,
        "supported_critical_job_types": list(CRITICAL_WORKER_JOB_TYPES),
        "supported_recent_workers_by_job_type": supported_map,
        "unsupported_critical_job_types": unsupported_critical,
        "workers_total": workers_total,
        "recent_workers_total": recent_workers_total,
        "worker_last_seen_at": latest_last_seen_at,
        "worker_age_seconds": latest_age_seconds,
        "worker_status": latest.status if latest is not None else None,
        "queued_jobs_count": queued_jobs_count,
        "stale_running_jobs_count": stale_running_jobs_count,
    }


__all__ = [
    "CRITICAL_WORKER_JOB_TYPES",
    "WorkerHeartbeatContext",
    "build_worker_heartbeat_context",
    "heartbeat_worker",
    "worker_health_snapshot",
]
