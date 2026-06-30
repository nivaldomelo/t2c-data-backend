from __future__ import annotations

import tempfile
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from t2c_data.features.data_quality.observability_store import purge_persisted_observability_artifacts
from t2c_data.features.export_jobs import cleanup_expired_export_artifacts
from t2c_data.features.platform.maintenance import purge_operational_history
from t2c_data.features.platform.retention_policy import RetentionPolicySnapshot, get_retention_policy_snapshot
from t2c_data.models.auth import UserAccessEvent, UserSession
from t2c_data.models.governance import CertificationDecisionEvent, PrivacyReviewEvent
from t2c_data.models.incident import Incident, IncidentEvent
from t2c_data.models.operations import OperationalFailureEvent
from t2c_data.models.platform import AssetRowCountSnapshot, PlatformDomainEvent, RetentionCleanupRun


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _execute_step(
    session: Session,
    *,
    step_name: str,
    summary: dict[str, Any],
    errors: list[dict[str, str]],
    step: Callable[[], Any],
) -> bool:
    try:
        result = step()
        if result is not None:
            summary[step_name] = result
            if step_name == "operational_history" and isinstance(result, dict):
                summary.update(result)
        session.commit()
        return True
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        errors.append({"step": step_name, "error": str(exc)})
        summary[step_name] = {"error": str(exc)}
        return False


def _close_expired_user_sessions(session: Session, *, retention_policy: RetentionPolicySnapshot) -> int:
    now = _now()
    cutoff = now - timedelta(days=retention_policy.user_session_retention_days)
    sessions = session.scalars(
        select(UserSession).where(
            UserSession.ended_at.is_(None),
            UserSession.revoked_at.is_(None),
            UserSession.last_seen_at < cutoff,
        )
    ).all()
    updated = 0
    for row in sessions:
        started_at = _as_utc(row.started_at) or cutoff
        end_at = _as_utc(row.expires_at)
        if end_at is None or end_at > cutoff:
            end_at = cutoff
        if end_at < started_at:
            end_at = started_at
        row.ended_at = end_at
        row.duration_seconds = max(0, int((end_at - started_at).total_seconds()))
        row.end_reason = "expired"
        updated += 1
    session.flush()
    return updated


def _purge_user_access_events(session: Session, *, retention_policy: RetentionPolicySnapshot) -> int:
    cutoff = _now() - timedelta(days=retention_policy.user_access_event_retention_days)
    deleted = session.execute(delete(UserAccessEvent).where(UserAccessEvent.created_at < cutoff)).rowcount or 0
    session.flush()
    return int(deleted)


def _purge_incident_evidence(session: Session, *, retention_policy: RetentionPolicySnapshot) -> dict[str, int]:
    cutoff = _now() - timedelta(days=retention_policy.incident_evidence_retention_days)
    incident_rows = session.scalars(
        select(Incident).where(
            Incident.evidence_json.is_not(None),
            Incident.closed_at.is_not(None),
            Incident.closed_at < cutoff,
        )
    ).all()
    cleared_incidents = 0
    for incident in incident_rows:
        incident.evidence_json = None
        cleared_incidents += 1
    event_rows = session.scalars(
        select(IncidentEvent).where(
            IncidentEvent.evidence_json.is_not(None),
            IncidentEvent.created_at < cutoff,
        )
    ).all()
    cleared_events = 0
    for event in event_rows:
        event.evidence_json = None
        cleared_events += 1
    session.flush()
    return {
        "incident_evidence_cleared": int(cleared_incidents),
        "incident_event_evidence_cleared": int(cleared_events),
    }


def _purge_row_count_snapshots(session: Session, *, retention_policy: RetentionPolicySnapshot) -> int:
    cutoff = _now() - timedelta(days=retention_policy.row_count_snapshot_retention_days)
    deleted = session.execute(delete(AssetRowCountSnapshot).where(AssetRowCountSnapshot.observed_at < cutoff)).rowcount or 0
    session.flush()
    return int(deleted)


def _purge_certification_history(session: Session, *, retention_policy: RetentionPolicySnapshot) -> int:
    cutoff = _now() - timedelta(days=retention_policy.certification_history_retention_days)
    deleted = session.execute(
        delete(CertificationDecisionEvent).where(CertificationDecisionEvent.created_at < cutoff)
    ).rowcount or 0
    session.flush()
    return int(deleted)


def _purge_privacy_review_events(session: Session, *, retention_policy: RetentionPolicySnapshot) -> int:
    cutoff = _now() - timedelta(days=retention_policy.privacy_review_event_retention_days)
    deleted = session.execute(delete(PrivacyReviewEvent).where(PrivacyReviewEvent.created_at < cutoff)).rowcount or 0
    session.flush()
    return int(deleted)


def _purge_system_logs(session: Session, *, retention_policy: RetentionPolicySnapshot) -> dict[str, int]:
    cutoff = _now() - timedelta(days=retention_policy.system_log_retention_days)
    operational_failure_deleted = session.execute(
        delete(OperationalFailureEvent).where(OperationalFailureEvent.created_at < cutoff)
    ).rowcount or 0
    platform_domain_deleted = session.execute(
        delete(PlatformDomainEvent).where(PlatformDomainEvent.created_at < cutoff)
    ).rowcount or 0
    session.flush()
    return {
        "operational_failure_events_deleted": int(operational_failure_deleted),
        "platform_domain_events_deleted": int(platform_domain_deleted),
    }


def _cleanup_stale_profiling_samples(*, retention_policy: RetentionPolicySnapshot) -> dict[str, int]:
    cutoff = _now() - timedelta(days=retention_policy.profiling_sample_retention_days)
    cutoff_ts = cutoff.timestamp()
    temp_dir = Path(tempfile.gettempdir())
    prefixes = ("profiling-run-",)
    deleted = 0
    errors = 0
    for prefix in prefixes:
        for path in temp_dir.glob(f"{prefix}*.json"):
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            except OSError:
                errors += 1
                continue
            if stat.st_mtime > cutoff_ts:
                continue
            try:
                path.unlink(missing_ok=True)
                deleted += 1
            except OSError:
                errors += 1
    return {"profiling_samples_deleted": int(deleted), "profiling_sample_errors": int(errors)}


def _cleanup_stale_temp_files(*, retention_policy: RetentionPolicySnapshot) -> dict[str, int]:
    cutoff = _now() - timedelta(hours=retention_policy.temp_file_ttl_hours)
    cutoff_ts = cutoff.timestamp()
    temp_dir = Path(tempfile.gettempdir())
    prefixes = ("profiling-conn-", "rules-conn-", "rules-run-")
    deleted = 0
    errors = 0
    for prefix in prefixes:
        for path in temp_dir.glob(f"{prefix}*.json"):
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            except OSError:
                errors += 1
                continue
            if stat.st_mtime > cutoff_ts:
                continue
            try:
                path.unlink(missing_ok=True)
                deleted += 1
            except OSError:
                errors += 1
    return {"temporary_files_deleted": int(deleted), "temporary_file_errors": int(errors)}


def run_retention_cleanup_job(
    session: Session,
    *,
    trigger_source: str = "scheduler",
) -> dict[str, Any]:
    now = _now()
    retention_policy = get_retention_policy_snapshot(session)
    total_steps = 12
    run = RetentionCleanupRun(
        job_name="retention_cleanup_job",
        trigger_source=trigger_source,
        status="running",
        started_at=now,
        retention_policy_json=asdict(retention_policy),
        summary_json={},
    )
    session.add(run)
    session.commit()

    summary: dict[str, Any] = {
        "job_name": run.job_name,
        "trigger_source": trigger_source,
        "started_at": now.isoformat(),
        "retention_policy": asdict(retention_policy),
    }
    errors: list[dict[str, str]] = []
    completed_steps = 0
    failed_steps = 0

    if _execute_step(
        session,
        step_name="operational_history",
        summary=summary,
        errors=errors,
        step=lambda: purge_operational_history(session, retention_policy=retention_policy),
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="user_sessions",
        summary=summary,
        errors=errors,
        step=lambda: {"user_sessions_closed": _close_expired_user_sessions(session, retention_policy=retention_policy)},
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="user_access_events",
        summary=summary,
        errors=errors,
        step=lambda: {"user_access_events_deleted": _purge_user_access_events(session, retention_policy=retention_policy)},
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="export_files",
        summary=summary,
        errors=errors,
        step=lambda: {"export_files_deleted": cleanup_expired_export_artifacts(session)},
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="dq_samples",
        summary=summary,
        errors=errors,
        step=lambda: purge_persisted_observability_artifacts(
            session,
            evidence_retention_days=retention_policy.dq_sample_retention_days,
        ),
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="incident_evidence",
        summary=summary,
        errors=errors,
        step=lambda: _purge_incident_evidence(session, retention_policy=retention_policy),
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="row_count_snapshots",
        summary=summary,
        errors=errors,
        step=lambda: {"row_count_snapshots_deleted": _purge_row_count_snapshots(session, retention_policy=retention_policy)},
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="certification_history",
        summary=summary,
        errors=errors,
        step=lambda: {"certification_history_deleted": _purge_certification_history(session, retention_policy=retention_policy)},
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="privacy_review_events",
        summary=summary,
        errors=errors,
        step=lambda: {"privacy_review_events_deleted": _purge_privacy_review_events(session, retention_policy=retention_policy)},
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="system_logs",
        summary=summary,
        errors=errors,
        step=lambda: _purge_system_logs(session, retention_policy=retention_policy),
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    if _execute_step(
        session,
        step_name="profiling_samples",
        summary=summary,
        errors=errors,
        step=lambda: _cleanup_stale_profiling_samples(retention_policy=retention_policy),
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    profiling_sample_errors = int((summary.get("profiling_samples") or {}).get("profiling_sample_errors", 0) or 0)
    if profiling_sample_errors > 0:
        errors.append({"step": "profiling_samples", "error": f"{profiling_sample_errors} profiling samples could not be removed"})
    if _execute_step(
        session,
        step_name="temporary_files",
        summary=summary,
        errors=errors,
        step=lambda: _cleanup_stale_temp_files(retention_policy=retention_policy),
    ):
        completed_steps += 1
    else:
        failed_steps += 1
    temporary_file_errors = int((summary.get("temporary_files") or {}).get("temporary_file_errors", 0) or 0)
    if temporary_file_errors > 0:
        errors.append({"step": "temporary_files", "error": f"{temporary_file_errors} temporary files could not be removed"})

    summary["errors"] = errors
    summary["steps_total"] = total_steps
    summary["steps_completed"] = completed_steps
    summary["steps_failed"] = failed_steps
    summary["finished_at"] = _now().isoformat()
    summary["warnings_count"] = len(errors) - failed_steps if len(errors) > failed_steps else 0
    summary["status"] = "success" if not errors else ("failed" if failed_steps == total_steps else "partial")
    run.status = summary["status"]
    run.finished_at = _now()
    run.summary_json = summary
    run.error_message = None if not errors else "; ".join(f"{item['step']}: {item['error']}" for item in errors)[:2000]
    session.add(run)
    session.commit()
    return summary


__all__ = ["run_retention_cleanup_job"]
