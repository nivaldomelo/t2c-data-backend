from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from hashlib import blake2b
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.features.export_security import audit_export_event
from t2c_data.features.platform.jobs import enqueue_integration_job, finish_integration_job_record
from t2c_data.models.auth import User
from t2c_data.models.platform import IntegrationSyncJob
from t2c_data.schemas.platform import IntegrationSyncJobOut
EXPORT_SOURCE = "export"
EXPORT_JOB_TYPES = {
    "privacy_access.csv",
    "privacy_access.events.csv",
    "governance.owners.csv",
    "certification.queue.csv",
    "certification.events.csv",
    "audit.history.csv",
    "audit.history.xlsx",
    "admin.access_log_archive.csv",
    "admin.access_log_archive.xlsx",
    "admin.audit_log_archive.csv",
    "admin.audit_log_archive.xlsx",
}


@dataclass(frozen=True)
class ExportArtifactResult:
    payload: bytes
    filename: str
    content_type: str
    row_count: int
    truncated: bool
    export_format: str


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json_text(value: dict[str, Any] | list | None) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, default=str)


def _parse_date(value: Any) -> Any:
    if isinstance(value, str) and value:
        try:
            from datetime import date as _date

            return _date.fromisoformat(value)
        except ValueError:
            return value
    return value


def _parse_datetime(value: Any) -> Any:
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def build_export_job_key(*, job_type: str, payload_json: dict[str, Any] | list | None) -> str:
    digest = blake2b(_json_text(payload_json).encode("utf-8"), digest_size=8).hexdigest()
    return f"{EXPORT_SOURCE}:{job_type}:{digest}"


def build_export_public_id() -> str:
    return uuid4().hex


def build_export_storage_path(public_id: str, filename: str) -> Path:
    safe_name = Path(filename).name or "export.bin"
    return Path(tempfile.gettempdir()) / "andromeda_exports" / public_id / safe_name


def serialize_export_job(job: IntegrationSyncJob, *, request: Request | None = None) -> IntegrationSyncJobOut:
    payload = IntegrationSyncJobOut.model_validate(job, from_attributes=True).model_dump()
    if job.artifact_public_id:
        payload["export_status_href"] = f"/api/v1/exports/{job.artifact_public_id}"
        payload["export_download_href"] = (
            f"/api/v1/exports/{job.artifact_public_id}/download" if job.artifact_storage_path and job.artifact_expires_at and job.artifact_expires_at > _now() else None
        )
        payload["export_download_available"] = bool(
            job.artifact_storage_path and job.artifact_expires_at and job.artifact_expires_at > _now()
        )
    if request is not None and job.artifact_public_id:
        payload["export_status_href"] = str(request.url_for("export_job_status", artifact_public_id=job.artifact_public_id))
        if job.artifact_storage_path and job.artifact_expires_at and job.artifact_expires_at > _now():
            payload["export_download_href"] = str(
                request.url_for("download_export_artifact", artifact_public_id=job.artifact_public_id)
            )
    return IntegrationSyncJobOut.model_validate(payload)


def enqueue_export_job(
    session: Session,
    *,
    job_type: str,
    target_name: str | None = None,
    requested_by_user_id: int | None = None,
    correlation_id: str | None = None,
    payload_json: dict[str, Any] | list | None = None,
    context_json: dict[str, Any] | list | None = None,
) -> IntegrationSyncJob:
    public_id = build_export_public_id()
    job_key = build_export_job_key(job_type=job_type, payload_json=payload_json)
    job = enqueue_integration_job(
        session,
        source=EXPORT_SOURCE,
        job_type=job_type,
        target_type="export",
        target_name=target_name,
        requested_by_user_id=requested_by_user_id,
        correlation_id=correlation_id,
        payload_json=payload_json,
        context_json={
            **(context_json if isinstance(context_json, dict) else {}),
            "export_public_id": public_id,
        },
        job_key=job_key,
        artifact_public_id=public_id,
    )
    if job is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Não foi possível enfileirar a exportação.")
    return job


def _write_artifact_file(job: IntegrationSyncJob, artifact: ExportArtifactResult) -> tuple[str, int]:
    public_id = job.artifact_public_id or build_export_public_id()
    storage_path = build_export_storage_path(public_id, artifact.filename)
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_bytes(artifact.payload)
    return str(storage_path), storage_path.stat().st_size


def finish_export_job(
    session: Session,
    job: IntegrationSyncJob,
    *,
    artifact: ExportArtifactResult,
    context_json: dict[str, Any] | list | None = None,
    requested_by_user_id: int | None = None,
) -> IntegrationSyncJob:
    storage_path, size_bytes = _write_artifact_file(job, artifact)
    expires_at = _now() + timedelta(hours=max(int(getattr(settings, "export_file_ttl_hours", 1) or 1), 1))
    job.artifact_filename = artifact.filename
    job.artifact_content_type = artifact.content_type
    job.artifact_storage_path = storage_path
    job.artifact_available_at = _now()
    job.artifact_expires_at = expires_at
    job.artifact_size_bytes = size_bytes
    job.artifact_download_count = int(job.artifact_download_count or 0)
    if context_json is not None:
        job.context_json = context_json
    result_summary = dict(job.result_summary_json or {}) if isinstance(job.result_summary_json, dict) else {}
    result_summary.update(
        {
            "export_format": artifact.export_format,
            "filename": artifact.filename,
            "row_count": artifact.row_count,
            "truncated": artifact.truncated,
            "artifact_available_at": _now().isoformat(),
            "artifact_expires_at": expires_at.isoformat(),
            "artifact_size_bytes": size_bytes,
        }
    )
    job.result_summary_json = result_summary
    return finish_integration_job_record(
        session,
        job,
        status="success",
        records_processed=artifact.row_count,
        context_json=job.context_json,
        result_summary_json=job.result_summary_json,
        progress_pct=100.0,
    ) or job


def audit_export_download(
    session: Session,
    *,
    request: Request,
    current_user: User,
    job: IntegrationSyncJob,
) -> None:
    audit_export_event(
        session,
        request=request,
        current_user=current_user,
        action=f"{job.job_type}.download",
        entity_type="export_artifact",
        source_module=job.source,
        row_count=int(job.records_processed or 0),
        truncated=False,
        limit=max(int(job.records_processed or 1), 1),
        export_format=str((job.result_summary_json or {}).get("export_format") or "unknown"),
        extra_metadata={
            "phase": "download",
            "job_id": job.id,
            "artifact_public_id": job.artifact_public_id,
            "artifact_expires_at": job.artifact_expires_at.isoformat() if job.artifact_expires_at else None,
        },
    )


def register_export_request_audit(
    session: Session,
    *,
    request: Request,
    current_user: User,
    job: IntegrationSyncJob,
    action: str,
    entity_type: str,
    source_module: str,
    export_format: str,
    filters: dict[str, Any],
) -> None:
    audit_export_event(
        session,
        request=request,
        current_user=current_user,
        action=action,
        entity_type=entity_type,
        source_module=source_module,
        row_count=int((job.result_summary_json or {}).get("row_count") or 0),
        truncated=bool((job.result_summary_json or {}).get("truncated") or False),
        limit=max(int(settings.export_sync_max_rows or 1), 1),
        export_format=export_format,
        extra_metadata={
            "phase": "queued",
            "job_id": job.id,
            "artifact_public_id": job.artifact_public_id,
            "export_download_ttl_minutes": settings.export_download_ttl_minutes,
            "filters": filters,
        },
    )


def load_export_job_by_public_id(session: Session, public_id: str) -> IntegrationSyncJob | None:
    return session.scalar(select(IntegrationSyncJob).where(IntegrationSyncJob.artifact_public_id == public_id).limit(1))


def cleanup_expired_export_artifacts(session: Session) -> int:
    now = _now()
    rows = session.scalars(
        select(IntegrationSyncJob).where(
            IntegrationSyncJob.artifact_expires_at.is_not(None),
            IntegrationSyncJob.artifact_expires_at <= now,
            IntegrationSyncJob.artifact_storage_path.is_not(None),
        )
    ).all()
    deleted = 0
    for job in rows:
        path = Path(job.artifact_storage_path or "")
        if path.exists():
            try:
                path.unlink()
                deleted += 1
            except OSError:
                continue
        try:
            if path.parent.exists() and not any(path.parent.iterdir()):
                path.parent.rmdir()
        except OSError:
            pass
    return deleted


def process_export_job(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
    payload = dict(job.payload_json or {}) if isinstance(job.payload_json, dict) else {}
    export_format = str(payload.get("export_format") or "").strip().lower()
    from t2c_data.models.auth import User

    if job.job_type in {"privacy_access.events.csv", "certification.events.csv"}:
        payload["date_from"] = _parse_date(payload.get("date_from"))
        payload["date_to"] = _parse_date(payload.get("date_to"))
    if job.job_type == "audit.history.csv" or job.job_type == "audit.history.xlsx":
        payload["date_from"] = _parse_datetime(payload.get("date_from"))
        payload["date_to"] = _parse_datetime(payload.get("date_to"))

    requested_by_user = session.get(User, job.requested_by_user_id) if job.requested_by_user_id else None
    if requested_by_user is None:
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error="Usuário solicitante não encontrado.",
            context_json={"error": "requested_by_user_not_found"},
            result_summary_json={"error": "requested_by_user_not_found"},
            progress_pct=100.0,
        ) or job

    try:
        if job.job_type == "privacy_access.csv":
            from t2c_data.api.privacy_access import build_privacy_access_export_artifact

            artifact = build_privacy_access_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "privacy_access.events.csv":
            from t2c_data.api.privacy_access import build_privacy_review_events_export_artifact

            artifact = build_privacy_review_events_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "governance.owners.csv":
            from t2c_data.api.governance import build_ownership_export_artifact

            artifact = build_ownership_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "certification.queue.csv":
            from t2c_data.api.certification import build_certification_queue_export_artifact

            artifact = build_certification_queue_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "certification.events.csv":
            from t2c_data.api.certification import build_certification_events_export_artifact

            artifact = build_certification_events_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "audit.history.csv":
            from t2c_data.api.audit import build_audit_history_csv_export_artifact

            artifact = build_audit_history_csv_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "audit.history.xlsx":
            from t2c_data.api.audit import build_audit_history_xlsx_export_artifact

            artifact = build_audit_history_xlsx_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "admin.access_log_archive.csv":
            from t2c_data.api.admin_routes.governance import build_access_log_archive_csv_export_artifact

            artifact = build_access_log_archive_csv_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "admin.access_log_archive.xlsx":
            from t2c_data.api.admin_routes.governance import build_access_log_archive_xlsx_export_artifact

            artifact = build_access_log_archive_xlsx_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "admin.audit_log_archive.csv":
            from t2c_data.api.admin_routes.governance import build_audit_log_archive_csv_export_artifact

            artifact = build_audit_log_archive_csv_export_artifact(session, current_user=requested_by_user, **payload)
        elif job.job_type == "admin.audit_log_archive.xlsx":
            from t2c_data.api.admin_routes.governance import build_audit_log_archive_xlsx_export_artifact

            artifact = build_audit_log_archive_xlsx_export_artifact(session, current_user=requested_by_user, **payload)
        else:
            return finish_integration_job_record(
                session,
                job,
                status="failed",
                error=f"Unsupported export job: {job.job_type}",
                context_json={"job_type": job.job_type, "payload": payload},
                result_summary_json={"error": "unsupported_export_job", "job_type": job.job_type},
                progress_pct=100.0,
            ) or job
    except Exception as exc:  # noqa: BLE001
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error=str(exc),
            context_json={"job_type": job.job_type, "payload": payload, "error": str(exc)},
            result_summary_json={"error": str(exc), "job_type": job.job_type},
            progress_pct=100.0,
        ) or job

    return finish_export_job(
        session,
        job,
        artifact=artifact,
        context_json={
            **(job.context_json if isinstance(job.context_json, dict) else {}),
            "job_type": job.job_type,
            "export_format": export_format or artifact.export_format,
        },
    )


def export_download_response_context(job: IntegrationSyncJob) -> dict[str, Any]:
    return {
        "export_status_href": f"/api/v1/exports/{job.artifact_public_id}" if job.artifact_public_id else None,
        "export_download_href": f"/api/v1/exports/{job.artifact_public_id}/download" if job.artifact_public_id and job.artifact_storage_path and job.artifact_expires_at and job.artifact_expires_at > _now() else None,
        "export_download_available": bool(job.artifact_public_id and job.artifact_storage_path and job.artifact_expires_at and job.artifact_expires_at > _now()),
    }
