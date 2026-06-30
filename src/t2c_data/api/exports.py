from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.features.export_jobs import audit_export_download, export_download_response_context, load_export_job_by_public_id, serialize_export_job
from t2c_data.models.auth import User
from t2c_data.schemas.platform import IntegrationSyncJobOut

router = APIRouter(prefix="/exports", tags=["exports"])


def _is_owner_or_admin(current_user: User, job_request_user_id: int | None) -> bool:
    if current_user.id == job_request_user_id:
        return True
    return is_admin_role(user_role_names(current_user))


def _job_file_path(job) -> Path | None:
    if not job.artifact_storage_path:
        return None
    return Path(job.artifact_storage_path)


@router.get("/{artifact_public_id}", response_model=IntegrationSyncJobOut, name="export_job_status")
def export_job_status(
    artifact_public_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner", "viewer")),
) -> IntegrationSyncJobOut:
    job = load_export_job_by_public_id(db, artifact_public_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Exportação não encontrada.")
    if not _is_owner_or_admin(current_user, job.requested_by_user_id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Você não tem permissão para ver esta exportação.")

    payload = serialize_export_job(job).model_dump()
    payload.update(export_download_response_context(job))
    if job.artifact_expires_at is not None and job.artifact_expires_at <= datetime.now(timezone.utc):
        payload["export_download_available"] = False
        payload["export_download_href"] = None
    return IntegrationSyncJobOut.model_validate(payload)


@router.get("/{artifact_public_id}/download", name="download_export_artifact")
def download_export_artifact(
    artifact_public_id: str,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner", "viewer")),
):
    job = load_export_job_by_public_id(db, artifact_public_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Exportação não encontrada.")
    if not _is_owner_or_admin(current_user, job.requested_by_user_id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Você não tem permissão para baixar esta exportação.")
    if job.artifact_storage_path is None or job.artifact_filename is None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A exportação ainda não está pronta para download.")
    if job.artifact_expires_at is not None and job.artifact_expires_at <= datetime.now(timezone.utc):
        path = _job_file_path(job)
        if path is not None and path.exists():
            try:
                path.unlink()
            except OSError:
                pass
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="O arquivo da exportação expirou.")

    path = _job_file_path(job)
    if path is None or not path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Arquivo da exportação não encontrado.")

    audit_export_download(
        db,
        request=request,
        current_user=current_user,
        job=job,
    )
    job.artifact_download_count = int(job.artifact_download_count or 0) + 1
    job.artifact_last_downloaded_at = datetime.now(timezone.utc)
    db.add(job)
    db.commit()
    db.refresh(job)
    media_type = job.artifact_content_type or "application/octet-stream"
    return FileResponse(
        path,
        media_type=media_type,
        filename=job.artifact_filename,
    )
