from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.responses import FileResponse

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.api.exports import download_export_artifact, export_job_status
from t2c_data.features import export_jobs


def _build_job(*, public_id: str = "pub123", expires_in_minutes: int = 30, requested_by_user_id: int = 7):
    storage_dir = Path(tempfile.gettempdir()) / "andromeda_exports_test"
    storage_dir.mkdir(parents=True, exist_ok=True)
    file_path = storage_dir / f"{public_id}.csv"
    file_path.write_text("id,name\n1,teste\n", encoding="utf-8")
    return SimpleNamespace(
        id=12,
        job_key="export:audit.history.csv:abc123",
        source="export",
        job_type="audit.history.csv",
        target_type="export",
        target_id=None,
        target_name=None,
        trigger_mode="manual",
        status="success",
        queued_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        started_at=datetime.now(timezone.utc) - timedelta(minutes=4),
        finished_at=datetime.now(timezone.utc) - timedelta(minutes=3),
        next_expected_run_at=None,
        records_processed=10,
        progress_pct=100.0,
        correlation_id="corr-1",
        requested_by_user_id=requested_by_user_id,
        error=None,
        context_json={},
        payload_json={},
        result_summary_json={"export_format": "csv"},
        artifact_public_id=public_id,
        artifact_filename=f"{public_id}.csv",
        artifact_content_type="text/csv; charset=utf-8",
        artifact_storage_path=str(file_path),
        artifact_available_at=datetime.now(timezone.utc) - timedelta(minutes=3),
        artifact_expires_at=datetime.now(timezone.utc) + timedelta(minutes=expires_in_minutes),
        artifact_size_bytes=file_path.stat().st_size,
        artifact_download_count=0,
        artifact_last_downloaded_at=None,
        created_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        updated_at=datetime.now(timezone.utc) - timedelta(minutes=3),
    )


def _owner_user():
    return SimpleNamespace(id=7, roles=[SimpleNamespace(name="editor", permissions=[])])


def _admin_user():
    return SimpleNamespace(id=2, roles=[SimpleNamespace(name="admin", permissions=[])])


class _FakeDB:
    def add(self, _obj) -> None:  # noqa: ANN001
        return None

    def commit(self) -> None:
        return None

    def refresh(self, _obj) -> None:  # noqa: ANN001
        return None


def test_export_job_status_shows_download_href_for_owner(monkeypatch) -> None:
    job = _build_job()
    monkeypatch.setattr("t2c_data.api.exports.load_export_job_by_public_id", lambda db, public_id: job)

    payload = export_job_status("pub123", db=SimpleNamespace(), current_user=_owner_user())

    assert payload.artifact_public_id == "pub123"
    assert payload.export_status_href.endswith("/api/v1/exports/pub123")
    assert payload.export_download_href.endswith("/api/v1/exports/pub123/download")
    assert payload.export_download_available is True


def test_export_job_status_denies_non_owner_non_admin(monkeypatch) -> None:
    job = _build_job(requested_by_user_id=99)
    monkeypatch.setattr("t2c_data.api.exports.load_export_job_by_public_id", lambda db, public_id: job)

    with pytest.raises(HTTPException) as excinfo:
        export_job_status("pub123", db=SimpleNamespace(), current_user=_owner_user())

    assert excinfo.value.status_code == 403


def test_download_export_artifact_returns_file_response_and_audits(monkeypatch) -> None:
    job = _build_job()
    captured: dict[str, object] = {}
    monkeypatch.setattr("t2c_data.api.exports.load_export_job_by_public_id", lambda db, public_id: job)
    monkeypatch.setattr("t2c_data.api.exports.audit_export_download", lambda *args, **kwargs: captured.update(kwargs))

    response = download_export_artifact(
        "pub123",
        request=SimpleNamespace(url=SimpleNamespace(path="/api/v1/exports/pub123/download"), method="GET", headers={}, state=SimpleNamespace(request_id="req-1"), client=SimpleNamespace(host="127.0.0.1")),
        db=_FakeDB(),
        current_user=_owner_user(),
    )

    assert isinstance(response, FileResponse)
    assert captured["job"] is job
    assert captured["current_user"].id == 7


def test_download_export_artifact_expires_and_removes_file(monkeypatch) -> None:
    job = _build_job(expires_in_minutes=-5)
    monkeypatch.setattr("t2c_data.api.exports.load_export_job_by_public_id", lambda db, public_id: job)

    with pytest.raises(HTTPException) as excinfo:
        download_export_artifact(
            "pub123",
            request=SimpleNamespace(url=SimpleNamespace(path="/api/v1/exports/pub123/download"), method="GET", headers={}, state=SimpleNamespace(request_id="req-1"), client=SimpleNamespace(host="127.0.0.1")),
            db=_FakeDB(),
            current_user=_owner_user(),
        )

    assert excinfo.value.status_code == 410
    assert not Path(job.artifact_storage_path).exists()
