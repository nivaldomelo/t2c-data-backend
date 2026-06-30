from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from t2c_data.features import export_jobs


def test_build_export_job_key_is_stable() -> None:
    payload = {"foo": "bar", "page": 1}
    assert export_jobs.build_export_job_key(job_type="audit.history.csv", payload_json=payload) == export_jobs.build_export_job_key(
        job_type="audit.history.csv",
        payload_json={"page": 1, "foo": "bar"},
    )


def test_export_job_types_include_admin_archive_exports() -> None:
    assert {"admin.access_log_archive.csv", "admin.access_log_archive.xlsx", "admin.audit_log_archive.csv", "admin.audit_log_archive.xlsx"}.issubset(export_jobs.EXPORT_JOB_TYPES)


def test_serialize_export_job_exposes_status_and_download_hrefs() -> None:
    job = SimpleNamespace(
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
        requested_by_user_id=7,
        error=None,
        context_json={},
        payload_json={},
        result_summary_json={"export_format": "csv"},
        artifact_public_id="pub123",
        artifact_filename="auditoria.csv",
        artifact_content_type="text/csv; charset=utf-8",
        artifact_storage_path="/tmp/andromeda_exports/pub123/auditoria.csv",
        artifact_available_at=datetime.now(timezone.utc) - timedelta(minutes=3),
        artifact_expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
        artifact_size_bytes=1234,
        artifact_download_count=0,
        artifact_last_downloaded_at=None,
        created_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        updated_at=datetime.now(timezone.utc) - timedelta(minutes=3),
    )
    request = SimpleNamespace(
        url_for=lambda name, **kwargs: f"http://backend/api/v1/exports/{kwargs['artifact_public_id']}" + ("/download" if name == "download_export_artifact" else ""),
    )

    payload = export_jobs.serialize_export_job(job, request=request)

    assert payload.export_status_href.endswith("/api/v1/exports/pub123")
    assert payload.export_download_href.endswith("/api/v1/exports/pub123/download")
    assert payload.export_download_available is True
    assert payload.artifact_filename == "auditoria.csv"


def test_enqueue_export_job_sets_public_id_and_job_key(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_enqueue_integration_job(session, **kwargs):  # noqa: ANN001
        captured.update(kwargs)
        return SimpleNamespace(id=99, artifact_public_id=kwargs["artifact_public_id"])

    monkeypatch.setattr(export_jobs, "enqueue_integration_job", fake_enqueue_integration_job)

    job = export_jobs.enqueue_export_job(
        SimpleNamespace(),
        job_type="audit.history.csv",
        requested_by_user_id=7,
        payload_json={"q": "foo"},
        context_json={"filters": {"q": "foo"}},
    )

    assert job.id == 99
    assert isinstance(captured["artifact_public_id"], str)
    assert str(captured["job_key"]).startswith("export:audit.history.csv:")
    assert captured["source"] == "export"


def test_process_export_job_dispatches_admin_archive_exports(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeSession:
        def get(self, model, identity):  # noqa: ANN001
            return SimpleNamespace(id=identity, email="admin@example.com", roles=[])

    def _fake_builder(session, *, current_user, **payload):  # noqa: ANN001
        captured["builder_payload"] = payload
        captured["builder_user"] = current_user
        return SimpleNamespace(
            payload=b"id,name\n1,teste\n",
            filename="access_log_archive.csv",
            content_type="text/csv; charset=utf-8",
            row_count=1,
            truncated=False,
            export_format="csv",
        )

    def _fake_finish_export_job(session, job, *, artifact, context_json=None, requested_by_user_id=None):  # noqa: ANN001
        captured["finish_job_type"] = job.job_type
        captured["finish_artifact_filename"] = artifact.filename
        return job

    monkeypatch.setattr("t2c_data.api.admin_routes.governance.build_access_log_archive_csv_export_artifact", _fake_builder)
    monkeypatch.setattr(export_jobs, "finish_export_job", _fake_finish_export_job)

    job = SimpleNamespace(
        job_type="admin.access_log_archive.csv",
        payload_json={"module_name": "admin", "api_version": "v1", "days": 30, "export_format": "csv"},
        requested_by_user_id=7,
        context_json={"filters": {"module_name": "admin"}},
    )

    result = export_jobs.process_export_job(_FakeSession(), job)

    assert result is job
    assert captured["finish_job_type"] == "admin.access_log_archive.csv"
    assert captured["finish_artifact_filename"] == "access_log_archive.csv"
    assert captured["builder_payload"]["module_name"] == "admin"
