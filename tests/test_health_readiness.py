from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import Response
from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api import system as system_api
from t2c_data.core.config import settings
from t2c_data.features.platform.jobs import enqueue_integration_job
from t2c_data.features.platform.worker_health import (
    build_worker_heartbeat_context,
    heartbeat_worker,
)
from t2c_data.models import Base

if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    session = SessionLocal()
    session.execute(text("CREATE TABLE IF NOT EXISTS t2c_data.alembic_version (version_num VARCHAR(32) NOT NULL)"))
    session.execute(text("DELETE FROM t2c_data.alembic_version"))
    session.execute(text("INSERT INTO t2c_data.alembic_version (version_num) VALUES ('worker-ready-test')"))
    session.commit()
    return session


def _set_healthy_defaults(monkeypatch) -> None:
    monkeypatch.setattr(system_api, "audit_plaintext_secrets", lambda db, fix=False: [])
    healthy_scheduler = lambda db: {"health": "healthy", "mode": "worker"}  # noqa: E731
    monkeypatch.setattr(system_api, "platform_scheduler_status_snapshot", healthy_scheduler)
    monkeypatch.setattr(system_api, "dq_scheduler_status_snapshot", healthy_scheduler)
    monkeypatch.setattr(system_api, "dq_profiling_scheduler_status_snapshot", healthy_scheduler)
    monkeypatch.setattr(system_api, "datasource_scheduler_status_snapshot", healthy_scheduler)
    monkeypatch.setattr(settings, "platform_scheduler_mode", "worker")
    monkeypatch.setattr(settings, "dq_scheduler_mode", "worker")
    monkeypatch.setattr(settings, "dq_profiling_scheduler_mode", "worker")
    monkeypatch.setattr(settings, "datasource_scan_scheduler_mode", "worker")
    monkeypatch.setattr(settings, "data_lake_scan_scheduler_mode", "worker")


def _worker_check(payload: dict[str, object]) -> dict[str, object]:
    return next(check for check in payload["checks"] if check.get("name") == "worker_health")


def test_health_endpoint_payload() -> None:
    payload = system_api.health()
    assert payload["status"] == "ok"
    assert "timestamp" in payload


def test_readiness_reports_checks(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    payload = system_api.readiness(Response(), session)

    assert payload["status"] in {"ready", "not_ready"}
    assert isinstance(payload["checks"], list)
    assert any(check.get("name") == "database" for check in payload["checks"])
    assert any(check.get("name") == "schema" for check in payload["checks"])
    assert any(check.get("name") == "migrations" for check in payload["checks"])
    assert any(check.get("name") == "secret_store" for check in payload["checks"])
    assert any(check.get("name") == "config" for check in payload["checks"])
    assert any(check.get("name") == "plaintext_secrets" for check in payload["checks"])
    assert any(check.get("name") == "worker_health" for check in payload["checks"])


def test_detailed_readiness_reports_operational_checks(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    payload = system_api.readiness_detailed(Response(), session)

    assert payload["mode"] == "detailed"
    assert any(check.get("name") == "job_queue" for check in payload["checks"])
    assert any(check.get("name") == "datasource_scanner" for check in payload["checks"])
    assert any(check.get("name") == "dq_engine" for check in payload["checks"])
    assert any(check.get("name") == "legacy_api_surface" for check in payload["checks"])
    assert any(check.get("name") == "critical_integrations" for check in payload["checks"])
    assert any(check.get("name") == "worker_health" for check in payload["checks"])


def test_system_module_imports() -> None:
    import importlib

    module = importlib.import_module("t2c_data.api.system")
    assert hasattr(module, "readiness")


def test_readiness_reports_embedded_scheduler_as_unsafe_outside_dev(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings, "dq_scheduler_mode", "embedded")

    payload = system_api.readiness(Response(), session)
    config_check = next(check for check in payload["checks"] if check.get("name") == "config")

    assert config_check["status"] == "error"
    assert config_check["detail"] == "Embedded schedulers are not allowed outside dev/test. Use worker mode."


def test_readiness_is_ready_with_recent_worker_in_prod(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    monkeypatch.setattr(settings, "env", "prod")
    heartbeat_worker(session, build_worker_heartbeat_context(), status="idle")

    response = Response()
    payload = system_api.readiness(response, session)
    check = _worker_check(payload)

    assert payload["status"] == "ready"
    assert response.status_code == 200
    assert check["status"] == "ok"
    assert check["worker_required"] is True
    assert check["worker_status"] == "idle"
    assert check["worker_age_seconds"] is not None


def test_readiness_fails_without_worker_in_prod(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    monkeypatch.setattr(settings, "env", "prod")

    response = Response()
    payload = system_api.readiness(response, session)
    check = _worker_check(payload)

    assert payload["status"] == "not_ready"
    assert response.status_code == 503
    assert check["status"] == "error"
    assert check["worker_required"] is True
    assert check["detail"] == "No worker heartbeat registered."


def test_readiness_warns_without_worker_in_dev(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    monkeypatch.setattr(settings, "env", "dev")

    response = Response()
    payload = system_api.readiness(response, session)
    check = _worker_check(payload)

    assert payload["status"] == "ready"
    assert response.status_code == 200
    assert check["status"] == "warning"
    assert check["worker_required"] is False


def test_readiness_detects_stale_worker(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings, "platform_worker_heartbeat_grace_seconds", 30)
    heartbeat = heartbeat_worker(session, build_worker_heartbeat_context(), status="idle")
    assert heartbeat is not None
    heartbeat.last_seen_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    session.add(heartbeat)
    session.commit()

    response = Response()
    payload = system_api.readiness(response, session)
    check = _worker_check(payload)

    assert payload["status"] == "not_ready"
    assert response.status_code == 503
    assert check["status"] == "error"
    assert check["detail"] == "Worker heartbeat is stale."
    assert int(check["worker_age_seconds"]) >= 120


def test_readiness_alerts_on_queued_jobs_without_worker(monkeypatch) -> None:
    _set_healthy_defaults(monkeypatch)
    session = _build_session()
    monkeypatch.setattr(settings, "env", "prod")
    job = enqueue_integration_job(
        session,
        source="datasource",
        job_type="scan",
        target_type="datasource",
        target_id=17,
        target_name="warehouse",
        trigger_mode="manual",
        payload_json={"datasource_id": 17},
        context_json={"datasource_id": 17},
    )
    assert job is not None

    response = Response()
    payload = system_api.readiness(response, session)
    check = _worker_check(payload)

    assert payload["status"] == "not_ready"
    assert response.status_code == 503
    assert check["status"] == "error"
    assert check["queued_jobs_count"] == 1
    assert check["detail"] == "No worker heartbeat registered."
