from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.config import settings
from t2c_data.core.db import get_db
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.governance import GovernanceSettings


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session_factory():
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
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            INSERT INTO t2c_data.governance_settings (id, legacy_api_cutoff_window_days)
            VALUES (1, 30)
            """
        )
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def test_legacy_ping_is_gone(monkeypatch) -> None:
    session_factory = _build_session_factory()

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    client = TestClient(app)
    response = client.get("/api/ping")

    assert response.status_code == 410
    assert response.json()["detail"] == "Esta rota legada foi removida. Use /api/v1/ping."
    assert response.json()["canonical_path"] == "/api/v1/ping"
    assert response.headers.get("Deprecation") == "true"
    assert response.headers.get("Sunset") == "Wed, 30 Sep 2026 23:59:59 GMT"
    assert response.headers.get("X-API-Canonical-Path") == "/api/v1/ping"
    assert "</api/v1/ping>; rel=\"successor-version\"" == response.headers.get("Link")


def test_v1_ping_does_not_expose_legacy_deprecation_headers() -> None:
    client = TestClient(app)

    response = client.get("/api/v1/ping")

    assert response.status_code == 200
    assert response.json() == {"message": "pong"}
    assert response.headers.get("Deprecation") is None
    assert response.headers.get("X-API-Canonical-Path") is None


def test_v1_ready_works_without_legacy_headers(monkeypatch) -> None:
    session_factory = _build_session_factory()

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    client = TestClient(app)
    response = client.get("/api/v1/ready")

    assert response.status_code in {200, 503}
    assert response.json()["status"] in {"ready", "not_ready"}
    assert response.headers.get("Deprecation") is None
    assert response.headers.get("X-API-Canonical-Path") is None


def test_legacy_ready_is_gone(monkeypatch) -> None:
    session_factory = _build_session_factory()

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    client = TestClient(app)
    response = client.get("/api/ready")

    assert response.status_code == 410
    assert response.json()["detail"] == "Esta rota legada foi removida. Use /api/v1/ready."
    assert response.json()["canonical_path"] == "/api/v1/ready"
    assert response.headers.get("Deprecation") == "true"
    assert response.headers.get("X-API-Canonical-Path") == "/api/v1/ready"
    assert "</api/v1/ready>; rel=\"successor-version\"" == response.headers.get("Link")


def test_legacy_api_root_is_gone(monkeypatch) -> None:
    session_factory = _build_session_factory()

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    client = TestClient(app)
    response = client.get("/api")

    assert response.status_code == 410
    assert response.json()["canonical_path"] == "/api/v1"
    assert response.headers.get("X-API-Canonical-Path") == "/api/v1"


def test_legacy_auth_login_is_gone(monkeypatch) -> None:
    session_factory = _build_session_factory()

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    client = TestClient(app)
    response = client.post(
        "/api/auth/login",
        json={"email": "admin@andromeda.com", "password": "admin123"},
    )

    assert response.status_code == 410
    assert response.json()["detail"] == "Esta rota legada foi removida. Use /api/v1/auth/login."
    assert response.json()["canonical_path"] == "/api/v1/auth/login"
    assert response.headers.get("X-API-Canonical-Path") == "/api/v1/auth/login"


def test_legacy_datasources_and_scan_runs_are_gone(monkeypatch) -> None:
    session_factory = _build_session_factory()

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)

        datasources_response = client.get("/api/datasources")
        scan_runs_response = client.get("/api/scan-runs")
        datasources_v1_response = client.get("/api/v1/datasources")
        scan_runs_v1_response = client.get("/api/v1/scan-runs")
    finally:
        app.dependency_overrides.clear()

    assert datasources_response.status_code == 410
    assert datasources_response.json()["detail"] == "Esta rota legada foi removida. Use /api/v1/datasources."
    assert datasources_response.json()["canonical_path"] == "/api/v1/datasources"
    assert datasources_response.headers.get("X-API-Canonical-Path") == "/api/v1/datasources"

    assert scan_runs_response.status_code == 410
    assert scan_runs_response.json()["detail"] == "Esta rota legada foi removida. Use /api/v1/scan-runs."
    assert scan_runs_response.json()["canonical_path"] == "/api/v1/scan-runs"
    assert scan_runs_response.headers.get("X-API-Canonical-Path") == "/api/v1/scan-runs"

    assert datasources_v1_response.status_code != 410
    assert scan_runs_v1_response.status_code != 410
