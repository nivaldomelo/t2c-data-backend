from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api import auth as auth_module
from t2c_data.core.config import settings
from t2c_data.core.db import get_db
from t2c_data.core.security import hash_password
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.auth import User


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
    def _attach_schema(dbapi_connection, _record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def _client(monkeypatch, session_factory) -> TestClient:
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
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *a, **k: None)
    monkeypatch.setattr(auth_module, "write_audit_log_sync", lambda *a, **k: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)
    monkeypatch.setattr(settings, "password_max_age_days", 90)
    monkeypatch.setattr(settings, "password_expiry_warning_days", 10)
    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)


def _add_user(session_factory, *, changed_days_ago: int) -> None:
    with session_factory() as db:
        db.add(
            User(
                email="u@x.com",
                name="U",
                full_name="U",
                password_hash=hash_password("secret123"),
                is_active=True,
                password_changed_at=datetime.now(timezone.utc) - timedelta(days=changed_days_ago),
            )
        )
        db.commit()


def _login(client: TestClient):
    return client.post("/api/v1/auth/login", json={"email": "u@x.com", "password": "secret123"})


def test_password_not_expired_logs_in(monkeypatch) -> None:
    session_factory = _build_session_factory()
    _add_user(session_factory, changed_days_ago=1)
    client = _client(monkeypatch, session_factory)
    try:
        resp = _login(client)
        assert resp.status_code == 200, resp.text
        assert resp.json()["password_warning"] is None
    finally:
        app.dependency_overrides.clear()


def test_password_warning_within_threshold(monkeypatch) -> None:
    session_factory = _build_session_factory()
    _add_user(session_factory, changed_days_ago=85)  # ~5 days left of 90
    client = _client(monkeypatch, session_factory)
    try:
        resp = _login(client)
        assert resp.status_code == 200
        body = resp.json()
        assert body["password_warning"]
        assert body["password_days_remaining"] is not None and body["password_days_remaining"] <= 10
    finally:
        app.dependency_overrides.clear()


def test_password_expired_blocks_login(monkeypatch) -> None:
    session_factory = _build_session_factory()
    _add_user(session_factory, changed_days_ago=91)
    client = _client(monkeypatch, session_factory)
    try:
        resp = _login(client)
        assert resp.status_code == 403
        assert "senha expirou" in resp.json()["detail"].lower() or "bloquead" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()
