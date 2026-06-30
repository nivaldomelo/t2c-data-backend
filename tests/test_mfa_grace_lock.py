from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

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
    monkeypatch.setattr(settings, "mfa_grace_logins", 3)
    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)


def _login(client: TestClient):
    return client.post("/api/v1/auth/login", json={"email": "new@user.com", "password": "secret123"})


def test_mfa_grace_then_lock(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        db.add(User(email="new@user.com", name="New", full_name="New", password_hash=hash_password("secret123"), is_active=True))
        db.commit()

    client = _client(monkeypatch, session_factory)
    try:
        # 3 grace logins, each succeeds with a decreasing remaining count.
        remaining_seen = []
        for _ in range(3):
            resp = _login(client)
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["mfa_enabled"] is False
            assert body["mfa_warning"]
            remaining_seen.append(body["mfa_grace_remaining"])
        assert remaining_seen == [2, 1, 0]

        # 4th attempt without MFA -> locked.
        locked = _login(client)
        assert locked.status_code == 403
        assert "bloquead" in locked.json()["detail"].lower()

        # Subsequent attempts stay blocked.
        assert _login(client).status_code == 403
    finally:
        app.dependency_overrides.clear()

    with session_factory() as db:
        user = db.scalar(select(User).where(User.email == "new@user.com"))
        assert user.mfa_locked is True
        assert user.mfa_grace_logins_used == 3


def test_admin_unlock_restores_grace(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        db.add(
            User(
                email="new@user.com",
                name="New",
                full_name="New",
                password_hash=hash_password("secret123"),
                is_active=True,
                mfa_locked=True,
                mfa_grace_logins_used=3,
            )
        )
        db.commit()

    client = _client(monkeypatch, session_factory)
    try:
        assert _login(client).status_code == 403  # locked

        # Simulate the admin unlock action (same field reset as the endpoint).
        with session_factory() as db:
            user = db.scalar(select(User).where(User.email == "new@user.com"))
            user.mfa_locked = False
            user.mfa_locked_at = None
            user.mfa_grace_logins_used = 0
            db.add(user)
            db.commit()

        resp = _login(client)
        assert resp.status_code == 200
        assert resp.json()["mfa_grace_remaining"] == 2
    finally:
        app.dependency_overrides.clear()
