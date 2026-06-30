from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

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
from t2c_data.models.auth import Role, User, UserAccessEvent, UserSession
from t2c_data.models.audit import AccessLog
from t2c_data.services import user_activity_tracker as tracker


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
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def _install_test_db(session_factory):
    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db


def _bootstrap(monkeypatch, session_factory):
    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)
    monkeypatch.setattr(settings, "enable_db_seed", False)
    monkeypatch.setattr(tracker, "_SESSION_HEARTBEAT_MIN_SECONDS", 0)
    _install_test_db(session_factory)


def _create_user(db: Session, email: str, password: str = "admin123", *, admin_role: bool = False) -> User:
    user = User(
        email=email,
        name="Admin",
        full_name="Admin User",
        password_hash=hash_password(password),
        is_active=True,
    )
    db.add(user)
    if admin_role:
        role = db.scalar(select(Role).where(Role.name == "admin"))
        if role is None:
            role = Role(name="admin", description="Full permissions")
            db.add(role)
            db.flush()
        user.roles.append(role)
    db.commit()
    return user


def test_login_creates_audit_session(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        _create_user(db, "admin@andromeda.com", admin_role=True)

    _bootstrap(monkeypatch, session_factory)
    try:
        client = TestClient(app)
        response = client.post("/api/v1/auth/login", json={"email": "admin@andromeda.com", "password": "admin123"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    with session_factory() as db:
        session = db.scalar(select(UserSession).where(UserSession.user_id == 1))
        assert session is not None
        assert session.started_at is not None
        assert session.last_seen_at is not None
        assert session.expires_at is not None
        assert session.success is True
        assert session.mfa_used is False


def test_session_heartbeat_accepts_user_id_without_user_object(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        _create_user(db, "admin@andromeda.com", admin_role=True)

    _bootstrap(monkeypatch, session_factory)
    try:
        client = TestClient(app)
        login = client.post("/api/v1/auth/login", json={"email": "admin@andromeda.com", "password": "admin123"})
        token = login.json()["access_token"]
    finally:
        app.dependency_overrides.clear()

    with session_factory() as db:
        session = db.scalar(select(UserSession).where(UserSession.user_id == 1))
        assert session is not None
        previous_last_seen = session.last_seen_at
        updated = tracker.record_session_heartbeat(
            db,
            user=None,
            user_id=session.user_id,
            session_jti=session.jti,
            user_agent="Mozilla/5.0",
            ip_address="127.0.0.1",
            force=True,
        )
        assert updated is not None
        updated_last_seen = updated.last_seen_at
        db.commit()

    assert token is not None
    assert updated_last_seen is not None


def test_logout_ends_session(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        _create_user(db, "admin@andromeda.com", admin_role=True)

    _bootstrap(monkeypatch, session_factory)
    try:
        client = TestClient(app)
        login = client.post("/api/v1/auth/login", json={"email": "admin@andromeda.com", "password": "admin123"})
        token = login.json()["access_token"]
        response = client.post("/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    with session_factory() as db:
        session = db.scalar(select(UserSession).where(UserSession.user_id == 1))
        assert session is not None
        assert session.ended_at is not None
        assert session.duration_seconds is not None
        assert session.end_reason == "logout"


def test_page_view_event_redacts_sensitive_metadata(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        _create_user(db, "admin@andromeda.com", admin_role=True)

    _bootstrap(monkeypatch, session_factory)
    try:
        client = TestClient(app)
        login = client.post("/api/v1/auth/login", json={"email": "admin@andromeda.com", "password": "admin123"})
        token = login.json()["access_token"]
        response = client.post(
            "/api/v1/activity/page-view",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "route_path": "/admin/audit",
                "page_key": "admin_users",
                "event_type": "asset_view",
                "action": "inspect",
                "resource_type": "table",
                "resource_fqn": "local-andromeda.analytics.customers",
                "metadata": {"cpf": "12345678901", "note": "audit"},
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    with session_factory() as db:
        event_row = db.scalar(select(UserAccessEvent).where(UserAccessEvent.route_path == "/admin/audit"))
        assert event_row is not None
        assert event_row.metadata_json["note"] == "audit"
        assert event_row.metadata_json["cpf"] != "12345678901"


def test_admin_audit_summary_requires_permission(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        _create_user(db, "admin@andromeda.com", admin_role=True)

    _bootstrap(monkeypatch, session_factory)
    try:
        client = TestClient(app)
        login = client.post("/api/v1/auth/login", json={"email": "admin@andromeda.com", "password": "admin123"})
        token = login.json()["access_token"]
        response = client.get("/api/v1/admin/user-audit/summary", headers={"Authorization": f"Bearer {token}"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["open_sessions"] >= 1
    assert payload["users_active_today"] >= 1


def test_user_audit_export_audits_itself(monkeypatch) -> None:
    session_factory = _build_session_factory()
    with session_factory() as db:
        _create_user(db, "admin@andromeda.com", admin_role=True)

    _bootstrap(monkeypatch, session_factory)
    try:
        client = TestClient(app)
        login = client.post("/api/v1/auth/login", json={"email": "admin@andromeda.com", "password": "admin123"})
        token = login.json()["access_token"]
        response = client.post(
            "/api/v1/admin/user-audit/events/export.csv",
            headers={"Authorization": f"Bearer {token}"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    with session_factory() as db:
        exported = db.scalar(select(AccessLog).where(AccessLog.route == "/api/v1/admin/user-audit/events/export.csv"))
        assert exported is not None
