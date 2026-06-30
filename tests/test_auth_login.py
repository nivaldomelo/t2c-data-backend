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
from t2c_data.core.security import generate_totp_code, hash_password
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.api import auth as auth_module


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


def test_login_success_does_not_duplicate_user_email_in_audit_kwargs(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    audit_calls = []

    def fake_write_audit_log_sync(_session, **kwargs):  # noqa: ANN001
        audit_calls.append(kwargs)

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(auth_module, "write_audit_log_sync", fake_write_audit_log_sync)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["token_type"] == "bearer"
    assert payload["access_token"]
    assert payload["roles"] == []
    assert payload["permissions"] == []
    assert len(audit_calls) == 1
    assert audit_calls[0]["user_email"] == "admin@andromeda.com"
    assert audit_calls[0]["action"] == "login_success"


def test_login_success_for_new_user(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="nivaldomelo@outlook.com.br",
                name="Nivaldo Melo",
                full_name="Nivaldo Melo",
                password_hash=hash_password("nova-senha-segura"),
                is_active=True,
            )
        )
        db.commit()

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
        response = client.post(
            "/api/v1/auth/login",
            json={"email": "nivaldomelo@outlook.com.br", "password": "nova-senha-segura"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["token_type"] == "bearer"
    assert payload["access_token"]
    assert payload["roles"] == []
    assert payload["permissions"] == []


def test_login_failure_still_returns_401(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

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
        response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "wrong-password"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid credentials"


def test_login_accepts_email_case_insensitive(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    audit_calls = []

    def fake_write_audit_log_sync(_session, **kwargs):  # noqa: ANN001
        audit_calls.append(kwargs)

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(auth_module, "write_audit_log_sync", fake_write_audit_log_sync)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        response = client.post(
            "/api/v1/auth/login",
            json={"email": "Admin@Andromeda.Com", "password": "admin123"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["access_token"]
    assert len(audit_calls) == 1
    assert audit_calls[0]["user_email"] == "admin@andromeda.com"


def test_change_password_requires_stronger_policy(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

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
        login_response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123"},
        )
        token = login_response.json()["access_token"]
        change_response = client.post(
            "/api/v1/me/change-password",
            headers={"Authorization": f"Bearer {token}"},
            json={"current_password": "admin123", "new_password": "SomenteLetras"},
        )
    finally:
        app.dependency_overrides.clear()

    assert login_response.status_code == 200
    assert change_response.status_code == 400
    assert change_response.json()["detail"] == "New password must have at least 12 chars and 3 of 4 character types"


def test_logout_revokes_current_session(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

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
        login_response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123"},
        )
        token = login_response.json()["access_token"]
        logout_response = client.post("/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"})
        profile_response = client.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"})
    finally:
        app.dependency_overrides.clear()

    assert login_response.status_code == 200
    assert logout_response.status_code == 200
    assert logout_response.json()["ok"] is True
    assert profile_response.status_code == 401
    assert profile_response.json()["detail"] == "Token expired or invalid"


def test_change_password_invalidates_previous_token(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

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
        login_response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123"},
        )
        token = login_response.json()["access_token"]
        change_response = client.post(
            "/api/v1/me/change-password",
            headers={"Authorization": f"Bearer {token}"},
            json={"current_password": "admin123", "new_password": "NovaSenha2026!"},
        )
        profile_response = client.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"})
    finally:
        app.dependency_overrides.clear()

    assert login_response.status_code == 200
    assert change_response.status_code == 200
    assert profile_response.status_code == 401
    assert profile_response.json()["detail"] == "Token expired or invalid"


def test_change_password_writes_sensitive_audit_log(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    audit_calls = []

    def fake_write_audit_log_sync(_session, **kwargs):  # noqa: ANN001
        audit_calls.append(kwargs)

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr("t2c_data.api.me.write_audit_log_sync", fake_write_audit_log_sync)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        login_response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123"},
        )
        token = login_response.json()["access_token"]
        change_response = client.post(
            "/api/v1/me/change-password",
            headers={"Authorization": f"Bearer {token}"},
            json={"current_password": "admin123", "new_password": "NovaSenha2026!"},
        )
    finally:
        app.dependency_overrides.clear()

    assert login_response.status_code == 200
    assert change_response.status_code == 200
    assert audit_calls
    payload = audit_calls[0]
    assert payload["action"] == "password.changed"
    assert payload["entity_type"] == "user"
    assert payload["is_sensitive_change"] is True
    assert payload["sensitive_category"] == "credential"
    assert payload["metadata"]["password_rotated"] is True


def test_mfa_setup_verify_and_login_requires_code(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        db.add(
            User(
                email="admin@andromeda.com",
                name="Admin",
                full_name="Admin User",
                password_hash=hash_password("admin123"),
                is_active=True,
            )
        )
        db.commit()

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
        login_response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123"},
        )
        token = login_response.json()["access_token"]
        setup_response = client.post("/api/v1/me/mfa/setup", headers={"Authorization": f"Bearer {token}"})
        setup_payload = setup_response.json()
        code = generate_totp_code(setup_payload["manual_secret"])
        verify_response = client.post(
            "/api/v1/me/mfa/verify",
            headers={"Authorization": f"Bearer {token}"},
            json={"code": code},
        )
        missing_code_login = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123"},
        )
        valid_login = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@andromeda.com", "password": "admin123", "mfa_code": code},
        )
    finally:
        app.dependency_overrides.clear()

    assert setup_response.status_code == 200
    assert setup_payload["setup_pending"] is True
    assert setup_payload["manual_secret"]
    assert verify_response.status_code == 200
    assert verify_response.json()["enabled"] is True
    assert missing_code_login.status_code == 401
    assert missing_code_login.json()["detail"] == "MFA required"
    assert valid_login.status_code == 200
    assert valid_login.json()["access_token"]
