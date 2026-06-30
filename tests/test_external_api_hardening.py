from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi.testclient import TestClient
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.db import get_db
from t2c_data.core.config import settings
from t2c_data.features.platform.api_keys import create_api_key
from t2c_data.features.platform.api_keys import ensure_scopes, resolve_api_key_from_token
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.tag import Tag


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


def _override_get_db(session_factory):
    def _dependency():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    return _dependency


def _prepare_client(monkeypatch, session_factory):
    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)
    monkeypatch.setattr(settings, "external_api_rate_limit_enabled", False)
    app.dependency_overrides[get_db] = _override_get_db(session_factory)
    return TestClient(app)


def _create_api_key(db: Session, *, scopes: list[str], allowed_ips: list[str] | None = None):
    key, token = create_api_key(
        db,
        name="External API",
        description="Integration test key",
        scopes=scopes,
        environment="shared",
        allowed_ips=allowed_ips or [],
        status_value="active",
        expires_at=None,
        expires_in_days=None,
        created_by=None,
    )
    db.commit()
    return key, token


def test_api_key_expiration_and_revocation_are_enforced(monkeypatch) -> None:
    session_factory = _build_session_factory()
    client = _prepare_client(monkeypatch, session_factory)

    with session_factory() as db:
        expired_key, expired_token = _create_api_key(db, scopes=["tags.read"])
        expired_key.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        db.commit()
        revoked_key, revoked_token = _create_api_key(db, scopes=["tags.read"])
        revoked_key.status = "revoked"
        db.commit()

    try:
        expired_response = client.get("/api/v1/external/ping", headers={"X-API-Key": expired_token})
        revoked_response = client.get("/api/v1/external/ping", headers={"X-API-Key": revoked_token})
    finally:
        app.dependency_overrides.clear()

    # Messages are intentionally generic (anti-enumeration): expired/revoked/not-found/bad-secret
    # all return the same detail so the response is not an oracle for key existence/state.
    assert expired_response.status_code == 401
    assert expired_response.json()["detail"] == "API key inválida"
    assert revoked_response.status_code == 401
    assert revoked_response.json()["detail"] == "API key inválida"


def test_api_key_rejects_authorization_header_bypass(monkeypatch) -> None:
    session_factory = _build_session_factory()
    client = _prepare_client(monkeypatch, session_factory)

    with session_factory() as db:
        _, token = _create_api_key(db, scopes=["tags.read"])

    try:
        response = client.get("/api/v1/external/ping", headers={"Authorization": f"Bearer {token}"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 401
    assert response.json()["detail"] == "API key ausente"


def test_api_key_scopes_cover_read_create_update_delete(monkeypatch) -> None:
    session_factory = _build_session_factory()
    client = _prepare_client(monkeypatch, session_factory)

    with session_factory() as db:
        _, read_token = _create_api_key(db, scopes=["tags.read"])
        _, full_token = _create_api_key(db, scopes=["tags.read", "tags.create", "tags.update", "tags.delete"])
        tag = Tag(
            slug="seed-tag",
            name="Seed Tag",
            status="active",
        )
        db.add(tag)
        db.commit()

    try:
        read_response = client.get("/api/v1/external/tags", headers={"X-API-Key": read_token})
        create_response = client.post(
            "/api/v1/external/tags",
            headers={"X-API-Key": full_token},
            json={"slug": "test-tag", "name": "Test Tag"},
        )
        created_tag_id = create_response.json()["id"]
        delete_response = client.delete(
            f"/api/v1/external/tags/{created_tag_id}",
            headers={"X-API-Key": full_token},
        )
        with session_factory() as db:
            read_result = resolve_api_key_from_token(read_token, db)
            full_result = resolve_api_key_from_token(full_token, db)
        ensure_scopes(full_result, ["tags.update"])
        ensure_scopes(full_result, ["tags.delete"])
        with pytest.raises(HTTPException):
            ensure_scopes(read_result, ["tags.update"])
    finally:
        app.dependency_overrides.clear()

    assert read_response.status_code == 200
    assert create_response.status_code == 201
    assert delete_response.status_code == 200
    assert delete_response.json()["ok"] is True


def test_api_key_allowlist_respects_real_ip_and_ignores_forwarded_headers(monkeypatch) -> None:
    session_factory = _build_session_factory()
    client = _prepare_client(monkeypatch, session_factory)

    with session_factory() as db:
        _, token = _create_api_key(db, scopes=["tags.read"], allowed_ips=["10.0.0.10"])

    monkeypatch.setattr("t2c_data.core.external_auth.get_request_client_ip", lambda request: "10.0.0.10")
    try:
        allowed_response = client.get(
            "/api/v1/external/ping",
            headers={
                "X-API-Key": token,
                "X-Forwarded-For": "203.0.113.10",
                "User-Agent": "pytest",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert allowed_response.status_code == 200

    session_factory = _build_session_factory()
    client = _prepare_client(monkeypatch, session_factory)
    with session_factory() as db:
        _, token = _create_api_key(db, scopes=["tags.read"], allowed_ips=["10.0.0.10"])

    monkeypatch.setattr("t2c_data.core.external_auth.get_request_client_ip", lambda request: "10.0.0.11")
    try:
        blocked_response = client.get(
            "/api/v1/external/ping",
            headers={
                "X-API-Key": token,
                "X-Forwarded-For": "10.0.0.10",
                "User-Agent": "pytest",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert blocked_response.status_code == 403
    assert blocked_response.json()["detail"] == "API key não autorizada para este IP"


def test_api_key_use_logs_include_key_metadata(monkeypatch) -> None:
    session_factory = _build_session_factory()
    captured: list[dict[str, object]] = []

    def fake_commit_access_log_with_repair(session, **kwargs):  # noqa: ANN001
        captured.append(kwargs.get("metadata", {}))

    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", fake_commit_access_log_with_repair)
    monkeypatch.setattr(settings, "platform_read_model_auto_refresh_enabled", False)
    monkeypatch.setattr(settings, "external_api_rate_limit_enabled", False)
    app.dependency_overrides[get_db] = _override_get_db(session_factory)
    client = TestClient(app)

    with session_factory() as db:
        _, token = _create_api_key(db, scopes=["tags.read"])

    try:
        response = client.get("/api/v1/external/ping", headers={"X-API-Key": token})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured
    metadata = captured[0]
    assert metadata["api_key_id"] is not None
    assert metadata["api_key_public_id"]
    assert metadata["api_key_name"] == "External API"
    assert metadata["api_key_token_prefix"]
    assert metadata["api_key_status"] == "active"
    assert metadata["api_key_environment"] == "shared"
    assert metadata["api_key_usage_count"] == 1
    assert metadata["api_key_scope_count"] >= 1
    assert metadata["api_key_allowed_ips_count"] == 0
