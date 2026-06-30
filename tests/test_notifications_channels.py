from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.security import hash_password
from t2c_data.features.notifications.service import get_user_notification_preferences_payload
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.auth import Role, User


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


def _seed_profile_user(session_factory) -> User:  # type: ignore[no-untyped-def]
    with session_factory() as db:
        role = Role(name="admin", description="Admin")
        user = User(
            email="admin@andromeda.com",
            name="Admin",
            full_name="Admin User",
            password_hash=hash_password("admin123"),
            is_active=True,
        )
        user.roles.append(role)
        db.add_all([role, user])
        db.commit()
        db.refresh(user)
        return user


def test_notification_preferences_payload_excludes_webhook_fields() -> None:
    session_factory = _build_session_factory()
    user = _seed_profile_user(session_factory)

    with session_factory() as db:
        payload = get_user_notification_preferences_payload(db, user)

    assert "slack_webhook_url" not in payload
    assert "teams_webhook_url" not in payload
    assert payload["in_app_enabled"] is True
    assert payload["email_enabled"] is False


def test_removed_webhook_routes_return_not_found() -> None:
    client = TestClient(app)

    assert client.get("/v1/platform/webhooks/subscriptions").status_code == 404
    assert client.get("/v1/platform/webhooks/deliveries").status_code == 404
    assert client.get("/v1/notifications/channels").status_code == 404

