from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import Request
from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.platform.alerting import emit_api_key_abuse_alert, emit_operational_alert_for_job, emit_permission_denied_alert
from t2c_data.features.platform.jobs import enqueue_integration_job, finish_integration_job_record
from t2c_data.models import Base, AuditLog, Incident, IntegrationSyncJob, PlatformDomainEvent, Role, User, UserInboxNotification

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
    return SessionLocal()


def _request(*, path: str, method: str = "GET", correlation_id: str = "corr-1", client_host: str = "127.0.0.1") -> Request:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": [
            (b"host", b"testserver"),
            (b"user-agent", b"pytest"),
            (b"x-correlation-id", correlation_id.encode("utf-8")),
        ],
        "client": (client_host, 12345),
        "server": ("testserver", 80),
        "root_path": "",
    }
    return Request(scope)


def _seed_admin(session: Session, email: str = "admin@example.com") -> User:
    role = Role(name="admin", description="Administrator")
    user = User(email=email, password_hash="x", is_active=True)
    user.roles.append(role)
    session.add_all([role, user])
    session.commit()
    session.refresh(user)
    return user


def test_operational_alert_redacts_secret_and_includes_causal_metadata() -> None:
    session = _build_session()
    admin = _seed_admin(session)

    prior_job = enqueue_integration_job(
        session,
        source="datasource",
        job_type="scan",
        target_type="datasource",
        target_id=44,
        target_name="warehouse",
        requested_by_user_id=admin.id,
        context_json={"table_fqn": "analytics.public.customers"},
    )
    assert prior_job is not None
    finish_integration_job_record(
        session,
        prior_job,
        status="failed",
        error="previous failure",
        context_json={"table_fqn": "analytics.public.customers"},
    )

    current_job = enqueue_integration_job(
        session,
        source="datasource",
        job_type="scan",
        target_type="datasource",
        target_id=44,
        target_name="warehouse",
        requested_by_user_id=admin.id,
        context_json={
            "table_fqn": "analytics.public.customers",
            "jdbc_password": "super-secret",
        },
    )
    assert current_job is not None

    finished = finish_integration_job_record(
        session,
        current_job,
        status="failed",
        error="jdbc_password=super-secret | authentication failed",
        context_json={
            "table_fqn": "analytics.public.customers",
            "jdbc_password": "super-secret",
        },
    )
    assert finished is not None

    diagnostics = session.scalar(
        select(IntegrationSyncJob).where(IntegrationSyncJob.id == finished.id).limit(1)
    )
    assert diagnostics is not None

    events = session.scalars(select(PlatformDomainEvent).order_by(PlatformDomainEvent.id.asc())).all()
    assert events
    payload = events[-1].payload_json or {}
    payload_text = json.dumps(payload)
    assert payload["diagnostic_impact"]
    assert payload["diagnostic_recurrence_count"] == 2
    assert "super-secret" not in payload_text

    inbox_items = session.scalars(select(UserInboxNotification)).all()
    assert inbox_items
    inbox = inbox_items[-1]
    assert inbox.user_id == admin.id
    assert inbox.source_entity_type == "integration_sync_job"
    context_text = json.dumps(inbox.context_json or {})
    assert "super-secret" not in context_text
    assert inbox.href == f"/ops-cockpit?jobId={finished.id}"

    incidents = session.scalars(select(Incident)).all()
    assert incidents


def test_permission_denied_alert_triggers_after_repeated_export_denials() -> None:
    session = _build_session()
    user = _seed_admin(session, email="viewer@example.com")
    request = _request(path="/api/v1/data-owners/export.csv", method="GET", correlation_id="corr-export")

    now = datetime.now(timezone.utc)
    for _ in range(3):
        session.add(
            AuditLog(
                user_id=user.id,
                actor_name=user.email,
                user_email=user.email,
                ip="127.0.0.1",
                user_agent="pytest",
                action="platform.permission.denied",
                entity_type="permission",
                entity_id="data_owners:export",
                field_name="data_owners:export",
                source_module="platform",
                method="GET",
                route="/api/v1/data-owners/export.csv",
                metadata_json={"path": "/api/v1/data-owners/export.csv", "method": "GET"},
                created_at=now - timedelta(minutes=1),
            )
        )
    session.commit()

    emitted = emit_permission_denied_alert(
        session,
        request=request,
        current_user=user,
        permission_name="data_owners:export",
    )

    assert emitted is True
    event = session.scalar(select(PlatformDomainEvent).where(PlatformDomainEvent.event_key == "platform.permission.denied").limit(1))
    assert event is not None
    inbox = session.scalar(select(UserInboxNotification).where(UserInboxNotification.source_module == "platform.alerting").limit(1))
    assert inbox is not None
    assert inbox.user_id == user.id
    assert inbox.context_json is not None
    assert inbox.context_json["permission_name"] == "data_owners:export"


def test_api_key_abuse_alert_triggers_after_repeated_failures() -> None:
    session = _build_session()
    admin = _seed_admin(session, email="admin-api@example.com")
    request = _request(path="/api/v1/external/tables", method="GET", correlation_id="corr-api", client_host="10.0.0.1")

    now = datetime.now(timezone.utc)
    for _ in range(5):
        session.add(
            AuditLog(
                user_id=admin.id,
                actor_name="External API",
                user_email=admin.email,
                ip="10.0.0.1",
                user_agent="pytest",
                action="platform.api_key.auth_failed",
                entity_type="platform_api_key",
                entity_id="42",
                field_name="missing",
                source_module="external_api",
                method="GET",
                route="/api/v1/external/tables",
                metadata_json={"outcome": "missing", "path": "/api/v1/external/tables"},
                created_at=now - timedelta(minutes=1),
            )
        )
    session.commit()

    emitted = emit_api_key_abuse_alert(
        session,
        request=request,
        outcome="missing",
        api_key_public_id=None,
    )

    assert emitted is True
    event = session.scalar(select(PlatformDomainEvent).where(PlatformDomainEvent.event_key == "platform.api_key.abuse").limit(1))
    assert event is not None
    inbox = session.scalar(select(UserInboxNotification).where(UserInboxNotification.source_module == "platform.alerting").order_by(UserInboxNotification.id.desc()).limit(1))
    assert inbox is not None
    assert inbox.context_json is not None
    assert inbox.context_json["outcome"] == "missing"
