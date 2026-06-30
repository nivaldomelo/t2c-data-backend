from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.platform.job_worker import process_next_integration_job
from t2c_data.features.platform.worker_health import worker_health_snapshot
from t2c_data.models import Base, PlatformWorkerHeartbeat

if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]


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


def test_idle_worker_heartbeat_is_recorded() -> None:
    SessionLocal = _build_session_factory()

    processed = process_next_integration_job(session_factory=SessionLocal)

    assert processed is None
    with SessionLocal() as session:
        heartbeats = session.query(PlatformWorkerHeartbeat).all()
        assert len(heartbeats) == 1
        heartbeat = heartbeats[0]
        assert heartbeat.status == "idle"
        assert heartbeat.supported_job_types_json == ["*"]
        snapshot = worker_health_snapshot(session)
        assert snapshot["workers_total"] == 1
        assert snapshot["recent_workers_total"] == 1


def test_filtered_worker_heartbeat_records_supported_job_type() -> None:
    SessionLocal = _build_session_factory()

    processed = process_next_integration_job(source="platform", job_type="maintenance", session_factory=SessionLocal)

    assert processed is None
    with SessionLocal() as session:
        heartbeats = session.query(PlatformWorkerHeartbeat).all()
        assert len(heartbeats) == 1
        heartbeat = heartbeats[0]
        assert heartbeat.status == "idle"
        assert heartbeat.supported_job_types_json == ["platform:maintenance"]
