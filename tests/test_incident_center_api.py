from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.db import get_db
from t2c_data.core.deps import get_current_user
from t2c_data.core.security import hash_password
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.incident import Incident
from t2c_data.schemas.incident import IncidentCenterSummaryOut


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


def _prepare_client(monkeypatch, session_factory, current_user: User):
    monkeypatch.setattr("t2c_data.main.SessionLocal", session_factory)
    monkeypatch.setattr("t2c_data.main.start_platform_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_dq_profiling_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.start_datasource_scan_scheduler", lambda: None)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", lambda *args, **kwargs: None)
    app.dependency_overrides[get_db] = _override_get_db(session_factory)
    app.dependency_overrides[get_current_user] = lambda: current_user
    return TestClient(app)


def test_incident_center_endpoint_returns_valid_summary(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as db:
        current_user = User(
            email="ops@example.com",
            name="Ops User",
            full_name="Ops User",
            password_hash=hash_password("secret123"),
            is_active=True,
        )
        db.add(current_user)
        db.flush()

        db.add_all(
            [
                Incident(
                    title="Pipeline falhando",
                    description="Incidente aberto",
                    entity_type="airflow_dag",
                    airflow_dag_id="dag_pipeline",
                    detected_at=datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
                    status="open",
                    severity="sev1",
                    source_type="platform_ops",
                    occurrences=1,
                ),
                Incident(
                    title="Incidente fechado",
                    description="Incidente encerrado",
                    entity_type="airflow_dag",
                    airflow_dag_id="dag_closed",
                    detected_at=datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc),
                    status="closed",
                    severity="sev4",
                    source_type="platform_ops",
                    occurrences=1,
                ),
                Incident(
                    title="Incidente recorrente",
                    description="Ocorrência repetida",
                    entity_type="airflow_dag",
                    airflow_dag_id="dag_recurring",
                    detected_at=datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc),
                    status="recurring",
                    severity="sev2",
                    source_type="platform_ops",
                    occurrences=3,
                ),
            ]
        )
        db.commit()

    client = _prepare_client(monkeypatch, session_factory, current_user)

    try:
        response = client.get("/api/v1/incidents/center")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    summary = IncidentCenterSummaryOut.model_validate(payload)

    assert summary.metrics
    assert any(metric.key == "active" for metric in summary.metrics)
    assert summary.by_status is not None
    assert summary.recent_incidents is not None
    assert payload["metrics"][0]["key"] == summary.metrics[0].key

