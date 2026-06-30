from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.operations.failures import classify_operational_error, record_operational_failure
from t2c_data.models import Base
from t2c_data.models.operations import OperationalFailureTaxonomy


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


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


def test_operational_failure_classification_and_recording() -> None:
    db = _build_session()
    taxonomy = OperationalFailureTaxonomy(code="TIMEOUT_ERROR", name="Timeout error", default_severity="high", retryable=True)
    db.add(taxonomy)
    db.commit()

    category, severity, retryable = classify_operational_error("timeout while connecting", source="test")
    event = record_operational_failure(
        db,
        source="test",
        message="timeout while connecting",
        category_code=category,
        severity=severity,
        retryable=retryable,
    )
    db.commit()

    assert event.category_code == "TIMEOUT_ERROR"
    assert event.retryable is True


def test_connectivity_messages_map_to_connectivity_error() -> None:
    category, severity, retryable = classify_operational_error("Network is unreachable while connecting to host.docker.internal", source="ingestion.operational_source")

    assert category == "CONNECTIVITY_ERROR"
    assert severity == "high"
    assert retryable is True
