from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.dashboard.executive_queries import get_dashboard_executive_overview, get_dashboard_executive_secondary, normalize_filters
from t2c_data.features.dashboard.strategy_queries import build_platform_strategic_summary
from t2c_data.models import Base


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


def test_dashboard_summary_and_strategic_summary_handle_empty_database() -> None:
    db = _build_session()

    executive_payload = get_dashboard_executive_overview(db, normalize_filters(), current_user=None)
    secondary_payload = get_dashboard_executive_secondary(db, normalize_filters(), current_user=None)
    strategic_payload = build_platform_strategic_summary(db, days=30, current_user=None)

    assert executive_payload["top_critical"]["total"] == 0
    assert executive_payload["kpis"]
    assert "operational_intelligence" in secondary_payload
    assert secondary_payload["incidents"]["open_total"] == 0
    assert secondary_payload["incidents"]["critical_open_total"] == 0
    assert secondary_payload["operational_intelligence"]["evaluated_assets"] == 0
    assert strategic_payload["value_score"] >= 0
    assert 0 <= strategic_payload["value_score"] <= 100
    assert strategic_payload["adoption"]["active_users"] == 0
    assert strategic_payload["benchmark"]["by_domain"] == []
    assert strategic_payload["roadmap"]
