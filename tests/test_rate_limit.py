from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.rate_limit import _rate_limit_decision
from t2c_data.models.platform import ApiRateLimitBucket, PlatformApiKey


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

    PlatformApiKey.__table__.create(engine)
    ApiRateLimitBucket.__table__.create(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def test_rate_limit_blocks_after_limit() -> None:
    session = _build_session()
    key = PlatformApiKey(
        public_id="ext-key",
        name="External Integration",
        status="active",
        scopes_json=["catalog.read"],
        token_hash="hash",
        token_prefix="hash",
    )
    session.add(key)
    session.commit()

    decision_1 = _rate_limit_decision(
        session,
        api_key_id=key.id,
        route_group="external.catalog",
        window_seconds=60,
        limit=2,
    )
    decision_2 = _rate_limit_decision(
        session,
        api_key_id=key.id,
        route_group="external.catalog",
        window_seconds=60,
        limit=2,
    )
    decision_3 = _rate_limit_decision(
        session,
        api_key_id=key.id,
        route_group="external.catalog",
        window_seconds=60,
        limit=2,
    )

    assert decision_1.allowed is True
    assert decision_2.allowed is True
    assert decision_3.allowed is False


if __name__ == "__main__":
    test_rate_limit_blocks_after_limit()
    print("rate limit tests: OK")
