from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.platform import DashboardAssetReadModel


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


def test_load_dashboard_profiles_prefers_materialized() -> None:
    session = _build_session()
    datasource = DataSource(
        name="local-andromeda",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="nivasmelo",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="customers", table_type="table", schema=schema)
    session.add_all([datasource, database, schema, table])
    session.flush()

    read_model = DashboardAssetReadModel(
        table_id=table.id,
        datasource_id=datasource.id,
        database_id=database.id,
        schema_id=schema.id,
        table_name=table.name,
        table_type=table.table_type,
        schema_name=schema.name,
        database_name=database.name,
        datasource_name=datasource.name,
        engine="postgres",
        owner_defined=False,
        description_complete=False,
        dictionary_complete=False,
        classification_defined=False,
        tags_count=0,
        terms_count=0,
        search_clicks_30d=0,
        active_dq_rules_count=0,
        recent_dq_failure_runs_30d=0,
        certification_status="not_eligible",
        certification_badges=[],
        review_recent=False,
        open_incidents=0,
        critical_open_incidents=0,
        has_personal_data=False,
        has_sensitive_personal_data=False,
    )
    session.add(read_model)
    session.commit()

    now = datetime.now(timezone.utc)
    profiles, source = load_dashboard_profiles_with_fallback(session, now)

    assert source == "materialized"
    assert len(profiles) == 1
    assert profiles[0].table_name == "customers"


if __name__ == "__main__":
    test_load_dashboard_profiles_prefers_materialized()
    print("read model tests: OK")
