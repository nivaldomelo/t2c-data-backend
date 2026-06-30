from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.data_quality import scheduler as dq_scheduler
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRule


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session():
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
    return SessionLocal


def test_dq_scheduler_cycle_executes_due_rules_and_persists_status() -> None:
    SessionLocal = _build_session()
    original_session_local = dq_scheduler.SessionLocal
    dq_scheduler.SessionLocal = SessionLocal  # type: ignore[assignment]
    db = SessionLocal()
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="categories", table_type="table", schema=schema)
    db.add_all([datasource, database, schema, table])
    db.flush()
    rule = DQRule(
        table_id=table.id,
        table_fqn="warehouse.bronze.categories",
        name="scheduler rule",
        rule_type="nullability",
        severity="medium",
        rule_builder_version=1,
        rule_definition_json={
            "version": 1,
            "type": "nullability",
            "target": {
                "datasource_id": datasource.id,
                "datasource_name": datasource.name,
                "schema_name": schema.name,
                "table_name": table.name,
                "table_id": table.id,
            },
            "logic": "AND",
            "conditions": [{"column": "category", "operator": "not_null", "value_type": "none"}],
        },
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=5,
    )
    legacy_rule = DQRule(
        table_id=table.id,
        table_fqn="warehouse.bronze.categories",
        name="legacy sql rule",
        rule_type="column_validation",
        severity="medium",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=5,
        archived=True,
        archived_reason="legacy_sql_rule_removed",
    )
    db.add_all([rule, legacy_rule])
    db.commit()
    db.refresh(rule)

    class _FakeGateway:
        def create_table_run(self, **_kwargs):
            return SimpleNamespace(id=777)

        def enqueue_rules(self, **_kwargs):
            return SimpleNamespace(id=778)

    original_gateway = dq_scheduler.DefaultDQExecutionGateway
    try:
        dq_scheduler.DefaultDQExecutionGateway = lambda: _FakeGateway()  # type: ignore[assignment]

        summary = dq_scheduler.run_dq_scheduler_cycle(trigger="manual", scheduler_mode="embedded")
        assert summary["rules_total"] == 1
        assert summary["due_rules"] == [rule.id]
        assert summary["executed_rules"][0]["rule_id"] == rule.id
        assert summary["executed_rules"][0]["dq_run_id"] == 777
        assert summary["executed_rules"][0]["job_run_id"] == 778
        assert summary["executed_rules"][0]["status"] == "queued"

        status = dq_scheduler.scheduler_status_snapshot(db)
        assert status["scheduler_name"] == "dq_rules"
        assert status["last_run_summary"]["trigger"] == "manual"
    finally:
        dq_scheduler.DefaultDQExecutionGateway = original_gateway  # type: ignore[assignment]
        dq_scheduler.SessionLocal = original_session_local  # type: ignore[assignment]


def test_dq_scheduler_snapshot_uses_empty_state_when_support_is_missing() -> None:
    SessionLocal = _build_session()
    db = SessionLocal()
    original_support_check = dq_scheduler._scheduler_support_is_ready
    try:
        dq_scheduler._scheduler_support_is_ready = lambda session: False  # type: ignore[assignment]
        snapshot = dq_scheduler.scheduler_status_snapshot(db)
        assert snapshot["scheduler_name"] == "dq_rules"
        assert snapshot["is_enabled"] in {True, False}
        assert snapshot["last_error"] is None
        assert snapshot["last_run_summary"] == {}
        assert snapshot["scheduled_rules_total"] == 0
        assert snapshot["next_expected_run_at"] is None
    finally:
        dq_scheduler._scheduler_support_is_ready = original_support_check  # type: ignore[assignment]


if __name__ == "__main__":
    test_dq_scheduler_cycle_executes_due_rules_and_persists_status()
    test_dq_scheduler_snapshot_uses_empty_state_when_support_is_missing()
    print("dq scheduler tests: OK")
