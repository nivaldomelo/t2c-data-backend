from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.data_quality.latest_runs import (
    backfill_latest_rule_runs,
    get_latest_rule_snapshots,
    sync_latest_snapshot_for_job,
)
from t2c_data.features.data_quality.rule_management import get_rule_detail
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleLatestRun, DQRuleRun


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


def _seed_rule(db: Session) -> tuple[User, DQRule]:
    role = Role(name="editor", description="Editor")
    user = User(email="maria@email.com.br", password_hash="hash", name="Maria", full_name="Maria Silva", is_active=True)
    user.roles.append(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="orders", table_type="table", schema=schema)

    db.add_all([role, user, datasource, database, schema, table])
    db.flush()
    db.add(DataAccessGrant(user=user, effect="allow", schema=schema))

    rule = DQRule(
        table_id=table.id,
        table_fqn=f"{datasource.name}.{schema.name}.{table.name}",
        name="Orders not null",
        severity="high",
        is_active=True,
        schedule_enabled=True,
        rule_definition_json={
            "version": 1,
            "target": {
                "table_id": table.id,
                "datasource_id": datasource.id,
                "datasource_name": datasource.name,
                "schema_name": schema.name,
                "table_name": table.name,
            },
            "logic": "AND",
            "conditions": [{"column": "order_id", "operator": "not_null", "value_type": "none"}],
        },
    )
    db.add(rule)
    db.commit()
    db.refresh(user)
    db.refresh(rule)
    return user, rule


def test_rule_detail_uses_materialized_latest_job_without_scanning_requested_rule_ids() -> None:
    db = _build_session()
    user, rule = _seed_rule(db)

    latest_rule_run = DQRuleRun(
        rule_id=rule.id,
        status="fail",
        execution_engine="spark",
        violations_count=4,
        created_at=datetime.now(timezone.utc),
    )
    job = DQJobRun(
        job_type="rules",
        status="success",
        execution_engine="spark",
        table_id=rule.table_id,
        table_fqn=rule.table_fqn,
        datasource_id=1,
        requested_by_user_id=user.id,
        result_json={
            "rows_checked_total": 512,
            "violations_count_total": 4,
            "summary": {
                "total_rules": 1,
                "passed_rules": 0,
                "failed_rules": 1,
                "error_rules": 0,
            },
        },
    )
    db.add_all([latest_rule_run, job])
    db.flush()
    db.add(
        DQRuleLatestRun(
            rule_id=rule.id,
            table_id=rule.table_id,
            latest_rule_run_id=latest_rule_run.id,
            latest_job_run_id=job.id,
        )
    )
    db.commit()

    detail = get_rule_detail(db=db, rule_id=rule.id, current_user=user)

    assert detail.last_run_id == latest_rule_run.id
    assert detail.last_job_run_id == job.id
    assert detail.last_rows_checked == 512
    assert detail.last_job_violations_count == 4
    assert detail.last_job_failed_rules == 1


def test_backfill_populates_latest_snapshot_with_most_recent_rule_run_and_job() -> None:
    db = _build_session()
    user, rule = _seed_rule(db)
    older = datetime.now(timezone.utc) - timedelta(days=1)
    newer = datetime.now(timezone.utc)

    older_run = DQRuleRun(
        rule_id=rule.id,
        status="pass",
        execution_engine="spark",
        violations_count=0,
        created_at=older,
    )
    newer_run = DQRuleRun(
        rule_id=rule.id,
        status="fail",
        execution_engine="spark",
        violations_count=3,
        created_at=newer,
    )
    older_job = DQJobRun(
        job_type="rules",
        status="success",
        execution_engine="spark",
        table_id=rule.table_id,
        table_fqn=rule.table_fqn,
        requested_by_user_id=user.id,
        result_json={"requested_rule_ids": [rule.id]},
        created_at=older,
    )
    newer_job = DQJobRun(
        job_type="rules",
        status="failed",
        execution_engine="spark",
        table_id=rule.table_id,
        table_fqn=rule.table_fqn,
        requested_by_user_id=user.id,
        result_json={"requested_rule_ids": [rule.id]},
        created_at=newer,
    )
    db.add_all([older_run, newer_run, older_job, newer_job])
    db.commit()

    summary = backfill_latest_rule_runs(db)
    db.commit()

    snapshot = get_latest_rule_snapshots(db, [rule.id])[rule.id]
    assert summary["rules_total"] >= 1
    assert snapshot.latest_rule_run_id == newer_run.id
    assert snapshot.latest_job_run_id == newer_job.id


def test_older_job_does_not_overwrite_newer_snapshot() -> None:
    db = _build_session()
    _user, rule = _seed_rule(db)
    older = datetime.now(timezone.utc) - timedelta(days=1)
    newer = datetime.now(timezone.utc)

    newer_job = DQJobRun(
        job_type="rules",
        status="success",
        execution_engine="spark",
        table_id=rule.table_id,
        table_fqn=rule.table_fqn,
        result_json={"requested_rule_ids": [rule.id]},
        created_at=newer,
    )
    older_job = DQJobRun(
        job_type="rules",
        status="failed",
        execution_engine="spark",
        table_id=rule.table_id,
        table_fqn=rule.table_fqn,
        result_json={"requested_rule_ids": [rule.id]},
        created_at=older,
    )
    db.add_all([newer_job, older_job])
    db.commit()

    sync_latest_snapshot_for_job(db, job_run=newer_job, rule_ids=[rule.id], table_id=rule.table_id)
    db.commit()
    sync_latest_snapshot_for_job(db, job_run=older_job, rule_ids=[rule.id], table_id=rule.table_id)
    db.commit()

    snapshot = get_latest_rule_snapshots(db, [rule.id])[rule.id]
    assert snapshot.latest_job_run_id == newer_job.id
