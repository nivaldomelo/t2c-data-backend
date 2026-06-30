from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.data_quality.rule_management import (
    get_rule_detail,
    list_rule_table_options,
    list_rules_with_filters,
    list_rules_with_filters_page,
)
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.audit import AuditLog
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleRun


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


def _seed_catalog(db: Session) -> tuple[User, DQRule, DQRule]:
    role = Role(name="editor", description="Editor")
    user = User(email="caio@email.com.br", password_hash="hash", name="Caio Wilson", full_name="Caio Wilson", is_active=True)
    user.roles.append(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    bronze = Schema(name="bronze", database=database)
    demo = Schema(name="demo", database=database)
    bronze_table = TableEntity(name="customers", table_type="table", schema=bronze)
    demo_table = TableEntity(name="tickets", table_type="table", schema=demo)

    db.add_all([role, user, datasource, database, bronze, demo, bronze_table, demo_table])
    db.flush()
    db.add_all(
        [
            DataAccessGrant(user=user, effect="allow", schema=bronze),
        ]
    )

    bronze_rule = DQRule(
        table_id=bronze_table.id,
        table_fqn=f"{datasource.name}.{bronze.name}.{bronze_table.name}",
        name="Bronze check",
        severity="medium",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=60,
    )
    demo_rule = DQRule(
        table_id=demo_table.id,
        table_fqn=f"{datasource.name}.{demo.name}.{demo_table.name}",
        name="Demo check",
        severity="high",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=60,
    )
    db.add_all([bronze_rule, demo_rule])
    db.commit()
    db.refresh(user)
    db.refresh(bronze_rule)
    db.refresh(demo_rule)
    return user, bronze_rule, demo_rule


def test_dq_rules_respect_schema_scope_for_list_detail_and_table_options() -> None:
    db = _build_session()
    user, bronze_rule, demo_rule = _seed_catalog(db)

    rows = list_rules_with_filters(
        db=db,
        rule_id=None,
        q=None,
        table_id=None,
        table_fqn=None,
        is_active=None,
        severity=None,
        last_status=None,
        current_user=user,
    )
    assert [item.id for item in rows] == [bronze_rule.id]
    assert rows[0].table_fqn == bronze_rule.table_fqn

    detail = get_rule_detail(db=db, rule_id=bronze_rule.id, current_user=user)
    assert detail.id == bronze_rule.id

    try:
        get_rule_detail(db=db, rule_id=demo_rule.id, current_user=user)
        raise AssertionError("expected demo rule to be hidden")
    except HTTPException as exc:
        assert exc.status_code == 404

    options = list_rule_table_options(db=db, q="", limit=20, current_user=user)
    assert [option.table_fqn for option in options] == [bronze_rule.table_fqn]


def test_rule_detail_includes_audit_authors_and_latest_job_metrics() -> None:
    db = _build_session()
    user, bronze_rule, _demo_rule = _seed_catalog(db)

    db.add(
        AuditLog(
            user_id=user.id,
            actor_name=user.full_name,
            user_email=user.email,
            action="dq_rule.create",
            entity_type="dq_rule",
            entity_id=str(bronze_rule.id),
        )
    )
    db.add(
        AuditLog(
            user_id=user.id,
            actor_name="Caio Atualizador",
            user_email="caio-update@email.com.br",
            action="dq_rule.update",
            entity_type="dq_rule",
            entity_id=str(bronze_rule.id),
        )
    )
    db.add(
        DQRuleRun(
            rule_id=bronze_rule.id,
            status="fail",
            execution_engine="spark",
            violations_count=3,
            error_message=None,
        )
    )
    db.add(
        DQJobRun(
            job_type="rules",
            status="success",
            execution_engine="spark",
            table_id=bronze_rule.table_id,
            table_fqn=bronze_rule.table_fqn,
            datasource_id=1,
            requested_by_user_id=user.id,
            spark_app_id="app-dq-rule-1",
            result_json={
                "requested_rule_ids": [bronze_rule.id],
                "rows_checked_total": 128,
                "violations_count_total": 3,
                "summary": {
                    "total_rules": 1,
                    "passed_rules": 0,
                    "failed_rules": 1,
                    "error_rules": 0,
                },
            },
        )
    )
    db.commit()

    detail = get_rule_detail(db=db, rule_id=bronze_rule.id, current_user=user)

    assert detail.created_by_user_email == user.email
    assert detail.updated_by_user_email == "caio-update@email.com.br"
    assert detail.last_audit_action == "dq_rule.update"
    assert detail.last_rows_checked == 128
    assert detail.last_job_violations_count == 3
    assert detail.last_job_total_rules == 1
    assert detail.last_job_failed_rules == 1
    assert detail.last_job_requested_by_user_email == user.email


def test_dq_rules_page_caps_page_size_and_preserves_scope() -> None:
    db = _build_session()
    user, bronze_rule, _demo_rule = _seed_catalog(db)

    page = list_rules_with_filters_page(
        db=db,
        rule_id=None,
        q=None,
        table_id=None,
        table_fqn=None,
        is_active=None,
        severity=None,
        last_status=None,
        page=1,
        page_size=999,
        current_user=user,
    )

    assert page.page == 1
    assert page.page_size == 100
    assert page.total == 1
    assert [item.id for item in page.items] == [bronze_rule.id]


if __name__ == "__main__":
    test_dq_rules_respect_schema_scope_for_list_detail_and_table_options()
    print("dq rule scope tests: OK")
