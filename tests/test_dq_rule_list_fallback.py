from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.data_quality.rule_management import list_rules_with_filters
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRule


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


def _seed_rule(db: Session) -> DQRule:
    role = Role(name="admin", description="Admin")
    user = User(email="owner@andromeda.local", password_hash="hash", name="Owner", full_name="Owner User", is_active=True)
    user.roles.append(role)
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="categories", table_type="table", schema=schema)
    db.add_all([role, user, datasource, database, schema, table])
    db.flush()
    rule = DQRule(
        table_id=table.id,
        table_fqn="warehouse.bronze.categories",
        name="Null check",
        severity="medium",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=5,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


def test_dq_rules_list_survives_missing_recipient_table() -> None:
    db = _build_session()
    rule = _seed_rule(db)
    db.execute(text("DROP TABLE t2c_data.dq_rule_notification_recipients"))
    db.commit()

    rows = list_rules_with_filters(
        db=db,
        rule_id=None,
        q=None,
        table_id=None,
        table_fqn=None,
        is_active=None,
        severity=None,
        last_status=None,
    )

    assert [item.id for item in rows] == [rule.id]
    assert rows[0].notification_recipient_users == []


def test_dq_rules_list_returns_empty_when_rules_table_is_missing() -> None:
    db = _build_session()
    db.execute(text("DROP TABLE t2c_data.dq_rules"))
    db.commit()

    rows = list_rules_with_filters(
        db=db,
        rule_id=None,
        q=None,
        table_id=None,
        table_fqn=None,
        is_active=None,
        severity=None,
        last_status=None,
    )

    assert rows == []


def test_dq_rules_list_survives_legacy_rules_table_without_schedule_columns() -> None:
    db = _build_session()
    db.execute(text("DROP TABLE t2c_data.dq_rules"))
    db.execute(
        text(
            """
            CREATE TABLE t2c_data.dq_rules (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              table_id INTEGER NULL,
              table_fqn VARCHAR(500) NOT NULL,
              name VARCHAR(255) NOT NULL,
              description TEXT NULL,
              rule_type VARCHAR(50) NOT NULL,
              severity VARCHAR(20) NOT NULL,
              is_active BOOLEAN NOT NULL DEFAULT 1,
              created_at DATETIME NOT NULL,
              updated_at DATETIME NOT NULL
            )
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO t2c_data.dq_rules (
              id, table_id, table_fqn, name, description, rule_type, severity, is_active, created_at, updated_at
            ) VALUES (
              1, 1, 'warehouse.bronze.categories', 'Null check', 'Legacy rule', 'row_violation', 'medium', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """
        )
    )
    db.commit()

    rows = list_rules_with_filters(
        db=db,
        rule_id=None,
        q=None,
        table_id=None,
        table_fqn=None,
        is_active=None,
        severity=None,
        last_status=None,
    )

    assert [item.id for item in rows] == [1]
    assert rows[0].name == "Null check"
    assert rows[0].schedule_mode == "manual"
    assert rows[0].schedule_enabled is False


if __name__ == "__main__":
    test_dq_rules_list_survives_missing_recipient_table()
    test_dq_rules_list_returns_empty_when_rules_table_is_missing()
    test_dq_rules_list_survives_legacy_rules_table_without_schedule_columns()
    print("dq rule list fallback tests: OK")
