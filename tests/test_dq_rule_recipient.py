from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool
from pydantic import ValidationError

from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRule
from t2c_data.features.data_quality.rule_management import create_rule_with_audit, search_rule_notification_users, update_rule_with_audit
from t2c_data.schemas.dq_rules import DQRuleCreate, DQRuleUpdate


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


def _seed_graph(db: Session) -> tuple[User, User, TableEntity]:
    active_role = Role(name="admin", description="Admin")
    admin = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    admin.roles.append(active_role)
    owner = User(email="owner@andromeda.local", password_hash="hash", name="Owner", full_name="Owner User", is_active=True)
    inactive = User(email="inactive@andromeda.local", password_hash="hash", name="Inactive", full_name="Inactive User", is_active=False)
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="categories", table_type="table", schema=schema)
    column = ColumnEntity(table=table, name="category", data_type="text", ordinal_position=1, is_nullable=True)
    db.add_all([active_role, admin, owner, inactive, datasource, database, schema, table, column])
    db.commit()
    db.refresh(admin)
    db.refresh(owner)
    db.refresh(inactive)
    db.refresh(table)
    return admin, owner, table


def test_dq_rule_recipient_is_persisted_and_searchable() -> None:
    db = _build_session()
    admin, owner, table = _seed_graph(db)

    users = search_rule_notification_users(db=db, q="own", limit=10)
    assert [user.id for user in users] == [owner.id]

    created = create_rule_with_audit(
        db=db,
        payload=DQRuleCreate(
            name="No nulls",
            description="Regra de teste",
            table_id=table.id,
            table_fqn="warehouse.bronze.categories",
            notification_recipient_user_ids=[owner.id, admin.id, owner.id],
            rule_type="nullability",
            severity="high",
            logic="AND",
            conditions=[{"column": "category", "operator": "not_null"}],
            is_active=True,
        ),
        audit_kwargs={},
    )
    assert created.notification_recipient_user_id == owner.id
    assert "sql_text" not in created.model_dump()
    assert created.notification_recipient_user_name == owner.name
    assert created.notification_recipient_user_email == owner.email
    assert sorted(user.id for user in created.notification_recipient_users) == [admin.id, owner.id]

    updated = update_rule_with_audit(
        db=db,
        rule_id=created.id,
        payload=DQRuleUpdate(notification_recipient_user_ids=[admin.id]),
        audit_kwargs={},
    )
    assert updated.notification_recipient_user_id == admin.id
    assert updated.notification_recipient_user_name == admin.name
    assert [user.id for user in updated.notification_recipient_users] == [admin.id]
    assert db.get(DQRule, created.id).notification_recipient_user_id == admin.id

    try:
        create_rule_with_audit(
            db=db,
            payload=DQRuleCreate(
                name="Invalid recipient",
                description=None,
                table_id=table.id,
                table_fqn="warehouse.bronze.categories",
                notification_recipient_user_ids=[999999],
                rule_type="nullability",
                severity="high",
                logic="AND",
                conditions=[{"column": "category", "operator": "not_null"}],
                is_active=True,
            ),
            audit_kwargs={},
        )
        raise AssertionError("Expected inactive/missing recipient to be rejected")
    except HTTPException as exc:
        assert exc.status_code == 422


@pytest.mark.parametrize("legacy_field", ["sql_text", "custom_sql", "raw_sql", "sql_expression"])
def test_dq_rule_create_rejects_legacy_sql_payload(legacy_field: str) -> None:
    base_payload = {
        "name": "Legacy SQL",
        "description": "não deve aceitar SQL livre",
        "table_id": 1,
        "table_fqn": "warehouse.bronze.categories",
        "notification_recipient_user_ids": [1],
        "rule_type": "nullability",
        "severity": "high",
        "logic": "AND",
        "conditions": [{"column": "category", "operator": "not_null"}],
        "is_active": True,
        legacy_field: "SELECT * FROM bronze.categories WHERE category IS NULL",
    }

    with pytest.raises(ValidationError):
        DQRuleCreate.model_validate(base_payload)


def test_dq_rule_create_rejects_custom_sql_rule_type() -> None:
    base_payload = {
        "name": "Legacy SQL type",
        "description": "não deve aceitar tipo legado",
        "table_id": 1,
        "table_fqn": "warehouse.bronze.categories",
        "notification_recipient_user_ids": [1],
        "rule_type": "custom_sql",
        "severity": "high",
        "logic": "AND",
        "conditions": [{"column": "category", "operator": "not_null"}],
        "is_active": True,
    }

    with pytest.raises(ValidationError):
        DQRuleCreate.model_validate(base_payload)


@pytest.mark.parametrize("legacy_field", ["sql_text", "custom_sql", "raw_sql", "sql_expression"])
def test_dq_rule_update_rejects_legacy_sql_payload(legacy_field: str) -> None:
    payload = {
        legacy_field: "SELECT * FROM bronze.categories WHERE category IS NULL",
    }

    with pytest.raises(ValidationError):
        DQRuleUpdate.model_validate(payload)


def test_dq_rule_update_rejects_custom_sql_rule_type() -> None:
    with pytest.raises(ValidationError):
        DQRuleUpdate.model_validate({"rule_type": "custom_sql"})


if __name__ == "__main__":
    test_dq_rule_recipient_is_persisted_and_searchable()
    test_dq_rule_create_rejects_legacy_sql_payload()
    print("dq rule recipient tests: OK")
