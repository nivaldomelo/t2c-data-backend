from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.tree_queries import (
    get_table_columns_summary,
    list_table_columns_page,
    list_tree_schema_tables_page,
)
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity


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


def _seed_catalog(db: Session) -> tuple[User, Schema, TableEntity]:
    role = Role(name="admin", description="Admin")
    user = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    user.roles.append(role)
    datasource = DataSource(
        name="operational-source",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="catalog",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="catalog", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="audit_logs", table_type="table", schema=schema)
    db.add_all([role, user, datasource, database, schema, table])
    db.commit()

    columns = [
        ColumnEntity(table_id=table.id, name="id", data_type="int", ordinal_position=1, is_nullable=False, is_primary_key=True),
        ColumnEntity(table_id=table.id, name="created_at", data_type="timestamp", ordinal_position=2, is_nullable=False),
        ColumnEntity(table_id=table.id, name="action_name", data_type="text", ordinal_position=3, is_nullable=True),
    ]
    db.add_all(columns)
    db.commit()

    extra_table = TableEntity(name="extra_table", table_type="table", schema=schema)
    another_table = TableEntity(name="another_table", table_type="table", schema=schema)
    db.add_all([extra_table, another_table])
    db.commit()
    return user, schema, table


def test_tree_schema_tables_page_is_paginated() -> None:
    db = _build_session()
    user, schema, _ = _seed_catalog(db)

    with patch("t2c_data.features.catalog.tree_queries.load_table_profiles", return_value=[]), patch(
        "t2c_data.features.catalog.tree_queries.build_governance_score_for_profile",
        return_value={},
    ), patch(
        "t2c_data.features.catalog.tree_queries.build_trust_score_for_profile",
        return_value=SimpleNamespace(score=None, label=None, tone=None),
    ), patch(
        "t2c_data.features.catalog.tree_queries.load_entity_tag_contexts",
        return_value={},
    ), patch(
        "t2c_data.features.catalog.tree_queries.get_governance_settings_snapshot",
        return_value={},
    ):
        page_1 = list_tree_schema_tables_page(db=db, schema_id=schema.id, page=1, page_size=2, current_user=user)
        page_2 = list_tree_schema_tables_page(db=db, schema_id=schema.id, page=2, page_size=2, current_user=user)

    assert page_1.page == 1
    assert page_1.page_size == 2
    assert len(page_1.items) == 2
    assert page_1.has_more is True
    assert page_2.page == 2
    assert len(page_2.items) >= 1


def test_table_columns_page_and_summary() -> None:
    db = _build_session()
    user, _, table = _seed_catalog(db)

    with patch("t2c_data.features.catalog.tree_queries.load_entity_tag_contexts", return_value={}):
        page = list_table_columns_page(db=db, table_id=table.id, page=1, page_size=2, current_user=user)

    assert page.total == 3
    assert len(page.items) == 2
    assert page.has_more is True

    summary = get_table_columns_summary(db=db, table_id=table.id, current_user=user)
    assert summary.total == 3
    assert summary.primary_keys == 1
    assert summary.required >= 2
