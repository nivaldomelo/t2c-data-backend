from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi import HTTPException

from t2c_data.features.catalog.column_dictionary_admin import ColumnDictionaryFilters, get_column_dictionary_detail, get_column_dictionary_summary, list_column_dictionary
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.core.security import hash_password


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


def _seed_scope_catalog(db: Session) -> tuple[User, ColumnEntity, ColumnEntity]:
    role_editor = Role(name="editor")
    db.add(role_editor)

    local = DataSource(
        name="local-andromeda",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="nivasmelo",
    )
    local.password = "secret"
    local_db = Database(name="andromeda", datasource=local)
    local_bronze = Schema(name="bronze", database=local_db)
    local_table = TableEntity(name="customers", table_type="table", schema=local_bronze)

    remote = DataSource(
        name="demo-source",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="demo",
        username="nivasmelo",
    )
    remote.password = "secret"
    remote_db = Database(name="demo", datasource=remote)
    remote_demo = Schema(name="demo", database=remote_db)
    remote_table = TableEntity(name="events", table_type="table", schema=remote_demo)

    db.add_all([local, local_db, local_bronze, local_table, remote, remote_db, remote_demo, remote_table])
    db.flush()

    local_column = ColumnEntity(
        table=local_table,
        name="id",
        data_type="integer",
        is_primary_key=True,
        is_nullable=False,
        ordinal_position=1,
        description_source="Identificador local",
        dictionary_description="Identificador do cliente local",
        dictionary_comment="Coluna visível para o escopo bronze",
    )
    demo_column = ColumnEntity(
        table=remote_table,
        name="id",
        data_type="integer",
        is_primary_key=True,
        is_nullable=False,
        ordinal_position=1,
        description_source="Identificador demo",
    )
    db.add_all([local_column, demo_column])

    user = User(
        email="caio@email.com.br",
        name="Caio",
        full_name="Caio",
        password_hash=hash_password("secret123"),
        is_active=True,
    )
    user.roles = [role_editor]
    db.add(user)
    db.flush()

    db.add_all(
        [
            DataAccessGrant(user=user, effect="allow", datasource=local),
            DataAccessGrant(user=user, effect="allow", schema=local_bronze),
        ]
    )
    db.commit()
    return user, local_column, demo_column


def test_column_dictionary_respects_data_scope_filters_and_detail_access() -> None:
    db = _build_session()
    user, local_column, demo_column = _seed_scope_catalog(db)

    summary = get_column_dictionary_summary(db, filters=ColumnDictionaryFilters(), current_user=user)
    page = list_column_dictionary(db, filters=ColumnDictionaryFilters(), page=1, page_size=25, current_user=user)

    assert summary.total_columns == 1
    assert summary.total_tables == 1
    assert summary.total_schemas == 1
    assert summary.documented_columns == 1
    assert page.total == 1
    assert [item.id for item in page.items] == [local_column.id]
    assert page.filters.datasources == ["local-andromeda"]
    assert page.filters.schemas == ["bronze"]
    assert page.filters.tables == ["customers"]

    detail = get_column_dictionary_detail(db, local_column.id, current_user=user)
    assert detail.schema_name == "bronze"
    assert detail.datasource_name == "local-andromeda"

    try:
        get_column_dictionary_detail(db, demo_column.id, current_user=user)
    except HTTPException as exc:
        assert exc.status_code == 404
    else:
        raise AssertionError("Expected unauthorized column to be hidden")


if __name__ == "__main__":
    test_column_dictionary_respects_data_scope_filters_and_detail_access()
    print("column dictionary scope tests: OK")
