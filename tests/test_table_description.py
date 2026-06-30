from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.metadata_actions import patch_table_with_audit
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.schemas.catalog import TablePatch


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


def _seed_table(db: Session) -> tuple[User, TableEntity]:
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
    table = TableEntity(name="orders", table_type="table", schema=schema, description_source="Descrição de origem")
    db.add_all([role, user, datasource, database, schema, table])
    db.commit()
    return user, table


def test_patch_table_updates_manual_description_without_excel_change() -> None:
    db = _build_session()
    user, table = _seed_table(db)

    updated = patch_table_with_audit(
        db=db,
        table_id=table.id,
        payload=TablePatch(description_manual="Descrição cadastrada no sistema"),
        user=user,
        audit_kwargs={},
    )

    assert updated.description_manual == "Descrição cadastrada no sistema"
    assert updated.description_source == "Descrição de origem"
    refreshed = db.get(TableEntity, table.id)
    assert refreshed is not None
    assert refreshed.description_manual == "Descrição cadastrada no sistema"


if __name__ == "__main__":
    test_patch_table_updates_manual_description_without_excel_change()
    print("table description tests: OK")
