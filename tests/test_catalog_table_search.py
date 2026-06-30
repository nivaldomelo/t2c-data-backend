from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.tree_queries import search_table_suggestions
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity


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


def _seed_graph(db: Session) -> tuple[User, TableEntity]:
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
    table = TableEntity(name="orders", table_type="table", schema=schema)
    db.add_all([role, user, datasource, database, schema, table])
    db.commit()
    return user, table


def test_search_table_suggestions_returns_fqn_matches() -> None:
    db = _build_session()
    user, table = _seed_graph(db)

    by_name = search_table_suggestions(db=db, q="orders", limit=10, current_user=user)
    by_schema = search_table_suggestions(db=db, q="bronze.orders", limit=10, current_user=user)
    by_source = search_table_suggestions(db=db, q="operational-source", limit=10, current_user=user)

    assert len(by_name) == 1
    assert by_name[0].id == table.id
    assert by_name[0].table_fqn == "operational-source.catalog.bronze.orders"
    assert by_schema[0].table_fqn == "operational-source.catalog.bronze.orders"
    assert by_source[0].table_fqn == "operational-source.catalog.bronze.orders"


if __name__ == "__main__":
    test_search_table_suggestions_returns_fqn_matches()
    print("catalog table search tests: OK")
