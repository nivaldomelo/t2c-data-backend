from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.contracts.service import contract_summary, create_contract, get_current_contract, validate_contract
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
    table = TableEntity(name="audit_logs", table_type="table", schema=schema)
    db.add_all([role, user, datasource, database, schema, table])
    db.commit()

    columns = [
        ColumnEntity(table_id=table.id, name="id", data_type="int", ordinal_position=1, is_nullable=False, is_primary_key=True),
        ColumnEntity(table_id=table.id, name="created_at", data_type="timestamp", ordinal_position=2, is_nullable=False),
    ]
    db.add_all(columns)
    db.commit()
    return user, table


def test_contract_creation_and_validation() -> None:
    db = _build_session()
    user, table = _seed_table(db)

    contract = create_contract(
        db,
        table_id=table.id,
        payload={
            "status": "published",
            "description": "Contrato básico",
            "columns": [
                {"column_name": "id", "data_type": "int", "is_primary_key": True, "is_nullable": False},
                {"column_name": "created_at", "data_type": "timestamp", "is_nullable": False},
            ],
        },
        created_by_user_id=user.id,
    )
    assert contract.version == 1

    summary = contract_summary(db, table_id=table.id)
    assert summary["status"] == "published"

    validation = validate_contract(db, contract_id=contract.id, created_by_user_id=user.id)
    assert validation.status == "passed"

    current = get_current_contract(db, table_id=table.id)
    assert current is not None
    assert current.last_validation_status == "passed"

    incompatible_contract = create_contract(
        db,
        table_id=table.id,
        payload={
            "status": "published",
            "description": "Contrato com coluna ausente",
            "columns": [
                {"column_name": "id", "data_type": "int", "is_primary_key": True, "is_nullable": False},
                {"column_name": "event_type", "data_type": "text", "is_nullable": False},
            ],
        },
        created_by_user_id=user.id,
    )
    incompatible_validation = validate_contract(db, contract_id=incompatible_contract.id, created_by_user_id=user.id)
    assert incompatible_validation.status == "failed"
    assert incompatible_contract.last_validation_status == "failed"
