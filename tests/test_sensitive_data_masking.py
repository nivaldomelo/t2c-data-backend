from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.table_detail import build_table_detail_out
from t2c_data.features.platform.sensitive_data import mask_sensitive_value
from t2c_data.models import Base
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity


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


def _seed_table(db: Session) -> TableEntity:
    owner = DataOwner(name="Maria Silva", email="maria.silva@example.com", area="Operações", is_active=True)
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
    table = TableEntity(
        name="clientes",
        table_type="table",
        schema=schema,
        owner="Maria Silva",
        owner_email="maria.silva@example.com",
        data_owner=owner,
        has_personal_data=True,
        has_sensitive_personal_data=True,
        sensitivity_level="restricted",
        certification_status="not_eligible",
        certification_criticality="high",
    )
    db.add_all([owner, datasource, database, schema, table])
    db.commit()
    db.refresh(table)
    return table


def test_mask_sensitive_value_masks_owner_and_email_fields() -> None:
    assert mask_sensitive_value("Maria Silva", field_name="owner") == "[masked]"
    assert mask_sensitive_value("maria.silva@example.com", field_name="owner_email") == "[masked]"
    assert mask_sensitive_value("123.456.789-10", field_name="cpf") == "[masked]"
    assert mask_sensitive_value("(11) 99999-9999", field_name="telefone") == "[masked]"
    assert mask_sensitive_value("1234 5678 9012 3456", field_name="bank_account") == "[masked]"


def test_build_table_detail_out_masks_personal_owner_data_without_sensitive_permission(monkeypatch) -> None:
    db = _build_session()
    table = _seed_table(db)
    monkeypatch.setattr("t2c_data.features.metabase.impact.get_table_metabase_impact", lambda *_args, **_kwargs: None)

    detail = build_table_detail_out(db, table, can_view_sensitive=False)

    assert detail.owner == "[masked]"
    assert detail.owner_email == "[masked]"
    assert detail.data_owner is not None
    assert detail.data_owner.name == "[masked]"
    assert detail.data_owner.email == "[masked]"
    assert detail.name == "clientes"


if __name__ == "__main__":
    test_mask_sensitive_value_masks_owner_and_email_fields()
    test_build_table_detail_out_masks_personal_owner_data_without_sensitive_permission()
    print("sensitive data masking tests: OK")
