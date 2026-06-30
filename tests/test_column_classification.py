from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.governance.column_classification import (
    build_column_classification_candidate,
    build_column_classification_map,
    column_classification_payload,
    load_column_classification_history,
    record_column_classification_decision,
)
from t2c_data.features.platform.sensitive_data import mask_row_by_classification
from t2c_data.models import Base
from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity


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


def _seed_table(db: Session) -> tuple[TableEntity, ColumnEntity, ColumnEntity, ColumnEntity]:
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
        name="contratos",
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
    cpf = ColumnEntity(table=table, name="cpf", data_type="varchar", ordinal_position=1, is_nullable=False)
    credit_value = ColumnEntity(table=table, name="valor_credito", data_type="numeric", ordinal_position=2, is_nullable=False)
    status = ColumnEntity(table=table, name="status_operacional", data_type="varchar", ordinal_position=3, is_nullable=True)
    db.add_all([owner, datasource, database, schema, table, cpf, credit_value, status])
    db.commit()
    db.refresh(table)
    db.refresh(cpf)
    db.refresh(credit_value)
    db.refresh(status)
    return table, cpf, credit_value, status


def test_financial_heuristic_detects_credit_value() -> None:
    db = _build_session()
    _table, _cpf, credit_value, _status = _seed_table(db)

    candidate = build_column_classification_candidate(credit_value, table=credit_value.table)

    assert candidate["classified"] is True
    assert candidate["suggestion"] is not None
    suggestion = candidate["suggestion"]
    assert suggestion["taxonomy_key"] == "valor_credito"
    assert suggestion["is_financial_data"] is True
    assert suggestion["is_sensitive_data"] is True


def test_column_classification_review_versions_history_and_current_state() -> None:
    db = _build_session()
    _table, _cpf, credit_value, _status = _seed_table(db)

    approved = record_column_classification_decision(
        db,
        column_id=credit_value.id,
        taxonomy_key="valor_credito",
        source_kind="heuristic",
        confidence_score=93,
        decision_status="approved",
        evidence_json={"matched_keyword": "valor_credito"},
        reviewed_by_user_id=None,
    )
    rejected = record_column_classification_decision(
        db,
        column_id=credit_value.id,
        taxonomy_key="dado_operacional",
        source_kind="manual",
        confidence_score=45,
        decision_status="rejected",
        evidence_json={"reason": "false_positive"},
        reviewed_by_user_id=None,
        persist_current=False,
    )

    history = load_column_classification_history(db, credit_value.id)

    assert approved["taxonomy_key"] == "valor_credito"
    assert rejected["decision_status"] == "rejected"
    assert len(history) == 2
    current_classification = db.scalar(
        __import__("sqlalchemy").select(__import__("t2c_data.models.classification").models.classification.ColumnClassification).where(  # type: ignore[attr-defined]
            __import__("t2c_data.models.classification").models.classification.ColumnClassification.column_id == credit_value.id
        )
    )
    assert current_classification is not None
    payload = column_classification_payload(current_classification)
    assert payload is not None
    assert payload["taxonomy_key"] == "valor_credito"
    assert payload["is_financial_data"] is True


def test_column_classification_masking_uses_column_classification_map() -> None:
    db = _build_session()
    table, cpf, credit_value, status = _seed_table(db)

    record_column_classification_decision(
        db,
        column_id=cpf.id,
        taxonomy_key="cpf",
        source_kind="heuristic",
        confidence_score=98,
        decision_status="approved",
        evidence_json={"matched_keyword": "cpf"},
        reviewed_by_user_id=None,
    )
    record_column_classification_decision(
        db,
        column_id=credit_value.id,
        taxonomy_key="valor_credito",
        source_kind="heuristic",
        confidence_score=95,
        decision_status="approved",
        evidence_json={"matched_keyword": "valor_credito"},
        reviewed_by_user_id=None,
    )
    db.commit()

    classification_map = build_column_classification_map(db, table_id=table.id, key_by="name")
    masked = mask_row_by_classification(
        {"cpf": "123.456.789-10", "valor_credito": 150000, "status_operacional": "ativo"},
        can_view_sensitive=False,
        column_classifications=classification_map,
    )

    assert masked["cpf"] == "[masked]"
    assert masked["valor_credito"] == "[masked]"
    assert masked["status_operacional"] == "ativo"


if __name__ == "__main__":
    test_financial_heuristic_detects_credit_value()
    test_column_classification_review_versions_history_and_current_state()
    test_column_classification_masking_uses_column_classification_map()
    print("column classification tests: OK")
