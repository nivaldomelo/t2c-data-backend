from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import t2c_data.features.tags.intelligence as tag_intelligence
import t2c_data.features.governance.classification_review as classification_review_module
from t2c_data.features.governance.classification_review import get_governance_classification_review
from t2c_data.features.tags.intelligence import reprocess_table_tag_intelligence
from t2c_data.models import Base
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.schemas.governance import ClassificationReviewOut


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session():
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
    return SessionLocal


def _seed_catalog(db: Session) -> TableEntity:
    datasource = DataSource(
        name="local-andromeda",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="nivasmelo",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="customers",
        table_type="table",
        schema=schema,
        description_manual="Cadastro de clientes com dados pessoais e contato.",
        owner="Governança",
        sensitivity_level="restricted_sensitive",
        has_personal_data=True,
        has_sensitive_personal_data=False,
    )
    db.add_all([datasource, database, schema, table])
    db.flush()

    db.add_all(
        [
            ColumnEntity(
                table=table,
                name="cpf",
                data_type="varchar",
                is_primary_key=False,
                is_nullable=False,
                ordinal_position=1,
                dictionary_description="Documento pessoal do cliente",
            ),
            ColumnEntity(
                table=table,
                name="email",
                data_type="varchar",
                is_primary_key=False,
                is_nullable=True,
                ordinal_position=2,
                dictionary_description="Canal de contato do cliente",
            ),
        ]
    )
    db.add_all(
        [
            GlossaryTerm(
                external_id=None,
                slug="pii",
                name="PII",
                definition="Dado pessoal identificável.",
                description="Contexto de privacidade.",
                steward="Governança",
                category="privacidade",
                subcategory="pessoal",
                status="active",
            ),
            GlossaryTerm(
                external_id=None,
                slug="cliente",
                name="Cliente",
                definition="Entidade de negócio cliente.",
                description="Termo de negócio.",
                steward="Negócio",
                category="negocio",
                subcategory="cadastro",
                status="active",
            ),
        ]
    )
    db.flush()
    term = db.scalar(select(GlossaryTerm).where(GlossaryTerm.slug == "pii"))
    if term is None:
        raise AssertionError("expected term")
    db.add(GlossaryAssignment(term_id=term.id, entity_type="table", entity_id=table.id))
    db.commit()
    return table


def test_classification_review_returns_unified_queue() -> None:
    SessionLocal = _build_session()

    with SessionLocal() as session:
        tag_intelligence.write_audit_log_sync = lambda *args, **kwargs: None  # type: ignore[assignment]
        table = _seed_catalog(session)
        reprocess_table_tag_intelligence(session, table_id=table.id, actor_user_id=None)

        payload = ClassificationReviewOut(**get_governance_classification_review(session, current_user=None, page_size=100))

    assert payload.total > 0
    assert payload.summary.pending_reviews == payload.total
    assert payload.summary.probable_pii > 0
    assert any(item.kind == "gap" for item in payload.items)
    assert any(item.entity_level in {"table", "column"} for item in payload.items)


def test_promote_classification_review_tables_triggers_refresh_for_selected_tables() -> None:
    SessionLocal = _build_session()

    with SessionLocal() as session:
        called: dict[str, object] = {}
        original_refresh = classification_review_module.refresh_governance_recommendations
        original_audit = classification_review_module.write_audit_log_sync
        classification_review_module.refresh_governance_recommendations = lambda *_args, **kwargs: called.update(kwargs) or {
            "generated_at": datetime.now(timezone.utc),
            "created": 1,
            "updated": 0,
            "reopened": 0,
            "resolved": 0,
            "purged": 0,
            "retention_days": 90,
        }  # type: ignore[assignment]
        classification_review_module.write_audit_log_sync = lambda *args, **kwargs: None  # type: ignore[assignment]
        try:
            requested_table_ids = [7, 7, 3]
            result = classification_review_module.promote_governance_classification_review_tables(
                session,
                table_ids=requested_table_ids,
                current_user=None,
                request_audit={"route": "/governance/classification-review/batch/promote"},
            )
            assert requested_table_ids == [7, 7, 3]
            assert called["table_ids"] == [7, 3]
            assert result["requested_table_ids"] == [7, 3]
            assert result["promoted_count"] == 2
            assert result["refresh_created"] == 1
        finally:
            classification_review_module.refresh_governance_recommendations = original_refresh  # type: ignore[assignment]
            classification_review_module.write_audit_log_sync = original_audit  # type: ignore[assignment]


if __name__ == "__main__":
    test_classification_review_returns_unified_queue()
    print("governance classification review tests: OK")
