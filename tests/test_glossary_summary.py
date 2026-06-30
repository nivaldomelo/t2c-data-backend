from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.glossary.api_support import glossary_summary_payload, list_terms_payload
from t2c_data.models import Base
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm


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


def _seed_terms(db: Session) -> tuple[GlossaryTerm, GlossaryTerm, GlossaryTerm]:
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
    table = TableEntity(name="customers", table_type="table", schema=schema)
    column = ColumnEntity(
        table=table,
        name="id",
        data_type="integer",
        is_primary_key=True,
        is_nullable=False,
        ordinal_position=1,
    )
    db.add_all([datasource, database, schema, table, column])
    db.flush()

    term_in_use = GlossaryTerm(
        slug="cliente",
        name="Cliente",
        definition="Entidade cliente",
        description="Termo de negócio",
        category="negocio",
        status="active",
    )
    term_unused = GlossaryTerm(
        slug="boleto",
        name="Boleto",
        definition="Título bancário",
        description="Termo financeiro",
        category="financeiro",
        status="active",
    )
    term_inactive = GlossaryTerm(
        slug="conta-corrente",
        name="Conta corrente",
        definition="Conta bancária",
        description="Termo bancário",
        category="financeiro",
        status="inactive",
    )
    db.add_all([term_in_use, term_unused, term_inactive])
    db.flush()
    db.add(GlossaryAssignment(term_id=term_in_use.id, datasource_id=datasource.id, entity_type="table", entity_id=table.id))
    db.commit()
    return term_in_use, term_unused, term_inactive


def test_glossary_summary_and_use_filters() -> None:
    SessionLocal = _build_session()

    with SessionLocal() as session:
        term_in_use, term_unused, term_inactive = _seed_terms(session)
        expected_used_slug = term_in_use.slug
        expected_unused_slugs = sorted([term_unused.slug, term_inactive.slug])

        summary = glossary_summary_payload(
            db=session,
            query=None,
            category=None,
            subcategory=None,
            status_filter=None,
            priority=None,
        )
        used_terms = list_terms_payload(
            db=session,
            query=None,
            category=None,
            subcategory=None,
            status_filter=None,
            priority=None,
            in_use=True,
        )
        unused_terms = list_terms_payload(
            db=session,
            query=None,
            category=None,
            subcategory=None,
            status_filter=None,
            priority=None,
            without_use=True,
        )

    assert summary == {"total": 3, "active": 2, "in_use": 1, "categories": 2}
    assert [item.slug for item in used_terms] == [expected_used_slug]
    assert sorted(item.slug for item in unused_terms) == expected_unused_slugs
