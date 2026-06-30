from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, delete, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.security import hash_password
from t2c_data.features.catalog.column_dictionary_admin import (
    clear_column_dictionary_item,
    reset_column_dictionary_curation,
)
from t2c_data.features.glossary.api_support import reset_glossary_terms
from t2c_data.features.tags.api_support import reset_tags
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.tag import Tag, TagAssignment


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


def _seed_scope_catalog(db: Session) -> tuple[User, ColumnEntity, ColumnEntity, Tag, Tag, GlossaryTerm, GlossaryTerm]:
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
    local_schema = Schema(name="bronze", database=local_db)
    local_table = TableEntity(name="customers", table_type="table", schema=local_schema)
    local_db.description_source = "Base local"
    local_db.description_manual = "Base manual local"
    local_schema.description_source = "Schema fonte local"
    local_schema.description_manual = "Schema manual local"
    local_table.description_source = "Tabela fonte local"
    local_table.description_manual = "Tabela manual local"

    demo = DataSource(
        name="demo-source",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="demo",
        username="nivasmelo",
    )
    demo.password = "secret"
    demo_db = Database(name="demo", datasource=demo)
    demo_schema = Schema(name="demo", database=demo_db)
    demo_table = TableEntity(name="events", table_type="table", schema=demo_schema)
    demo_db.description_source = "Base demo"
    demo_db.description_manual = "Base manual demo"
    demo_schema.description_source = "Schema fonte demo"
    demo_schema.description_manual = "Schema manual demo"
    demo_table.description_source = "Tabela fonte demo"
    demo_table.description_manual = "Tabela manual demo"

    db.add_all([local, local_db, local_schema, local_table, demo, demo_db, demo_schema, demo_table])
    db.flush()

    local_column = ColumnEntity(
        table=local_table,
        name="id",
        data_type="integer",
        is_primary_key=True,
        is_nullable=False,
        ordinal_position=1,
        description_source="Identificador local",
        description_manual="Descrição manual local",
        dictionary_description="Identificador do cliente local",
        dictionary_comment="Curadoria local",
        existing_comment="Comentário existente local",
        slug="local-customers-id",
        external_id="col-local-1",
    )
    demo_column = ColumnEntity(
        table=demo_table,
        name="id",
        data_type="integer",
        is_primary_key=True,
        is_nullable=False,
        ordinal_position=1,
        description_source="Identificador demo",
        description_manual="Descrição manual demo",
        dictionary_description="Identificador demo",
        dictionary_comment="Curadoria demo",
        existing_comment="Comentário existente demo",
        slug="demo-events-id",
        external_id="col-demo-1",
    )

    tag_local = Tag(slug="financeiro", name="Financeiro", description="Tag local", group_name="governance", status="active")
    tag_demo = Tag(slug="demo", name="Demo", description="Tag demo", group_name="governance", status="active")

    term_local = GlossaryTerm(
        slug="cliente",
        name="Cliente",
        definition="Entidade de cliente",
        description="Termo local",
        category="negocio",
        status="active",
    )
    term_demo = GlossaryTerm(
        slug="evento-demo",
        name="Evento Demo",
        definition="Entidade demo",
        description="Termo demo",
        category="demo",
        status="active",
    )

    db.add_all([local_column, demo_column, tag_local, tag_demo, term_local, term_demo])
    db.flush()

    db.add_all(
        [
            TagAssignment(tag=tag_local, datasource_id=local.id, entity_type="table", entity_id=local_table.id),
            TagAssignment(tag=tag_demo, datasource_id=demo.id, entity_type="table", entity_id=demo_table.id),
            GlossaryAssignment(term=term_local, datasource_id=local.id, entity_type="table", entity_id=local_table.id),
            GlossaryAssignment(term=term_demo, datasource_id=demo.id, entity_type="table", entity_id=demo_table.id),
        ]
    )

    user = User(
        email="editor@example.com",
        name="Editor",
        full_name="Editor",
        password_hash=hash_password("secret123"),
        is_active=True,
    )
    user.roles = [role_editor]
    db.add(user)
    db.flush()
    db.add_all(
        [
            DataAccessGrant(user=user, effect="allow", datasource=local),
            DataAccessGrant(user=user, effect="allow", schema=local_schema),
        ]
    )
    db.commit()
    return user, local_column, demo_column, tag_local, tag_demo, term_local, term_demo


def test_dictionary_clear_and_reset_delete_all_rows() -> None:
    db = _build_session()
    user, local_column, demo_column, *_ = _seed_scope_catalog(db)

    cleared = clear_column_dictionary_item(db, column_id=local_column.id, current_user=user)
    assert cleared.dictionary_description is None
    assert cleared.dictionary_comment is None
    assert cleared.existing_comment is None
    assert cleared.external_id is None
    assert cleared.description_manual is None
    assert cleared.description_source is None
    assert cleared.slug is None

    demo_before = db.get(ColumnEntity, demo_column.id)
    assert demo_before is not None
    assert demo_before.dictionary_description == "Identificador demo"
    assert demo_before.description_manual == "Descrição manual demo"

    local_before_reset = db.get(ColumnEntity, local_column.id)
    assert local_before_reset is not None
    local_before_reset.external_id = "col-local-1"
    local_before_reset.description_manual = "Descrição manual local"
    local_before_reset.existing_comment = "Comentário existente local"
    local_before_reset.dictionary_description = "Identificador do cliente local"
    local_before_reset.dictionary_comment = "Curadoria local"
    db.commit()

    deleted = reset_column_dictionary_curation(db, current_user=user)
    assert deleted == 2

    local_after = db.get(ColumnEntity, local_column.id)
    demo_after = db.get(ColumnEntity, demo_column.id)
    local_table_after = db.get(TableEntity, local_column.table_id)
    local_schema_after = db.get(Schema, local_table_after.schema_id) if local_table_after else None
    local_db_after = db.get(Database, local_schema_after.database_id) if local_schema_after else None
    demo_table_after = db.get(TableEntity, demo_column.table_id)
    demo_schema_after = db.get(Schema, demo_table_after.schema_id) if demo_table_after else None
    demo_db_after = db.get(Database, demo_schema_after.database_id) if demo_schema_after else None

    assert local_after is None
    assert demo_after is None
    assert local_table_after is not None and local_table_after.description_source == "Tabela fonte local"
    assert local_schema_after is not None and local_schema_after.description_source == "Schema fonte local"
    assert local_db_after is not None and local_db_after.description_source == "Base local"

    assert demo_table_after is not None and demo_table_after.description_source == "Tabela fonte demo"
    assert demo_schema_after is not None and demo_schema_after.description_source == "Schema fonte demo"
    assert demo_db_after is not None and demo_db_after.description_source == "Base demo"


def test_tags_delete_and_reset_remove_assignments() -> None:
    db = _build_session()
    _, _, _, tag_local, tag_demo, _, _ = _seed_scope_catalog(db)
    tag_local_id = tag_local.id
    tag_demo_id = tag_demo.id

    db.execute(delete(TagAssignment).where(TagAssignment.tag_id == tag_local_id))
    db.delete(tag_local)
    db.commit()

    assert db.scalar(select(TagAssignment.id).where(TagAssignment.tag_id == tag_local_id)) is None
    assert db.get(Tag, tag_local_id) is None

    deleted_tags, deleted_assignments, deleted_overrides, deleted_events = reset_tags(db)
    assert deleted_tags == 1
    assert deleted_assignments == 1
    assert deleted_overrides == 0
    assert deleted_events == 0
    assert db.scalar(select(Tag.id)) is None
    assert db.scalar(select(TagAssignment.id)) is None
    assert db.get(Tag, tag_demo_id) is None


def test_glossary_delete_and_reset_remove_assignments() -> None:
    db = _build_session()
    _, _, _, _, _, term_local, term_demo = _seed_scope_catalog(db)
    term_local_id = term_local.id
    term_demo_id = term_demo.id

    db.execute(delete(GlossaryAssignment).where(GlossaryAssignment.term_id == term_local_id))
    db.delete(term_local)
    db.commit()

    assert db.scalar(select(GlossaryAssignment.id).where(GlossaryAssignment.term_id == term_local_id)) is None
    assert db.get(GlossaryTerm, term_local_id) is None

    deleted_terms, deleted_assignments = reset_glossary_terms(db)
    assert deleted_terms == 1
    assert deleted_assignments == 1
    assert db.scalar(select(GlossaryTerm.id)) is None
    assert db.scalar(select(GlossaryAssignment.id)) is None
    assert db.get(GlossaryTerm, term_demo_id) is None


if __name__ == "__main__":
    test_dictionary_clear_and_reset_keep_structure_and_scope()
    test_tags_delete_and_reset_remove_assignments()
    test_glossary_delete_and_reset_remove_assignments()
    print("curation cleanup tests: OK")
