from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
import t2c_data.features.tags.intelligence as tag_intelligence
from t2c_data.features.tags.intelligence import (
    apply_tag_intelligence_event,
    batch_apply_tag_intelligence_events,
    batch_dismiss_tag_intelligence_events,
    dismiss_tag_intelligence_event,
    ensure_core_intelligence_tags,
    load_pending_tag_intelligence_events,
    reprocess_table_tag_intelligence,
)
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.tag import Tag, TagAssignment, TagAssignmentOverride, TagIntelligenceEvent, TagAutomationRule


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _):  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        conn.exec_driver_sql("INSERT INTO users (id) VALUES (7), (11)")
        conn.exec_driver_sql("CREATE TABLE data_owners (id INTEGER PRIMARY KEY)")
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        ColumnEntity.__table__.create(bind=conn)
        Tag.__table__.create(bind=conn)
        TagAssignment.__table__.create(bind=conn)
        TagAssignmentOverride.__table__.create(bind=conn)
        TagIntelligenceEvent.__table__.create(bind=conn)
        TagAutomationRule.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed_table(session):
    datasource = DataSource(
        name="local-andromeda",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="tester",
    )
    datasource.set_secret_values({"password": "secret"})
    session.add(datasource)
    session.flush()

    database = Database(datasource_id=datasource.id, name="andromeda")
    session.add(database)
    session.flush()

    schema = Schema(database_id=database.id, name="bronze")
    session.add(schema)
    session.flush()

    table = TableEntity(
        schema_id=schema.id,
        name="customer_profile",
        table_type="table",
        description_manual="Cadastro de clientes com dados sensíveis e identificadores pessoais.",
        owner="Governança",
    )
    session.add(table)
    session.flush()

    columns = [
        ColumnEntity(
            table_id=table.id,
            name="customer_name",
            data_type="varchar",
            is_primary_key=False,
            is_nullable=False,
            ordinal_position=1,
            dictionary_description="Nome do cliente",
        ),
        ColumnEntity(
            table_id=table.id,
            name="customer_email",
            data_type="numeric",
            is_primary_key=False,
            is_nullable=True,
            ordinal_position=2,
            dictionary_description="Email de contato do cliente",
        ),
    ]
    session.add_all(columns)
    session.flush()
    ensure_core_intelligence_tags(session)
    return table, columns


def test_reprocess_table_tag_intelligence_applies_column_and_table_tags():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        tag_intelligence.write_audit_log_sync = lambda *args, **kwargs: None  # type: ignore[assignment]
        table, columns = _seed_table(session)
        summary = reprocess_table_tag_intelligence(session, table_id=table.id, actor_user_id=None)
        assignments = session.execute(
            select(Tag.slug, TagAssignment.entity_type, TagAssignment.entity_id)
            .join(Tag, Tag.id == TagAssignment.tag_id)
            .where(TagAssignment.entity_id.in_([table.id, columns[0].id, columns[1].id]))
        ).all()
        events = session.scalars(select(TagIntelligenceEvent)).all()

    assignment_slugs = {(slug, entity_type) for slug, entity_type, _ in assignments}

    assert summary["table_id"] == table.id
    assert summary["column_tags_applied"] >= 2
    assert ("nome", "column") in assignment_slugs
    assert ("email", "column") in assignment_slugs
    assert len(events) > 0


def test_pending_queue_apply_and_block():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        table, columns = _seed_table(session)
        datasource_id = session.scalar(select(DataSource.id)) or 0
        tag = session.scalar(select(Tag).where(Tag.slug == "pii"))
        if tag is None:
            raise AssertionError("expected core tag")
        pending_event = TagIntelligenceEvent(
            tag_id=tag.id,
            datasource_id=int(datasource_id),
            entity_type="column",
            entity_id=columns[0].id,
            rule_key="column_pii",
            rule_label="Nome ou comentário indicam dado pessoal",
            inference_source="dictionary_description",
            inference_reason="Documento fiscal do cliente",
            confidence_score=72,
            applied_automatically=False,
            review_status="suggested",
            evidence={"column_name": "cpf"},
        )
        blocked_event = TagIntelligenceEvent(
            tag_id=tag.id,
            datasource_id=int(datasource_id),
            entity_type="table",
            entity_id=table.id,
            rule_key="table_pii",
            rule_label="Contém PII",
            inference_source="column_tags",
            inference_reason="Existem colunas PII",
            confidence_score=84,
            applied_automatically=False,
            review_status="suggested",
            evidence={"source_columns": ["cpf"]},
        )
        session.add_all([pending_event, blocked_event])
        session.commit()

        queue = load_pending_tag_intelligence_events(session, limit=10)
        apply_result = apply_tag_intelligence_event(session, event_id=pending_event.id, actor_user_id=7)
        block_result = dismiss_tag_intelligence_event(session, event_id=blocked_event.id, actor_user_id=7)
        assignment = session.scalar(
            select(TagAssignment).where(
                TagAssignment.tag_id == tag.id,
                TagAssignment.entity_type == "column",
                TagAssignment.entity_id == columns[0].id,
            )
        )
        override = session.scalar(
            select(TagAssignmentOverride).where(
                TagAssignmentOverride.tag_id == tag.id,
                TagAssignmentOverride.entity_type == "table",
                TagAssignmentOverride.entity_id == table.id,
            )
        )
        refreshed_events = session.execute(
            select(TagIntelligenceEvent.review_status, TagIntelligenceEvent.reviewed_by_user_id)
            .where(TagIntelligenceEvent.id.in_([pending_event.id, blocked_event.id]))
        ).all()

    assert len(queue) == 2
    assert apply_result["status"] == "manual_applied"
    assert block_result["status"] == "blocked"
    assert assignment is not None
    assert override is not None
    assert sorted((row.review_status, row.reviewed_by_user_id) for row in refreshed_events) == [
        ("blocked", 7),
        ("manual_applied", 7),
    ]


def test_pending_queue_filters_sort_and_batch_actions():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        table, columns = _seed_table(session)
        datasource_id = session.scalar(select(DataSource.id)) or 0
        tag = session.scalar(select(Tag).where(Tag.slug == "pii"))
        if tag is None:
            raise AssertionError("expected core tag")
        other_tag = session.scalar(select(Tag).where(Tag.slug == "sensivel"))
        if other_tag is None:
            raise AssertionError("expected core tag")
        events = [
            TagIntelligenceEvent(
                tag_id=tag.id,
                datasource_id=int(datasource_id),
                entity_type="column",
                entity_id=columns[0].id,
                rule_key="column_pii",
                rule_label="Nome ou comentário indicam dado pessoal",
                inference_source="dictionary_description",
                inference_reason="Documento fiscal do cliente",
                confidence_score=55,
                applied_automatically=False,
                review_status="suggested",
                evidence={"column_name": "cpf"},
            ),
            TagIntelligenceEvent(
                tag_id=other_tag.id,
                datasource_id=int(datasource_id),
                entity_type="column",
                entity_id=columns[1].id,
                rule_key="column_sensitive",
                rule_label="Nome ou comentário indicam dado sensível",
                inference_source="dictionary_description",
                inference_reason="Remuneração mensal do colaborador",
                confidence_score=91,
                applied_automatically=False,
                review_status="suggested",
                evidence={"column_name": "salario_mensal"},
            ),
            TagIntelligenceEvent(
                tag_id=tag.id,
                datasource_id=int(datasource_id),
                entity_type="table",
                entity_id=table.id,
                rule_key="table_pii",
                rule_label="Contém PII",
                inference_source="column_tags",
                inference_reason="Existem colunas PII",
                confidence_score=63,
                applied_automatically=False,
                review_status="suggested",
                evidence={"source_columns": ["cpf"]},
            ),
        ]
        session.add_all(events)
        session.commit()

        filtered_table = load_pending_tag_intelligence_events(session, limit=10, table_query="customer")
        filtered_column = load_pending_tag_intelligence_events(session, limit=10, column_query="email")
        risk_sorted = load_pending_tag_intelligence_events(session, limit=10, sort_by="risk_desc")
        certainty_sorted = load_pending_tag_intelligence_events(session, limit=10, sort_by="certainty_desc")
        batch_apply = batch_apply_tag_intelligence_events(session, event_ids=[events[0].id, events[1].id], actor_user_id=11)
        batch_block = batch_dismiss_tag_intelligence_events(session, event_ids=[events[2].id], actor_user_id=11)
        applied_assignment = session.scalar(
            select(TagAssignment).where(
                TagAssignment.tag_id == tag.id,
                TagAssignment.entity_type == "column",
                TagAssignment.entity_id == columns[0].id,
            )
        )
        blocked_override = session.scalar(
            select(TagAssignmentOverride).where(
                TagAssignmentOverride.tag_id == tag.id,
                TagAssignmentOverride.entity_type == "table",
                TagAssignmentOverride.entity_id == table.id,
            )
        )

    assert len(filtered_table) == 3
    assert all("customer_profile" in (item["table_fqn"] or "") or (item["table_name"] or "") == "customer_profile" for item in filtered_table)
    assert filtered_table[0]["explorer_url"] is not None
    assert len(filtered_column) == 1
    assert filtered_column[0]["column_name"] == "customer_email"
    assert [item["confidence_score"] for item in risk_sorted[:3]] == [55, 63, 91]
    assert [item["confidence_score"] for item in certainty_sorted[:3]] == [91, 63, 55]
    assert batch_apply["succeeded"] == 2
    assert batch_block["succeeded"] == 1
    assert applied_assignment is not None
    assert blocked_override is not None


def test_table_flags_do_not_apply_semantic_tags_to_all_columns():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        tag_intelligence.write_audit_log_sync = lambda *args, **kwargs: None  # type: ignore[assignment]
        table, columns = _seed_table(session)
        table.name = "audit_logs"
        table.description_manual = "Tabela de auditoria do sistema"
        table.has_personal_data = True
        table.has_sensitive_personal_data = True
        session.add_all(
            [
                ColumnEntity(
                    table_id=table.id,
                    name="id",
                    data_type="uuid",
                    is_primary_key=True,
                    is_nullable=False,
                    ordinal_position=3,
                ),
                ColumnEntity(
                    table_id=table.id,
                    name="entity_id",
                    data_type="uuid",
                    is_primary_key=False,
                    is_nullable=False,
                    ordinal_position=4,
                ),
                ColumnEntity(
                    table_id=table.id,
                    name="created_at",
                    data_type="timestamp",
                    is_primary_key=False,
                    is_nullable=False,
                    ordinal_position=5,
                ),
                ColumnEntity(
                    table_id=table.id,
                    name="action_name",
                    data_type="varchar",
                    is_primary_key=False,
                    is_nullable=False,
                    ordinal_position=6,
                ),
            ]
        )
        session.flush()

        summary = reprocess_table_tag_intelligence(session, table_id=table.id, actor_user_id=None)
        assignments = session.execute(
            select(Tag.slug, TagAssignment.entity_type)
            .join(Tag, Tag.id == TagAssignment.tag_id)
            .where(TagAssignment.entity_type == "column")
        ).all()

    column_slugs = {slug for slug, _ in assignments}
    forbidden = {"contato", "documento", "endereco", "pii", "sensivel", "financeiro"}
    assert summary["table_id"] == table.id
    assert forbidden.isdisjoint(column_slugs)


def test_pending_queue_supports_page_filters_and_new_sorts():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        table, columns = _seed_table(session)
        datasource_id = session.scalar(select(DataSource.id)) or 0
        tag = session.scalar(select(Tag).where(Tag.slug == "pii"))
        if tag is None:
            raise AssertionError("expected core tag")
        extra_tag = session.scalar(select(Tag).where(Tag.slug == "sensivel"))
        if extra_tag is None:
            raise AssertionError("expected core tag")
        session.add_all(
            [
                TagIntelligenceEvent(
                    tag_id=tag.id,
                    datasource_id=int(datasource_id),
                    entity_type="column",
                    entity_id=columns[0].id,
                    rule_key="column_pii",
                    rule_label="PII",
                    inference_source="dictionary_description",
                    inference_reason="Nome do cliente",
                    confidence_score=52,
                    applied_automatically=False,
                    review_status="suggested",
                    evidence={"column_name": "customer_name"},
                ),
                TagIntelligenceEvent(
                    tag_id=extra_tag.id,
                    datasource_id=int(datasource_id),
                    entity_type="table",
                    entity_id=table.id,
                    rule_key="table_sensitive",
                    rule_label="Sensível",
                    inference_source="regex",
                    inference_reason="Tabela financeira",
                    confidence_score=86,
                    applied_automatically=False,
                    review_status="suggested",
                    evidence={"table_name": "customer_profile"},
                ),
            ]
        )
        session.commit()

        all_events = load_pending_tag_intelligence_events(session, limit=None, sort_by="tag_asc")
        filtered_slug = load_pending_tag_intelligence_events(session, limit=None, tag_slug="sensivel")
        filtered_risk = load_pending_tag_intelligence_events(session, limit=None, risk_band="high")
        table_sorted = load_pending_tag_intelligence_events(session, limit=None, sort_by="table_asc")

    assert len(all_events) >= 2
    assert all_events[0]["tag_name"] <= all_events[-1]["tag_name"]
    assert len(filtered_slug) == 1
    assert filtered_slug[0]["tag_slug"] == "sensivel"
    assert len(filtered_risk) == 1
    assert filtered_risk[0]["confidence_score"] == 52
    assert table_sorted[0]["table_fqn"] is not None or table_sorted[0]["table_name"] is not None


def test_semantic_tags_respect_thresholds_and_technical_guards():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        tag_intelligence.write_audit_log_sync = lambda *args, **kwargs: None  # type: ignore[assignment]
        table, _columns = _seed_table(session)
        table.name = "audit_logs"
        table.description_manual = "Tabela de auditoria do sistema"
        session.add_all(
            [
                ColumnEntity(
                    table_id=table.id,
                    name="id",
                    data_type="uuid",
                    is_primary_key=True,
                    is_nullable=False,
                    ordinal_position=3,
                ),
                ColumnEntity(
                    table_id=table.id,
                    name="created_at",
                    data_type="timestamp",
                    is_primary_key=False,
                    is_nullable=False,
                    ordinal_position=4,
                ),
                ColumnEntity(
                    table_id=table.id,
                    name="details_json",
                    data_type="jsonb",
                    is_primary_key=False,
                    is_nullable=True,
                    ordinal_position=5,
                    dictionary_description="Payload pode conter CPF do cliente.",
                ),
            ]
        )
        session.flush()

        reprocess_table_tag_intelligence(session, table_id=table.id, actor_user_id=None)
        assignments = session.execute(
            select(Tag.slug, TagAssignment.entity_type, TagAssignment.entity_id)
            .join(Tag, Tag.id == TagAssignment.tag_id)
            .where(TagAssignment.entity_type == "column")
        ).all()
    column_slugs = {slug for slug, _entity_type, _entity_id in assignments}
    forbidden = {"contato", "documento", "endereco", "pii", "sensivel", "financeiro", "coluna-critica"}
    assert forbidden.isdisjoint(column_slugs)
    assert "json" in column_slugs


if __name__ == "__main__":
    test_reprocess_table_tag_intelligence_applies_column_and_table_tags()
    print("tag intelligence tests: OK")
