from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.timeline.service import (
    _dedupe_timeline_episodes,
    _dedupe_episode_keys,
    _episode_identifier,
    _profile_matches_filters,
    TimelineQuery,
    get_asset_timeline,
    get_governance_timeline,
    record_timeline_episode_action,
)
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRule, DQRuleRun, DQRun, DQTableMetric
from t2c_data.models.governance import AssetSla, GovernanceSettings, GovernanceTrustSnapshot, OperationalStabilitySnapshot
from t2c_data.models.incident import Incident
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.platform import TimelineEpisodeAction
from t2c_data.models.search import SearchResultClick
from t2c_data.models.tag import Tag, TagAssignment, TagIntelligenceEvent
from t2c_data.schemas.timeline import TimelineEpisodeActionIn
from t2c_data.schemas.timeline import TimelineEpisodeOut, TimelineEventOut


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _session_factory():
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

    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t2c_data.users (id INTEGER PRIMARY KEY)")
        conn.exec_driver_sql(
            """
            CREATE TABLE t2c_data.data_owners (
                id INTEGER PRIMARY KEY,
                name VARCHAR(160) NOT NULL,
                email VARCHAR(255) NOT NULL,
                area VARCHAR(160),
                description TEXT,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME,
                updated_at DATETIME
            )
            """
        )
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        ColumnEntity.__table__.create(bind=conn)
        Tag.__table__.create(bind=conn)
        TagAssignment.__table__.create(bind=conn)
        TagIntelligenceEvent.__table__.create(bind=conn)
        GlossaryTerm.__table__.create(bind=conn)
        GlossaryAssignment.__table__.create(bind=conn)
        AuditLog.__table__.create(bind=conn)
        GovernanceSettings.__table__.create(bind=conn)
        AssetSla.__table__.create(bind=conn)
        SearchResultClick.__table__.create(bind=conn)
        TimelineEpisodeAction.__table__.create(bind=conn)
        DQRun.__table__.create(bind=conn)
        DQTableMetric.__table__.create(bind=conn)
        DQRule.__table__.create(bind=conn)
        DQRuleRun.__table__.create(bind=conn)
        Incident.__table__.create(bind=conn)
        OperationalStabilitySnapshot.__table__.create(bind=conn)
        GovernanceTrustSnapshot.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed_catalog(session: Session) -> int:
    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="analytics",
        username="tester",
    )
    datasource.password = "secret"
    session.add(datasource)
    session.flush()

    database = Database(datasource_id=datasource.id, name="analytics")
    session.add(database)
    session.flush()

    schema = Schema(database_id=database.id, name="gold")
    session.add(schema)
    session.flush()

    data_owner = DataOwner(
        name="Governança",
        email="gov@example.com",
        area="Data Governance",
        description="Responsável pelo ativo",
        is_active=True,
    )
    session.add(data_owner)
    session.flush()

    table = TableEntity(
        schema_id=schema.id,
        data_owner_id=data_owner.id,
        name="audit_logs",
        table_type="table",
        description_manual="Log operacional com dados sensíveis e identificadores.",
        owner="Governança",
        owner_email="gov@example.com",
        certification_status="certified",
        sensitivity_level="restricted_sensitive",
        has_personal_data=True,
        has_sensitive_personal_data=True,
    )
    session.add(table)
    session.flush()

    columns = [
        ColumnEntity(
            table_id=table.id,
            name="cpf",
            data_type="varchar",
            is_primary_key=False,
            is_nullable=False,
            ordinal_position=1,
            dictionary_description="Documento pessoal do cliente",
        ),
        ColumnEntity(
            table_id=table.id,
            name="evento",
            data_type="varchar",
            is_primary_key=False,
            is_nullable=False,
            ordinal_position=2,
            dictionary_description="Descrição do evento de governança",
        ),
    ]
    session.add_all(columns)
    session.flush()

    pii_tag = Tag(
        external_id=None,
        slug="pii",
        name="PII",
        color="#ef4444",
        description="Dado pessoal identificável.",
        group_name="sensibilidade",
        subgroup_name="pessoal",
        tag_type="classification",
        suggested_scope="column",
        status="active",
    )
    table_tag = Tag(
        external_id=None,
        slug="contem-pii",
        name="Contém PII",
        color="#f59e0b",
        description="Tabela com colunas pessoais.",
        group_name="sensibilidade",
        subgroup_name="table",
        tag_type="classification",
        suggested_scope="table",
        status="active",
    )
    session.add_all([pii_tag, table_tag])
    session.flush()

    now = datetime.now(timezone.utc)
    session.add_all(
        [
            TagIntelligenceEvent(
                tag_id=pii_tag.id,
                datasource_id=datasource.id,
                entity_type="column",
                entity_id=columns[0].id,
                rule_key="column_pii",
                rule_label="Nome ou comentário indicam dado pessoal",
                inference_source="dictionary_description",
                inference_reason="Documento pessoal do cliente",
                confidence_score=96,
                applied_automatically=False,
                review_status="pending_review",
                evidence={"column_name": "cpf"},
            ),
            TagIntelligenceEvent(
                tag_id=table_tag.id,
                datasource_id=datasource.id,
                entity_type="table",
                entity_id=table.id,
                rule_key="table_pii",
                rule_label="Contém PII",
                inference_source="column_tags",
                inference_reason="Existem colunas PII",
                confidence_score=88,
                applied_automatically=True,
                review_status="applied",
                evidence={"source_columns": ["cpf"]},
            ),
            AuditLog(
                user_id=None,
                actor_name="Analista de governança",
                user_email="gov@example.com",
                ip=None,
                user_agent=None,
                action="table.metadata.patch",
                entity_type="table",
                entity_id=str(table.id),
                parent_entity_type=None,
                parent_entity_id=None,
                change_set_id="cs-1",
                change_type="update",
                field_name="owner",
                source_module="catalog",
                is_sensitive_change=False,
                sensitive_category=None,
                route="/api/v1/tables",
                method="PATCH",
                status_code=200,
                request_id="req-1",
                before_json={"owner": None},
                after_json={"owner": "Governança"},
                metadata_json={"message": "Owner atualizado"},
            ),
            AuditLog(
                user_id=None,
                actor_name="Analista de governança",
                user_email="gov@example.com",
                ip=None,
                user_agent=None,
                action="table.metadata.patch",
                entity_type="table",
                entity_id=str(table.id),
                parent_entity_type=None,
                parent_entity_id=None,
                change_set_id="cs-2",
                change_type="update",
                field_name="classification",
                source_module="catalog",
                is_sensitive_change=False,
                sensitive_category=None,
                route="/api/v1/tables",
                method="PATCH",
                status_code=200,
                request_id="req-2",
                before_json={"classification": "internal"},
                after_json={"classification": "restricted"},
                metadata_json={"message": "Classificação atualizada"},
            ),
            DQRun(
                datasource_id=datasource.id,
                table_id=table.id,
                scope="table",
                schema_name=schema.name,
                status="success",
                execution_engine="python",
                queued_at=now - timedelta(days=1, hours=2),
                started_at=now - timedelta(days=1, hours=2),
                finished_at=now - timedelta(days=1, hours=1, minutes=30),
            ),
        ]
    )
    session.flush()

    dq_run = session.scalar(select(DQRun).where(DQRun.table_id == table.id))
    if dq_run is None:
        raise AssertionError("expected dq run")
    session.add(
        DQTableMetric(
            run_id=dq_run.id,
            table_id=table.id,
            row_count=1000,
            column_count=2,
            completeness_pct_avg=88.0,
            dq_score=63.0,
            duplicates_count=4,
            failed_rules=2,
            metrics_json={"failed_rules": 2},
        )
    )
    dq_rule = DQRule(
        table_id=table.id,
        table_fqn=f"{datasource.name}.{schema.name}.{table.name}",
        name="cpf_not_null",
        description="CPF não pode ser nulo.",
        rule_type="row_violation",
        severity="high",
        is_active=True,
    )
    session.add(dq_rule)
    session.flush()
    session.add(
        DQRuleRun(
            rule_id=dq_rule.id,
            status="fail",
            execution_engine="python",
            violations_count=2,
            error_message="2 violações encontradas",
        )
    )
    session.add(
        Incident(
            title="Falha de atualização",
            description="Pipeline atrasado no carregamento diário.",
            entity_type="table",
            table_fqn=f"{schema.name}.{table.name}",
            airflow_dag_id="dag_audit_logs",
            detected_at=now - timedelta(days=1),
            last_seen_at=now - timedelta(hours=12),
            status="open",
            severity="sev1",
            owner_user_id=None,
            reporter_user_id=None,
            tags=["operacao"],
            source_type="dq_profile",
            source_ref_id=1,
            evidence_json={"status": "open"},
            occurrences=2,
        )
    )
    session.add(
        OperationalStabilitySnapshot(
            table_id=table.id,
            datasource_id=datasource.id,
            schema_name=schema.name,
            table_name=table.name,
            pipeline_name="daily_audit_logs",
            dag_id="dag_audit_logs",
            task_name="load_audit_logs",
            latest_status_label="Falha",
            last_success_at=now - timedelta(days=2),
            last_execution_finished_at=now - timedelta(days=1, hours=1),
            rows_processed=2500,
            window_runs=6,
            success_rate_pct=66.7,
            failed_runs=2,
            recurrent_degradation=True,
            currently_stale=True,
            bucket_start_at=now - timedelta(days=1),
        )
    )
    session.add(
        GovernanceTrustSnapshot(
            table_id=table.id,
            datasource_id=datasource.id,
            owner_name="Governança",
            domain_label="Risco",
            score=84,
            label="Confiável",
            tone="accent",
            readiness_score=88,
            governance_score=86,
            operational_score=82,
            dq_score=63.0,
            open_incidents=1,
            critical_open_incidents=1,
            active_dq_violation=True,
            recent_dq_failure_runs_30d=1,
            trust_context_json={
                "base_score": 84,
                "penalties": [{"key": "critical_incident", "label": "Incidente crítico aberto", "points": 16}],
                "adjustments": [{"key": "domain", "scope": "domain", "value": "risco", "points": 4, "label": "Domínio risco"}],
            },
            bucket_date=now - timedelta(days=1),
        )
    )
    session.commit()
    return int(table.id)


def test_asset_timeline_consolidates_governance_operation_quality_and_incidents() -> None:
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        table_id = _seed_catalog(session)
        payload = get_asset_timeline(session, table_id=table_id, current_user=None, page_size=50)

    categories = {item.category for item in payload.items}
    event_types = {item.event_type for item in payload.items}

    assert payload.scope == "asset"
    assert payload.total > 0
    assert payload.episode_total > 0
    assert {"governance", "operation", "quality", "incident"}.issubset(categories)
    assert "owner_changed" in event_types
    assert "tag_applied" in event_types or "tag_suggestion" in event_types
    assert any(item.event_type.startswith("trust_") for item in payload.items)
    assert any(item.active_dq_violation for item in payload.items)
    assert any(item.href and "explorer" in item.href for item in payload.items)
    assert any(episode.event_count > 1 for episode in payload.episodes)
    assert any("Episódio" in episode.title for episode in payload.episodes)
    assert len({episode.id for episode in payload.episodes}) == len(payload.episodes)


def test_governance_timeline_supports_global_filters() -> None:
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        table_id = _seed_catalog(session)
        payload = get_governance_timeline(
            session,
            current_user=None,
            page_size=50,
            q="audit",
            contains_pii=True,
            open_incidents=True,
            dq_recent=True,
        )

    assert payload.scope == "global"
    assert payload.total > 0
    assert payload.table_id is None
    assert any(item.table_id == table_id for item in payload.items)


def test_timeline_episode_actions_persist_and_filter_state() -> None:
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        table_id = _seed_catalog(session)
        initial = get_asset_timeline(session, table_id=table_id, current_user=None, page_size=50)
        target_episode = next(episode for episode in initial.episodes if episode.event_count > 0)

        acknowledged = record_timeline_episode_action(
            session,
            payload=TimelineEpisodeActionIn(
                episode_key=target_episode.episode_key,
                action_type="acknowledge",
                table_id=table_id,
                column_id=None,
                reason="Reconhecido nos testes",
                silent_until=None,
            ),
            current_user=None,
        )
        session.commit()
        assert acknowledged.action_type == "acknowledge"

        after_ack = get_asset_timeline(session, table_id=table_id, current_user=None, page_size=50)
        ack_episode = next(episode for episode in after_ack.episodes if episode.episode_key == target_episode.episode_key)
        assert ack_episode.status == "acknowledged"
        assert ack_episode.action_count >= 1
        assert after_ack.analytics.acknowledged_episodes >= 1

        silenced = record_timeline_episode_action(
            session,
            payload=TimelineEpisodeActionIn(
                episode_key=target_episode.episode_key,
                action_type="silence",
                table_id=table_id,
                column_id=None,
                reason="Silenciado nos testes",
                silent_until=None,
            ),
            current_user=None,
        )
        session.commit()
        assert silenced.action_type == "silence"

        after_silence = get_asset_timeline(session, table_id=table_id, current_user=None, page_size=50)
        silenced_episode = next(episode for episode in after_silence.episodes if episode.episode_key == target_episode.episode_key)
        assert silenced_episode.status == "silenced"
        assert silenced_episode.silenced_until is not None
        assert after_silence.analytics.silenced_episodes >= 1

        filtered = get_governance_timeline(
            session,
            current_user=None,
            page_size=50,
            table_id=table_id,
            episode_status="silenced",
        )
        assert filtered.episode_total >= 1
        assert any(episode.episode_key == target_episode.episode_key for episode in filtered.episodes)


def test_timeline_episode_identifier_is_deterministic_and_unique_per_signature() -> None:
    event_a = TimelineEventOut(
        id="event-a",
        occurred_at=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
        category="operation",
        event_type="pipeline_stale",
        title="Pipeline stale",
    )
    event_b = TimelineEventOut(
        id="event-b",
        occurred_at=datetime(2025, 1, 1, 12, 5, tzinfo=timezone.utc),
        category="operation",
        event_type="pipeline_failure",
        title="Pipeline failure",
    )

    episode_key = "operational:table:1:123315"

    assert _episode_identifier(episode_key, [event_a]) == _episode_identifier(episode_key, [event_a])
    assert _episode_identifier(episode_key, [event_a]) != _episode_identifier(episode_key, [event_b])


def test_timeline_episode_deduplication_keeps_first_instance() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    episode_one = TimelineEpisodeOut(
        episode_key="operational:table:1:123315",
        id="episode:operational:table:1:123315:abc123",
        episode_type="ingestion",
        title="Episódio 1",
        summary="Resumo 1",
        impact_summary="Impacto 1",
        why_it_matters="Importa 1",
        next_action="Ação 1",
        category="operation",
        occurred_at=now,
        updated_at=now,
        window_start=now,
        window_end=now,
    )
    episode_two = TimelineEpisodeOut(
        episode_key="operational:table:1:123315",
        id="episode:operational:table:1:123315:abc123",
        episode_type="ingestion",
        title="Episódio 2",
        summary="Resumo 2",
        impact_summary="Impacto 2",
        why_it_matters="Importa 2",
        next_action="Ação 2",
        category="operation",
        occurred_at=now,
        updated_at=now,
        window_start=now,
        window_end=now,
    )

    deduped = _dedupe_timeline_episodes([episode_one, episode_two])

    assert len(deduped) == 1
    assert deduped[0].title == "Episódio 1"


def test_timeline_episode_keys_are_deduplicated_before_loading_actions() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    episodes = [
        TimelineEpisodeOut(
            episode_key="episode:one",
            id="episode:one:aaa",
            episode_type="ingestion",
            title="Episódio 1",
            summary="Resumo 1",
            impact_summary="Impacto 1",
            why_it_matters="Importa 1",
            next_action="Ação 1",
            category="operation",
            occurred_at=now,
            updated_at=now,
            window_start=now,
            window_end=now,
        ),
        TimelineEpisodeOut(
            episode_key="episode:one",
            id="episode:one:bbb",
            episode_type="ingestion",
            title="Episódio 2",
            summary="Resumo 2",
            impact_summary="Impacto 2",
            why_it_matters="Importa 2",
            next_action="Ação 2",
            category="operation",
            occurred_at=now,
            updated_at=now,
            window_start=now,
            window_end=now,
        ),
        TimelineEpisodeOut(
            episode_key="episode:two",
            id="episode:two:ccc",
            episode_type="governance",
            title="Episódio 3",
            summary="Resumo 3",
            impact_summary="Impacto 3",
            why_it_matters="Importa 3",
            next_action="Ação 3",
            category="governance",
            occurred_at=now,
            updated_at=now,
            window_start=now,
            window_end=now,
        ),
    ]

    assert _dedupe_episode_keys(episodes) == ["episode:one", "episode:two"]


def test_asset_timeline_handles_missing_optional_metadata() -> None:
    profile = SimpleNamespace(
        table_id=1,
        datasource_name="warehouse",
        schema_name=None,
        owner_name=None,
        certification_status=None,
    )

    assert _profile_matches_filters(profile, TimelineQuery(table_id=1))
    assert not _profile_matches_filters(profile, TimelineQuery(table_id=2))
    assert not _profile_matches_filters(profile, TimelineQuery(schema_name="gold"))


def test_asset_timeline_continues_when_one_source_block_fails(monkeypatch) -> None:
    SessionLocal = _session_factory()

    def _boom(*_args, **_kwargs):
        raise RuntimeError("incidents source unavailable")

    with SessionLocal() as session:
        table_id = _seed_catalog(session)
        monkeypatch.setattr("t2c_data.features.timeline.service._rows_to_events_from_incidents", _boom)
        payload = get_asset_timeline(session, table_id=table_id, current_user=None, page_size=50)

    assert payload.scope == "asset"
    assert payload.total > 0
    assert payload.episode_total > 0
    assert all("incident" not in item.event_type for item in payload.items)


if __name__ == "__main__":
    test_asset_timeline_consolidates_governance_operation_quality_and_incidents()
    test_governance_timeline_supports_global_filters()
    print("timeline tests: OK")
