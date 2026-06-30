from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.governance import recommendations as governance_recommendations
from t2c_data.features.governance.recommendations import (
    apply_governance_policy_recommendations,
    refresh_governance_recommendations,
    resolve_governance_recommendations,
)
from t2c_data.features.governance.assistant import (
    build_governance_recommendation_assistant_payload,
    execute_governance_assistant_action,
)
from t2c_data.features.governance.score_config import normalize_governance_policy_rules
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import GovernanceRecommendation


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


def test_policy_rules_are_normalized_for_recommendations() -> None:
    rules = normalize_governance_policy_rules(
        [
            {
                "name": "Financeiro",
                "trigger_key": "owner_missing",
                "action_key": "define_owner",
                "requires_owner": True,
                "requires_classification": True,
                "requires_sla": True,
                "priority": 10,
            }
        ]
    )

    assert len(rules) == 1
    assert rules[0]["trigger_key"] == "owner_missing"
    assert rules[0]["action_key"] == "define_owner"
    assert rules[0]["requires_owner"] is True
    assert rules[0]["requires_classification"] is True
    assert rules[0]["requires_sla"] is True


def test_refresh_recommendations_persists_signals_in_context_json() -> None:
    session = _build_session()
    candidate = {
        "dedupe_key": "owner_missing:1:0:base",
        "recommendation_key": "owner_missing",
        "policy_rule_key": None,
        "entity_type": "table",
        "entity_id": 1,
        "table_id": 1,
        "column_id": None,
        "datasource_id": None,
        "source_kind": "governance",
        "source_label": "Governança",
        "title": "Definir owner do ativo",
        "detail": "Owner ausente",
        "severity": "high",
        "impact": "high",
        "priority": 200,
        "confidence_score": 96,
        "trust_score": 52,
        "risk_score": 84,
        "action_key": "define_owner",
        "action_label": "Definir owner",
        "due_at": datetime.now(timezone.utc),
        "context_value": "Owner ausente",
        "reason": "Owner obrigatório.",
        "summary": "Recomenda-se definir owner formal.",
        "signals": [{"key": "owner", "label": "Owner", "value": "Não definido"}],
        "context_json": {"governance_score": {"score": 55}},
        "explanation_json": {"source": "rule"},
        "tag_name": None,
    }
    original = governance_recommendations._recommendation_candidates
    governance_recommendations._recommendation_candidates = lambda *_args, **_kwargs: ([candidate], {1: SimpleNamespace(table_id=1)})
    try:
        summary = refresh_governance_recommendations(session)
        row = session.scalar(select(GovernanceRecommendation))
        assert summary["created"] == 1
        assert row is not None
        assert row.context_json is not None
        assert isinstance(row.context_json.get("signals"), list)
        assert row.context_json["signals"][0]["key"] == "owner"
    finally:
        governance_recommendations._recommendation_candidates = original


def test_resolve_recommendations_updates_status_in_batch() -> None:
    session = _build_session()
    row = GovernanceRecommendation(
        dedupe_key="owner_missing:1:0:base",
        recommendation_key="owner_missing",
        entity_type="table",
        entity_id=1,
        table_id=1,
        column_id=None,
        datasource_id=None,
        source_kind="governance",
        source_label="Governança",
        title="Definir owner do ativo",
        detail="Owner ausente",
        severity="high",
        impact="high",
        status="open",
        priority=200,
        confidence_score=96,
        trust_score=52,
        risk_score=84,
        action_key="define_owner",
        action_label="Definir owner",
        due_at=datetime.now(timezone.utc),
    )
    session.add(row)
    session.commit()

    result = resolve_governance_recommendations(
        session,
        recommendation_ids=[row.id],
        resolution_action="applied",
        resolution_note="Resolvido manualmente em lote.",
        actor_user_id=None,
    )
    session.commit()
    refreshed = session.get(GovernanceRecommendation, row.id)

    assert result["succeeded"] == 1
    assert refreshed is not None
    assert refreshed.status == "applied"
    assert refreshed.resolution_action == "applied"


def test_apply_policy_recommendations_applies_only_policy_driven_items() -> None:
    session = _build_session()
    policy_row = GovernanceRecommendation(
        dedupe_key="policy-1:1:0:policy",
        recommendation_key="policy-1",
        policy_rule_key="policy-1",
        entity_type="table",
        entity_id=1,
        table_id=1,
        column_id=None,
        datasource_id=None,
        source_kind="policy",
        source_label="Política",
        title="Aplicar política",
        detail="Recomendação de política",
        severity="high",
        impact="high",
        status="open",
        priority=220,
        confidence_score=90,
        trust_score=75,
        risk_score=80,
        action_key="apply_policy",
        action_label="Aplicar política",
        due_at=datetime.now(timezone.utc),
    )
    manual_row = GovernanceRecommendation(
        dedupe_key="manual-1:1:0:base",
        recommendation_key="manual-1",
        entity_type="table",
        entity_id=1,
        table_id=1,
        column_id=None,
        datasource_id=None,
        source_kind="governance",
        source_label="Governança",
        title="Revisão manual",
        detail="Recomendação manual",
        severity="medium",
        impact="medium",
        status="open",
        priority=150,
        confidence_score=70,
        trust_score=65,
        risk_score=55,
        action_key="manual_review",
        action_label="Revisão manual",
        due_at=datetime.now(timezone.utc),
    )
    session.add_all([policy_row, manual_row])
    session.commit()

    result = apply_governance_policy_recommendations(
        session,
        recommendation_ids=[policy_row.id, manual_row.id],
        resolution_note="Aplicação em massa de políticas.",
        actor_user_id=None,
    )
    session.commit()
    refreshed_policy = session.get(GovernanceRecommendation, policy_row.id)
    refreshed_manual = session.get(GovernanceRecommendation, manual_row.id)

    assert result["succeeded"] == 1
    assert result["failed"] == 1
    assert refreshed_policy is not None
    assert refreshed_policy.status == "applied"
    assert refreshed_policy.resolution_action == "policy_applied"
    assert refreshed_manual is not None
    assert refreshed_manual.status == "open"


def test_recommendation_context_falls_back_when_profile_is_unavailable(monkeypatch) -> None:
    session = _build_session()
    datasource = DataSource(
        name="src",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="db",
        username="user",
    )
    database = Database(name="db")
    schema = Schema(name="public")
    table = TableEntity(
        name="orders",
        table_type="table",
        description_manual="Tabela de pedidos",
        owner="owner",
        certification_status="certified",
    )
    database.datasource = datasource
    schema.database = database
    table.schema = schema
    session.add(table)
    session.flush()

    row = GovernanceRecommendation(
        dedupe_key="owner_missing:1:0:base",
        recommendation_key="owner_missing",
        entity_type="table",
        entity_id=table.id,
        table_id=table.id,
        column_id=None,
        datasource_id=datasource.id,
        source_kind="governance",
        source_label="Governança",
        title="Definir owner do ativo",
        detail="Owner ausente",
        severity="high",
        impact="high",
        status="open",
        priority=200,
        confidence_score=96,
        trust_score=52,
        risk_score=84,
        action_key="define_owner",
        action_label="Definir owner",
        due_at=datetime.now(timezone.utc),
    )
    session.add(row)
    session.commit()

    monkeypatch.setattr(governance_recommendations, "load_table_profiles", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        governance_recommendations,
        "load_table_canonical_context",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("canonical unavailable")),
    )
    monkeypatch.setattr(
        governance_recommendations,
        "get_governance_timeline",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("timeline unavailable")),
    )

    context = governance_recommendations.get_governance_recommendation_context(
        session,
        recommendation_ref=row.dedupe_key,
        current_user=None,
    )

    assert context["recommendation"]["id"] == row.id
    assert context["canonical_asset"] is None
    assert context["policy_matches"] == []
    assert context["trust_history"] == []
    assert context["recent_events"] == []
    assert "Contexto canônico indisponível" in context["assistant_summary"]


def test_recommendation_context_accepts_numeric_id_reference() -> None:
    session = _build_session()
    datasource = DataSource(
        name="src",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="db",
        username="user",
    )
    database = Database(name="db")
    schema = Schema(name="public")
    table = TableEntity(
        name="orders",
        table_type="table",
        description_manual="Tabela de pedidos",
        owner="owner",
        certification_status="certified",
    )
    database.datasource = datasource
    schema.database = database
    table.schema = schema
    session.add(table)
    session.flush()

    row = GovernanceRecommendation(
        dedupe_key="owner_missing:1:0:base",
        recommendation_key="owner_missing",
        entity_type="table",
        entity_id=table.id,
        table_id=table.id,
        column_id=None,
        datasource_id=datasource.id,
        source_kind="governance",
        source_label="Governança",
        title="Definir owner do ativo",
        detail="Owner ausente",
        severity="high",
        impact="high",
        status="open",
        priority=200,
        confidence_score=96,
        trust_score=52,
        risk_score=84,
        action_key="define_owner",
        action_label="Definir owner",
        due_at=datetime.now(timezone.utc),
    )
    session.add(row)
    session.commit()

    context = governance_recommendations.get_governance_recommendation_context(
        session,
        recommendation_ref=str(row.id),
        current_user=None,
    )

    assert context["recommendation"]["id"] == row.id


def test_assistant_payload_and_actions_update_feedback_and_resolution() -> None:
    session = _build_session()
    datasource = DataSource(
        name="src",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="db",
        username="user",
    )
    database = Database(name="db")
    schema = Schema(name="public")
    table = TableEntity(
        name="orders",
        table_type="table",
        description_manual="Tabela de pedidos",
        owner="owner",
        certification_status="certified",
    )
    database.datasource = datasource
    schema.database = database
    table.schema = schema
    session.add(table)
    session.flush()

    row = GovernanceRecommendation(
        dedupe_key="owner_missing:1:0:base",
        recommendation_key="owner_missing",
        entity_type="table",
        entity_id=table.id,
        table_id=table.id,
        column_id=None,
        datasource_id=datasource.id,
        source_kind="governance",
        source_label="Governança",
        title="Definir owner do ativo",
        detail="Owner ausente",
        severity="high",
        impact="high",
        status="open",
        priority=200,
        confidence_score=96,
        trust_score=52,
        risk_score=84,
        action_key="define_owner",
        action_label="Definir owner",
        due_at=datetime.now(timezone.utc),
    )
    session.add(row)
    session.commit()

    payload = build_governance_recommendation_assistant_payload(
        session,
        recommendation_ref=row.dedupe_key,
        current_user=None,
    )
    tool_keys = {str(tool["key"]) for tool in payload["assistant_tools"]}
    assert "resolve_apply" in tool_keys
    assert "feedback_helpful" in tool_keys

    feedback_result = execute_governance_assistant_action(
        session,
        recommendation_ref=row.dedupe_key,
        tool_key="feedback_helpful",
        confirm=True,
        resolution_note="Ajuda na priorização",
        actor_user_id=None,
    )
    session.commit()
    refreshed_after_feedback = session.get(GovernanceRecommendation, row.id)
    assert feedback_result["executed"] is True
    assert refreshed_after_feedback is not None
    assert refreshed_after_feedback.feedback_rating == "helpful"

    apply_result = execute_governance_assistant_action(
        session,
        recommendation_ref=row.dedupe_key,
        tool_key="resolve_apply",
        confirm=True,
        resolution_note="Executado via assistente",
        actor_user_id=None,
    )
    session.commit()
    refreshed_after_apply = session.get(GovernanceRecommendation, row.id)
    assert apply_result["executed"] is True
    assert refreshed_after_apply is not None
    assert refreshed_after_apply.status == "applied"


def test_refresh_recommendations_limits_scope_to_selected_tables() -> None:
    session = _build_session()
    row_one = GovernanceRecommendation(
        dedupe_key="owner_missing:1:0:base",
        recommendation_key="owner_missing",
        entity_type="table",
        entity_id=1,
        table_id=1,
        column_id=None,
        datasource_id=None,
        source_kind="governance",
        source_label="Governança",
        title="Definir owner do ativo",
        detail="Owner ausente",
        severity="high",
        impact="high",
        status="open",
        priority=200,
        confidence_score=96,
        trust_score=52,
        risk_score=84,
        action_key="define_owner",
        action_label="Definir owner",
        due_at=datetime.now(timezone.utc),
    )
    row_two = GovernanceRecommendation(
        dedupe_key="owner_missing:2:0:base",
        recommendation_key="owner_missing",
        entity_type="table",
        entity_id=2,
        table_id=2,
        column_id=None,
        datasource_id=None,
        source_kind="governance",
        source_label="Governança",
        title="Definir owner do ativo",
        detail="Owner ausente",
        severity="high",
        impact="high",
        status="open",
        priority=200,
        confidence_score=96,
        trust_score=52,
        risk_score=84,
        action_key="define_owner",
        action_label="Definir owner",
        due_at=datetime.now(timezone.utc),
    )
    session.add_all([row_one, row_two])
    session.commit()

    candidate = {
        "dedupe_key": "owner_missing:1:0:base",
        "recommendation_key": "owner_missing",
        "policy_rule_key": None,
        "entity_type": "table",
        "entity_id": 1,
        "table_id": 1,
        "column_id": None,
        "datasource_id": None,
        "source_kind": "governance",
        "source_label": "Governança",
        "title": "Definir owner do ativo",
        "detail": "Owner ausente",
        "severity": "high",
        "impact": "high",
        "priority": 200,
        "confidence_score": 96,
        "trust_score": 52,
        "risk_score": 84,
        "action_key": "define_owner",
        "action_label": "Definir owner",
        "due_at": datetime.now(timezone.utc),
        "context_value": "Owner ausente",
        "reason": "Owner obrigatório.",
        "summary": "Recomenda-se definir owner formal.",
        "signals": [{"key": "owner", "label": "Owner", "value": "Não definido"}],
        "context_json": {"governance_score": {"score": 55}},
        "explanation_json": {"source": "rule"},
        "tag_name": None,
    }
    original = governance_recommendations._recommendation_candidates
    governance_recommendations._recommendation_candidates = lambda *_args, **_kwargs: ([candidate], {1: SimpleNamespace(table_id=1)})
    try:
        summary = refresh_governance_recommendations(session, table_ids=[1])
        session.commit()
        refreshed_one = session.get(GovernanceRecommendation, row_one.id)
        refreshed_two = session.get(GovernanceRecommendation, row_two.id)
        assert summary["updated"] == 1
        assert refreshed_one is not None
        assert refreshed_one.title == candidate["title"]
        assert refreshed_two is not None
        assert refreshed_two.status == "open"
    finally:
        governance_recommendations._recommendation_candidates = original


if __name__ == "__main__":
    test_policy_rules_are_normalized_for_recommendations()
    test_refresh_recommendations_persists_signals_in_context_json()
    test_resolve_recommendations_updates_status_in_batch()
    test_apply_policy_recommendations_applies_only_policy_driven_items()
    test_recommendation_context_accepts_numeric_id_reference()
    print("governance recommendations tests: OK")
