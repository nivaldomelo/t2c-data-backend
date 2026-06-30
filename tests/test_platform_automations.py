from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.platform.automations import (
    ACTION_BY_KEY,
    create_automation_rule,
    evaluate_automation_rules,
    execute_automation_action,
    list_available_automation_actions,
)
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.schemas.platform import PlatformAutomationRuleIn


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


def _create_user(db: Session, *, email: str = "automation@example.com") -> User:
    user = User(email=email, password_hash="hash", name="Automation", full_name="Automation User", is_active=True)
    db.add(user)
    db.commit()
    return user


def test_list_available_automation_actions_exposes_core_actions() -> None:
    payload = list_available_automation_actions()

    keys = {item["key"] for item in payload["items"]}
    assert payload["total"] >= 5
    assert "open_incident" in keys
    assert "reexecute_dag" in keys
    assert any(item["suggestion_only"] for item in payload["items"] if item["key"] == "reexecute_dag")


def test_create_and_evaluate_automation_rule_without_match() -> None:
    db = _build_session()
    user = _create_user(db)

    rule = create_automation_rule(
        db,
        payload=PlatformAutomationRuleIn(
            name="Somente para validação",
            description="Regra que não deve disparar no contexto vazio.",
            status="active",
            scope_kind="global",
            condition_kind="risk_score",
            condition_operator="gte",
            threshold_value=999,
            window_days=7,
            action_key="open_incident",
            execution_mode="suggested",
        ),
        current_user=user,
        audit_kwargs={},
    )

    assert rule.id is not None

    payload = evaluate_automation_rules(db, current_user=user, audit_kwargs={})
    assert payload["rules_evaluated"] == 1
    assert payload["actions_executed"] == 0
    assert payload["suggestions_created"] == 0
    assert payload["skipped"] == 1
    assert payload["items"] == []


def test_execute_automation_action_can_record_suggested_dag_reexecution() -> None:
    db = _build_session()
    user = _create_user(db)

    execution = execute_automation_action(
        db,
        action_key="reexecute_dag",
        current_user=user,
        target_json={"dag_id": "example_dag", "airflow_href": "https://airflow.example.com"},
        execution_mode="manual",
        trigger_source="manual",
        audit_kwargs={},
    )

    assert execution.action_key == "reexecute_dag"
    assert execution.status == "suggested"
    assert execution.execution_mode == "suggested"
    assert execution.result_json is not None
    assert execution.result_json["dag_id"] == "example_dag"
    assert execution.result_json["ok"] is True
    assert ACTION_BY_KEY["reexecute_dag"].suggestion_only is True
