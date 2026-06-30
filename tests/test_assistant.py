from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.core.rbac import can_access_path
from t2c_data.features.assistant import service as assistant_service
from t2c_data.features.assistant.service import build_assistant_explanation, execute_assistant_action
from t2c_data.schemas.assistant import AssistantActionIn


class DummySession:
    def __init__(self) -> None:
        self.committed = False

    def commit(self) -> None:
        self.committed = True


def _build_asset_bundle(*, owner_defined: bool = False) -> dict[str, object]:
    asset = SimpleNamespace(
        entity_kind="table",
        table_id=7,
        column_id=None,
        display_name="Clientes",
        table_fqn="datalake.integracao.bronze.clientes",
        owner=SimpleNamespace(
            owner_defined=owner_defined,
            data_owner_id=None,
            owner_name=None,
            owner_email=None,
        ),
        classification=SimpleNamespace(
            classification_defined=False,
            sensitivity_level=None,
            trust_score=68,
            trust_label="Média",
            trust_tone="warning",
        ),
        evidence=SimpleNamespace(
            description_complete=False,
            dictionary_complete=False,
            dq_score=95,
            open_incidents=0,
            critical_open_incidents=0,
        ),
        links=SimpleNamespace(
            change_management="/governance/change-management?assetType=table&assetId=7",
            incidents="/incidents/tickets?tableId=7",
            data_quality="/data-quality?tableId=7",
            explorer="/explorer?tableId=7",
            datasource="/explorer?datasourceId=1",
            metabase_consumption="/explorer?tableId=7&tab=consumption",
        ),
        lineage=SimpleNamespace(
            impact=SimpleNamespace(
                upstream_count=1,
                downstream_count=2,
                impact_level="medium",
            ),
        ),
        source=SimpleNamespace(
            datasource_id=1,
            database_id=2,
            schema_id=3,
        ),
    )
    operational_context = {
        "criticality_score": 55,
        "criticality_label": "Média",
        "criticality_tone": "warning",
        "recommended_actions": ["review_owner"],
        "open_incidents": 0,
        "links": {
            "incidents": "/incidents/tickets?tableId=7",
            "data_quality": "/data-quality?tableId=7",
            "certification": "/certification?tableId=7",
            "privacy": "/privacy-access?tableId=7",
            "owners": "/data-owners?ownerId=1",
            "audit": "/audit?entity_type=table&entity_id=7",
            "lineage": "/explorer?tableId=7&tab=lineage",
            "change_management": "/governance/change-management?assetType=table&assetId=7",
        },
    }
    correlation_summary = SimpleNamespace(
        signals=SimpleNamespace(operational_failure=False, stale_pipeline=False),
        priority_score=1,
        correlation_type="Sem correlação crítica relevante",
        summary="Nenhum sinal crítico relevante foi identificado neste momento.",
    )
    return {
        "asset_ref": "table:7",
        "asset": asset,
        "operational_context": operational_context,
        "correlation_summary": correlation_summary,
        "slas": {"items": []},
        "active_sla": None,
    }


def test_load_asset_bundle_uses_keyword_correlation_summary_call(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    user = SimpleNamespace(id=10, email="user@example.com", name="User")
    loaded = SimpleNamespace(
        entity_kind="table",
        table_id=7,
        column_id=None,
        display_name="Clientes",
        table_fqn="datalake.integracao.bronze.clientes",
        owner=SimpleNamespace(owner_defined=True, data_owner_id=1, owner_name="Owner", owner_email="owner@example.com"),
        classification=SimpleNamespace(
            classification_defined=True,
            sensitivity_level=None,
            has_personal_data=False,
            has_sensitive_personal_data=False,
            trust_score=90,
            trust_label="Alta",
            trust_tone="success",
        ),
        evidence=SimpleNamespace(
            description_complete=True,
            dictionary_complete=True,
            dq_score=100,
            open_incidents=0,
            critical_open_incidents=0,
        ),
        links=SimpleNamespace(change_management="/governance/change-management?assetType=table&assetId=7"),
        source=SimpleNamespace(datasource_id=1, database_id=2, schema_id=3),
    )
    called: dict[str, object] = {}

    monkeypatch.setattr(assistant_service, "load_table_canonical_context", lambda *args, **kwargs: loaded)
    monkeypatch.setattr(assistant_service, "compact_canonical_asset_context", lambda asset: asset)
    monkeypatch.setattr(assistant_service, "load_table_operational_context", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        assistant_service,
        "build_table_correlation_summary",
        lambda *args, **kwargs: called.update({"args": args, "kwargs": kwargs}) or SimpleNamespace(signals=None, priority_score=0, correlation_type="none", summary="ok"),
    )
    monkeypatch.setattr(assistant_service, "list_asset_slas", lambda *args, **kwargs: {"items": []})

    assistant_service._load_asset_bundle(session, asset_type="table", asset_id=7, current_user=user)

    assert called["args"] == ()
    assert called["kwargs"]["db"] is session
    assert called["kwargs"]["table_id"] == 7
    assert called["kwargs"]["current_user"] is user


def test_assistant_explanation_returns_problems_impact_and_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    user = SimpleNamespace(id=10, email="user@example.com", name="User")
    bundle = _build_asset_bundle(owner_defined=False)

    monkeypatch.setattr(assistant_service, "_load_asset_bundle", lambda *args, **kwargs: bundle)
    monkeypatch.setattr(assistant_service, "track_usage_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(assistant_service, "write_audit_log_sync", lambda *args, **kwargs: None)

    payload = build_assistant_explanation(session, asset_ref="table:7", current_user=user)

    problem_keys = {item.key for item in payload.problems}
    action_keys = {item.key for item in payload.actions}

    assert payload.asset_ref == "table:7"
    assert payload.asset_type == "table"
    assert payload.asset_id == 7
    assert "owner_missing" in problem_keys
    assert "classification_missing" in problem_keys
    assert "sla_missing" in problem_keys
    assert payload.recommendation.key == "define_owner"
    assert payload.recommendation.can_execute is True
    assert "define_owner" in action_keys
    assert "reprocess_pipeline" in action_keys
    assert "open_incident" in action_keys
    assert session.committed is True


def test_assistant_explanation_uses_asset_signals_as_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    user = SimpleNamespace(id=10, email="user@example.com", name="User")
    bundle = _build_asset_bundle(owner_defined=True)
    asset_intelligence = SimpleNamespace(
        risk_score=82,
        priority_score=91,
        trust_score=44,
        signals=[
            SimpleNamespace(type="dq_active_violation", severity="high"),
            SimpleNamespace(type="freshness_delayed", severity="high"),
        ],
        impact=SimpleNamespace(dashboards=4, users=8),
        recommended_actions=["corrigir violação de DQ"],
    )

    monkeypatch.setattr(assistant_service, "_load_asset_bundle", lambda *args, **kwargs: bundle)
    monkeypatch.setattr(assistant_service, "build_asset_intelligence", lambda *args, **kwargs: asset_intelligence)
    monkeypatch.setattr(assistant_service, "track_usage_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(assistant_service, "write_audit_log_sync", lambda *args, **kwargs: None)

    payload = build_assistant_explanation(session, asset_ref="table:7", current_user=user)

    problem_keys = {item.key for item in payload.problems}
    impact_keys = {item.key for item in payload.impact}

    assert "dq_degraded" in problem_keys
    assert "stale_pipeline" in problem_keys
    assert "asset_signal_usage_impact" in impact_keys
    assert "asset_signal_priority" in impact_keys
    assert payload.recommendation.key == "open_incident"
    assert payload.context["asset_intelligence"]["priority_score"] == 91
    assert payload.context["asset_intelligence"]["signals"][0]["type"] == "dq_active_violation"
    assert session.committed is True


def test_define_owner_action_creates_change_request(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    user = SimpleNamespace(id=10, email="user@example.com", name="User")
    bundle = _build_asset_bundle(owner_defined=False)
    created: dict[str, object] = {}

    def fake_create_metadata_change_request(*args, **kwargs):
      created.update(kwargs)
      return {"request_key": "MCR-001"}

    monkeypatch.setattr(assistant_service, "_load_asset_bundle", lambda *args, **kwargs: bundle)
    monkeypatch.setattr(assistant_service, "create_metadata_change_request", fake_create_metadata_change_request)
    monkeypatch.setattr(assistant_service, "track_usage_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(assistant_service, "write_audit_log_sync", lambda *args, **kwargs: None)
    monkeypatch.setattr(assistant_service, "execute_automation_action", lambda *args, **kwargs: None)

    result = execute_assistant_action(
        session,
        asset_ref="table:7",
        payload=AssistantActionIn(action_key="define_owner", data_owner_id=42, resolution_note="Auditoria assistida"),
        current_user=user,
    )

    assert result.ok is True
    assert result.executed is True
    assert result.action_key == "define_owner"
    assert result.follow_up_href == "/governance/change-management?assetType=table&assetId=7"
    assert created["change_kind"] == "owner_assignment"
    assert created["proposed_value_json"] == {"data_owner_id": 42}
    assert created["current_value_json"] == {"data_owner_id": None}
    assert session.committed is True


@pytest.mark.parametrize("action_key", ["reprocess_pipeline", "open_incident"])
def test_confirmed_actions_require_confirmation(monkeypatch: pytest.MonkeyPatch, action_key: str) -> None:
    session = DummySession()
    user = SimpleNamespace(id=10, email="user@example.com", name="User")
    bundle = _build_asset_bundle(owner_defined=True)

    monkeypatch.setattr(assistant_service, "_load_asset_bundle", lambda *args, **kwargs: bundle)
    monkeypatch.setattr(assistant_service, "track_usage_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(assistant_service, "write_audit_log_sync", lambda *args, **kwargs: None)

    with pytest.raises(HTTPException, match="confirma"):
        execute_assistant_action(
            session,
            asset_ref="table:7",
            payload=AssistantActionIn(action_key=action_key, confirm=False),
            current_user=user,
        )


@pytest.mark.parametrize(
    ("action_key", "follow_up_href"),
    [
        ("reprocess_pipeline", "/explorer?tableId=7"),
        ("open_incident", "/incidents/tickets?tableId=7"),
    ],
)
def test_confirmed_actions_dispatch_to_automation(
    monkeypatch: pytest.MonkeyPatch,
    action_key: str,
    follow_up_href: str,
) -> None:
    session = DummySession()
    user = SimpleNamespace(id=10, email="user@example.com", name="User")
    bundle = _build_asset_bundle(owner_defined=True)
    calls: list[tuple[str, int]] = []

    def fake_execute_automation_action(*args, **kwargs):
        calls.append((kwargs["action_key"], kwargs["table_id"]))
        return SimpleNamespace(id=99, status="queued", entity_id=kwargs["table_id"])

    monkeypatch.setattr(assistant_service, "_load_asset_bundle", lambda *args, **kwargs: bundle)
    monkeypatch.setattr(assistant_service, "execute_automation_action", fake_execute_automation_action)
    monkeypatch.setattr(assistant_service, "track_usage_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(assistant_service, "write_audit_log_sync", lambda *args, **kwargs: None)

    result = execute_assistant_action(
        session,
        asset_ref="table:7",
        payload=AssistantActionIn(action_key=action_key, confirm=True),
        current_user=user,
    )

    assert result.ok is True
    assert result.action_key == action_key
    assert result.executed is True
    assert result.follow_up_href == follow_up_href
    assert calls == [(action_key, 7)]
    assert session.committed is True


def test_viewer_can_only_access_explain_path() -> None:
    assert can_access_path({"viewer"}, "POST", "/api/v1/assistant/explain/table:7") is True
    assert can_access_path({"viewer"}, "POST", "/api/v1/assistant/actions/table:7") is False
