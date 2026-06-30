from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.governance import intelligence_feed
from t2c_data.features.governance.intelligence_feed import (
    build_governance_intelligence_feed,
    build_governance_intelligence_timeline,
)
from t2c_data.models import Base
from t2c_data.schemas.governance_intelligence import (
    GovernanceIntelligenceFeedOut,
    GovernanceIntelligenceTimelineOut,
)


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


def test_feed_handles_empty_database() -> None:
    """An empty platform must not break the page: well-formed, empty payload."""
    db = _build_session()

    payload = build_governance_intelligence_feed(db, current_user=None)

    assert payload["total_assets"] == 0
    assert payload["asset_risk"] == []
    assert payload["attention_now"] == []
    assert payload["by_domain"] == []
    assert payload["next_best_actions"] == []
    # tracks are always present (with zero counts) so the UI can render the skeleton.
    assert {track["key"] for track in payload["tracks"]} == {"certification", "operational", "documentation"}
    assert all(track["total"] == 0 for track in payload["tracks"])
    assert payload["metabase_priority_count"] == 0
    # Validates against the response schema (route contract).
    model = GovernanceIntelligenceFeedOut(**payload)
    assert model.generated_at


def test_asset_risk_prioritizes_metabase_consumed_tables(monkeypatch) -> None:
    by_asset = [
        {"table_id": 1, "label": "a", "priority_score": 90, "score": 80, "suggested_actions": ["X"]},
        {"table_id": 2, "label": "b", "priority_score": 40, "score": 30, "suggested_actions": ["Y", "X"]},
        {"table_id": 3, "label": "c", "priority_score": 50, "score": 50, "suggested_actions": []},
    ]
    # Only table 2 is consumed by Metabase dashboards.
    monkeypatch.setattr(intelligence_feed, "_metabase_dashboard_impact_map", lambda session, ids: {2: 4})

    result = intelligence_feed._build_asset_risk(None, by_asset, asset_limit=10)

    # Metabase-consumed first, then by priority among the rest.
    assert [item["table_id"] for item in result] == [2, 1, 3]
    assert result[0]["metabase_dashboards"] == 4
    assert result[0]["next_action"] == "Y"
    assert result[1]["metabase_dashboards"] == 0


def test_asset_risk_respects_limit(monkeypatch) -> None:
    by_asset = [
        {"table_id": i, "label": f"t{i}", "priority_score": i, "score": i, "suggested_actions": []}
        for i in range(10)
    ]
    monkeypatch.setattr(intelligence_feed, "_metabase_dashboard_impact_map", lambda session, ids: {})

    result = intelligence_feed._build_asset_risk(None, by_asset, asset_limit=3)

    assert len(result) == 3
    # Highest priority first (no Metabase consumption present).
    assert [item["table_id"] for item in result] == [9, 8, 7]


def test_next_best_actions_rank_by_frequency() -> None:
    asset_risk = [
        {"suggested_actions": ["Definir owner", "Completar dicionário"]},
        {"suggested_actions": ["Definir owner"]},
        {"suggested_actions": []},
    ]
    actions = intelligence_feed._build_next_best_actions(asset_risk)

    assert actions[0]["action"] == "Definir owner"
    assert actions[0]["count"] == 2
    assert actions[0]["order"] == 1
    assert any(action["action"] == "Completar dicionário" for action in actions)


def test_tracks_aggregate_counts() -> None:
    summary = {
        "governance_gaps": {
            "items": [
                {"key": "no_owner", "count": 5},
                {"key": "no_dictionary", "count": 3},
                {"key": "no_tags", "count": 2},
                {"key": "no_recent_review", "count": 1},
            ]
        },
        "certification": {"eligible_not_certified": 4},
        "incidents": {"critical_open_total": 2},
        "dq": {"score_bands": [{"key": "critical", "value": 7}]},
        "kpis": [
            {"key": "assets_with_open_incidents", "value": 6},
            {"key": "critical_assets", "value": 8},
        ],
    }

    tracks = {track["key"]: track for track in intelligence_feed._build_tracks(summary)}

    certification = tracks["certification"]
    assert certification["total"] == 5 + 3 + 2 + 4
    operational = tracks["operational"]
    # open incidents(6) + low_dq(7) + critical_assets(8) + critical_open_total(2)
    assert operational["total"] == 6 + 7 + 8 + 2


def test_timeline_handles_empty_database() -> None:
    db = _build_session()

    payload = build_governance_intelligence_timeline(db, current_user=None)

    assert payload["episodes"] == []
    model = GovernanceIntelligenceTimelineOut(**payload)
    assert model.generated_at


def test_severity_tone_mapping() -> None:
    assert intelligence_feed._severity_tone("critical") == "danger"
    assert intelligence_feed._severity_tone("high") == "accent"
    assert intelligence_feed._severity_tone("medium") == "warning"
    assert intelligence_feed._severity_tone("low") == "neutral"
    assert intelligence_feed._severity_tone(None) == "neutral"
    assert intelligence_feed._severity_tone("unknown") == "neutral"


def _fake_episode(key: str, *, importance: int, chain: list[str], children: int) -> SimpleNamespace:
    now = datetime(2026, 6, 30, 9, 0, tzinfo=timezone.utc)
    child_events = [
        SimpleNamespace(occurred_at=now, title=f"evt-{key}-{i}", severity="high", event_type="incident_opened")
        for i in range(children)
    ]
    return SimpleNamespace(
        episode_key=key,
        title=f"Episode {key}",
        summary="s",
        impact_summary="i",
        why_it_matters="w",
        next_action="a",
        status="open",
        severity="critical",
        importance_score=importance,
        occurred_at=now,
        correlation_label="cadeia",
        correlation_chain=chain,
        affected_assets_count=1,
        impacted_table_ids=[1],
        child_events=child_events,
    )


def test_timeline_prefers_correlated_episodes(monkeypatch) -> None:
    episodes = [
        _fake_episode("A", importance=50, chain=["x -> y"], children=2),
        _fake_episode("B", importance=90, chain=[], children=1),  # not a chain
        _fake_episode("C", importance=70, chain=[], children=3),  # chain via children
    ]
    monkeypatch.setattr(
        intelligence_feed,
        "get_governance_timeline",
        lambda session, **kwargs: SimpleNamespace(episodes=episodes),
    )

    payload = build_governance_intelligence_timeline(None, current_user=None, limit=8)

    # Only correlated episodes (A, C), ordered by importance desc.
    assert [episode["episode_key"] for episode in payload["episodes"]] == ["C", "A"]
    first = payload["episodes"][0]
    assert first["tone"] == "danger"
    assert first["correlation_chain"] == []
    assert len(first["steps"]) == 3
    assert first["steps"][0]["title"].startswith("evt-C")
    # Validates against the response schema.
    GovernanceIntelligenceTimelineOut(**payload)


def test_timeline_dedupes_repeated_episode_keys(monkeypatch) -> None:
    episodes = [
        _fake_episode("DUP", importance=80, chain=["a -> b"], children=2),
        _fake_episode("DUP", importance=80, chain=["a -> b"], children=2),
        _fake_episode("OTHER", importance=60, chain=["c -> d"], children=2),
    ]
    monkeypatch.setattr(
        intelligence_feed,
        "get_governance_timeline",
        lambda session, **kwargs: SimpleNamespace(episodes=episodes),
    )

    payload = build_governance_intelligence_timeline(None, current_user=None)

    keys = [episode["episode_key"] for episode in payload["episodes"]]
    assert keys == ["DUP", "OTHER"]
    assert len(keys) == len(set(keys))
