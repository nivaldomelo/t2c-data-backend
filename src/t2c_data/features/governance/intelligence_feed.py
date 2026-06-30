"""Governance Intelligence feed.

Aggregates already-computed platform intelligence (executive dashboard) into a
focused, decision-oriented payload for the Governance Intelligence page. It does
NOT recompute scores: it reuses ``get_dashboard_executive_summary`` (privacy-aware
and cached) and reshapes it, with one product requirement layered on top — tables
consumed by Metabase dashboards are surfaced explicitly and ranked first.

Read-only. No new tables/migrations.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from t2c_data.features.dashboard.executive_queries import (
    _metabase_dashboard_impact_map,
    get_dashboard_executive_summary,
)
from t2c_data.features.dashboard.executive_scoring import risk_tone
from t2c_data.features.timeline.service import get_governance_timeline

ASSET_LIMIT = 20
ATTENTION_LIMIT = 6
NEXT_ACTIONS_LIMIT = 6
TIMELINE_LIMIT = 8
TIMELINE_WINDOW_DAYS = 7
TIMELINE_CANDIDATE_POOL = 24

_SEVERITY_TONE = {
    "critical": "danger",
    "high": "accent",
    "medium": "warning",
    "low": "neutral",
}


def _as_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _impact_text(asset: dict[str, object]) -> str | None:
    metabase = _as_int(asset.get("metabase_dashboards"))
    critical = _as_int(asset.get("critical_open_incidents"))
    incidents = _as_int(asset.get("open_incidents"))
    stale = asset.get("stale_hours")
    if metabase > 0:
        return f"Consumido por {metabase} dashboard(s) Metabase — impacto direto em relatórios."
    if critical > 0:
        return "Incidente crítico aberto pode bloquear certificação e decisões."
    if incidents > 0:
        return "Incidente aberto impactando o ativo."
    if stale is not None and _as_int(stale) >= 24:
        return f"Dados possivelmente desatualizados ({_as_int(stale)}h sem frescor)."
    return asset.get("risk_label")  # type: ignore[return-value]


def _build_asset_risk(session: Session, by_asset: list[dict[str, object]], *, asset_limit: int) -> list[dict[str, object]]:
    table_ids = [_as_int(item.get("table_id")) for item in by_asset if item.get("table_id") is not None]
    metabase_map = _metabase_dashboard_impact_map(session, table_ids) if table_ids else {}

    asset_risk: list[dict[str, object]] = []
    for item in by_asset:
        table_id = item.get("table_id")
        metabase = metabase_map.get(_as_int(table_id), 0) if table_id is not None else 0
        suggested = list(item.get("suggested_actions") or [])
        asset_risk.append(
            {
                "table_id": table_id,
                "label": item.get("label"),
                "href": item.get("href"),
                "domain_name": item.get("domain_name"),
                "owner_name": item.get("owner_name"),
                "risk_score": _as_int(item.get("score")),
                "priority_score": _as_int(item.get("priority_score")),
                "risk_label": item.get("risk_label"),
                "risk_tone": item.get("risk_tone"),
                "reasons": list(item.get("reasons") or []),
                "suggested_actions": suggested,
                "next_action": suggested[0] if suggested else None,
                "metabase_dashboards": int(metabase),
                "stale_hours": item.get("stale_hours"),
                "open_incidents": _as_int(item.get("open_incidents")),
                "critical_open_incidents": _as_int(item.get("critical_open_incidents")),
                "suggested_incident": bool(item.get("suggested_incident")),
            }
        )

    # Requirement: tables consumed by Metabase dashboards always take precedence,
    # then highest priority/risk.
    asset_risk.sort(
        key=lambda a: (
            0 if _as_int(a["metabase_dashboards"]) > 0 else 1,
            -_as_int(a["priority_score"]),
            -_as_int(a["risk_score"]),
            str(a["label"] or ""),
        )
    )
    return asset_risk[:asset_limit]


def _build_attention_now(asset_risk: list[dict[str, object]]) -> list[dict[str, object]]:
    attention: list[dict[str, object]] = []
    for asset in asset_risk[:ATTENTION_LIMIT]:
        reasons = list(asset.get("reasons") or [])
        attention.append(
            {
                "table_id": asset.get("table_id"),
                "signal": asset.get("label"),
                "priority_score": _as_int(asset.get("priority_score")),
                "tone": asset.get("risk_tone"),
                "metabase_dashboards": _as_int(asset.get("metabase_dashboards")),
                "cause": reasons[0] if reasons else None,
                "causes": reasons[:3],
                "impact": _impact_text(asset),
                "action": asset.get("next_action"),
                "href": asset.get("href"),
            }
        )
    return attention


def _build_by_domain(risk: dict[str, object]) -> list[dict[str, object]]:
    rows = list(risk.get("by_domain") or [])
    domains: list[dict[str, object]] = []
    for row in rows:
        max_score = _as_int(row.get("max_score"))
        domains.append(
            {
                "domain": row.get("label"),
                "asset_count": _as_int(row.get("asset_count")),
                "risk_score": float(row.get("avg_score") or 0.0),
                "max_score": max_score,
                "critical_assets": _as_int(row.get("critical_assets")),
                "open_incidents": _as_int(row.get("open_incidents")),
                "tone": risk_tone(max_score),
            }
        )
    return domains


def _build_tracks(summary: dict[str, object]) -> list[dict[str, object]]:
    gaps = {item.get("key"): item for item in (summary.get("governance_gaps") or {}).get("items", [])}
    certification = summary.get("certification") or {}
    incidents = summary.get("incidents") or {}
    dq = summary.get("dq") or {}
    kpis = {item.get("key"): item for item in summary.get("kpis", [])}

    def gap_count(key: str) -> int:
        return _as_int((gaps.get(key) or {}).get("count"))

    def kpi_value(key: str) -> int:
        return _as_int((kpis.get(key) or {}).get("value"))

    low_dq = 0
    for band in dq.get("score_bands", []):
        if band.get("key") == "critical":
            low_dq = _as_int(band.get("value"))
            break

    tracks = [
        {
            "key": "certification",
            "label": "Aumentar certificação",
            "description": "Itens que bloqueiam a certificação de ativos.",
            "href": "/certification",
            "items": [
                {"key": "no_owner", "label": "Ativos sem owner", "count": gap_count("no_owner")},
                {"key": "no_dictionary", "label": "Sem dicionário completo", "count": gap_count("no_dictionary")},
                {"key": "no_tags", "label": "Sem tags relevantes", "count": gap_count("no_tags")},
                {
                    "key": "eligible_not_certified",
                    "label": "Elegíveis ainda não certificados",
                    "count": _as_int(certification.get("eligible_not_certified")),
                },
            ],
        },
        {
            "key": "operational",
            "label": "Reduzir risco operacional",
            "description": "Incidentes, qualidade baixa e ativos críticos.",
            "href": "/ops/cockpit",
            "items": [
                {"key": "open_incidents", "label": "Com incidente aberto", "count": kpi_value("assets_with_open_incidents")},
                {"key": "low_dq", "label": "DQ abaixo do mínimo", "count": low_dq},
                {"key": "critical_assets", "label": "Ativos críticos", "count": kpi_value("critical_assets")},
                {"key": "critical_open_total", "label": "Incidentes críticos abertos", "count": _as_int(incidents.get("critical_open_total"))},
            ],
        },
        {
            "key": "documentation",
            "label": "Melhorar documentação",
            "description": "Lacunas de descrição, dicionário e revisão.",
            "href": "/explorer",
            "items": [
                {"key": "no_dictionary", "label": "Sem dicionário completo", "count": gap_count("no_dictionary")},
                {"key": "no_tags", "label": "Sem tags relevantes", "count": gap_count("no_tags")},
                {"key": "no_recent_review", "label": "Sem revisão recente", "count": gap_count("no_recent_review")},
            ],
        },
    ]
    for track in tracks:
        track["total"] = sum(_as_int(item["count"]) for item in track["items"])
    return tracks


def _build_next_best_actions(asset_risk: list[dict[str, object]]) -> list[dict[str, object]]:
    counter: Counter[str] = Counter()
    for asset in asset_risk:
        for action in asset.get("suggested_actions") or []:
            if action:
                counter[str(action)] += 1
    ranked = counter.most_common(NEXT_ACTIONS_LIMIT)
    return [
        {"order": index + 1, "action": action, "count": count, "tone": "accent"}
        for index, (action, count) in enumerate(ranked)
    ]


def build_governance_intelligence_feed(
    session: Session,
    *,
    current_user=None,
    asset_limit: int = ASSET_LIMIT,
) -> dict[str, object]:
    summary = get_dashboard_executive_summary(session, include_secondary=True, current_user=current_user)
    operational = summary.get("operational_intelligence") or {}
    by_asset = list(operational.get("by_asset") or [])

    asset_risk = _build_asset_risk(session, by_asset, asset_limit=asset_limit)
    attention_now = _build_attention_now(asset_risk)
    by_domain = _build_by_domain(summary.get("risk") or {})
    tracks = _build_tracks(summary)
    next_best_actions = _build_next_best_actions(asset_risk)
    metabase_priority_count = sum(1 for asset in asset_risk if _as_int(asset.get("metabase_dashboards")) > 0)

    return {
        "generated_at": str(summary.get("generated_at")),
        "total_assets": _as_int(
            next((kpi.get("value") for kpi in summary.get("kpis", []) if kpi.get("key") == "total_assets"), 0)
        ),
        "metabase_priority_count": metabase_priority_count,
        "kpis": list(summary.get("kpis") or []),
        "attention_now": attention_now,
        "asset_risk": asset_risk,
        "by_domain": by_domain,
        "tracks": tracks,
        "next_best_actions": next_best_actions,
    }


def _severity_tone(severity: str | None) -> str:
    return _SEVERITY_TONE.get((severity or "").lower(), "neutral")


def _episode_has_chain(episode) -> bool:
    """An episode is a real causal chain if it links steps or several events."""
    chain = getattr(episode, "correlation_chain", None) or []
    children = getattr(episode, "child_events", None) or []
    return bool(chain) or len(children) >= 2


def _serialize_timeline_episode(episode) -> dict[str, object]:
    children = sorted(
        getattr(episode, "child_events", None) or [],
        key=lambda member: getattr(member, "occurred_at", None) or datetime.min.replace(tzinfo=timezone.utc),
    )
    steps = [
        {
            "occurred_at": member.occurred_at.isoformat() if getattr(member, "occurred_at", None) else "",
            "title": getattr(member, "title", "") or "",
            "severity": getattr(member, "severity", None),
            "event_type": getattr(member, "event_type", None),
        }
        for member in children
    ]
    occurred_at = getattr(episode, "occurred_at", None)
    return {
        "episode_key": getattr(episode, "episode_key", "") or "",
        "title": getattr(episode, "title", "") or "",
        "summary": getattr(episode, "summary", None),
        "impact_summary": getattr(episode, "impact_summary", None),
        "why_it_matters": getattr(episode, "why_it_matters", None),
        "next_action": getattr(episode, "next_action", None),
        "status": getattr(episode, "status", "open") or "open",
        "severity": getattr(episode, "severity", None),
        "tone": _severity_tone(getattr(episode, "severity", None)),
        "importance_score": _as_int(getattr(episode, "importance_score", 0)),
        "occurred_at": occurred_at.isoformat() if occurred_at else "",
        "correlation_label": getattr(episode, "correlation_label", None),
        "correlation_chain": list(getattr(episode, "correlation_chain", None) or []),
        "affected_assets_count": _as_int(getattr(episode, "affected_assets_count", 0)),
        "impacted_table_ids": list(getattr(episode, "impacted_table_ids", None) or []),
        "steps": steps,
    }


def build_governance_intelligence_timeline(
    session: Session,
    *,
    current_user=None,
    limit: int = TIMELINE_LIMIT,
    days: int = TIMELINE_WINDOW_DAYS,
) -> dict[str, object]:
    """Recent correlated episodes ("intelligent timeline").

    Reuses the timeline service's episode correlation (no recompute) and keeps the
    most important correlated chains for the decision center.
    """
    now = datetime.now(timezone.utc)
    page = get_governance_timeline(
        session,
        current_user=current_user,
        page=1,
        page_size=1,
        episode_page=1,
        episode_page_size=TIMELINE_CANDIDATE_POOL,
        date_from=now - timedelta(days=days),
    )
    episodes = list(getattr(page, "episodes", None) or [])

    # Defensive de-dup: the timeline service can emit the same episode_key more
    # than once; keep the first (highest-importance) occurrence.
    seen_keys: set[str] = set()
    unique_episodes = []
    for episode in episodes:
        key = getattr(episode, "episode_key", "") or ""
        if key and key in seen_keys:
            continue
        seen_keys.add(key)
        unique_episodes.append(episode)
    episodes = unique_episodes

    # Prefer episodes that actually correlate multiple events; fall back to the
    # most important standalone episodes so the block is never empty when there
    # is signal.
    correlated = [episode for episode in episodes if _episode_has_chain(episode)]
    chosen = correlated or episodes
    chosen = sorted(chosen, key=lambda episode: -_as_int(getattr(episode, "importance_score", 0)))[:limit]

    return {
        "generated_at": now.isoformat(),
        "episodes": [_serialize_timeline_episode(episode) for episode in chosen],
    }
