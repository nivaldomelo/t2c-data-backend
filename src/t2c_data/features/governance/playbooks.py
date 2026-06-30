from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.models.governance import GovernanceRecommendation


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _policy_rule_playbook(rule: dict[str, Any]) -> dict[str, Any]:
    title = str(rule.get("name") or rule.get("key") or "Playbook")
    action_label = str(rule.get("action_label") or "Ação")
    recommendation_title = str(rule.get("recommendation_title") or action_label)
    recommendation_detail = str(rule.get("recommendation_detail") or "")
    return {
        "key": str(rule.get("key") or rule.get("action_key") or title),
        "title": title,
        "description": rule.get("description"),
        "scope": str(rule.get("scope") or "table"),
        "trigger_key": str(rule.get("trigger_key") or ""),
        "domain_name": rule.get("domain_name"),
        "datasource_name": rule.get("datasource_name"),
        "criticality": rule.get("criticality"),
        "sensitivity_level": rule.get("sensitivity_level"),
        "severity": str(rule.get("severity") or "medium"),
        "impact": str(rule.get("impact") or "medium"),
        "sla_days": rule.get("sla_days"),
        "action_key": str(rule.get("action_key") or ""),
        "action_label": action_label,
        "recommendation_title": recommendation_title,
        "recommendation_detail": recommendation_detail,
        "auto_create_recommendation": bool(rule.get("auto_create_recommendation", True)),
        "requires_owner": bool(rule.get("requires_owner", False)),
        "requires_classification": bool(rule.get("requires_classification", False)),
        "requires_dictionary": bool(rule.get("requires_dictionary", False)),
        "requires_active_dq": bool(rule.get("requires_active_dq", False)),
        "requires_sla": bool(rule.get("requires_sla", False)),
        "priority": int(rule.get("priority", 100) or 100),
        "is_active": bool(rule.get("is_active", True)),
    }


def get_governance_playbooks(
    session: Session,
    *,
    table_id: int | None = None,
    include_inactive: bool = False,
) -> dict[str, Any]:
    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    items: list[dict[str, Any]] = []
    rules = list(getattr(settings_snapshot, "governance_policy_rules", ()) or ())
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        playbook = _policy_rule_playbook(rule)
        if not include_inactive and not playbook["is_active"]:
            continue
        count_stmt = select(func.count(GovernanceRecommendation.id))
        last_stmt = select(func.max(GovernanceRecommendation.created_at))
        conditions = [GovernanceRecommendation.policy_rule_key == playbook["key"]]
        if table_id is not None:
            conditions.append(GovernanceRecommendation.table_id == int(table_id))
        count_stmt = count_stmt.where(*conditions)
        last_stmt = last_stmt.where(*conditions)
        open_count = int(
            session.scalar(
                select(func.count(GovernanceRecommendation.id)).where(
                    *conditions,
                    GovernanceRecommendation.status == "open",
                )
            )
            or 0
        )
        playbook["matched_recommendations"] = int(session.scalar(count_stmt) or 0)
        playbook["open_recommendations"] = open_count
        playbook["last_matched_at"] = session.scalar(last_stmt)
        playbook["recommended_actions"] = [
            {
                "key": playbook["action_key"],
                "label": playbook["action_label"],
                "description": playbook["recommendation_detail"] or None,
            }
        ]
        items.append(playbook)
    items.sort(key=lambda item: (int(item.get("priority", 100) or 100), str(item.get("key") or "")))
    return {
        "generated_at": now,
        "total": len(items),
        "items": items,
    }


__all__ = ["get_governance_playbooks"]
