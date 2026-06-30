from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from t2c_data.features.governance.assistant_tools import build_governance_assistant_tools
from t2c_data.features.governance.recommendations import (
    apply_governance_policy_recommendations,
    get_governance_recommendation_context,
    resolve_governance_recommendations,
    _load_recommendation_by_ref,
)
from t2c_data.models.governance import GovernanceRecommendation
from t2c_data.services.audit import write_audit_log_sync

_FEEDBACK_LABELS = {
    "helpful": "Útil",
    "neutral": "Neutro",
    "not_helpful": "Pouco útil",
}

_FEEDBACK_TONES = {
    "helpful": "success",
    "neutral": "neutral",
    "not_helpful": "warning",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str:
    return (value or "").strip().lower()


def _feedback_rating(value: str | None) -> str:
    normalized = _normalize_text(value)
    if normalized in {"helpful", "neutral", "not_helpful"}:
        return normalized
    raise ValueError("Unsupported feedback rating")


def _feedback_priority_offset(feedback_rating: str | None) -> int:
    normalized = _normalize_text(feedback_rating)
    if normalized == "helpful":
        return 12
    if normalized == "not_helpful":
        return -12
    return 0


def _recommendation_action_by_tool(tool_key: str) -> tuple[str, dict[str, Any]]:
    normalized = _normalize_text(tool_key)
    if normalized == "resolve_apply":
        return "resolve", {"resolution_action": "applied"}
    if normalized == "resolve_dismiss":
        return "resolve", {"resolution_action": "dismissed"}
    if normalized == "resolve_snooze":
        return "resolve", {"resolution_action": "snoozed"}
    if normalized == "apply_policy":
        return "apply_policy", {}
    if normalized == "feedback_helpful":
        return "feedback", {"feedback_rating": "helpful"}
    if normalized == "feedback_not_helpful":
        return "feedback", {"feedback_rating": "not_helpful"}
    raise ValueError("Unsupported assistant tool")


def build_governance_recommendation_assistant_payload(
    session: Session,
    *,
    recommendation_ref: str,
    current_user=None,
) -> dict[str, object]:
    context = get_governance_recommendation_context(session, recommendation_ref=recommendation_ref, current_user=current_user)
    tools = build_governance_assistant_tools(context["recommendation"], policy_matches=list(context.get("policy_matches") or []))
    context["assistant_tools"] = tools
    return context


def set_governance_recommendation_feedback(
    session: Session,
    *,
    recommendation_ref: str,
    feedback_rating: str,
    feedback_note: str | None,
    actor_user_id: int | None,
    request_audit=None,
) -> dict[str, object]:
    row = _load_recommendation_by_ref(session, recommendation_ref)
    if row is None:
        raise ValueError("Recommendation not found")
    normalized_rating = _feedback_rating(feedback_rating)
    row.feedback_rating = normalized_rating
    row.feedback_note = feedback_note
    row.feedback_updated_at = _now()
    row.feedback_updated_by_user_id = actor_user_id
    if request_audit is not None:
        write_audit_log_sync(
            session,
            action="governance.recommendation.feedback",
            entity_type="governance_recommendation",
            entity_id=row.id,
            after={
                "recommendation_id": row.id,
                "recommendation_key": row.dedupe_key,
                "feedback_rating": normalized_rating,
                "feedback_note": feedback_note,
            },
            metadata={"message": "Governance recommendation feedback updated"},
            **request_audit,
        )
    session.flush()
    return {
        "recommendation_id": row.id,
        "recommendation_key": row.dedupe_key,
        "feedback_rating": row.feedback_rating,
        "feedback_label": _FEEDBACK_LABELS.get(row.feedback_rating or "", row.feedback_rating or "—"),
        "feedback_tone": _FEEDBACK_TONES.get(row.feedback_rating or "", "neutral"),
        "feedback_note": row.feedback_note,
        "feedback_updated_at": row.feedback_updated_at,
        "feedback_updated_by_user_id": row.feedback_updated_by_user_id,
        "message": "Feedback registrado com sucesso.",
    }


def execute_governance_assistant_action(
    session: Session,
    *,
    recommendation_ref: str,
    tool_key: str,
    confirm: bool,
    resolution_note: str | None,
    actor_user_id: int | None,
    request_audit=None,
) -> dict[str, object]:
    row = _load_recommendation_by_ref(session, recommendation_ref)
    if row is None:
        raise ValueError("Recommendation not found")
    action_kind, payload = _recommendation_action_by_tool(tool_key)
    tools = build_governance_assistant_tools(
        {
            "severity": row.severity,
            "impact": row.impact,
            "status": row.status,
            "policy_rule_key": row.policy_rule_key,
            "confidence_score": row.confidence_score,
            "reason": row.reason,
            "summary": row.summary,
            "detail": row.detail,
        }
    )
    allowed = {tool["key"]: tool for tool in tools}
    tool = allowed.get(tool_key)
    if tool is None:
        raise ValueError("Assistant tool not available for this recommendation")
    if bool(tool.get("confirmation_required", True)) and not confirm:
        raise ValueError("Confirmation required to execute this tool")

    executed = False
    result: dict[str, object] = {"action_kind": action_kind, "tool_key": tool_key}
    if action_kind == "resolve":
        resolution_action = str(payload["resolution_action"])
        result = resolve_governance_recommendations(
            session,
            recommendation_ids=[row.id],
            resolution_action=resolution_action,
            resolution_note=resolution_note,
            actor_user_id=actor_user_id,
            request_audit=None,
        )
        executed = bool(result.get("succeeded"))
    elif action_kind == "apply_policy":
        result = apply_governance_policy_recommendations(
            session,
            recommendation_ids=[row.id],
            resolution_note=resolution_note,
            actor_user_id=actor_user_id,
            request_audit=None,
        )
        executed = bool(result.get("succeeded"))
    elif action_kind == "feedback":
        result = set_governance_recommendation_feedback(
            session,
            recommendation_ref=recommendation_ref,
            feedback_rating=str(payload["feedback_rating"]),
            feedback_note=resolution_note,
            actor_user_id=actor_user_id,
            request_audit=None,
        )
        executed = True
    else:  # pragma: no cover - defensive
        raise ValueError("Unsupported assistant action")

    if request_audit is not None:
        write_audit_log_sync(
            session,
            action="governance.recommendation.assistant.execute",
            entity_type="governance_recommendation",
            entity_id=row.id,
            after={
                "recommendation_id": row.id,
                "recommendation_key": row.dedupe_key,
                "tool_key": tool_key,
                "result": result,
                "confirmation": bool(confirm),
                "resolution_note": resolution_note,
            },
            metadata={"message": "Governance assistant action executed"},
            **request_audit,
        )
    session.flush()
    return {
        "ok": True,
        "recommendation_id": row.id,
        "recommendation_key": row.dedupe_key,
        "tool_key": tool_key,
        "executed": executed,
        "message": "Ação assistida executada com sucesso." if executed else "Ação assistida registrada.",
        "result": result,
    }


__all__ = [
    "build_governance_recommendation_assistant_payload",
    "execute_governance_assistant_action",
    "set_governance_recommendation_feedback",
]
