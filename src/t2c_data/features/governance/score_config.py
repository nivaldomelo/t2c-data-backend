from __future__ import annotations

import json
from typing import Mapping


DEFAULT_GOVERNANCE_SCORE_WEIGHTS = {
    "owner_defined": 10,
    "table_description_complete": 10,
    "column_description_complete": 12,
    "tags_applied": 8,
    "glossary_terms": 8,
    "dq_score": 15,
    "certification": 10,
    "incident_health": 10,
    "owner_review": 7,
    "privacy_review": 5,
    "certification_review": 5,
}

GOVERNANCE_SCORE_WEIGHT_KEYS = tuple(DEFAULT_GOVERNANCE_SCORE_WEIGHTS.keys())
DEFAULT_TRUST_SCORE_DOMAIN_ADJUSTMENTS: dict[str, int] = {}
DEFAULT_TRUST_SCORE_CRITICALITY_ADJUSTMENTS: dict[str, int] = {}


def _default_governance_policy_rules() -> list[dict[str, object]]:
    return [
        {
            "key": "policy_owner_required",
            "name": "Owner obrigatório",
            "description": "Garante que o ativo tenha accountability formal antes de avançar na operação e na governança.",
            "trigger_key": "owner_missing",
            "scope": "table",
            "severity": "high",
            "impact": "high",
            "sla_days": 2,
            "action_key": "define_owner",
            "action_label": "Definir owner",
            "recommendation_title": "Definir owner do ativo",
            "recommendation_detail": "O ativo precisa de owner formal para accountability e revisão contínua.",
            "auto_create_recommendation": True,
            "requires_owner": True,
            "priority": 10,
            "is_active": True,
        },
        {
            "key": "policy_classification_required",
            "name": "Classificação obrigatória",
            "description": "Exige classificação antes do uso amplo do ativo.",
            "trigger_key": "classification_missing",
            "scope": "table",
            "severity": "high",
            "impact": "high",
            "sla_days": 2,
            "action_key": "review_classification",
            "action_label": "Revisar classificação",
            "recommendation_title": "Classificação obrigatória",
            "recommendation_detail": "O ativo precisa de classificação consolidada para cumprir a política de governança.",
            "auto_create_recommendation": True,
            "requires_classification": True,
            "priority": 20,
            "is_active": True,
        },
        {
            "key": "policy_sla_required",
            "name": "SLA obrigatório",
            "description": "Garante que cada ativo tenha um SLA formal de atualização ou revisão.",
            "trigger_key": "sla_missing",
            "scope": "table",
            "severity": "medium",
            "impact": "high",
            "sla_days": 2,
            "action_key": "define_sla",
            "action_label": "Definir SLA",
            "recommendation_title": "SLA obrigatório",
            "recommendation_detail": "O ativo precisa de SLA formal para monitoramento, auditoria e escalonamento.",
            "auto_create_recommendation": True,
            "requires_sla": True,
            "priority": 30,
            "is_active": True,
        },
    ]


def normalize_governance_score_weights(raw_weights: Mapping[str, object] | None) -> dict[str, int]:
    if not raw_weights:
        return dict(DEFAULT_GOVERNANCE_SCORE_WEIGHTS)
    normalized = dict(DEFAULT_GOVERNANCE_SCORE_WEIGHTS)
    for key in GOVERNANCE_SCORE_WEIGHT_KEYS:
        value = raw_weights.get(key) if isinstance(raw_weights, Mapping) else None
        if value is None:
            continue
        normalized[key] = max(int(value), 0)
    if sum(normalized.values()) != 100:
        return dict(DEFAULT_GOVERNANCE_SCORE_WEIGHTS)
    return normalized


def normalize_trust_score_adjustments(raw_weights: Mapping[str, object] | None) -> dict[str, int]:
    if not raw_weights:
        return {}
    normalized: dict[str, int] = {}
    for key, value in raw_weights.items():
        normalized_key = str(key).strip().lower()
        if not normalized_key:
            continue
        try:
            normalized[normalized_key] = int(value)
        except Exception:
            continue
    return normalized


def normalize_governance_policy_rules(raw_rules: object | None) -> list[dict[str, object]]:
    if raw_rules is None or raw_rules == "":
        return _default_governance_policy_rules()
    parsed = raw_rules
    if isinstance(raw_rules, str):
        try:
            parsed = json.loads(raw_rules)
        except Exception:
            return _default_governance_policy_rules()
    if not isinstance(parsed, list):
        return _default_governance_policy_rules()
    normalized: list[dict[str, object]] = []
    for index, item in enumerate(parsed, start=1):
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or f"rule-{index}").strip()
        if not key:
            continue
        trigger_key = str(item.get("trigger_key") or "").strip().lower()
        action_key = str(item.get("action_key") or "").strip().lower()
        if not trigger_key or not action_key:
            continue
        normalized.append(
            {
                "key": key,
                "name": str(item.get("name") or key.replace("-", " ").title()).strip(),
                "description": str(item.get("description") or "").strip() or None,
                "trigger_key": trigger_key,
                "scope": str(item.get("scope") or "table").strip().lower() or "table",
                "domain_name": str(item.get("domain_name") or "").strip() or None,
                "datasource_name": str(item.get("datasource_name") or "").strip() or None,
                "criticality": str(item.get("criticality") or "").strip().lower() or None,
                "sensitivity_level": str(item.get("sensitivity_level") or "").strip().lower() or None,
                "min_trust_score": int(item.get("min_trust_score")) if item.get("min_trust_score") is not None and item.get("min_trust_score") != "" else None,
                "min_risk_score": int(item.get("min_risk_score")) if item.get("min_risk_score") is not None and item.get("min_risk_score") != "" else None,
                "min_search_clicks": int(item.get("min_search_clicks"))
                if item.get("min_search_clicks") is not None and item.get("min_search_clicks") != ""
                else None,
                "requires_owner": bool(item.get("requires_owner", False)),
                "requires_classification": bool(item.get("requires_classification", False)),
                "requires_dictionary": bool(item.get("requires_dictionary", False)),
                "requires_active_dq": bool(item.get("requires_active_dq", False)),
                "requires_sla": bool(item.get("requires_sla", False)),
                "severity": str(item.get("severity") or "medium").strip().lower() or "medium",
                "impact": str(item.get("impact") or "medium").strip().lower() or "medium",
                "sla_days": int(item.get("sla_days")) if item.get("sla_days") is not None and item.get("sla_days") != "" else None,
                "action_key": action_key,
                "action_label": str(item.get("action_label") or action_key.replace("_", " ").title()).strip(),
                "recommendation_title": str(item.get("recommendation_title") or "").strip() or None,
                "recommendation_detail": str(item.get("recommendation_detail") or "").strip() or None,
                "auto_create_recommendation": bool(item.get("auto_create_recommendation", True)),
                "priority": int(item.get("priority")) if item.get("priority") is not None and item.get("priority") != "" else 100,
                "is_active": bool(item.get("is_active", True)),
            }
        )
    return sorted(
        normalized,
        key=lambda item: (
            int(item["priority"]),
            str(item["scope"]),
            str(item["trigger_key"]),
            str(item["key"]),
        ),
    )


__all__ = [
    "DEFAULT_GOVERNANCE_SCORE_WEIGHTS",
    "DEFAULT_TRUST_SCORE_DOMAIN_ADJUSTMENTS",
    "DEFAULT_TRUST_SCORE_CRITICALITY_ADJUSTMENTS",
    "GOVERNANCE_SCORE_WEIGHT_KEYS",
    "normalize_governance_policy_rules",
    "normalize_governance_score_weights",
    "normalize_trust_score_adjustments",
]
