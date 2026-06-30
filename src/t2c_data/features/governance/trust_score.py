from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from t2c_data.features.governance.scoring import build_governance_score_for_profile


@dataclass(frozen=True)
class TrustScoreEvaluation:
    score: int
    label: str
    tone: str
    summary: str
    operational_score: int
    readiness_score: int
    governance_score: int
    context: dict[str, Any]


def trust_score_label(score: int) -> tuple[str, str]:
    if score >= 85:
        return "Muito confiável", "success"
    if score >= 70:
        return "Confiável", "accent"
    if score >= 50:
        return "Em atenção", "warning"
    return "Baixa confiança", "danger"


def _clamp(value: int) -> int:
    return max(0, min(100, value))


def _trust_adjustments(table, *, settings_snapshot) -> tuple[int, list[dict[str, object]]]:
    adjustments: list[dict[str, object]] = []
    score = 0
    domain_key = str(getattr(table, "domain_name", "") or "").strip().lower()
    criticality_key = str(getattr(table, "certification_criticality", "") or "").strip().lower()

    domain_adjustments = dict(getattr(settings_snapshot, "trust_score_domain_adjustments", None) or {})
    criticality_adjustments = dict(getattr(settings_snapshot, "trust_score_criticality_adjustments", None) or {})

    if domain_key and domain_key in domain_adjustments:
        delta = int(domain_adjustments[domain_key] or 0)
        if delta:
            score += delta
            adjustments.append(
                {
                    "key": "domain",
                    "scope": "domain",
                    "value": domain_key,
                    "points": delta,
                    "label": f"Domínio {domain_key}",
                }
            )

    if criticality_key and criticality_key in criticality_adjustments:
        delta = int(criticality_adjustments[criticality_key] or 0)
        if delta:
            score += delta
            adjustments.append(
                {
                    "key": "criticality",
                    "scope": "criticality",
                    "value": criticality_key,
                    "points": delta,
                    "label": f"Criticidade {criticality_key}",
                }
            )

    return score, adjustments


def _operational_component(table, *, high_usage_threshold: int) -> tuple[int, list[dict[str, object]]]:
    penalties: list[dict[str, object]] = []
    score = 100

    if not bool(getattr(table, "owner_defined", False)):
        score -= 12
        penalties.append({"key": "no_owner", "label": "Owner ausente", "points": 12})

    if not bool(getattr(table, "classification_defined", False)):
        score -= 8
        penalties.append({"key": "no_classification", "label": "Sem classificação", "points": 8})

    if not bool(getattr(table, "description_complete", False)):
        score -= 6
        penalties.append({"key": "no_description", "label": "Descrição incompleta", "points": 6})

    if not bool(getattr(table, "dictionary_complete", False)):
        score -= 8
        penalties.append({"key": "no_dictionary", "label": "Dicionário incompleto", "points": 8})

    if not bool(getattr(table, "sla_defined", False)):
        score -= 6
        penalties.append({"key": "no_sla", "label": "SLA ausente", "points": 6})

    if int(getattr(table, "tags_count", 0) or 0) <= 0:
        score -= 4
        penalties.append({"key": "no_tags", "label": "Sem tags", "points": 4})

    if int(getattr(table, "terms_count", 0) or 0) <= 0:
        score -= 4
        penalties.append({"key": "no_terms", "label": "Sem termos", "points": 4})

    dq_score = getattr(table, "dq_score", None)
    if dq_score is None:
        score -= 10
        penalties.append({"key": "no_dq", "label": "Sem DQ recente", "points": 10})
    elif float(dq_score) >= 90:
        pass
    elif float(dq_score) >= 70:
        score -= 4
        penalties.append({"key": "dq_degraded", "label": "DQ degradada", "points": 4})
    else:
        score -= 14
        penalties.append({"key": "dq_low", "label": "DQ baixa", "points": 14})

    if bool(getattr(table, "active_dq_violation", False)):
        score -= 14
        penalties.append({"key": "active_dq_violation", "label": "Violação ativa de DQ", "points": 14})

    if int(getattr(table, "critical_open_incidents", 0) or 0) > 0:
        score -= 16
        penalties.append({"key": "critical_incident", "label": "Incidente crítico aberto", "points": 16})
    elif int(getattr(table, "open_incidents", 0) or 0) > 0:
        score -= 8
        penalties.append({"key": "open_incidents", "label": "Incidentes abertos", "points": 8})

    recent_failures = int(getattr(table, "recent_dq_failure_runs_30d", 0) or 0)
    if recent_failures >= 2:
        score -= 10
        penalties.append({"key": "recurrent_dq_failure", "label": "Falha DQ recorrente", "points": 10})
    elif recent_failures == 1:
        score -= 5
        penalties.append({"key": "recent_dq_failure", "label": "Falha DQ recente", "points": 5})

    freshness_seconds = getattr(table, "freshness_seconds", None)
    if freshness_seconds is None:
        score -= 4
        penalties.append({"key": "no_freshness", "label": "Sem evidência de atualização", "points": 4})
    elif freshness_seconds > 72 * 3600:
        score -= 8
        penalties.append({"key": "stale_pipeline", "label": "Atualização antiga", "points": 8})
    elif freshness_seconds > 24 * 3600:
        score -= 4
        penalties.append({"key": "freshness_warn", "label": "Atualização recente limitada", "points": 4})

    search_clicks_30d = int(getattr(table, "search_clicks_30d", 0) or 0)
    if search_clicks_30d >= high_usage_threshold and not bool(getattr(table, "classification_defined", False)):
        score -= 6
        penalties.append({"key": "high_usage_no_classification", "label": "Alto uso sem classificação", "points": 6})
    if search_clicks_30d >= high_usage_threshold and not bool(getattr(table, "dictionary_complete", False)):
        score -= 6
        penalties.append({"key": "high_usage_no_dictionary", "label": "Alto uso sem dicionário", "points": 6})

    active_dq_rules_count = int(getattr(table, "active_dq_rules_count", 0) or 0)
    is_critical = (getattr(table, "certification_criticality", None) or "").strip().lower() in {"high", "critical"}
    if is_critical and active_dq_rules_count <= 0:
        score -= 8
        penalties.append({"key": "critical_without_dq", "label": "Crítico sem DQ mínima", "points": 8})

    return _clamp(score), penalties


def build_trust_score(
    *,
    readiness_score: int,
    governance_score: int,
    operational_score: int,
    penalties: list[dict[str, object]],
    adjustments: list[dict[str, object]] | None = None,
) -> TrustScoreEvaluation:
    base_score = int(round((readiness_score * 0.35) + (governance_score * 0.25) + (operational_score * 0.40)))
    adjustment_points = sum(int(item.get("points") or 0) for item in (adjustments or []))
    score = _clamp(base_score + adjustment_points)
    label, tone = trust_score_label(score)
    penalty_detail = ", ".join(str(item["label"]) for item in penalties[:4]) if penalties else "Sem penalidades operacionais relevantes."
    adjustment_detail = ", ".join(
        f"{item['label']} {item['points']:+d}"
        for item in (adjustments or [])[:4]
        if int(item.get("points") or 0) != 0
    )
    if adjustment_detail:
        penalty_detail = f"{penalty_detail} · Ajustes: {adjustment_detail}"
    return TrustScoreEvaluation(
        score=score,
        label=label,
        tone=tone,
        summary=f"Prontidão {readiness_score} · Governança {governance_score} · Operação {operational_score}. {penalty_detail}",
        operational_score=operational_score,
        readiness_score=readiness_score,
        governance_score=governance_score,
        context={
            "base_score": base_score,
            "penalties": penalties,
            "adjustments": adjustments or [],
        },
    )


def build_trust_score_for_profile(table, *, settings_snapshot) -> TrustScoreEvaluation:
    governance_score = build_governance_score_for_profile(table, settings_snapshot=settings_snapshot)
    operational_score, penalties = _operational_component(
        table,
        high_usage_threshold=max(int(getattr(settings_snapshot, "governance_high_usage_click_threshold", 20) or 20), 1),
    )
    _, adjustments = _trust_adjustments(table, settings_snapshot=settings_snapshot)
    return build_trust_score(
        readiness_score=int(getattr(table, "readiness_score", 0) or 0),
        governance_score=int(governance_score["score"]),
        operational_score=operational_score,
        penalties=penalties,
        adjustments=adjustments,
    )


__all__ = [
    "TrustScoreEvaluation",
    "build_trust_score",
    "build_trust_score_for_profile",
    "trust_score_label",
]
