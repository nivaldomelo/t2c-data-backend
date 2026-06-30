from __future__ import annotations

from typing import Any

from t2c_data.features.dashboard.executive_scoring import risk_label, risk_tone

PENDING_ORIGIN_LABELS = {
    "governance": "Governança",
    "metadata": "Metadados",
    "glossary": "Glossário",
    "certification": "Certificação",
    "quality": "Qualidade",
    "operations": "Operação",
    "incidents": "Incidentes",
}


def build_risk_payload(
    table,
    *,
    severity: str,
    origin: str,
    trust_score: int,
    sla_status: str | None,
    context_value: str | None,
) -> dict[str, Any]:
    components: list[str] = []
    score = max(0, min(100, 100 - max(trust_score, 0)))
    components.append(f"Trust {trust_score}")

    if sla_status == "overdue":
        score += 18
        components.append("SLA fora do prazo")
    elif sla_status == "due_soon":
        score += 10
        components.append("SLA próximo do vencimento")

    if severity == "critical":
        score += 18
        components.append("Severidade crítica")
    elif severity == "high":
        score += 12
        components.append("Severidade alta")
    elif severity == "medium":
        score += 6

    if origin in {"incidents", "quality", "operations"}:
        score += 8
        components.append(f"Origem {PENDING_ORIGIN_LABELS.get(origin, origin.title())}")

    if int(getattr(table, "critical_open_incidents", 0) or 0) > 0:
        score += 12
        components.append("Incidente crítico aberto")
    elif int(getattr(table, "open_incidents", 0) or 0) > 0:
        score += 6
        components.append("Incidentes abertos")

    if not bool(getattr(table, "owner_defined", False)):
        score += 8
        components.append("Owner ausente")

    search_clicks_30d = int(getattr(table, "search_clicks_30d", 0) or 0)
    if not bool(getattr(table, "classification_defined", False)):
        score += 4
        components.append("Classificação ausente")
        if search_clicks_30d >= 20:
            score += 4
            components.append("Sem classificação com alto uso")

    if not bool(getattr(table, "dictionary_complete", False)) and int(getattr(table, "search_clicks_30d", 0) or 0) >= 20:
        score += 6
        components.append("Sem dicionário com alto uso")

    if not bool(getattr(table, "sla_defined", False)):
        score += 6
        components.append("SLA ausente")

    if bool(getattr(table, "active_dq_violation", False)):
        score += 10
        components.append("Violação ativa de DQ")

    risk_score = max(0, min(100, int(round(score))))
    return {
        "risk_score": risk_score,
        "risk_label": risk_label(risk_score),
        "risk_tone": risk_tone(risk_score),
        "risk_reason": " · ".join(components[:3] or [context_value or "Sinais operacionais e de governança"]),
        "risk_components": components,
    }


__all__ = ["build_risk_payload"]
