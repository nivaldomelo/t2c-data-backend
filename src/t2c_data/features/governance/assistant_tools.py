from __future__ import annotations

from typing import Any


def _confirmation_hint(label: str, impact: str | None, severity: str | None) -> str:
    parts = [label]
    if severity:
        parts.append(f"severidade {severity}")
    if impact:
        parts.append(f"impacto {impact}")
    return " · ".join(parts)


def build_governance_assistant_tools(
    recommendation: dict[str, Any],
    *,
    policy_matches: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    tools: list[dict[str, object]] = []
    severity = str(recommendation.get("severity") or "medium")
    impact = str(recommendation.get("impact") or severity)
    confidence_score = int(recommendation.get("confidence_score") or 0)
    status = str(recommendation.get("status") or "").strip().lower()
    policy_rule_key = recommendation.get("policy_rule_key")
    reason = str(recommendation.get("reason") or recommendation.get("summary") or recommendation.get("detail") or "").strip()

    if status in {"open", "resolved"}:
        tools.extend(
            [
                {
                    "key": "resolve_apply",
                    "label": "Aplicar",
                    "description": "Marca a recomendação como aplicada e registra o fechamento no histórico.",
                    "kind": "resolution",
                    "action": "applied",
                    "confirmation_required": True,
                    "confirmation_label": "Confirmar aplicação",
                    "confirmation_hint": _confirmation_hint("Ação potencialmente irreversível", impact, severity),
                    "severity": severity,
                    "impact": impact,
                    "confidence_score": confidence_score,
                    "can_execute": True,
                },
                {
                    "key": "resolve_dismiss",
                    "label": "Dispensar",
                    "description": "Remove a recomendação da fila ativa com trilha auditável.",
                    "kind": "resolution",
                    "action": "dismissed",
                    "confirmation_required": True,
                    "confirmation_label": "Confirmar dispensa",
                    "confirmation_hint": _confirmation_hint("A recomendação ficará fora da fila ativa", impact, severity),
                    "severity": severity,
                    "impact": impact,
                    "confidence_score": confidence_score,
                    "can_execute": True,
                },
                {
                    "key": "resolve_snooze",
                    "label": "Adiar",
                    "description": "Mantém a recomendação viva, mas reduz sua urgência por um período curto.",
                    "kind": "resolution",
                    "action": "snoozed",
                    "confirmation_required": True,
                    "confirmation_label": "Confirmar adiamento",
                    "confirmation_hint": _confirmation_hint("A recomendação continuará aberta em seguida", impact, severity),
                    "severity": severity,
                    "impact": impact,
                    "confidence_score": confidence_score,
                    "can_execute": True,
                },
            ]
        )

    if policy_rule_key:
        tools.append(
            {
                "key": "apply_policy",
                "label": "Aplicar política",
                "description": "Executa o desfecho recomendado pela política configurada.",
                "kind": "policy",
                "action": "policy_applied",
                "confirmation_required": True,
                "confirmation_label": "Confirmar aplicação da política",
                "confirmation_hint": _confirmation_hint("A política está vinculada ao contexto do ativo", impact, severity),
                "severity": severity,
                "impact": impact,
                "confidence_score": confidence_score,
                "can_execute": True,
            }
        )

    tools.extend(
        [
            {
                "key": "feedback_helpful",
                "label": "Útil",
                "description": "Marca a recomendação como útil para priorização futura.",
                "kind": "feedback",
                "action": "helpful",
                "confirmation_required": True,
                "confirmation_label": "Confirmar feedback útil",
                "confirmation_hint": reason or "Esse feedback ajusta a priorização em próximas recomputações.",
                "severity": severity,
                "impact": impact,
                "confidence_score": confidence_score,
                "can_execute": True,
            },
            {
                "key": "feedback_not_helpful",
                "label": "Não útil",
                "description": "Marca a recomendação como pouco útil para reduzir seu peso em recomputações futuras.",
                "kind": "feedback",
                "action": "not_helpful",
                "confirmation_required": True,
                "confirmation_label": "Confirmar feedback",
                "confirmation_hint": reason or "Esse feedback ajusta a priorização em próximas recomputações.",
                "severity": severity,
                "impact": impact,
                "confidence_score": confidence_score,
                "can_execute": True,
            },
        ]
    )

    return tools


__all__ = ["build_governance_assistant_tools"]
