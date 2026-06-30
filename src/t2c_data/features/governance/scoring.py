from __future__ import annotations

from typing import Mapping

from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.governance.score_config import (
    DEFAULT_GOVERNANCE_SCORE_WEIGHTS,
    GOVERNANCE_SCORE_WEIGHT_KEYS,
    normalize_governance_score_weights,
)
from t2c_data.features.governance.rules import certification_review_due, owner_review_due, privacy_review_due


def _scaled_points(weight: int, ratio: float) -> int:
    return int(round(max(weight, 0) * max(min(ratio, 1.0), 0.0)))


def _factor(
    *,
    key: str,
    label: str,
    max_points: int,
    points: int,
    detail: str,
) -> dict[str, object]:
    normalized = max(0, min(points, max_points))
    if normalized >= max_points:
        status = "met"
    elif normalized > 0:
        status = "partial"
    else:
        status = "missing"
    return {
        "key": key,
        "label": label,
        "points": normalized,
        "max_points": max_points,
        "status": status,
        "detail": detail,
    }


def governance_score_label(score: int) -> tuple[str, str]:
    if score >= 85:
        return "Forte", "success"
    if score >= 70:
        return "Boa", "accent"
    if score >= 50:
        return "Em evolução", "warning"
    return "Crítica", "danger"


def build_governance_score(
    *,
    owner_defined: bool,
    table_description_complete: bool,
    column_description_complete: bool,
    tags_count: int,
    terms_count: int,
    dq_score: float | None,
    certification_status: str,
    eligible_for_certification: bool,
    open_incidents: int,
    critical_open_incidents: int,
    owner_review_current: bool,
    privacy_review_current: bool,
    certification_review_current: bool,
    weights: Mapping[str, int] | None = None,
) -> dict[str, object]:
    effective_weights = normalize_governance_score_weights(weights)
    factors = [
        _factor(
            key="owner_defined",
            label="Responsável definido",
            max_points=effective_weights["owner_defined"],
            points=effective_weights["owner_defined"] if owner_defined else 0,
            detail="O ativo já possui owner claramente atribuído." if owner_defined else "Ainda falta definir ownership formal para o ativo.",
        ),
        _factor(
            key="table_description_complete",
            label="Descrição da tabela",
            max_points=effective_weights["table_description_complete"],
            points=effective_weights["table_description_complete"] if table_description_complete else 0,
            detail="A tabela possui descrição utilizável para contexto de negócio." if table_description_complete else "A descrição principal da tabela ainda está ausente ou insuficiente.",
        ),
        _factor(
            key="column_description_complete",
            label="Descrição de colunas",
            max_points=effective_weights["column_description_complete"],
            points=effective_weights["column_description_complete"] if column_description_complete else 0,
            detail="As colunas já contam com dicionário ou descrição suficiente." if column_description_complete else "Ainda existem colunas sem descrição consistente no dicionário.",
        ),
        _factor(
            key="tags_applied",
            label="Tags aplicadas",
            max_points=effective_weights["tags_applied"],
            points=effective_weights["tags_applied"] if tags_count > 0 else 0,
            detail=f"{tags_count} tag(s) aplicadas ao ativo." if tags_count > 0 else "O ativo ainda não recebeu tags de classificação.",
        ),
        _factor(
            key="glossary_terms",
            label="Termos de glossário",
            max_points=effective_weights["glossary_terms"],
            points=effective_weights["glossary_terms"] if terms_count > 0 else 0,
            detail=f"{terms_count} termo(s) associados ao ativo." if terms_count > 0 else "Ainda falta associar termos de glossário ao ativo.",
        ),
    ]

    if dq_score is None:
        dq_points = 0
        dq_detail = "Sem avaliação recente de Data Quality."
    elif dq_score >= 90:
        dq_points = effective_weights["dq_score"]
        dq_detail = f"Data Quality em {round(dq_score, 1)} pts, acima do limiar saudável."
    elif dq_score >= 70:
        dq_points = _scaled_points(effective_weights["dq_score"], 8 / 15)
        dq_detail = f"Data Quality em {round(dq_score, 1)} pts, com cobertura parcial."
    else:
        dq_points = 0
        dq_detail = f"Data Quality em {round(dq_score, 1)} pts, abaixo do mínimo recomendado."
    factors.append(
        _factor(
            key="dq_score",
            label="Qualidade de dados",
            max_points=effective_weights["dq_score"],
            points=dq_points,
            detail=dq_detail,
        )
    )

    if certification_status == "certified":
        certification_points = effective_weights["certification"]
        certification_detail = "O ativo já está certificado."
    elif certification_status in {"in_review", "revalidation_pending"} or eligible_for_certification:
        certification_points = _scaled_points(effective_weights["certification"], 0.6)
        certification_detail = "O ativo já tem maturidade para avançar na certificação, mas ainda não concluiu o ciclo."
    else:
        certification_points = 0
        certification_detail = "A certificação ainda não foi concluída para este ativo."
    factors.append(
        _factor(
            key="certification",
            label="Certificação",
            max_points=effective_weights["certification"],
            points=certification_points,
            detail=certification_detail,
        )
    )

    if critical_open_incidents > 0:
        incident_points = 0
        incident_detail = f"Há {critical_open_incidents} incidente(s) crítico(s) aberto(s) para o ativo."
    elif open_incidents > 0:
        incident_points = _scaled_points(effective_weights["incident_health"], 0.6)
        incident_detail = f"Há {open_incidents} incidente(s) aberto(s), porém sem criticidade máxima."
    else:
        incident_points = effective_weights["incident_health"]
        incident_detail = "Não há incidentes abertos associados ao ativo."
    factors.append(
        _factor(
            key="incident_health",
            label="Saúde operacional",
            max_points=effective_weights["incident_health"],
            points=incident_points,
            detail=incident_detail,
        )
    )

    factors.extend(
        [
            _factor(
                key="owner_review",
                label="Revisão de owner",
                max_points=effective_weights["owner_review"],
                points=effective_weights["owner_review"] if owner_review_current else 0,
                detail="A revisão de owner está em dia." if owner_review_current else "A revisão de owner está vencida.",
            ),
            _factor(
                key="privacy_review",
                label="Revisão de privacidade",
                max_points=effective_weights["privacy_review"],
                points=effective_weights["privacy_review"] if privacy_review_current else 0,
                detail="A revisão de privacidade está em dia." if privacy_review_current else "A revisão de privacidade está vencida.",
            ),
            _factor(
                key="certification_review",
                label="Revisão de certificação",
                max_points=effective_weights["certification_review"],
                points=effective_weights["certification_review"] if certification_review_current else 0,
                detail="A revisão de certificação está em dia." if certification_review_current else "A revisão de certificação está vencida ou pendente.",
            ),
        ]
    )

    score = int(sum(int(factor["points"]) for factor in factors))
    max_score = int(sum(int(factor["max_points"]) for factor in factors))
    label, tone = governance_score_label(score)
    completed_factors = sum(1 for factor in factors if factor["status"] == "met")
    partial_factors = sum(1 for factor in factors if factor["status"] == "partial")
    total_factors = len(factors)
    return {
        "score": score,
        "max_score": max_score,
        "label": label,
        "tone": tone,
        "completed_factors": completed_factors,
        "partial_factors": partial_factors,
        "total_factors": total_factors,
        "summary": f"{completed_factors} de {total_factors} dimensões atendidas integralmente.",
        "factors": factors,
    }


def build_governance_score_for_profile(table, *, settings_snapshot) -> dict[str, object]:
    certification_status = resolve_certification_status_for_profile(table)
    return build_governance_score(
        owner_defined=bool(table.owner_defined),
        table_description_complete=bool(table.description_complete),
        column_description_complete=bool(table.dictionary_complete),
        tags_count=int(table.tags_count or 0),
        terms_count=int(table.terms_count or 0),
        dq_score=float(table.dq_score) if table.dq_score is not None else None,
        certification_status=certification_status,
        eligible_for_certification=bool(getattr(table, "eligible_for_certification", False)),
        open_incidents=int(table.open_incidents or 0),
        critical_open_incidents=int(table.critical_open_incidents or 0),
        owner_review_current=not owner_review_due(table, settings_snapshot=settings_snapshot),
        privacy_review_current=not privacy_review_due(table, settings_snapshot=settings_snapshot),
        certification_review_current=not certification_review_due(table, settings_snapshot=settings_snapshot),
        weights=getattr(settings_snapshot, "governance_score_weights", None),
    )


__all__ = [
    "DEFAULT_GOVERNANCE_SCORE_WEIGHTS",
    "GOVERNANCE_SCORE_WEIGHT_KEYS",
    "build_governance_score",
    "build_governance_score_for_profile",
    "governance_score_label",
    "normalize_governance_score_weights",
]
