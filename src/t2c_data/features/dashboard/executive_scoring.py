from __future__ import annotations

from dataclasses import dataclass

from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.support import TableProfile


@dataclass(frozen=True)
class ExecutiveScoreFactor:
    key: str
    label: str
    points: int
    applied: bool
    detail: str


def _has_sensitive_governance_risk(table: TableProfile) -> bool:
    return (table.sensitivity_level or "") in {"confidential", "restricted", "personal_data"} and (
        not table.owner_defined
        or not table.dictionary_complete
        or table.tags_count <= 0
        or not table.review_recent
    )


def build_score_factors(table: TableProfile, recent_incident_count: int = 0, recent_occurrences: int = 0) -> list[ExecutiveScoreFactor]:
    certification_status = resolve_certification_status_for_profile(table)
    eligible_not_certified = table.eligible_for_certification and certification_status != "certified"
    recurring_incidents = recent_incident_count >= 2 or recent_occurrences >= 3
    factors = [
        ExecutiveScoreFactor(
            key="critical_incident",
            label="Incidente crítico aberto",
            points=35,
            applied=table.critical_open_incidents > 0,
            detail=(
                f"{table.critical_open_incidents} incidente(s) crítico(s) em aberto."
                if table.critical_open_incidents > 0
                else "Nenhum incidente crítico aberto."
            ),
        ),
        ExecutiveScoreFactor(
            key="low_dq",
            label="Data Quality abaixo do mínimo",
            points=25,
            applied=table.dq_score is not None and table.dq_score < 70,
            detail=(
                f"DQ score atual em {round(table.dq_score or 0, 1)}."
                if table.dq_score is not None
                else "Sem dados suficientes de Qualidade."
            ),
        ),
        ExecutiveScoreFactor(
            key="missing_owner",
            label="Owner não definido",
            points=20,
            applied=not table.owner_defined,
            detail=(
                "Ativo sem owner definido."
                if not table.owner_defined
                else f"Owner atual: {table.owner_name or 'Definido'}"
            ),
        ),
        ExecutiveScoreFactor(
            key="missing_dictionary",
            label="Dicionário incompleto",
            points=15,
            applied=not table.dictionary_complete,
            detail=(
                "Ainda há colunas sem descrição de negócio."
                if not table.dictionary_complete
                else "Dicionário completo."
            ),
        ),
        ExecutiveScoreFactor(
            key="eligible_not_certified",
            label="Elegível sem certificação",
            points=10,
            applied=eligible_not_certified,
            detail=(
                "Ativo já atende aos critérios mínimos, mas ainda não foi certificado."
                if eligible_not_certified
                else "Ativo certificado ou ainda não elegível."
            ),
        ),
        ExecutiveScoreFactor(
            key="sensitive_governance_gap",
            label="Sensibilidade alta com pendências de governança",
            points=10,
            applied=_has_sensitive_governance_risk(table),
            detail=(
                "Ativo sensível exige reforço de governança."
                if _has_sensitive_governance_risk(table)
                else "Sem risco adicional por sensibilidade."
            ),
        ),
        ExecutiveScoreFactor(
            key="recurring_incidents",
            label="Reincidência de incidentes recentes",
            points=5,
            applied=recurring_incidents,
            detail=(
                f"{recent_incident_count} incidente(s) recente(s), {recent_occurrences} ocorrência(s)."
                if recurring_incidents
                else "Sem reincidência relevante de incidentes."
            ),
        ),
    ]
    return factors


def compute_priority_score(table: TableProfile, recent_incident_count: int = 0, recent_occurrences: int = 0) -> tuple[int, list[ExecutiveScoreFactor]]:
    factors = build_score_factors(table, recent_incident_count=recent_incident_count, recent_occurrences=recent_occurrences)
    score = max(0, min(100, sum(factor.points for factor in factors if factor.applied)))
    return score, factors


def impact_score(
    *,
    dashboards: int = 0,
    users: int = 0,
    upstream: int = 0,
    downstream: int = 0,
) -> float:
    """Multiplier for real blast radius without letting popularity alone dominate."""
    dashboard_factor = min(max(dashboards, 0), 20) * 0.035
    user_factor = min(max(users, 0), 50) * 0.01
    lineage_factor = min(max(upstream, 0) + max(downstream, 0), 30) * 0.012
    return min(2.2, 1.0 + dashboard_factor + user_factor + lineage_factor)


def freshness_factor(freshness_seconds: int | float | None) -> float:
    if freshness_seconds is None:
        return 1.0
    if freshness_seconds >= 86_400:
        return 1.25
    if freshness_seconds >= 21_600:
        return 1.12
    return 1.0


def dq_factor(dq_score: int | float | None) -> float:
    if dq_score is None:
        return 1.08
    if dq_score < 70:
        return 1.25
    if dq_score < 90:
        return 1.1
    return 1.0


def compute_final_priority_score(
    risk_score: int | float,
    *,
    dashboards: int = 0,
    users: int = 0,
    upstream: int = 0,
    downstream: int = 0,
    freshness_seconds: int | float | None = None,
    dq_score: int | float | None = None,
) -> int:
    priority = (
        max(float(risk_score or 0), 0.0)
        * impact_score(dashboards=dashboards, users=users, upstream=upstream, downstream=downstream)
        * freshness_factor(freshness_seconds)
        * dq_factor(dq_score)
    )
    return max(0, min(100, int(round(priority))))


def compute_profile_priority_score(
    table: TableProfile,
    risk_score: int | float,
    *,
    dashboards: int = 0,
    users: int | None = None,
    upstream: int = 0,
    downstream: int = 0,
) -> int:
    return compute_final_priority_score(
        risk_score,
        dashboards=dashboards,
        users=int(table.search_clicks_30d if users is None else users),
        upstream=upstream,
        downstream=downstream,
        freshness_seconds=table.freshness_seconds,
        dq_score=table.dq_score,
    )


def risk_level(score: int) -> str:
    if score >= 75:
        return "critical"
    if score >= 50:
        return "high"
    if score >= 25:
        return "moderate"
    return "low"


def risk_label(score: int) -> str:
    level = risk_level(score)
    return {
        "low": "Baixo",
        "moderate": "Moderado",
        "high": "Alto",
        "critical": "Crítico",
    }[level]


def risk_tone(score: int) -> str:
    level = risk_level(score)
    return {
        "low": "neutral",
        "moderate": "warning",
        "high": "accent",
        "critical": "danger",
    }[level]


def recommended_actions(table: TableProfile, recent_incident_count: int = 0) -> list[str]:
    actions: list[str] = []
    if not table.owner_defined:
        actions.append("Definir owner responsável pelo ativo")
    if not table.dictionary_complete:
        actions.append("Completar o dicionário de dados das colunas")
    if table.tags_count <= 0:
        actions.append("Aplicar tags relevantes para facilitar descoberta e gestão")
    if table.dq_score is None:
        actions.append("Executar ou revisar monitoramento de Data Quality")
    elif table.dq_score < 70:
        actions.append("Corrigir regras e causas-raiz da nota baixa de Data Quality")
    if table.open_incidents > 0:
        actions.append("Tratar incidentes em aberto vinculados ao ativo")
    if table.eligible_for_certification and resolve_certification_status_for_profile(table) != "certified":
        actions.append("Revisar certificação do ativo")
    if _has_sensitive_governance_risk(table):
        actions.append("Revisar privacidade, sensibilidade e controles de acesso")
    if recent_incident_count >= 2:
        actions.append("Investigar reincidência operacional e reforçar monitoração")
    if not table.review_recent:
        actions.append("Atualizar revisão de governança do ativo")
    return actions
