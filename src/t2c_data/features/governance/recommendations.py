from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.canonical_assets import load_column_canonical_context, load_table_canonical_context
from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.dashboard.executive_scoring import risk_label, risk_tone
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.assistant_tools import build_governance_assistant_tools
from t2c_data.features.governance.active_governance import build_active_governance_findings
from t2c_data.features.governance.playbooks import get_governance_playbooks
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.risk import build_risk_payload
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.contracts.service import latest_contract_validation_map
from t2c_data.features.governance.trust_history import get_table_governance_trust_history
from t2c_data.features.governance.trust_score import build_trust_score_for_profile
from t2c_data.features.tags.intelligence import load_pending_tag_intelligence_events
from t2c_data.features.timeline.service import get_governance_timeline
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import GovernanceRecommendation
from t2c_data.services.audit import write_audit_log_sync

RECOMMENDATION_STATUS_LABELS = {
    "open": "Aberta",
    "applied": "Aplicada",
    "dismissed": "Dispensada",
    "snoozed": "Adiada",
    "resolved": "Resolvida",
}

RECOMMENDATION_SEVERITY_LABELS = {
    "critical": "Crítica",
    "high": "Alta",
    "medium": "Média",
    "low": "Baixa",
}

RECOMMENDATION_IMPACT_LABELS = {
    "critical": "Crítico",
    "high": "Alto",
    "medium": "Médio",
    "low": "Baixo",
}

RECOMMENDATION_SOURCE_LABELS = {
    "governance": "Governança",
    "quality": "Qualidade",
    "operations": "Operação",
    "incidents": "Incidentes",
    "tags": "Tags",
    "policy": "Política",
    "assistant": "Assistente",
}

RECOMMENDATION_FEEDBACK_LABELS = {
    "helpful": "Útil",
    "neutral": "Neutro",
    "not_helpful": "Pouco útil",
}

RECOMMENDATION_FEEDBACK_TONES = {
    "helpful": "success",
    "neutral": "neutral",
    "not_helpful": "warning",
}

RECOMMENDATION_TRIGGER_KEYS = {
    "owner_missing",
    "classification_missing",
    "no_classification",
    "sla_missing",
    "no_sla",
    "critical_without_dq",
    "classification_high_usage",
    "dictionary_high_usage",
    "recurring_dq_failure_critical",
    "trust_low",
    "certification_reassessment",
    "pii_classification",
    "sensitive_classification",
}


@dataclass(frozen=True)
class RecommendationContext:
    table: Any
    table_profile: Any
    governance_score: dict[str, object]
    trust_score: Any
    risk_payload: dict[str, Any]
    base_actions: list[str]
    policy_matches: list[dict[str, object]]
    signals: list[dict[str, object]]
    canonical_asset: dict[str, object] | None
    recent_events: list[dict[str, object]]
    trust_history: list[dict[str, object]]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _label(mapping: dict[str, str], value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if not normalized:
        return "—"
    return mapping.get(normalized, normalized.replace("_", " ").title())


def _normalize_text(value: str | None) -> str:
    return (value or "").strip().lower()


def _feedback_priority_offset(feedback_rating: str | None) -> int:
    normalized = _normalize_text(feedback_rating)
    if normalized == "helpful":
        return 12
    if normalized == "not_helpful":
        return -12
    return 0


def _recommendation_links(table_profile, column_id: int | None = None) -> dict[str, str]:
    return build_asset_links(
        table_id=table_profile.table_id,
        datasource_id=table_profile.datasource_id,
        database_id=table_profile.database_id,
        schema_id=table_profile.schema_id,
        data_owner_id=table_profile.data_owner_id,
        column_id=column_id,
    )


def _policy_reason(rule: dict[str, object], table_profile, *, trust_score: int, risk_score: int) -> str:
    parts: list[str] = []
    if rule.get("domain_name"):
        parts.append(f"domínio {rule['domain_name']}")
    if rule.get("datasource_name"):
        parts.append(f"fonte {rule['datasource_name']}")
    if rule.get("criticality"):
        parts.append(f"criticidade {rule['criticality']}")
    if rule.get("sensitivity_level"):
        parts.append(f"sensibilidade {rule['sensitivity_level']}")
    if rule.get("requires_owner") and not table_profile.owner_defined:
        parts.append("owner ausente")
    if rule.get("requires_classification") and not table_profile.classification_defined:
        parts.append("classificação ausente")
    if rule.get("requires_dictionary") and not table_profile.dictionary_complete:
        parts.append("dicionário incompleto")
    if rule.get("requires_sla") and not table_profile.sla_defined:
        parts.append("SLA ausente")
    if rule.get("requires_active_dq") and not table_profile.active_dq_violation and int(table_profile.active_dq_rules_count or 0) <= 0:
        parts.append("sem DQ ativa")
    if rule.get("min_trust_score") is not None:
        parts.append(f"trust >= {int(rule['min_trust_score'])}")
    if rule.get("min_risk_score") is not None:
        parts.append(f"risco >= {int(rule['min_risk_score'])}")
    if rule.get("min_search_clicks") is not None:
        parts.append(f"uso >= {int(rule['min_search_clicks'])}")
    if not parts:
        parts.append(f"trust {trust_score} · risco {risk_score}")
    return " · ".join(parts)


def _make_candidate(
    *,
    recommendation_key: str,
    entity_type: str,
    entity_id: int,
    table_profile,
    source_kind: str,
    source_label: str,
    title: str,
    detail: str,
    severity: str,
    impact: str,
    priority: int,
    confidence_score: int,
    trust_score: int,
    risk_score: int,
    action_key: str,
    action_label: str,
    policy_rule_key: str | None = None,
    due_at: datetime | None = None,
    context_value: str | None = None,
    reason: str | None = None,
    summary: str | None = None,
    signals: list[dict[str, object]] | None = None,
    context_json: dict[str, object] | None = None,
    explanation_json: dict[str, object] | None = None,
    column_id: int | None = None,
    tag_name: str | None = None,
) -> dict[str, object]:
    dedupe_key = f"{recommendation_key}:{table_profile.table_id}:{column_id or 0}:{policy_rule_key or 'base'}"
    merged_context_json = dict(context_json or {})
    if signals:
        merged_context_json["signals"] = signals
    return {
        "dedupe_key": dedupe_key,
        "recommendation_key": recommendation_key,
        "policy_rule_key": policy_rule_key,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "table_id": table_profile.table_id,
        "column_id": column_id,
        "datasource_id": table_profile.datasource_id,
        "source_kind": source_kind,
        "source_label": source_label,
        "title": title,
        "detail": detail,
        "severity": severity,
        "impact": impact,
        "priority": priority,
        "confidence_score": confidence_score,
        "trust_score": trust_score,
        "risk_score": risk_score,
        "action_key": action_key,
        "action_label": action_label,
        "due_at": due_at,
        "context_value": context_value,
        "reason": reason,
        "summary": summary or title,
        "signals": signals or [],
        "context_json": merged_context_json,
        "explanation_json": explanation_json or {},
        "tag_name": tag_name,
    }


def _signals_for_profile(table_profile, *, trust_score: int, risk_score: int) -> list[dict[str, object]]:
    signals = [
        {
            "key": "owner",
            "label": "Owner",
            "value": table_profile.owner_name or "Não definido",
            "tone": "success" if table_profile.owner_defined else "warning",
            "detail": "Owner já definido." if table_profile.owner_defined else "Ainda falta accountability clara.",
        },
        {
            "key": "classification",
            "label": "Classificação",
            "value": "Definida" if table_profile.classification_defined else "Pendente",
            "tone": "success" if table_profile.classification_defined else "warning",
            "detail": "A classificação já orienta governança." if table_profile.classification_defined else "Sem classificação consolidada.",
        },
        {
            "key": "dictionary",
            "label": "Dicionário",
            "value": "Completo" if table_profile.dictionary_complete else "Pendente",
            "tone": "success" if table_profile.dictionary_complete else "warning",
            "detail": "O dicionário está em dia." if table_profile.dictionary_complete else "Ainda há lacunas no dicionário.",
        },
        {
            "key": "sla",
            "label": "SLA",
            "value": f"{int(table_profile.sla_hours)} h" if table_profile.sla_defined and table_profile.sla_hours is not None else "Não definido",
            "tone": "success" if table_profile.sla_defined else "warning",
            "detail": "O ativo já possui SLA formal." if table_profile.sla_defined else "Ainda falta um SLA formal para o ativo.",
        },
        {
            "key": "trust",
            "label": "Trust",
            "value": str(trust_score),
            "tone": "success" if trust_score >= 85 else "accent" if trust_score >= 70 else "warning" if trust_score >= 50 else "danger",
            "detail": "Score de confiança atual do ativo.",
        },
        {
            "key": "risk",
            "label": "Risco",
            "value": str(risk_score),
            "tone": "danger" if risk_score >= 80 else "warning" if risk_score >= 60 else "accent" if risk_score >= 35 else "neutral",
            "detail": "Risco composto por trust, SLA e sinais operacionais.",
        },
    ]
    if table_profile.active_dq_violation:
        signals.append(
            {
                "key": "dq",
                "label": "DQ ativa",
                "value": ", ".join(table_profile.active_dq_rule_names[:3]) if table_profile.active_dq_rule_names else "Violação ativa",
                "tone": "danger",
                "detail": "Existe violação ativa de Data Quality.",
            }
        )
    if table_profile.critical_open_incidents > 0:
        signals.append(
            {
                "key": "incidents",
                "label": "Incidentes críticos",
                "value": str(table_profile.critical_open_incidents),
                "tone": "danger",
                "detail": "Há incidente(s) crítico(s) aberto(s).",
            }
        )
    if table_profile.search_clicks_30d:
        signals.append(
            {
                "key": "usage",
                "label": "Uso recente",
                "value": str(table_profile.search_clicks_30d),
                "tone": "accent" if table_profile.search_clicks_30d >= 20 else "neutral",
                "detail": "Cliques recentes no período de 30 dias.",
            }
        )
    return signals


def _base_recommendations_for_profile(
    table_profile,
    *,
    settings_snapshot,
    governance_score: dict[str, object],
    trust_score_eval,
    risk_score: int,
) -> list[dict[str, object]]:
    table = table_profile
    links = _recommendation_links(table)
    now = _now()
    recommendations: list[dict[str, object]] = []
    trust_score = int(trust_score_eval.score)
    signals = _signals_for_profile(table, trust_score=trust_score, risk_score=risk_score)

    for finding in build_active_governance_findings(table, settings_snapshot=settings_snapshot, links=links, now=now):
        due_at = _aware(finding.due_at)
        base_confidence = 92 if finding.severity == "critical" else 84 if finding.severity == "high" else 74
        recommendations.append(
            _make_candidate(
                recommendation_key=finding.key,
                entity_type="table",
                entity_id=table.table_id,
                table_profile=table,
                source_kind=finding.origin,
                source_label=_label(RECOMMENDATION_SOURCE_LABELS, finding.origin),
                title=finding.title,
                detail=finding.description,
                severity=finding.severity,
                impact="critical" if finding.severity == "critical" else "high" if finding.severity == "high" else "medium",
                priority=finding.base_priority + (120 if finding.severity == "critical" else 90 if finding.severity == "high" else 60),
                confidence_score=base_confidence,
                trust_score=trust_score,
                risk_score=risk_score,
                action_key=finding.key,
                action_label=finding.action_label,
                due_at=due_at,
                context_value=finding.context_value,
                reason=finding.description,
                summary=finding.title,
                signals=signals + [{"key": "finding", "label": "Sinal", "value": finding.key, "tone": "warning", "detail": finding.description}],
                context_json={
                    "finding_key": finding.key,
                    "origin": finding.origin,
                    "action_href": finding.action_href,
                    "governance_score": governance_score,
                    "trust_score": trust_score,
                },
                explanation_json={
                    "source": "governance_active",
                    "reason": finding.description,
                    "action_href": finding.action_href,
                },
            )
        )

    if not table.owner_defined:
        recommendations.append(
            _make_candidate(
                recommendation_key="owner_missing",
                entity_type="table",
                entity_id=table.table_id,
                table_profile=table,
                source_kind="governance",
                source_label="Governança",
                title="Definir owner do ativo",
                detail="O ativo perdeu ou nunca teve owner definido. Atribua um responsável claro para reduzir risco e fila futura.",
                severity="high",
                impact="high",
                priority=210,
                confidence_score=96,
                trust_score=trust_score,
                risk_score=risk_score,
                action_key="define_owner",
                action_label="Definir owner",
                due_at=now + timedelta(days=1),
                context_value="Owner ausente",
                reason="Owner obrigatório para accountability e revisão.",
                summary="Recomenda-se definir owner formal.",
                signals=signals,
                context_json={"owner_defined": False, "governance_score": governance_score},
                explanation_json={"source": "rule", "trigger": "owner_missing"},
            )
        )

    if table.has_personal_data or table.has_sensitive_personal_data:
        if not table.classification_defined:
            recommendations.append(
                _make_candidate(
                    recommendation_key="classification_missing",
                    entity_type="table",
                    entity_id=table.table_id,
                    table_profile=table,
                    source_kind="governance",
                    source_label="Governança",
                    title="Classificar ativo com dado pessoal/sensível",
                    detail="Há sinal de dado pessoal ou sensível sem classificação consolidada. Recomenda-se revisar privacidade e classificação.",
                    severity="critical" if table.has_sensitive_personal_data else "high",
                    impact="critical" if table.has_sensitive_personal_data else "high",
                    priority=225 if table.has_sensitive_personal_data else 200,
                    confidence_score=98 if table.has_sensitive_personal_data else 94,
                    trust_score=trust_score,
                    risk_score=risk_score,
                    action_key="review_classification",
                    action_label="Revisar classificação",
                    due_at=now + timedelta(days=2),
                    context_value="PII/sensível sem classificação",
                    reason="Classificação precisa refletir sinais de sensibilidade já presentes.",
                    summary="Recomenda-se revisar a classificação do ativo.",
                    signals=signals,
                    context_json={
                        "has_personal_data": bool(table.has_personal_data),
                        "has_sensitive_personal_data": bool(table.has_sensitive_personal_data),
                        "sensitivity_level": table.sensitivity_level,
                    },
                    explanation_json={"source": "rule", "trigger": "pii_classification"},
                )
            )
        elif table.has_sensitive_personal_data and (table.sensitivity_level or "").strip().lower() not in {"confidential", "restricted", "personal_data"}:
            recommendations.append(
                _make_candidate(
                    recommendation_key="sensitive_classification_review",
                    entity_type="table",
                    entity_id=table.table_id,
                    table_profile=table,
                    source_kind="governance",
                    source_label="Governança",
                    title="Revisar sensibilidade do ativo",
                    detail="O ativo contém dado sensível, mas a classificação atual ainda pode estar subdimensionada.",
                    severity="high",
                    impact="high",
                    priority=190,
                    confidence_score=90,
                    trust_score=trust_score,
                    risk_score=risk_score,
                    action_key="review_sensitivity",
                    action_label="Revisar sensibilidade",
                    due_at=now + timedelta(days=2),
                    context_value=table.sensitivity_level or "Não informada",
                    reason="A sensibilidade detectada pede revalidação da classificação.",
                    summary="Recomenda-se revalidar a sensibilidade do ativo.",
                    signals=signals,
                    context_json={"sensitivity_level": table.sensitivity_level},
                    explanation_json={"source": "rule", "trigger": "sensitive_classification"},
                )
            )

    if not table.dictionary_complete and int(table.search_clicks_30d or 0) >= max(int(settings_snapshot.governance_high_usage_click_threshold or 20), 1):
        recommendations.append(
            _make_candidate(
                recommendation_key="dictionary_missing_high_usage",
                entity_type="table",
                entity_id=table.table_id,
                table_profile=table,
                source_kind="metadata",
                source_label="Metadados",
                title="Completar dicionário do ativo de alto uso",
                detail="O ativo já é consumido com frequência e ainda falta documentação suficiente para operar com confiança.",
                severity="high",
                impact="high",
                priority=180,
                confidence_score=91,
                trust_score=trust_score,
                risk_score=risk_score,
                action_key="complete_dictionary",
                action_label="Completar dicionário",
                due_at=now + timedelta(days=2),
                context_value=f"{int(table.search_clicks_30d or 0)} clique(s) recentes",
                reason="Alto uso com dicionário incompleto aumenta risco operacional.",
                summary="Recomenda-se completar o dicionário.",
                signals=signals,
                context_json={"search_clicks_30d": int(table.search_clicks_30d or 0)},
                explanation_json={"source": "rule", "trigger": "dictionary_high_usage"},
            )
        )

    if not table.classification_defined and int(table.search_clicks_30d or 0) >= max(int(settings_snapshot.governance_high_usage_click_threshold or 20), 1):
        recommendations.append(
            _make_candidate(
                recommendation_key="classification_missing_high_usage",
                entity_type="table",
                entity_id=table.table_id,
                table_profile=table,
                source_kind="governance",
                source_label="Governança",
                title="Classificar ativo de alto uso",
                detail="Ativos muito utilizados sem classificação tendem a gerar uso ambíguo e maior exposição operacional.",
                severity="high",
                impact="high",
                priority=185,
                confidence_score=92,
                trust_score=trust_score,
                risk_score=risk_score,
                action_key="classify_asset",
                action_label="Classificar ativo",
                due_at=now + timedelta(days=2),
                context_value=f"{int(table.search_clicks_30d or 0)} clique(s) recentes",
                reason="Alto uso exige classificação explícita.",
                summary="Recomenda-se classificar o ativo de alto uso.",
                signals=signals,
                context_json={"search_clicks_30d": int(table.search_clicks_30d or 0)},
                explanation_json={"source": "rule", "trigger": "classification_high_usage"},
            )
        )

    if table.certification_status == "certified" and (table.critical_open_incidents > 0 or int(table.recent_dq_failure_runs_30d or 0) >= 2):
        recommendations.append(
            _make_candidate(
                recommendation_key="certification_reassessment",
                entity_type="table",
                entity_id=table.table_id,
                table_profile=table,
                source_kind="certification",
                source_label="Certificação",
                title="Reavaliar certificação do ativo",
                detail="A certificação vigente precisa ser reavaliada por incidentes recorrentes ou falhas de DQ recentes.",
                severity="critical" if table.critical_open_incidents > 0 else "high",
                impact="critical" if table.critical_open_incidents > 0 else "high",
                priority=220 if table.critical_open_incidents > 0 else 175,
                confidence_score=95 if table.critical_open_incidents > 0 else 88,
                trust_score=trust_score,
                risk_score=risk_score,
                action_key="revalidate_certification",
                action_label="Reavaliar certificação",
                due_at=now + timedelta(days=1),
                context_value="Certificado com conflito operacional",
                reason="Incidentes recorrentes ou falhas de DQ indicam necessidade de revalidação.",
                summary="Recomenda-se reavaliar a certificação.",
                signals=signals,
                context_json={
                    "certification_status": table.certification_status,
                    "open_incidents": int(table.open_incidents or 0),
                    "recent_dq_failure_runs_30d": int(table.recent_dq_failure_runs_30d or 0),
                },
                explanation_json={"source": "rule", "trigger": "certification_reassessment"},
            )
        )

    if trust_score < 60:
        recommendations.append(
            _make_candidate(
                recommendation_key="trust_low",
                entity_type="table",
                entity_id=table.table_id,
                table_profile=table,
                source_kind="assistant",
                source_label="Assistente",
                title="Melhorar trust do ativo",
                detail="O trust score está baixo e indica que owner, documentação, DQ e operação precisam de reforço coordenado.",
                severity="medium" if trust_score >= 45 else "high",
                impact="medium" if trust_score >= 45 else "high",
                priority=140,
                confidence_score=max(65, 100 - (100 - trust_score)),
                trust_score=trust_score,
                risk_score=risk_score,
                action_key="improve_trust",
                action_label="Melhorar confiança",
                due_at=now + timedelta(days=3),
                context_value=f"Trust {trust_score}",
                reason="Score de confiança baixo aumenta a necessidade de ação coordenada.",
                summary="Recomenda-se um plano de melhoria de trust.",
                signals=signals,
                context_json={"trust_score": trust_score, "trust_label": trust_score_eval.label},
                explanation_json={"source": "assistant", "trigger": "trust_low"},
            )
        )

    if not table.classification_defined and (
        table.sensitivity_level
        or table.has_personal_data
        or table.has_sensitive_personal_data
        or int(table.tags_count or 0) <= 0
        or int(table.terms_count or 0) <= 0
        or table.active_dq_violation
        or int(table.critical_open_incidents or 0) > 0
    ):
        is_conflict = bool(table.active_dq_violation or int(table.critical_open_incidents or 0) > 0)
        recommendations.append(
            _make_candidate(
                recommendation_key="classification_conflict" if is_conflict else "classification_gap",
                entity_type="table",
                entity_id=table.table_id,
                table_profile=table,
                source_kind="governance",
                source_label="Governança",
                title="Revisar classificação com conflito operacional" if is_conflict else "Revisar classificação do ativo",
                detail=(
                    "Há conflito operacional e a classificação precisa ser reavaliada antes de consolidar o ativo."
                    if is_conflict
                    else "O ativo apresenta lacunas de governança e precisa de revisão de classificação."
                ),
                severity="critical" if is_conflict else "high",
                impact="critical" if is_conflict else "high",
                priority=205 if is_conflict else 172,
                confidence_score=93 if is_conflict else 88,
                trust_score=trust_score,
                risk_score=risk_score,
                action_key="review_classification",
                action_label="Revisar classificação",
                due_at=now + timedelta(days=1 if is_conflict else 2),
                context_value=table.sensitivity_level or "Lacuna de classificação",
                reason="Ativo sem classificação consolidada e com sinais de governança relevantes.",
                summary="Recomenda-se promover a revisão de classificação.",
                signals=signals,
                context_json={
                    "classification_defined": False,
                    "has_personal_data": bool(table.has_personal_data),
                    "has_sensitive_personal_data": bool(table.has_sensitive_personal_data),
                    "active_dq_violation": bool(table.active_dq_violation),
                    "critical_open_incidents": int(table.critical_open_incidents or 0),
                },
                explanation_json={
                    "source": "classification_review",
                    "kind": "conflict" if is_conflict else "gap",
                },
            )
        )

    return recommendations


def _policy_match_for_profile(rule: dict[str, object], table_profile, *, risk_score: int, trust_score: int) -> bool:
    if not bool(rule.get("is_active", True)):
        return False
    trigger_key = str(rule.get("trigger_key") or "").strip().lower()
    if trigger_key and trigger_key not in RECOMMENDATION_TRIGGER_KEYS and trigger_key != "any":
        return False
    scope = str(rule.get("scope") or "table").strip().lower() or "table"
    if scope not in {"table", "any"}:
        return False
    domain_name = _normalize_text(str(rule.get("domain_name") or None))
    datasource_name = _normalize_text(str(rule.get("datasource_name") or None))
    criticality = _normalize_text(str(rule.get("criticality") or None))
    sensitivity = _normalize_text(str(rule.get("sensitivity_level") or None))
    if domain_name and domain_name != _normalize_text(table_profile.domain_name):
        return False
    if datasource_name and datasource_name != _normalize_text(table_profile.datasource_name):
        return False
    if criticality and criticality != _normalize_text(table_profile.certification_criticality):
        return False
    if sensitivity and sensitivity != _normalize_text(table_profile.sensitivity_level):
        return False
    min_trust_score = rule.get("min_trust_score")
    if min_trust_score not in {None, ""} and trust_score < int(min_trust_score):
        return False
    min_risk_score = rule.get("min_risk_score")
    if min_risk_score not in {None, ""} and risk_score < int(min_risk_score):
        return False
    min_search_clicks = rule.get("min_search_clicks")
    if min_search_clicks not in {None, ""} and int(table_profile.search_clicks_30d or 0) < int(min_search_clicks):
        return False
    if bool(rule.get("requires_owner")) and table_profile.owner_defined:
        return False
    if bool(rule.get("requires_classification")) and table_profile.classification_defined:
        return False
    if bool(rule.get("requires_dictionary")) and table_profile.dictionary_complete:
        return False
    if bool(rule.get("requires_sla")) and table_profile.sla_defined:
        return False
    if bool(rule.get("requires_active_dq")) and (
        table_profile.active_dq_violation or int(table_profile.active_dq_rules_count or 0) > 0
    ):
        return False
    return True


def _policy_recommendations_for_profile(
    table_profile,
    *,
    settings_snapshot,
    governance_score: dict[str, object],
    trust_score_eval,
    risk_score: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    recommendations: list[dict[str, object]] = []
    matches: list[dict[str, object]] = []
    trust_score = int(trust_score_eval.score)
    signals = _signals_for_profile(table_profile, trust_score=trust_score, risk_score=risk_score)
    for rule in getattr(settings_snapshot, "governance_policy_rules", ()) or ():
        if not isinstance(rule, dict):
            continue
        if not _policy_match_for_profile(rule, table_profile, risk_score=risk_score, trust_score=trust_score):
            continue
        base_trigger = str(rule.get("trigger_key") or "policy").strip().lower()
        action_key = str(rule.get("action_key") or base_trigger or "policy")
        action_label = str(rule.get("action_label") or action_key.replace("_", " ").title())
        title = str(rule.get("recommendation_title") or rule.get("name") or action_label)
        detail = str(rule.get("recommendation_detail") or rule.get("description") or "").strip()
        severity = str(rule.get("severity") or "medium").strip().lower() or "medium"
        impact = str(rule.get("impact") or severity).strip().lower() or severity
        priority = int(rule.get("priority") or 100)
        sla_days_raw = rule.get("sla_days")
        due_at = None
        if sla_days_raw not in {None, ""}:
            due_at = _now() + timedelta(days=max(int(sla_days_raw), 1))
        confidence = 85
        if rule.get("min_trust_score") not in {None, ""}:
            confidence = max(confidence, min(98, int(rule["min_trust_score"]) + 15))
        if rule.get("min_risk_score") not in {None, ""}:
            confidence = max(confidence, min(98, int(rule["min_risk_score"]) + 10))
        if bool(rule.get("requires_owner")):
            confidence = max(confidence, 92)
        if bool(rule.get("requires_classification")):
            confidence = max(confidence, 92)
        if bool(rule.get("requires_dictionary")):
            confidence = max(confidence, 90)
        if bool(rule.get("requires_active_dq")):
            confidence = max(confidence, 94)
        if bool(rule.get("requires_sla")):
            confidence = max(confidence, 90)
        recommendation_key = f"policy:{rule.get('key') or action_key}:{base_trigger}"
        matches.append(
            {
                "rule_key": str(rule.get("key") or action_key),
                "name": str(rule.get("name") or action_label),
                "description": detail or None,
                "trigger_key": base_trigger,
                "scope": str(rule.get("scope") or "table"),
                "severity": severity,
                "impact": impact,
                "priority": priority,
                "action_key": action_key,
                "action_label": action_label,
                "recommendation_title": title,
                "recommendation_detail": detail or None,
                "auto_create_recommendation": bool(rule.get("auto_create_recommendation", True)),
                "due_at": due_at.isoformat() if due_at else None,
            }
        )
        if not bool(rule.get("auto_create_recommendation", True)):
            continue
        recommendations.append(
            _make_candidate(
                recommendation_key=recommendation_key,
                entity_type="table",
                entity_id=table_profile.table_id,
                table_profile=table_profile,
                source_kind="policy",
                source_label="Política",
                title=title,
                detail=detail or title,
                severity=severity,
                impact=impact,
                priority=priority,
                confidence_score=confidence,
                trust_score=trust_score,
                risk_score=risk_score,
                action_key=action_key,
                action_label=action_label,
                policy_rule_key=str(rule.get("key") or action_key),
                due_at=due_at,
                context_value=table_profile.domain_name or table_profile.datasource_name,
                reason=_policy_reason(rule, table_profile, trust_score=trust_score, risk_score=risk_score),
                summary=title,
                signals=signals + [
                    {
                        "key": "policy",
                        "label": "Política",
                        "value": str(rule.get("name") or rule.get("key") or action_label),
                        "tone": "accent",
                        "detail": detail or None,
                    }
                ],
                context_json={
                    "policy_rule": rule,
                    "governance_score": governance_score,
                    "trust_score": trust_score,
                    "risk_score": risk_score,
                },
                explanation_json={
                    "source": "policy_engine",
                    "rule_key": str(rule.get("key") or action_key),
                    "trigger_key": base_trigger,
                },
            )
        )
    return recommendations, matches


def _tag_recommendations_for_pending_events(
    session: Session,
    *,
    current_user=None,
    limit: int = 500,
    table_ids: list[int] | None = None,
) -> list[dict[str, object]]:
    recommendations: list[dict[str, object]] = []
    events = load_pending_tag_intelligence_events(
        session,
        limit=limit,
        sort_by="certainty_desc",
    )
    if not events:
        return recommendations
    selected_table_ids = {int(table_id) for table_id in table_ids or []}
    if selected_table_ids:
        events = [event for event in events if event.get("table_id") is not None and int(event["table_id"]) in selected_table_ids]
        if not events:
            return recommendations
    table_ids = sorted({int(event["table_id"]) for event in events if event.get("table_id")})
    profiles = load_table_profiles(session, _now(), table_ids=table_ids, current_user=current_user) if table_ids else []
    profile_map = {profile.table_id: profile for profile in profiles}
    for event in events:
        if str(event.get("review_status") or "") not in {"pending_review", "suggested"}:
            continue
        table_id = event.get("table_id")
        if table_id is None:
            continue
        profile = profile_map.get(int(table_id))
        if profile is None:
            continue
        tag_name = str(event.get("tag_name") or "").strip()
        if not tag_name:
            continue
        confidence = int(event.get("confidence_score") or 0)
        severity = "critical" if tag_name in {"PII", "Sensível"} and confidence >= 90 else "high" if confidence >= 80 else "medium"
        risk_payload = build_risk_payload(
            profile,
            severity=severity,
            origin="governance",
            trust_score=int(getattr(profile, "trust_score", 0) or 0),
            sla_status=None,
            context_value=tag_name,
        )
        recommendations.append(
            _make_candidate(
                recommendation_key=f"tag_suggestion:{int(event['id'])}",
                entity_type=str(event.get("entity_type") or "table"),
                entity_id=int(event.get("entity_id") or table_id),
                table_profile=profile,
                column_id=int(event["column_id"]) if event.get("column_id") is not None else None,
                source_kind="tags",
                source_label="Tags",
                title=f"Revisar tag sugerida: {tag_name}",
                detail=str(event.get("inference_reason") or "Sugestão automática de classificação."),
                severity=severity,
                impact="high" if severity in {"critical", "high"} else "medium",
                priority=150 + max(0, confidence - 50),
                confidence_score=confidence,
                trust_score=int(getattr(profile, "trust_score", 0) or 0),
                risk_score=int(risk_payload["risk_score"]),
                action_key="review_tag_suggestion",
                action_label="Abrir revisão",
                policy_rule_key=str(event.get("rule_key") or None),
                due_at=_aware(event.get("created_at")) + timedelta(days=3) if event.get("created_at") else _now() + timedelta(days=3),
                context_value=str(event.get("column_name") or event.get("table_name") or tag_name),
                reason=str(event.get("inference_reason") or "Sugestão automática de classificação."),
                summary=f"{tag_name} sugerida automaticamente",
                signals=[
                    {
                        "key": "tag",
                        "label": "Tag",
                        "value": tag_name,
                        "tone": "accent" if confidence >= 90 else "warning",
                        "detail": str(event.get("inference_reason") or ""),
                    },
                    {
                        "key": "confidence",
                        "label": "Confiança",
                        "value": f"{confidence}%",
                        "tone": "success" if confidence >= 90 else "accent" if confidence >= 70 else "warning",
                        "detail": "Confiança da inferência automática.",
                    },
                ],
                context_json={
                    "tag_event_id": int(event["id"]),
                    "rule_key": event.get("rule_key"),
                    "rule_label": event.get("rule_label"),
                    "inference_source": event.get("inference_source"),
                    "evidence": event.get("evidence") or {},
                    "explorer_url": event.get("explorer_url"),
                },
                explanation_json={
                    "source": "tag_intelligence",
                    "event_id": int(event["id"]),
                },
                tag_name=tag_name,
            )
        )
    return recommendations


def _recommendation_candidates(
    session: Session,
    *,
    current_user=None,
    table_ids: list[int] | None = None,
) -> tuple[list[dict[str, object]], dict[int, object]]:
    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    profiles = load_table_profiles(session, now, table_ids=table_ids, current_user=current_user) if table_ids else load_table_profiles(session, now, current_user=current_user)
    profile_map = {profile.table_id: profile for profile in profiles}
    contract_map = latest_contract_validation_map(
        session,
        table_ids=[profile.table_id for profile in profiles],
    )
    candidates: dict[str, dict[str, object]] = {}
    tag_recommendations = _tag_recommendations_for_pending_events(session, current_user=current_user, table_ids=table_ids)
    for profile in profiles:
        governance_score = build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
        trust_eval = build_trust_score_for_profile(profile, settings_snapshot=settings_snapshot)
        risk_score = int(
            build_risk_payload(
                profile,
                severity="high" if int(trust_eval.score) < 60 else "medium",
                origin="governance",
                trust_score=int(trust_eval.score),
                sla_status="overdue" if profile.open_incidents > 0 or profile.active_dq_violation else None,
                context_value=None,
            )["risk_score"]
        )
        base_recommendations = _base_recommendations_for_profile(
            profile,
            settings_snapshot=settings_snapshot,
            governance_score=governance_score,
            trust_score_eval=trust_eval,
            risk_score=risk_score,
        )
        policy_recommendations, _policy_matches = _policy_recommendations_for_profile(
            profile,
            settings_snapshot=settings_snapshot,
            governance_score=governance_score,
            trust_score_eval=trust_eval,
            risk_score=risk_score,
        )
        contract_status = contract_map.get(int(profile.table_id))
        if contract_status and contract_status.get("validation_status") == "failed":
            contract_candidate = _make_candidate(
                recommendation_key="data_contract_violation",
                entity_type="table",
                entity_id=profile.table_id,
                table_profile=profile,
                source_kind="governance",
                source_label="Governança",
                title="Corrigir violação do data contract",
                detail="A última validação do contrato do ativo apresentou divergências de schema, tipos ou obrigatoriedade.",
                severity="high",
                impact="high",
                priority=200,
                confidence_score=92,
                trust_score=int(trust_eval.score),
                risk_score=risk_score,
                action_key="review_data_contract",
                action_label="Revisar contrato",
                due_at=_now() + timedelta(days=2),
                context_value=f"Contrato v{contract_status.get('version')}",
                reason="Contrato formal está divergente do estado atual do ativo.",
                summary="Recomenda-se corrigir a divergência do data contract.",
                signals=[
                    {
                        "key": "data_contract",
                        "label": "Contrato",
                        "value": "Falhou",
                        "tone": "warning",
                        "detail": "Validação mais recente com divergências.",
                    }
                ],
                context_json=contract_status,
                explanation_json={"source": "contract_validation", "trigger": "failed"},
            )
            candidates[contract_candidate["dedupe_key"]] = contract_candidate
        for candidate in base_recommendations + policy_recommendations:
            candidates[candidate["dedupe_key"]] = candidate
        for candidate in tag_recommendations:
            candidates[candidate["dedupe_key"]] = candidate
    return list(candidates.values()), profile_map


def refresh_governance_recommendations(
    session: Session,
    *,
    current_user=None,
    retention_days: int = 90,
    table_ids: list[int] | None = None,
) -> dict[str, object]:
    now = _now()
    normalized_table_ids = list(dict.fromkeys(int(table_id) for table_id in table_ids or []))
    candidates, profile_map = _recommendation_candidates(session, current_user=current_user, table_ids=table_ids)
    existing_query = select(GovernanceRecommendation)
    if normalized_table_ids:
        existing_query = existing_query.where(GovernanceRecommendation.table_id.in_(normalized_table_ids))
    existing = {
        row.dedupe_key: row
        for row in session.scalars(
            existing_query.options(
                selectinload(GovernanceRecommendation.table).selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
                selectinload(GovernanceRecommendation.column),
                selectinload(GovernanceRecommendation.resolved_by_user),
            )
        ).all()
    }
    active_candidate_keys = {candidate["dedupe_key"] for candidate in candidates}
    created = 0
    updated = 0
    reopened = 0
    resolved = 0

    for candidate in candidates:
        row = existing.get(candidate["dedupe_key"])
        table_profile = profile_map.get(int(candidate["table_id"]))
        if table_profile is None:
            continue
        domain_name = getattr(table_profile, "domain_name", None)
        context_json = dict(candidate["context_json"] or {})
        if candidate.get("signals") and not context_json.get("signals"):
            context_json["signals"] = list(candidate["signals"] or [])
        if row is None:
            row = GovernanceRecommendation(
                dedupe_key=candidate["dedupe_key"],
                recommendation_key=candidate["recommendation_key"],
                policy_rule_key=candidate["policy_rule_key"],
                entity_type=candidate["entity_type"],
                entity_id=int(candidate["entity_id"]),
                table_id=int(candidate["table_id"]),
                column_id=candidate["column_id"],
                datasource_id=candidate["datasource_id"],
                source_kind=str(candidate["source_kind"]),
                source_label=str(candidate["source_label"]),
                title=str(candidate["title"]),
                detail=str(candidate["detail"]),
                severity=str(candidate["severity"]),
                impact=str(candidate["impact"]),
                status="open",
                priority=int(candidate["priority"]),
                confidence_score=int(candidate["confidence_score"]),
                trust_score=int(candidate["trust_score"]),
                risk_score=int(candidate["risk_score"]),
                action_key=str(candidate["action_key"]),
                action_label=str(candidate["action_label"]),
                due_at=_aware(candidate["due_at"]),
                context_value=candidate["context_value"],
                reason=candidate["reason"],
                summary=candidate["summary"],
                context_json=context_json,
                explanation_json=dict(candidate["explanation_json"] or {}),
                domain_name=domain_name,
            )
            session.add(row)
            created += 1
        else:
            if row.status == "snoozed" and row.due_at is not None and row.due_at > now:
                continue
            if row.status in {"applied", "dismissed"}:
                continue
            if row.status == "resolved" and row.resolution_action == "auto_resolved":
                row.status = "open"
                row.resolved_at = None
                row.resolved_by_user_id = None
                row.resolution_action = None
                row.resolution_note = None
                reopened += 1
            row.recommendation_key = str(candidate["recommendation_key"])
            row.policy_rule_key = candidate["policy_rule_key"]
            row.entity_type = str(candidate["entity_type"])
            row.entity_id = int(candidate["entity_id"])
            row.table_id = int(candidate["table_id"])
            row.column_id = candidate["column_id"]
            row.datasource_id = candidate["datasource_id"]
            row.source_kind = str(candidate["source_kind"])
            row.source_label = str(candidate["source_label"])
            row.title = str(candidate["title"])
            row.detail = str(candidate["detail"])
            row.severity = str(candidate["severity"])
            row.impact = str(candidate["impact"])
            row.status = "open"
            row.priority = int(candidate["priority"])
            row.confidence_score = int(candidate["confidence_score"])
            row.trust_score = int(candidate["trust_score"])
            row.risk_score = int(candidate["risk_score"])
            row.action_key = str(candidate["action_key"])
            row.action_label = str(candidate["action_label"])
            row.due_at = _aware(candidate["due_at"])
            row.context_value = candidate["context_value"]
            row.reason = candidate["reason"]
            row.summary = candidate["summary"]
            row.context_json = context_json
            row.explanation_json = dict(candidate["explanation_json"] or {})
            row.domain_name = domain_name
            updated += 1

    for key, row in existing.items():
        if key in active_candidate_keys:
            continue
        if row.status != "open":
            continue
        row.status = "resolved"
        row.resolved_at = now
        row.resolution_action = "auto_resolved"
        row.resolution_note = "A recomendação deixou de se aplicar após a recomputação."
        resolved += 1

    cutoff = now - timedelta(days=max(retention_days, 30))
    stale_query = select(GovernanceRecommendation).where(
        GovernanceRecommendation.created_at < cutoff,
        GovernanceRecommendation.status.in_(["dismissed", "resolved"]),
    )
    if normalized_table_ids:
        stale_query = stale_query.where(GovernanceRecommendation.table_id.in_(normalized_table_ids))
    stale_rows = session.scalars(stale_query).all()
    purged = len(stale_rows)
    for row in stale_rows:
        session.delete(row)
    session.flush()
    return {
        "generated_at": now.isoformat(),
        "candidates": len(candidates),
        "created": created,
        "updated": updated,
        "reopened": reopened,
        "resolved": resolved,
        "purged": purged,
        "retention_days": max(retention_days, 30),
    }


def _recommendation_base_query(session: Session):
    return (
        select(GovernanceRecommendation)
        .options(
            selectinload(GovernanceRecommendation.table).selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
            selectinload(GovernanceRecommendation.table).selectinload(TableEntity.data_owner),
            selectinload(GovernanceRecommendation.column),
            selectinload(GovernanceRecommendation.resolved_by_user),
        )
    )


def _recommendation_filters(items: list[dict[str, object]]) -> dict[str, object]:
    def _options(values: list[tuple[str, str]]) -> list[dict[str, str]]:
        return [{"value": value, "label": label} for value, label in values if value]

    statuses = [("open", "Aberta"), ("applied", "Aplicada"), ("dismissed", "Dispensada"), ("snoozed", "Adiada"), ("resolved", "Resolvida")]
    severities = [("critical", "Crítica"), ("high", "Alta"), ("medium", "Média"), ("low", "Baixa")]
    impacts = [("critical", "Crítico"), ("high", "Alto"), ("medium", "Médio"), ("low", "Baixo")]
    sources = sorted({(str(item["source_kind"]), str(item["source_label"])) for item in items if item.get("source_kind")}, key=lambda pair: pair[1].lower())
    datasources = sorted({(str(item["datasource_name"]), str(item["datasource_name"])) for item in items if item.get("datasource_name")}, key=lambda pair: pair[1].lower())
    schemas = sorted({(str(item["schema_name"]), str(item["schema_name"])) for item in items if item.get("schema_name")}, key=lambda pair: pair[1].lower())
    domains = sorted({(str(item["domain_name"]), str(item["domain_name"])) for item in items if item.get("domain_name")}, key=lambda pair: pair[1].lower())
    owners = sorted({(str(item["owner_name"]), str(item["owner_name"])) for item in items if item.get("owner_name")}, key=lambda pair: pair[1].lower())
    return {
        "statuses": _options(statuses),
        "severities": _options(severities),
        "impacts": _options(impacts),
        "sources": _options(sources),
        "datasources": _options(datasources),
        "schemas": _options(schemas),
        "domains": _options(domains),
        "owners": _options(owners),
    }


def _build_recommendation_item(row: GovernanceRecommendation, *, now: datetime) -> dict[str, object] | None:
    table = row.table
    if table is None or table.schema is None or table.schema.database is None or table.schema.database.datasource is None:
        return None
    datasource = table.schema.database.datasource
    column_name = row.column.name if row.column is not None else None
    table_fqn = f"{datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}"
    owner_name = table.owner or (table.data_owner.name if table.data_owner else None)
    due_at = _aware(row.due_at)
    aging_days = max((now - _aware(row.created_at)).days, 0)
    confidence = int(row.confidence_score or 0)
    trust_score = int(row.trust_score or 0)
    risk_score = int(row.risk_score or 0)
    feedback_rating = _normalize_text(row.feedback_rating)
    effective_priority = int(row.priority or 0) + _feedback_priority_offset(feedback_rating)
    risk_payload = {
        "risk_score": risk_score,
        "risk_label": risk_label(risk_score),
        "risk_tone": risk_tone(risk_score),
        "risk_reason": row.reason or row.detail,
        "risk_components": [row.reason or row.detail],
    }
    links = _recommendation_links(
        type(
            "_Profile",
            (),
            {
                "table_id": table.id,
                "datasource_id": datasource.id,
                "database_id": table.schema.database.id,
                "schema_id": table.schema.id,
                "data_owner_id": table.data_owner_id,
            },
        )(),
        column_id=row.column_id,
    )
    return {
        "id": row.id,
        "key": row.dedupe_key,
        "recommendation_key": row.recommendation_key,
        "policy_rule_key": row.policy_rule_key,
        "entity_type": row.entity_type,
        "entity_id": row.entity_id,
        "table_id": table.id,
        "table_name": table.name,
        "table_fqn": table_fqn,
        "column_id": row.column_id,
        "column_name": column_name,
        "datasource_id": datasource.id,
        "datasource_name": datasource.name,
        "database_name": table.schema.database.name,
        "schema_name": table.schema.name,
        "domain_name": row.domain_name or getattr(table.data_owner, "area", None),
        "owner_name": owner_name,
        "certification_status": table.certification_status,
        "certification_status_label": "Certificada" if table.certification_status == "certified" else table.certification_status,
        "sensitivity_level": table.sensitivity_level,
        "sensitivity_label": table.sensitivity_level or "Não informada",
        "confidence_score": confidence,
        "trust_score": trust_score,
        "trust_label": None,
        "trust_tone": None,
        "risk_score": risk_score,
        "risk_label": risk_payload["risk_label"],
        "risk_tone": risk_payload["risk_tone"],
        "severity": row.severity,
        "severity_label": RECOMMENDATION_SEVERITY_LABELS.get(row.severity, row.severity.title()),
        "impact": row.impact,
        "impact_label": RECOMMENDATION_IMPACT_LABELS.get(row.impact, row.impact.title()),
        "status": row.status,
        "status_label": RECOMMENDATION_STATUS_LABELS.get(row.status, row.status.title()),
        "action_key": row.action_key,
        "action_label": row.action_label,
        "due_at": due_at.isoformat() if due_at else None,
        "aging_days": aging_days,
        "context_value": row.context_value,
        "reason": row.reason,
        "summary": row.summary,
        "source_kind": row.source_kind,
        "source_label": row.source_label,
        "priority": effective_priority,
        "assistant_summary": row.summary or row.detail,
        "feedback_rating": feedback_rating or None,
        "feedback_label": RECOMMENDATION_FEEDBACK_LABELS.get(feedback_rating, "Neutro") if feedback_rating else None,
        "feedback_tone": RECOMMENDATION_FEEDBACK_TONES.get(feedback_rating, "neutral") if feedback_rating else "neutral",
        "feedback_note": row.feedback_note,
        "feedback_updated_at": row.feedback_updated_at,
        "feedback_updated_by_user_id": row.feedback_updated_by_user_id,
        "signals": list(
            row.context_json.get("signals")
            if isinstance(row.context_json, dict) and row.context_json.get("signals")
            else row.explanation_json.get("signals")
            if isinstance(row.explanation_json, dict) and row.explanation_json.get("signals")
            else []
        ),
        "context": dict(row.context_json or {}),
        "links": links,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "resolved_at": row.resolved_at,
        "resolved_by_user_id": row.resolved_by_user_id,
        "resolution_action": row.resolution_action,
        "resolution_note": row.resolution_note,
    }


def get_governance_recommendations(
    session: Session,
    *,
    q: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    impact: str | None = None,
    source: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    owner: str | None = None,
    min_confidence: int | None = None,
    max_confidence: int | None = None,
    policy_driven: bool | None = None,
    page: int = 1,
    page_size: int = 25,
    current_user=None,
) -> dict[str, object]:
    refresh_governance_recommendations(session, current_user=current_user)
    now = _now()
    rows = session.scalars(_recommendation_base_query(session).order_by(GovernanceRecommendation.priority.desc(), GovernanceRecommendation.risk_score.desc(), GovernanceRecommendation.created_at.desc())).all()
    items: list[dict[str, object]] = []
    for row in rows:
        item = _build_recommendation_item(row, now=now)
        if item is None:
            continue
        if current_user is not None and row.table is not None:
            # reuse table visibility from canonical context reader
            try:
                load_table_canonical_context(session, row.table_id, current_user=current_user)
            except Exception:
                continue
        items.append(item)

    normalized_q = _normalize_text(q)
    normalized_status = _normalize_text(status)
    normalized_severity = _normalize_text(severity)
    normalized_impact = _normalize_text(impact)
    normalized_source = _normalize_text(source)
    normalized_datasource = _normalize_text(datasource)
    normalized_schema = _normalize_text(schema_name)
    normalized_domain = _normalize_text(domain)
    normalized_owner = _normalize_text(owner)
    filtered: list[dict[str, object]] = []
    for item in items:
        searchable = " ".join(
            [
                str(item.get("title") or ""),
                str(item.get("detail") or ""),
                str(item.get("table_fqn") or ""),
                str(item.get("column_name") or ""),
                str(item.get("owner_name") or ""),
                str(item.get("source_label") or ""),
                str(item.get("summary") or ""),
            ]
        ).lower()
        if normalized_q and normalized_q not in searchable:
            continue
        if normalized_status and str(item.get("status") or "").lower() != normalized_status:
            continue
        if normalized_severity and str(item.get("severity") or "").lower() != normalized_severity:
            continue
        if normalized_impact and str(item.get("impact") or "").lower() != normalized_impact:
            continue
        if normalized_source and normalized_source not in _normalize_text(str(item.get("source_kind") or "")):
            continue
        if normalized_datasource and normalized_datasource not in _normalize_text(str(item.get("datasource_name") or "")):
            continue
        if normalized_schema and normalized_schema not in _normalize_text(str(item.get("schema_name") or "")):
            continue
        if normalized_domain and normalized_domain not in _normalize_text(str(item.get("domain_name") or "")):
            continue
        if normalized_owner and normalized_owner not in _normalize_text(str(item.get("owner_name") or "")):
            continue
        if min_confidence is not None and int(item.get("confidence_score") or 0) < min_confidence:
            continue
        if max_confidence is not None and int(item.get("confidence_score") or 0) > max_confidence:
            continue
        if policy_driven is True and not item.get("policy_rule_key"):
            continue
        if policy_driven is False and item.get("policy_rule_key"):
            continue
        filtered.append(item)

    filtered.sort(
        key=lambda item: (
            -int(item.get("priority") or 0),
            -int(item.get("risk_score") or 0),
            -int(item.get("confidence_score") or 0),
            str(item.get("table_fqn") or "").lower(),
            str(item.get("title") or "").lower(),
        )
    )

    open_count = sum(1 for item in filtered if str(item.get("status") or "") == "open")
    high_confidence = sum(1 for item in filtered if int(item.get("confidence_score") or 0) >= 80 and str(item.get("status") or "") == "open")
    due_soon = sum(
        1
        for item in filtered
        if str(item.get("status") or "") == "open"
        and item.get("due_at")
        and _aware(datetime.fromisoformat(str(item["due_at"]))) <= now + timedelta(days=3)
    )
    policy_driven_count = sum(1 for item in filtered if item.get("policy_rule_key") and str(item.get("status") or "") == "open")
    applied_recently = sum(
        1
        for item in filtered
        if str(item.get("status") or "") == "applied" and _aware(item.get("resolved_at")) and _aware(item.get("resolved_at")) >= now - timedelta(days=7)
    )
    dismissed_recently = sum(
        1
        for item in filtered
        if str(item.get("status") or "") == "dismissed" and _aware(item.get("resolved_at")) and _aware(item.get("resolved_at")) >= now - timedelta(days=7)
    )

    total = len(filtered)
    start = max(page - 1, 0) * page_size
    end = start + page_size
    return {
        "generated_at": now.isoformat(),
        "total": total,
        "page": page,
        "page_size": page_size,
        "summary": {
            "open_recommendations": open_count,
            "high_confidence": high_confidence,
            "due_soon": due_soon,
            "policy_driven": policy_driven_count,
            "applied_recently": applied_recently,
            "dismissed_recently": dismissed_recently,
        },
        "filters": _recommendation_filters(filtered),
        "items": filtered[start:end],
    }


def resolve_governance_recommendations(
    session: Session,
    *,
    recommendation_ids: list[int],
    resolution_action: str,
    resolution_note: str | None,
    actor_user_id: int | None,
    request_audit=None,
) -> dict[str, object]:
    now = _now()
    normalized_action = _normalize_text(resolution_action)
    if normalized_action not in {"applied", "apply", "dismissed", "dismiss", "snoozed", "snooze"}:
        raise ValueError("Unsupported resolution action")
    target_status = {
        "apply": "applied",
        "applied": "applied",
        "dismiss": "dismissed",
        "dismissed": "dismissed",
        "snooze": "snoozed",
        "snoozed": "snoozed",
    }[normalized_action]
    rows = {
        row.id: row
        for row in session.scalars(
            select(GovernanceRecommendation).where(GovernanceRecommendation.id.in_(dict.fromkeys(recommendation_ids)))
        ).all()
    }
    applied_ids: list[int] = []
    failed_items: list[dict[str, object]] = []
    for recommendation_id in dict.fromkeys(recommendation_ids):
        row = rows.get(recommendation_id)
        if row is None:
            failed_items.append({"recommendation_id": recommendation_id, "message": "Recommendation not found"})
            continue
        if row.status not in {"open", "resolved"} and not (row.status == "resolved" and row.resolution_action == "auto_resolved"):
            failed_items.append({"recommendation_id": recommendation_id, "message": f"Recommendation already {row.status}"})
            continue
        row.status = target_status
        row.resolved_at = now
        row.resolved_by_user_id = actor_user_id
        row.resolution_action = normalized_action
        row.resolution_note = resolution_note
        applied_ids.append(recommendation_id)
    if request_audit is not None and applied_ids:
        write_audit_log_sync(
            session,
            action="governance.recommendation.batch_resolve",
            entity_type="governance_recommendation",
            entity_id="batch",
            after={
                "recommendation_ids": applied_ids,
                "resolution_action": normalized_action,
                "resolution_note": resolution_note,
            },
            metadata={
                "message": "Governance recommendation batch resolved",
                "count": len(applied_ids),
            },
            **request_audit,
        )
    session.flush()
    return {
        "requested": len(dict.fromkeys(recommendation_ids)),
        "succeeded": len(applied_ids),
        "failed": len(failed_items),
        "applied_ids": applied_ids,
        "failed_items": failed_items,
    }


def apply_governance_policy_recommendations(
    session: Session,
    *,
    recommendation_ids: list[int],
    resolution_note: str | None,
    actor_user_id: int | None,
    request_audit=None,
) -> dict[str, object]:
    now = _now()
    rows = {
        row.id: row
        for row in session.scalars(
            select(GovernanceRecommendation).where(GovernanceRecommendation.id.in_(dict.fromkeys(recommendation_ids)))
        ).all()
    }
    applied_ids: list[int] = []
    failed_items: list[dict[str, object]] = []

    for recommendation_id in dict.fromkeys(recommendation_ids):
        row = rows.get(recommendation_id)
        if row is None:
            failed_items.append({"recommendation_id": recommendation_id, "message": "Recommendation not found"})
            continue
        if not row.policy_rule_key:
            failed_items.append({"recommendation_id": recommendation_id, "message": "Recommendation is not policy-driven"})
            continue
        if row.status not in {"open", "resolved"} or (row.status == "resolved" and row.resolution_action != "auto_resolved"):
            failed_items.append({"recommendation_id": recommendation_id, "message": f"Recommendation already {row.status}"})
            continue
        row.status = "applied"
        row.resolved_at = now
        row.resolved_by_user_id = actor_user_id
        row.resolution_action = "policy_applied"
        row.resolution_note = resolution_note
        applied_ids.append(recommendation_id)

    if request_audit is not None and applied_ids:
        write_audit_log_sync(
            session,
            action="governance.recommendation.batch_policy_apply",
            entity_type="governance_recommendation",
            entity_id="batch",
            after={
                "recommendation_ids": applied_ids,
                "resolution_action": "policy_applied",
                "resolution_note": resolution_note,
            },
            metadata={
                "message": "Governance policy recommendations batch applied",
                "count": len(applied_ids),
            },
            **request_audit,
        )
    session.flush()
    return {
        "requested": len(dict.fromkeys(recommendation_ids)),
        "succeeded": len(applied_ids),
        "failed": len(failed_items),
        "applied_ids": applied_ids,
        "failed_items": failed_items,
    }


def _load_recommendation_by_ref(session: Session, recommendation_ref: str) -> GovernanceRecommendation | None:
    normalized_ref = str(recommendation_ref or "").strip()
    if not normalized_ref:
        return None

    row = None
    if normalized_ref.isdigit():
        row = session.scalar(
            _recommendation_base_query(session).where(GovernanceRecommendation.id == int(normalized_ref))
        )
    if row is None:
        row = session.scalar(
            _recommendation_base_query(session).where(GovernanceRecommendation.dedupe_key == normalized_ref)
        )
    return row


def get_governance_recommendation_context(
    session: Session,
    *,
    recommendation_ref: str,
    current_user=None,
) -> dict[str, object]:
    row = _load_recommendation_by_ref(session, recommendation_ref)
    if row is None:
        raise ValueError("Recommendation not found")
    now = _now()
    item = _build_recommendation_item(row, now=now)
    if item is None:
        raise ValueError("Recommendation asset context not available")
    table_id = int(item["table_id"])
    column_id = int(item["column_id"]) if item.get("column_id") is not None else None
    profile = load_table_profiles(session, now, table_ids=[table_id], current_user=current_user)
    table_profile = profile[0] if profile else None
    trust_history: list[dict[str, object]] = []
    policy_matches = []
    settings_snapshot = get_governance_settings_snapshot(session)
    risk_payload = build_risk_payload(
        table_profile or type(
            "_Profile",
            (),
            {
                "table_id": table_id,
                "datasource_id": item.get("datasource_id"),
                "database_id": None,
                "schema_id": None,
                "data_owner_id": None,
                "open_incidents": 0,
                "active_dq_violation": False,
            },
        )(),
        severity=str(item.get("severity") or "medium"),
        origin=str(item.get("source_kind") or "governance"),
        trust_score=int(item.get("trust_score") or 0),
        sla_status="overdue" if table_profile is not None and int(table_profile.open_incidents or 0) > 0 else None,
        context_value=str(item.get("context_value") or item.get("summary") or item.get("action_label") or ""),
    )
    governance_score = None
    trust_eval = None
    if table_profile is not None:
        trust_history = get_table_governance_trust_history(session, table_id=table_id, limit=14)
        governance_score = build_governance_score_for_profile(table_profile, settings_snapshot=settings_snapshot)
        trust_eval = build_trust_score_for_profile(table_profile, settings_snapshot=settings_snapshot)
        risk_payload = build_risk_payload(
            table_profile,
            severity=str(item.get("severity") or "medium"),
            origin=str(item.get("source_kind") or "governance"),
            trust_score=int(trust_eval.score),
            sla_status="overdue" if int(table_profile.open_incidents or 0) > 0 else None,
            context_value=str(item.get("context_value") or item.get("summary") or item.get("action_label") or ""),
        )
        for rule in getattr(settings_snapshot, "governance_policy_rules", ()) or ():
            if not isinstance(rule, dict):
                continue
            if _policy_match_for_profile(
                rule,
                table_profile,
                risk_score=int(risk_payload["risk_score"]),
                trust_score=int(trust_eval.score),
            ):
                policy_matches.append(
                    {
                        "key": str(rule.get("key") or rule.get("action_key") or "rule"),
                        "name": str(rule.get("name") or rule.get("key") or rule.get("action_key") or "Política"),
                        "description": rule.get("description"),
                        "trigger_key": rule.get("trigger_key"),
                        "scope": rule.get("scope"),
                        "severity": rule.get("severity"),
                        "impact": rule.get("impact"),
                        "action_key": rule.get("action_key"),
                        "action_label": rule.get("action_label"),
                        "auto_create_recommendation": bool(rule.get("auto_create_recommendation", True)),
                        "min_trust_score": rule.get("min_trust_score"),
                        "min_risk_score": rule.get("min_risk_score"),
                        "min_search_clicks": rule.get("min_search_clicks"),
                        "requires_owner": bool(rule.get("requires_owner")),
                        "requires_classification": bool(rule.get("requires_classification")),
                        "requires_dictionary": bool(rule.get("requires_dictionary")),
                        "requires_active_dq": bool(rule.get("requires_active_dq")),
                        "requires_sla": bool(rule.get("requires_sla")),
                    }
                )
    canonical_asset = None
    try:
        canonical_asset = (
            load_column_canonical_context(session, column_id, current_user=current_user).model_dump(mode="json")
            if column_id is not None
            else load_table_canonical_context(session, table_id, current_user=current_user).model_dump(mode="json")
        )
    except Exception:
        canonical_asset = None
    recent_events: list[dict[str, object]] = []
    try:
        timeline_payload = get_governance_timeline(
            session,
            current_user=current_user,
            table_id=table_id,
            column_id=column_id,
            page=1,
            page_size=12,
        )
        recent_events = [
            {
                "id": event.id,
                "occurred_at": event.occurred_at,
                "category": event.category,
                "event_type": event.event_type,
                "title": event.title,
                "detail": event.detail,
                "source_module": event.source_module,
                "source_label": event.source_label,
                "actor_name": event.actor_name,
                "actor_email": event.actor_email,
                "mode": event.mode,
                "severity": event.severity,
                "priority": event.priority,
                "table_id": event.table_id,
                "column_id": event.column_id,
                "table_name": event.table_name,
                "column_name": event.column_name,
                "schema_name": event.schema_name,
                "database_name": event.database_name,
                "datasource_name": event.datasource_name,
                "table_fqn": event.table_fqn,
                "owner_name": event.owner_name,
                "certification_status": event.certification_status,
                "certification_status_label": event.certification_status_label,
                "readiness_score": event.readiness_score,
                "trust_score": event.trust_score,
                "trust_label": event.trust_label,
                "trust_tone": event.trust_tone,
                "trust_delta": event.trust_delta,
                "trust_summary": event.trust_summary,
                "active_dq_violation": event.active_dq_violation,
                "active_dq_rule_names": event.active_dq_rule_names,
                "href": event.href,
                "metadata_json": event.metadata_json,
            }
            for event in timeline_payload.items
        ]
    except Exception:
        recent_events = []
    playbooks: list[dict[str, object]] = []
    try:
        playbooks_payload = get_governance_playbooks(session, table_id=table_id)
        playbooks = list(playbooks_payload.get("items") or [])
    except Exception:
        playbooks = []
    assistant_tools = build_governance_assistant_tools(item, policy_matches=policy_matches)
    if trust_eval is None:
        trust_eval = type(
            "_TrustEval",
            (),
            {
                "score": int(item.get("trust_score") or 0),
                "label": item.get("trust_label") or "Contexto parcial",
                "tone": item.get("trust_tone") or "neutral",
                "summary": (
                    f"Trust {int(item.get('trust_score') or 0)} · contexto parcial"
                    if item.get("trust_score") is not None
                    else "Contexto parcial"
                ),
                "context": {"fallback": True, "reason": "profile_unavailable"},
            },
        )()
    assistant_summary_parts = [
        str(item.get("summary") or item.get("action_label") or item.get("reason") or item["recommendation_key"]),
        f"Trust {int(item.get('trust_score') or 0)}",
        f"Risco {int(item.get('risk_score') or 0)}",
    ]
    if table_profile is None:
        assistant_summary_parts.append("Contexto canônico indisponível no momento")
    if policy_matches:
        assistant_summary_parts.append(f"{len(policy_matches)} política(s) aplicável(is)")
    if trust_history:
        assistant_summary_parts.append(f"{len(trust_history)} ponto(s) de histórico de trust")
    if recent_events:
        assistant_summary_parts.append(f"{len(recent_events)} evento(s) recentes")
    if playbooks:
        assistant_summary_parts.append(f"{len(playbooks)} playbook(s) aplicável(is)")
    if assistant_tools:
        assistant_summary_parts.append(f"{len(assistant_tools)} ação(ões) assistida(s)")
    return {
        "generated_at": now.isoformat(),
        "recommendation": item,
        "assistant_summary": " · ".join(assistant_summary_parts),
        "assistant_tools": assistant_tools,
        "policy_matches": policy_matches,
        "playbooks": playbooks,
        "recent_events": recent_events,
        "trust_history": trust_history,
        "canonical_asset": canonical_asset,
        "governance_score": governance_score,
        "trust_score": {
            "score": int(trust_eval.score),
            "label": trust_eval.label,
            "tone": trust_eval.tone,
            "summary": trust_eval.summary,
            "context": trust_eval.context,
        },
        "risk_payload": risk_payload,
    }


__all__ = [
    "RECOMMENDATION_IMPACT_LABELS",
    "RECOMMENDATION_SEVERITY_LABELS",
    "RECOMMENDATION_SOURCE_LABELS",
    "RECOMMENDATION_STATUS_LABELS",
    "get_governance_recommendation_context",
    "get_governance_recommendations",
    "refresh_governance_recommendations",
    "resolve_governance_recommendations",
]
