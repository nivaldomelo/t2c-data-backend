from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from t2c_data.features.catalog.canonical_assets import (
    compact_canonical_asset_context,
    load_column_canonical_context,
    load_table_canonical_context,
)
from t2c_data.features.catalog.correlation import build_table_correlation_summary
from t2c_data.features.catalog.operational_context import load_table_operational_context
from t2c_data.features.governance.change_management import create_metadata_change_request
from t2c_data.features.governance import list_asset_slas
from t2c_data.features.intelligence.asset_signals import build_asset_intelligence
from t2c_data.features.platform.analytics import track_usage_event
from t2c_data.features.platform.automations import execute_automation_action
from t2c_data.models.auth import User
from t2c_data.schemas.assistant import (
    AssistantActionIn,
    AssistantActionOut,
    AssistantActionOptionOut,
    AssistantExplainImpactOut,
    AssistantExplainOut,
    AssistantExplainProblemOut,
    AssistantRecommendationOut,
)
from t2c_data.services.audit import write_audit_log_sync

_ASSET_REF_RE = re.compile(r"^(?:(table|column)[/:])?(\d+)$", re.IGNORECASE)

_SEVERITY_ORDER = {
    "critical": 0,
    "high": 1,
    "warning": 2,
    "info": 3,
    "neutral": 4,
}

_ASSET_SIGNAL_SEVERITY = {
    "critical": "critical",
    "high": "high",
    "medium": "warning",
    "low": "info",
}

_ASSET_SIGNAL_PROBLEMS = {
    "dq_not_evaluated": ("dq_not_evaluated", "Data Quality sem avaliação", "O ativo ainda não tem avaliação consolidada de qualidade.", "Revisar regras ou execução de DQ para criar uma linha de base confiável.", "data_quality"),
    "dq_low": ("dq_degraded", "Data Quality degradada", "O agregador de sinais indica score baixo de Data Quality.", "Revalidar regras de qualidade e priorizar correção antes de ampliar consumo.", "data_quality"),
    "dq_active_violation": ("dq_degraded", "Violação ativa de Data Quality", "Há violação ativa de qualidade associada ao ativo.", "Tratar a violação de DQ e acompanhar o resultado no contexto do ativo.", "data_quality"),
    "freshness_delayed": ("stale_pipeline", "Freshness atrasado", "O ativo está fora da janela esperada de atualização.", "Validar ingestão, freshness e execução do pipeline.", "datasource"),
    "critical_incident_open": ("open_incidents", "Incidente crítico em aberto", "O agregador encontrou incidente crítico em aberto para este ativo.", "Acompanhar ou abrir incidente operacional com prioridade alta.", "incidents"),
    "incident_open": ("open_incidents", "Incidente em aberto", "Há incidente em aberto associado ao ativo.", "Acompanhar o incidente antes de aprovar mudanças ou certificação.", "incidents"),
    "no_owner": ("owner_missing", "Owner não definido", "O ativo não tem owner formal consolidado.", "Definir owner para criar accountability e fila de revisão.", "change_management"),
    "description_incomplete": ("description_missing", "Descrição incompleta", "A documentação principal do ativo está incompleta.", "Completar a descrição para reduzir ambiguidade operacional.", "explorer"),
    "dictionary_incomplete": ("dictionary_missing", "Dicionário parcial", "O dicionário do ativo ainda não cobre os metadados esperados.", "Completar o dicionário de dados no Explorer.", "explorer"),
    "trust_low": ("trust_low", "Trust score baixo", "A confiança consolidada do ativo está baixa.", "Tratar os sinais de governança, qualidade e ownership antes de promover uso.", "explorer"),
    "airflow_failure": ("operational_failure", "Falha no Airflow", "O pipeline associado está em estado de falha ou degradação.", "Reprocessar pipeline ou abrir incidente para investigação.", "datasource"),
    "data_lake_error": ("data_lake_error", "Erro no Data Lake", "O inventário do Data Lake reportou erro para o ativo.", "Abrir a visão de Data Lake e validar scan, path e qualidade.", "datasource"),
    "data_lake_quality_low": ("data_lake_quality_low", "Qualidade baixa no Data Lake", "O último score de qualidade do Data Lake está baixo.", "Revisar o inventário e reconciliar com as regras de DQ.", "data_quality"),
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: Any | None) -> str:
    return str(value or "").strip()


def _asset_ref_from(kind: str, asset_id: int) -> str:
    return f"{kind}:{asset_id}"


def _dump_object(value: Any | None) -> Any | None:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return _dump_object(value.model_dump())
    if isinstance(value, dict):
        return {str(key): _dump_object(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_dump_object(item) for item in value]
    if hasattr(value, "__dict__"):
        return {str(key): _dump_object(item) for key, item in vars(value).items()}
    return value


def _safe_asset_intelligence(session: Session, *, table_id: int, current_user: User | None) -> Any | None:
    if current_user is None:
        return None
    try:
        return build_asset_intelligence(session, asset_id=table_id, current_user=current_user)
    except HTTPException:
        raise
    except Exception:
        return None


def _asset_intelligence_dict(asset_intelligence: Any | None) -> dict[str, object] | None:
    dumped = _dump_object(asset_intelligence)
    if isinstance(dumped, dict):
        return dumped
    return None


def _parse_asset_ref(asset_ref: str) -> tuple[str, int]:
    normalized = _normalize_text(asset_ref)
    if not normalized:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="asset_ref is required")
    if normalized.isdigit():
        return "table", int(normalized)
    match = _ASSET_REF_RE.match(normalized)
    if match is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Unsupported asset reference")
    asset_type = (match.group(1) or "table").strip().lower()
    asset_id = int(match.group(2))
    if asset_type not in {"table", "column"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Unsupported asset reference")
    return asset_type, asset_id


def _load_asset_bundle(
    session: Session,
    *,
    asset_type: str,
    asset_id: int,
    current_user: User | None = None,
) -> dict[str, object]:
    if asset_type == "column":
        asset = compact_canonical_asset_context(load_column_canonical_context(session, asset_id, current_user=current_user))
    else:
        asset = compact_canonical_asset_context(load_table_canonical_context(session, asset_id, current_user=current_user))

    operational_context = load_table_operational_context(
        session,
        table_id=asset.table_id,
        datasource_id=asset.source.datasource_id,
        database_id=asset.source.database_id,
        schema_id=asset.source.schema_id,
        column_id=asset.column_id,
    )
    correlation_summary = build_table_correlation_summary(db=session, table_id=asset.table_id, current_user=current_user)
    slas = list_asset_slas(
        session,
        asset_type=asset.entity_kind,
        asset_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
    )

    active_sla: dict[str, object] | None = None
    for item in slas.get("items", []):
        if str(item.get("status") or "").strip().lower() == "active":
            active_sla = item
            break
    if active_sla is None and slas.get("items"):
        active_sla = slas["items"][0]

    return {
        "asset_ref": _asset_ref_from(asset.entity_kind, asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id),
        "asset": asset,
        "operational_context": operational_context,
        "correlation_summary": correlation_summary,
        "slas": slas,
        "active_sla": active_sla,
    }


def _problem(
    key: str,
    label: str,
    severity: str,
    detail: str,
    *,
    evidence: dict[str, object] | None = None,
    action_hint: str | None = None,
    href: str | None = None,
) -> AssistantExplainProblemOut:
    return AssistantExplainProblemOut(
        key=key,
        label=label,
        severity=severity,
        detail=detail,
        evidence=evidence or {},
        action_hint=action_hint,
        href=href,
    )


def _impact(
    key: str,
    label: str,
    detail: str,
    *,
    tone: str = "neutral",
    evidence: dict[str, object] | None = None,
) -> AssistantExplainImpactOut:
    return AssistantExplainImpactOut(
        key=key,
        label=label,
        tone=tone,
        detail=detail,
        evidence=evidence or {},
    )


def _sort_by_severity(items: list[AssistantExplainProblemOut]) -> list[AssistantExplainProblemOut]:
    return sorted(items, key=lambda item: (_SEVERITY_ORDER.get(item.severity, 99), item.label))


def _signal_severity(value: str | None) -> str:
    return _ASSET_SIGNAL_SEVERITY.get(str(value or "").strip().lower(), "info")


def _signal_href(asset: Any, link_key: str | None) -> str | None:
    links = getattr(asset, "links", None)
    if links is None or not link_key:
        return None
    return getattr(links, link_key, None)


def _merge_asset_signal_problems(
    problems: list[AssistantExplainProblemOut],
    *,
    asset: Any,
    asset_intelligence: Any | None,
) -> list[AssistantExplainProblemOut]:
    if asset_intelligence is None:
        return problems
    existing = {problem.key for problem in problems}
    signals = list(getattr(asset_intelligence, "signals", []) or [])
    risk_score = getattr(asset_intelligence, "risk_score", None)
    trust_score = getattr(asset_intelligence, "trust_score", None)
    priority_score = getattr(asset_intelligence, "priority_score", None)

    for signal in signals:
        signal_type = str(getattr(signal, "type", "") or "")
        mapped = _ASSET_SIGNAL_PROBLEMS.get(signal_type)
        if mapped is None:
            continue
        key, label, detail, action_hint, link_key = mapped
        if key in existing:
            continue
        problems.append(
            _problem(
                key,
                label,
                _signal_severity(getattr(signal, "severity", None)),
                detail,
                evidence={
                    "source": "asset_signals",
                    "signal_type": signal_type,
                    "signal_severity": getattr(signal, "severity", None),
                    "risk_score": risk_score,
                    "trust_score": trust_score,
                    "priority_score": priority_score,
                },
                action_hint=action_hint,
                href=_signal_href(asset, link_key),
            )
        )
        existing.add(key)

    return _sort_by_severity(problems)


def _build_problems(asset: Any, operational_context: dict[str, object] | None, correlation_summary: Any, active_sla: dict[str, object] | None) -> list[AssistantExplainProblemOut]:
    problems: list[AssistantExplainProblemOut] = []
    links = getattr(asset, "links", None)
    owner_defined = bool(asset.owner.owner_defined)
    classification_defined = bool(asset.classification.classification_defined)
    description_complete = bool(asset.evidence.description_complete)
    dictionary_complete = bool(asset.evidence.dictionary_complete)
    dq_score = asset.evidence.dq_score
    open_incidents = int(asset.evidence.open_incidents or 0)
    critical_open_incidents = int(asset.evidence.critical_open_incidents or 0)

    if not owner_defined:
        problems.append(
            _problem(
                "owner_missing",
                "Owner não definido",
                "high",
                "Este ativo ainda não tem owner formal mapeado no catálogo.",
                evidence={"owner_defined": False, "data_owner_id": asset.owner.data_owner_id},
                action_hint="Definir owner antes de executar mudanças mais sensíveis.",
                href=getattr(links, "owners", None) if links is not None else None,
            )
        )

    if not classification_defined:
        problems.append(
            _problem(
                "classification_missing",
                "Classificação pendente",
                "warning",
                "A classificação de privacidade e sensibilidade ainda não foi consolidada.",
                evidence={
                    "classification_defined": False,
                    "sensitivity_level": getattr(asset.classification, "sensitivity_level", None),
                    "has_personal_data": getattr(asset.classification, "has_personal_data", None),
                    "has_sensitive_personal_data": getattr(asset.classification, "has_sensitive_personal_data", None),
                },
                action_hint="Revisar a classificação antes de ampliar o uso do ativo.",
                href=getattr(links, "privacy", None) if links is not None else None,
            )
        )

    if not description_complete:
        problems.append(
            _problem(
                "description_missing",
                "Descrição incompleta",
                "warning",
                "A descrição principal do ativo ainda não está completa.",
                evidence={"description_complete": False},
                action_hint="Completar a descrição reduz ambiguidade e retrabalho operacional.",
                href=getattr(links, "explorer", None) if links is not None else None,
            )
        )

    if not dictionary_complete:
        problems.append(
            _problem(
                "dictionary_missing",
                "Dicionário parcial",
                "warning",
                "O dicionário ainda não cobre os metadados esperados para leitura confiável.",
                evidence={"dictionary_complete": False},
                action_hint="Completar o dicionário para melhorar a confiança do catálogo.",
                href=getattr(links, "explorer", None) if links is not None else None,
            )
        )

    if active_sla is None:
        problems.append(
            _problem(
                "sla_missing",
                "SLA ausente",
                "warning",
                "Não há SLA ativo consolidado para este ativo.",
                evidence={"sla_defined": False},
                action_hint="Definir um SLA ajuda a diferenciar atraso operacional de atraso aceitável.",
                href=getattr(links, "change_management", None) if links is not None else None,
            )
        )

    if dq_score is not None and dq_score < 90:
        severity = "critical" if dq_score < 70 else "warning"
        problems.append(
            _problem(
                "dq_degraded",
                "Data Quality degradada",
                severity,
                f"O score de Data Quality está em {dq_score:.1f} pontos.",
                evidence={"dq_score": dq_score},
                action_hint="Revalidar as regras de qualidade e o estado da ingestão.",
                href=getattr(links, "data_quality", None) if links is not None else None,
            )
        )

    if open_incidents > 0:
        problems.append(
            _problem(
                "open_incidents",
                "Incidente(s) em aberto",
                "critical" if critical_open_incidents > 0 else "high",
                f"Há {open_incidents} incidente(s) aberto(s) para este ativo.",
                evidence={"open_incidents": open_incidents, "critical_open_incidents": critical_open_incidents},
                action_hint="Abrir ou acompanhar o incidente a partir do contexto do ativo.",
                href=getattr(links, "incidents", None) if links is not None else None,
            )
        )

    correlation_signals = getattr(correlation_summary, "signals", None) if correlation_summary is not None else None
    if correlation_signals is not None and bool(getattr(correlation_signals, "operational_failure", False)):
        problems.append(
            _problem(
                "operational_failure",
                "Falha operacional",
                "critical",
                "A leitura operacional aponta uma falha recente no pipeline associado.",
                evidence={"operational_failure": True, "priority_score": getattr(correlation_summary, "priority_score", None)},
                action_hint="Reprocessar o pipeline ou abrir incidente para investigação.",
                href=getattr(links, "change_management", None) if links is not None else None,
            )
        )
    elif correlation_signals is not None and bool(getattr(correlation_signals, "stale_pipeline", False)):
        problems.append(
            _problem(
                "stale_pipeline",
                "Pipeline sem atualização recente",
                "warning",
                "O pipeline associado parece não ter executado com sucesso dentro da janela esperada.",
                evidence={"stale_pipeline": True, "priority_score": getattr(correlation_summary, "priority_score", None)},
                action_hint="Reprocessar o pipeline para validar se o atraso é transitório.",
                href=getattr(links, "datasource", None) if links is not None else None,
            )
        )

    if operational_context is not None and int(operational_context.get("criticality_score") or 0) >= 75:
        problems.append(
            _problem(
                "high_criticality",
                "Alta criticidade operacional",
                "warning",
                f"O ativo foi classificado com criticidade operacional de {operational_context.get('criticality_score')} pontos.",
                evidence={
                    "criticality_score": operational_context.get("criticality_score"),
                    "criticality_label": operational_context.get("criticality_label"),
                },
                action_hint="Revisar a fila operacional antes de mudanças estruturais.",
                href=getattr(links, "explorer", None) if links is not None else None,
            )
        )

    if not problems:
        problems.append(
            _problem(
                "no_blockers",
                "Nenhum bloqueio crítico",
                "info",
                "Não há bloqueios críticos detectados para este ativo neste momento.",
                evidence={"dq_score": dq_score, "open_incidents": open_incidents},
                action_hint="Monitorar e revisar novamente se os sinais mudarem.",
                href=getattr(links, "explorer", None) if links is not None else None,
            )
        )

    return _sort_by_severity(problems)


def _build_impact(asset: Any, operational_context: dict[str, object] | None, correlation_summary: Any) -> list[AssistantExplainImpactOut]:
    impacts: list[AssistantExplainImpactOut] = []
    lineage = getattr(asset, "lineage", None)
    links = getattr(asset, "links", None)

    if not bool(asset.owner.owner_defined) or not bool(asset.classification.classification_defined):
        impacts.append(
            _impact(
                "governance_trust",
                "Confiança e governança",
                "A ausência de owner ou classificação reduz a leitura confiável e a rastreabilidade do ativo.",
                tone="warning",
                evidence={
                    "owner_defined": asset.owner.owner_defined,
                    "classification_defined": asset.classification.classification_defined,
                    "trust_score": asset.classification.trust_score,
                    "trust_label": asset.classification.trust_label,
                },
            )
        )

    downstream_count = None
    upstream_count = None
    impact_level = None
    if lineage is not None and getattr(lineage, "impact", None) is not None:
        impact = lineage.impact
        downstream_count = getattr(impact, "downstream_count", None)
        upstream_count = getattr(impact, "upstream_count", None)
        impact_level = getattr(impact, "impact_level", None)
        if (downstream_count or 0) > 0 or (upstream_count or 0) > 0:
            impacts.append(
                _impact(
                    "lineage_impact",
                    "Impacto de linhagem",
                    f"O ativo participa de {upstream_count or 0} origem(ns) e {downstream_count or 0} consumo(s) na linhagem.",
                    tone="accent",
                    evidence={
                        "upstream_count": upstream_count or 0,
                        "downstream_count": downstream_count or 0,
                        "impact_level": impact_level,
                    },
                )
            )

    priority_score = getattr(correlation_summary, "priority_score", None)
    if priority_score is not None:
        tone = "danger" if int(priority_score) >= 6 else "warning" if int(priority_score) >= 3 else "neutral"
        impacts.append(
            _impact(
                "operational_priority",
                "Prioridade operacional",
                f"O score de correlação operacional está em {int(priority_score)} ponto(s), indicando a pressão de tratamento do ativo.",
                tone=tone,
                evidence={
                    "priority_score": int(priority_score),
                    "correlation_type": getattr(correlation_summary, "correlation_type", None),
                },
            )
        )

    if operational_context is not None:
        impacts.append(
            _impact(
                "operational_context",
                "Contexto operacional",
                f"O ativo está classificado como {operational_context.get('criticality_label')} e orienta o foco do tratamento.",
                tone=str(operational_context.get("criticality_tone") or "neutral"),
                evidence={
                    "criticality_score": operational_context.get("criticality_score"),
                    "criticality_label": operational_context.get("criticality_label"),
                    "recommended_actions": list(operational_context.get("recommended_actions") or []),
                    "open_incidents": operational_context.get("open_incidents"),
                },
            )
        )

    if getattr(links, "metabase_consumption", None):
        impacts.append(
            _impact(
                "analytical_consumption",
                "Consumo analítico",
                "O ativo está ligado ao contexto de consumo analítico do Explorer e deve ser tratado com cuidado antes de alterações.",
                tone="accent",
                evidence={"href": links.metabase_consumption},
            )
        )

    return impacts[:4]


def _merge_asset_signal_impact(
    impacts: list[AssistantExplainImpactOut],
    *,
    asset_intelligence: Any | None,
) -> list[AssistantExplainImpactOut]:
    if asset_intelligence is None:
        return impacts

    impact = getattr(asset_intelligence, "impact", None)
    dashboards = int(getattr(impact, "dashboards", 0) or 0) if impact is not None else 0
    users = int(getattr(impact, "users", 0) or 0) if impact is not None else 0
    risk_score = int(getattr(asset_intelligence, "risk_score", 0) or 0)
    trust_score = int(getattr(asset_intelligence, "trust_score", 0) or 0)
    priority_score = int(getattr(asset_intelligence, "priority_score", 0) or 0)

    signal_impacts = [
        _impact(
            "asset_signal_priority",
            "Prioridade consolidada",
            f"Prioridade {priority_score}/100, risco {risk_score}/100 e confiança {trust_score}/100 calculados pela camada unificada de sinais.",
            tone="danger" if priority_score >= 80 else "warning" if priority_score >= 60 else "neutral",
            evidence={
                "source": "asset_signals",
                "priority_score": priority_score,
                "risk_score": risk_score,
                "trust_score": trust_score,
            },
        )
    ]

    if dashboards > 0 or users > 0:
        signal_impacts.append(
            _impact(
                "asset_signal_usage_impact",
                "Impacto real de consumo",
                f"O ativo impacta {dashboards} dashboard(s) e {users} usuário(s) identificados em sinais recentes.",
                tone="warning" if dashboards >= 3 or users >= 5 else "accent",
                evidence={
                    "source": "asset_signals",
                    "dashboards": dashboards,
                    "users": users,
                },
            )
        )

    seen: set[str] = set()
    compacted: list[AssistantExplainImpactOut] = []
    for item in signal_impacts + impacts:
        if item.key in seen:
            continue
        compacted.append(item)
        seen.add(item.key)
    return compacted[:5]


def _recommended_action(
    asset: Any,
    *,
    problems: list[AssistantExplainProblemOut],
    operational_context: dict[str, object] | None,
    correlation_summary: Any,
    asset_intelligence: Any | None = None,
) -> AssistantRecommendationOut:
    links = getattr(asset, "links", None)
    problem_keys = {problem.key for problem in problems}
    has_operational_problem = bool({"open_incidents", "operational_failure", "dq_degraded"} & problem_keys)
    has_pipeline_problem = "stale_pipeline" in problem_keys

    if "owner_missing" in problem_keys:
        return AssistantRecommendationOut(
            key="define_owner",
            label="Definir owner",
            detail="O ativo ainda não tem owner formal. Criar a solicitação de ownership é o primeiro passo para governar o ativo.",
            action_key="define_owner",
            action_label="Definir owner",
            tone="warning",
            destructive=False,
            confirmation_required=False,
            confirmation_hint=None,
            can_execute=True,
            href=getattr(links, "change_management", None) if links is not None else None,
        )

    if has_operational_problem:
        return AssistantRecommendationOut(
            key="open_incident",
            label="Abrir incidente",
            detail="Há sinais operacionais ou de qualidade que justificam registrar um incidente e acompanhar o tratamento.",
            action_key="open_incident",
            action_label="Abrir incidente",
            tone="danger",
            destructive=True,
            confirmation_required=True,
            confirmation_hint="A abertura do incidente será auditada e precisa de confirmação explícita.",
            can_execute=True,
            href=getattr(links, "incidents", None) if links is not None else None,
        )

    if has_pipeline_problem:
        return AssistantRecommendationOut(
            key="reprocess_pipeline",
            label="Reprocessar pipeline",
            detail="O pipeline associado parece estar atrasado ou sem execução recente. Reprocessar ajuda a validar se o problema é transitório.",
            action_key="reprocess_pipeline",
            action_label="Reprocessar pipeline",
            tone="warning",
            destructive=False,
            confirmation_required=True,
            confirmation_hint="A reexecução do pipeline será registrada e confirmada antes do disparo.",
            can_execute=True,
            href=getattr(links, "datasource", None) if links is not None else None,
        )

    if asset_intelligence is not None and int(getattr(asset_intelligence, "priority_score", 0) or 0) >= 70:
        actions = [str(item) for item in (getattr(asset_intelligence, "recommended_actions", []) or []) if str(item).strip()]
        action_hint = actions[0] if actions else "revisar sinais, impacto e ownership do ativo"
        return AssistantRecommendationOut(
            key="investigate_priority_asset",
            label="Investigar ativo prioritário",
            detail=f"A camada de sinais marcou este ativo como alta prioridade. Próximo passo sugerido: {action_hint}.",
            action_key="monitor",
            action_label="Abrir contexto",
            tone="warning",
            destructive=False,
            confirmation_required=False,
            confirmation_hint=None,
            can_execute=False,
            href=getattr(links, "explorer", None) if links is not None else None,
        )

    if operational_context is not None:
        action_keys = list(operational_context.get("recommended_actions") or [])
        if action_keys:
            return AssistantRecommendationOut(
                key="monitor",
                label="Monitorar ativo",
                detail="Os sinais atuais não exigem intervenção imediata; acompanhe o ativo e reavalie se o estado mudar.",
                action_key="monitor",
                action_label="Monitorar",
                tone="neutral",
                destructive=False,
                confirmation_required=False,
                confirmation_hint=None,
                can_execute=False,
                href=getattr(links, "explorer", None) if links is not None else None,
            )

    return AssistantRecommendationOut(
        key="monitor",
        label="Monitorar ativo",
        detail="Não há ação automática mais forte neste momento. O melhor próximo passo é acompanhar os sinais do ativo.",
        action_key="monitor",
        action_label="Monitorar",
        tone="neutral",
        destructive=False,
        confirmation_required=False,
        confirmation_hint=None,
        can_execute=False,
        href=getattr(links, "explorer", None) if links is not None else None,
    )


def build_assistant_explanation(
    session: Session,
    *,
    asset_ref: str,
    current_user: User | None = None,
) -> AssistantExplainOut:
    asset_type, asset_id = _parse_asset_ref(asset_ref)
    bundle = _load_asset_bundle(session, asset_type=asset_type, asset_id=asset_id, current_user=current_user)
    asset = bundle["asset"]
    operational_context = bundle["operational_context"]
    correlation_summary = bundle["correlation_summary"]
    slas = bundle["slas"]
    active_sla = bundle["active_sla"]
    asset_intelligence = _safe_asset_intelligence(session, table_id=asset.table_id, current_user=current_user)

    problems = _merge_asset_signal_problems(
        _build_problems(asset, operational_context, correlation_summary, active_sla),
        asset=asset,
        asset_intelligence=asset_intelligence,
    )
    impact = _merge_asset_signal_impact(
        _build_impact(asset, operational_context, correlation_summary),
        asset_intelligence=asset_intelligence,
    )
    recommendation = _recommended_action(
        asset,
        problems=problems,
        operational_context=operational_context,
        correlation_summary=correlation_summary,
        asset_intelligence=asset_intelligence,
    )

    sla_defined = active_sla is not None
    sla_hours = None
    if active_sla is not None:
        sla_hours = active_sla.get("sla_hours")

    summary = recommendation.detail
    if problems and problems[0].severity in {"critical", "high"}:
        summary = f"{problems[0].label}: {problems[0].detail}"
    elif recommendation.key == "monitor":
        summary = "Nenhum bloqueio crítico detectado; mantenha o ativo em observação."

    context = {
        "asset": {
            "asset_ref": bundle["asset_ref"],
            "entity_kind": asset.entity_kind,
            "table_id": asset.table_id,
            "column_id": asset.column_id,
            "asset_name": asset.display_name,
            "asset_fqn": asset.table_fqn,
        },
        "owner": {
            "defined": asset.owner.owner_defined,
            "data_owner_id": asset.owner.data_owner_id,
            "name": asset.owner.owner_name,
            "email": asset.owner.owner_email,
        },
        "classification": {
            "defined": asset.classification.classification_defined,
            "sensitivity_level": asset.classification.sensitivity_level,
            "trust_score": asset.classification.trust_score,
            "trust_label": asset.classification.trust_label,
            "trust_tone": asset.classification.trust_tone,
        },
        "evidence": {
            "description_complete": asset.evidence.description_complete,
            "dictionary_complete": asset.evidence.dictionary_complete,
            "dq_score": asset.evidence.dq_score,
            "open_incidents": asset.evidence.open_incidents,
            "critical_open_incidents": asset.evidence.critical_open_incidents,
        },
        "operational_context": operational_context,
        "correlation_summary": {
            "priority_score": getattr(correlation_summary, "priority_score", None),
            "correlation_type": getattr(correlation_summary, "correlation_type", None),
            "summary": getattr(correlation_summary, "summary", None),
            "signals": _dump_object(getattr(correlation_summary, "signals", None)),
        },
        "asset_intelligence": _asset_intelligence_dict(asset_intelligence),
        "slas": slas,
    }

    payload = AssistantExplainOut(
        generated_at=_now(),
        asset_ref=bundle["asset_ref"],
        asset_type="column" if asset.entity_kind == "column" else "table",
        asset_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
        entity_kind=asset.entity_kind,
        asset_name=asset.display_name,
        asset_fqn=asset.table_fqn,
        table_id=asset.table_id,
        column_id=asset.column_id,
        asset_owner_id=asset.owner.data_owner_id,
        asset_owner_name=asset.owner.owner_name,
        asset_owner_email=asset.owner.owner_email,
        asset_owner_defined=asset.owner.owner_defined,
        sla_defined=sla_defined,
        sla_hours=int(sla_hours) if sla_hours is not None else None,
        summary=summary,
        problems=problems,
        impact=impact,
        recommendation=recommendation,
        actions=[
            AssistantActionOptionOut(
                key="define_owner",
                label="Definir owner",
                description="Criar uma solicitação auditável para registrar o owner do ativo.",
                tone="warning",
                destructive=False,
                confirmation_required=False,
                confirmation_hint=None,
                can_execute=True,
                requires_owner_id=True,
                recommended=recommendation.key == "define_owner",
                href=getattr(asset.links, "change_management", None),
            ),
            AssistantActionOptionOut(
                key="reprocess_pipeline",
                label="Reprocessar pipeline",
                description="Reexecutar o scan/pipeline associado para validar atualização e falhas transitórias.",
                tone="warning",
                destructive=False,
                confirmation_required=True,
                confirmation_hint="Essa ação será registrada e exige confirmação antes da execução.",
                can_execute=True,
                requires_owner_id=False,
                recommended=recommendation.key == "reprocess_pipeline",
                href=getattr(asset.links, "datasource", None),
            ),
            AssistantActionOptionOut(
                key="open_incident",
                label="Abrir incidente",
                description="Registrar um incidente operacional com o contexto já carregado no ativo.",
                tone="danger",
                destructive=True,
                confirmation_required=True,
                confirmation_hint="A abertura do incidente será auditada e precisa de confirmação explícita.",
                can_execute=True,
                requires_owner_id=False,
                recommended=recommendation.key == "open_incident",
                href=getattr(asset.links, "incidents", None),
            ),
        ],
        context=context,
    )

    track_usage_event(
        session,
        user=current_user,
        event_name="assistant_explain",
        module_name="assistant",
        entity_type=asset.entity_kind,
        entity_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
        metadata={"asset_ref": bundle["asset_ref"], "problems": [item.key for item in problems], "recommendation": recommendation.key},
    )
    write_audit_log_sync(
        session,
        action="platform.assistant.explain",
        user_id=getattr(current_user, "id", None),
        entity_type=asset.entity_kind,
        entity_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
        source_module="platform.assistant",
        metadata={
            "asset_ref": bundle["asset_ref"],
            "recommendation": recommendation.key,
            "problem_count": len(problems),
        },
    )
    session.commit()
    return payload


def execute_assistant_action(
    session: Session,
    *,
    asset_ref: str,
    payload: AssistantActionIn,
    current_user: User,
    request_audit: dict[str, object] | None = None,
) -> AssistantActionOut:
    asset_type, asset_id = _parse_asset_ref(asset_ref)
    bundle = _load_asset_bundle(session, asset_type=asset_type, asset_id=asset_id, current_user=current_user)
    asset = bundle["asset"]
    action_key = _normalize_text(payload.action_key)
    request_audit = request_audit or {}

    if action_key in {"reprocess_pipeline", "open_incident"} and not payload.confirm:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Ação assistida requer confirmação explícita")

    target_table_id = asset.table_id
    result: dict[str, object] = {}
    follow_up_href: str | None = None
    executed = True

    if action_key == "define_owner":
        owner_id = payload.data_owner_id
        if owner_id is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="data_owner_id is required for define_owner")
        change_request = create_metadata_change_request(
            session,
            asset_type=asset.entity_kind,
            asset_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
            change_kind="owner_assignment",
            title=f"Definir owner para {asset.display_name}",
            description=payload.resolution_note or "Solicitação criada pelo assistente para registrar o owner do ativo.",
            proposed_value_json={"data_owner_id": owner_id},
            current_value_json={"data_owner_id": asset.owner.data_owner_id},
            context_json={
                "assistant": True,
                "asset_ref": bundle["asset_ref"],
                "asset_type": asset.entity_kind,
                "source": "assistant",
            },
            actor_user_id=current_user.id,
            request_audit=request_audit,
        )
        session.commit()
        result = {
            "change_request": change_request,
            "message": "Solicitação de owner criada e registrada como rascunho para aprovação.",
        }
        follow_up_href = getattr(asset.links, "change_management", None)
        message = "Solicitação de owner criada com sucesso."
    elif action_key == "reprocess_pipeline":
        execution = execute_automation_action(
            session,
            action_key="reprocess_pipeline",
            current_user=current_user,
            table_id=target_table_id,
            datasource_id=asset.source.datasource_id,
            scope_kind="pipeline",
            scope_value=asset.table_fqn,
            target_json={
                "table_id": target_table_id,
                "datasource_id": asset.source.datasource_id,
                "asset_ref": bundle["asset_ref"],
                "source": "assistant",
            },
            trigger_source="assistant",
            audit_kwargs=request_audit,
        )
        result = {
            "execution_id": execution.id,
            "status": execution.status,
            "target_id": execution.entity_id,
        }
        follow_up_href = getattr(asset.links, "explorer", None)
        message = "Pipeline reenfileirado com sucesso."
    elif action_key == "open_incident":
        execution = execute_automation_action(
            session,
            action_key="open_incident",
            current_user=current_user,
            table_id=target_table_id,
            scope_kind="asset",
            scope_value=asset.table_fqn,
            target_json={
                "table_id": target_table_id,
                "asset_ref": bundle["asset_ref"],
                "source": "assistant",
            },
            trigger_source="assistant",
            audit_kwargs=request_audit,
        )
        result = {
            "execution_id": execution.id,
            "status": execution.status,
            "target_id": execution.entity_id,
        }
        follow_up_href = getattr(asset.links, "incidents", None)
        message = "Incidente aberto com sucesso."
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Unsupported assistant action")

    track_usage_event(
        session,
        user=current_user,
        event_name="assistant_action_execute",
        module_name="assistant",
        entity_type=asset.entity_kind,
        entity_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
        metadata={"asset_ref": bundle["asset_ref"], "action_key": action_key},
    )
    write_audit_log_sync(
        session,
        action="platform.assistant.action.execute",
        user_id=current_user.id,
        entity_type=asset.entity_kind,
        entity_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
        source_module="platform.assistant",
        metadata={
            "asset_ref": bundle["asset_ref"],
            "action_key": action_key,
            "executed": executed,
        },
        after={"result": result, "message": message},
    )
    session.commit()
    return AssistantActionOut(
        ok=True,
        asset_ref=bundle["asset_ref"],
        asset_type="column" if asset.entity_kind == "column" else "table",
        asset_id=asset.column_id if asset.entity_kind == "column" and asset.column_id is not None else asset.table_id,
        action_key=action_key,
        executed=executed,
        message=message,
        result=result,
        follow_up_href=follow_up_href,
    )


__all__ = ["build_assistant_explanation", "execute_assistant_action"]
