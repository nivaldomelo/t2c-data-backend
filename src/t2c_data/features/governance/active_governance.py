from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from t2c_data.features.dashboard.support import TableProfile
from t2c_data.features.governance.risk import build_risk_payload
from t2c_data.features.governance.settings import GovernanceSettingsSnapshot


@dataclass(frozen=True)
class ActiveGovernanceFinding:
    key: str
    title: str
    description: str
    severity: str
    origin: str
    action_label: str
    action_href: str
    detected_at: datetime
    due_at: datetime | None = None
    sla_days: int | None = None
    context_value: str | None = None
    base_priority: int = 0

    def as_pending_item(
        self,
        table: TableProfile,
        *,
        governance_score: dict[str, object],
        links: dict[str, str],
    ) -> dict[str, object]:
        now = self.detected_at if self.detected_at.tzinfo is not None else self.detected_at.replace(tzinfo=timezone.utc)
        due_at = self.due_at if self.due_at is not None else None
        if due_at is not None and due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=timezone.utc)
        aging_days = max((datetime.now(timezone.utc) - now).days, 0)
        sla_status = None
        sla_status_label = None
        if due_at is not None:
            remaining_days = (due_at - datetime.now(timezone.utc)).total_seconds() / 86400
            if remaining_days < 0:
                sla_status = "overdue"
                sla_status_label = "Fora do SLA"
            elif remaining_days <= 3:
                sla_status = "due_soon"
                sla_status_label = "Próximo do vencimento"
            else:
                sla_status = "within_sla"
                sla_status_label = "Dentro do SLA"
        risk_payload = build_risk_payload(
            table,
            severity=self.severity,
            origin=self.origin,
            trust_score=int(getattr(table, "trust_score", 0) or 0),
            sla_status=sla_status,
            context_value=self.context_value,
        )
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "severity": self.severity,
            "severity_label": {
                "critical": "Crítica",
                "high": "Alta",
                "medium": "Moderada",
                "low": "Baixa",
            }.get(self.severity, self.severity.title()),
            "priority": self.base_priority
            + {
                "critical": 400,
                "high": 300,
                "medium": 200,
                "low": 100,
            }.get(self.severity, 0),
            "origin": self.origin,
            "origin_label": {
                "governance": "Governança",
                "metadata": "Metadados",
                "glossary": "Glossário",
                "certification": "Certificação",
                "quality": "Qualidade",
                "operations": "Operação",
                "incidents": "Incidentes",
            }.get(self.origin, self.origin.title()),
            "status": "open",
            "status_label": "Aberta",
            "table_id": table.table_id,
            "table_name": table.table_name,
            "table_fqn": table.table_fqn,
            "datasource_name": table.datasource_name,
            "database_name": table.database_name,
            "schema_name": table.schema_name,
            "domain_name": table.domain_name,
            "owner_name": table.owner_name or "Não definido",
            "data_owner_id": table.data_owner_id,
            "detected_at": now.isoformat(),
            "aging_days": aging_days,
            "sla_days": self.sla_days,
            "due_at": due_at.isoformat() if due_at else None,
            "sla_status": sla_status,
            "sla_status_label": sla_status_label,
            "governance_score": governance_score,
            "trust_score": int(getattr(table, "trust_score", 0) or 0),
            "trust_label": getattr(table, "trust_label", None),
            "trust_tone": getattr(table, "trust_tone", None),
            "risk_score": risk_payload["risk_score"],
            "risk_label": risk_payload["risk_label"],
            "risk_tone": risk_payload["risk_tone"],
            "risk_reason": risk_payload["risk_reason"],
            "risk_components": risk_payload["risk_components"],
            "context_value": self.context_value,
            "action_label": self.action_label,
            "action_href": self.action_href,
            "links": links,
        }


def _has_high_usage(table: TableProfile, *, settings_snapshot: GovernanceSettingsSnapshot) -> bool:
    threshold = max(int(getattr(settings_snapshot, "governance_high_usage_click_threshold", 20) or 20), 1)
    return int(getattr(table, "search_clicks_30d", 0) or 0) >= threshold


def _is_critical_asset(table: TableProfile) -> bool:
    criticality = (table.certification_criticality or "").strip().lower()
    return criticality in {"high", "critical"}


def build_active_governance_findings(
    table: TableProfile,
    *,
    settings_snapshot: GovernanceSettingsSnapshot,
    links: dict[str, str],
    now: datetime,
) -> list[ActiveGovernanceFinding]:
    findings: list[ActiveGovernanceFinding] = []
    is_certified = (table.certification_status or "").strip().lower() == "certified"
    is_high_usage = _has_high_usage(table, settings_snapshot=settings_snapshot)
    search_clicks_30d = int(getattr(table, "search_clicks_30d", 0) or 0)
    recent_dq_failures = int(getattr(table, "recent_dq_failure_runs_30d", 0) or 0)
    active_dq_rules = int(getattr(table, "active_dq_rules_count", 0) or 0)
    is_critical_asset = _is_critical_asset(table)

    if not table.owner_defined:
        findings.append(
            ActiveGovernanceFinding(
                key="no_owner",
                title="Owner ausente ou removido",
                description=(
                    "O ativo está sem owner definido ou teve o responsável removido e precisa de revisão imediata."
                ),
                severity="high",
                origin="governance",
                action_label="Definir owner",
                action_href=links["owners"],
                detected_at=table.owner_reviewed_at or now,
                due_at=(table.owner_reviewed_at or now) + timedelta(days=1),
                sla_days=1,
                context_value="Sem owner formal",
                base_priority=110 if is_certified else 95,
            )
        )

    if is_critical_asset and active_dq_rules <= 0:
        findings.append(
            ActiveGovernanceFinding(
                key="critical_without_dq",
                title="Ativo crítico sem DQ mínimo",
                description="O ativo é crítico, mas ainda não possui regra DQ mínima ativa para proteção contínua.",
                severity="critical",
                origin="quality",
                action_label="Criar regra DQ",
                action_href=links["data_quality"],
                detected_at=table.last_sync_at or now,
                due_at=(table.last_sync_at or now) + timedelta(days=1),
                sla_days=1,
                context_value="Sem regra DQ mínima ativa",
                base_priority=108,
            )
        )

    if not table.classification_defined and not is_high_usage:
        findings.append(
            ActiveGovernanceFinding(
                key="no_classification",
                title="Classificação ausente",
                description=(
                    "O ativo ainda não possui classificação formal e precisa de revisão para privacidade, acesso e governança."
                ),
                severity="high" if (table.has_personal_data or table.has_sensitive_personal_data) else "medium",
                origin="governance",
                action_label="Classificar ativo",
                action_href=links["privacy"],
                detected_at=table.last_updated_at or now,
                due_at=(table.last_updated_at or now) + timedelta(days=2),
                sla_days=2,
                context_value="Classificação obrigatória",
                base_priority=92,
            )
        )

    if not table.classification_defined and is_high_usage:
        findings.append(
            ActiveGovernanceFinding(
                key="classification_high_usage",
                title="Ativo de alto uso sem classificação",
                description=(
                    "O ativo tem consumo elevado e ainda não possui classificação suficiente para orientar governança e acesso."
                ),
                severity="high",
                origin="governance",
                action_label="Classificar ativo",
                action_href=links["privacy"],
                detected_at=table.last_updated_at or now,
                due_at=(table.last_updated_at or now) + timedelta(days=2),
                sla_days=2,
                context_value=f"{search_clicks_30d} clique(s) no recorte recente",
                base_priority=102,
            )
        )

    if not table.sla_defined:
        findings.append(
            ActiveGovernanceFinding(
                key="no_sla",
                title="SLA ausente",
                description="O ativo ainda não possui SLA formal para atualização, revisão ou tratamento de incidentes.",
                severity="high" if is_critical_asset else "medium",
                origin="governance",
                action_label="Definir SLA",
                action_href=links["change_management"],
                detected_at=table.last_updated_at or now,
                due_at=(table.last_updated_at or now) + timedelta(days=2),
                sla_days=2,
                context_value="SLA obrigatório",
                base_priority=90,
            )
        )

    if not table.dictionary_complete and is_high_usage:
        findings.append(
            ActiveGovernanceFinding(
                key="dictionary_high_usage",
                title="Ativo de alto uso sem dicionário",
                description=(
                    "O ativo já é consumido com frequência, mas o dicionário ainda não descreve todas as colunas."
                ),
                severity="high",
                origin="metadata",
                action_label="Completar dicionário",
                action_href=links["explorer"],
                detected_at=table.last_updated_at or now,
                due_at=(table.last_updated_at or now) + timedelta(days=2),
                sla_days=2,
                context_value=f"{search_clicks_30d} clique(s) no recorte recente",
                base_priority=98,
            )
        )

    if is_critical_asset and recent_dq_failures >= 2:
        findings.append(
            ActiveGovernanceFinding(
                key="recurring_dq_failure_critical",
                title="Falha DQ recorrente em ativo crítico",
                description=(
                    "Falhas recorrentes de Data Quality em ativo crítico exigem tratativa conjunta e abertura/atualização de incidente."
                ),
                severity="critical",
                origin="incidents",
                action_label="Abrir ou revisar incidente",
                action_href=links["incidents"],
                detected_at=table.last_sync_at or now,
                due_at=(table.last_sync_at or now) + timedelta(days=1),
                sla_days=1,
                context_value=f"{recent_dq_failures} falha(s) de DQ em janela recente",
                base_priority=112,
            )
        )

    return findings


__all__ = ["ActiveGovernanceFinding", "build_active_governance_findings"]
