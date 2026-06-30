from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session

from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.dashboard.support import TableProfile
from t2c_data.features.pagination import paginate_items
from t2c_data.features.tags.spreadsheet import slugify_tag
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.contracts import DataContract
from t2c_data.models.dq import DQRule, DQRuleRun
from t2c_data.models.incident import Incident
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.semantic import SemanticDataProduct, SemanticDomain, SemanticLink
from t2c_data.schemas.semantic import (
    SemanticAssetOut,
    SemanticDomainCreate,
    SemanticDomainDetailOut,
    SemanticDomainOut,
    SemanticDomainSuggestionOut,
    SemanticLinkCreate,
    SemanticLinkOut,
    SemanticProductCreate,
    SemanticProductDetailOut,
    SemanticProductOut,
    SemanticProductUpdate,
)


def _normalize_slug(value: str) -> str:
    return slugify_tag(value or "")


def _maturity_label(score: int) -> str:
    if score >= 85:
        return "Otimizado"
    if score >= 70:
        return "Gerenciado"
    if score >= 50:
        return "Definido"
    return "Em evolução"


def _asset_href(table_id: int) -> str:
    return f"/explorer?tableId={table_id}"


def _link_href(table_id: int, kind: str) -> str:
    if kind == "data_quality":
        return f"/data-quality?tableId={table_id}"
    if kind == "incidents":
        return f"/incidents/tickets?tableId={table_id}"
    if kind == "lineage":
        return f"/lineage?tableId={table_id}"
    if kind == "dashboard":
        return f"/dashboard?tableId={table_id}"
    if kind == "contract":
        return f"/explorer?tableId={table_id}&tab=consumption"
    return _asset_href(table_id)


def _asset_label(profile: TableProfile) -> str:
    return profile.table_fqn


def _asset_summary(profile: TableProfile) -> SemanticAssetOut:
    return SemanticAssetOut(
        entity_id=profile.table_id,
        label=_asset_label(profile),
        href=_asset_href(profile.table_id),
        table_fqn=profile.table_fqn,
        domain_name=profile.domain_name,
        datasource_name=profile.datasource_name,
        database_name=profile.database_name,
        schema_name=profile.schema_name,
        owner_name=profile.owner_name,
        dq_score=profile.dq_score,
        trust_score=profile.trust_score,
        readiness_score=profile.readiness_score,
        documentation_score=profile.documentation_score,
        open_incidents=profile.open_incidents,
        critical_open_incidents=profile.critical_open_incidents,
    )


def _score_from_profiles(profiles: list[TableProfile]) -> tuple[int, int, int]:
    if not profiles:
        return 0, 0, 0
    quality = round(sum((profile.dq_score or 0) for profile in profiles) / len(profiles))
    governance = round(sum(profile.trust_score for profile in profiles) / len(profiles))
    coverage = round(sum(profile.readiness_score for profile in profiles) / len(profiles))
    return int(quality), int(governance), int(coverage)


def _domain_maturity_score(profiles: list[TableProfile], links: list[SemanticLink], product_count: int) -> int:
    if not profiles:
        return 0
    quality, governance, coverage = _score_from_profiles(profiles)
    owner_pct = round(sum(1 for profile in profiles if profile.owner_defined) / len(profiles) * 100)
    documentation_pct = round(sum(profile.documentation_score for profile in profiles) / len(profiles))
    contract_pct = round(
        sum(1 for profile in profiles if profile.readiness_score >= 50 and profile.description_complete) / len(profiles) * 100
    )
    linkage_pct = round(len({link.entity_kind + ":" + str(link.entity_id or link.entity_label) for link in links}) / max(len(profiles), 1) * 100)
    score = round(
        (quality * 0.22)
        + (governance * 0.22)
        + (coverage * 0.16)
        + (owner_pct * 0.12)
        + (documentation_pct * 0.12)
        + (contract_pct * 0.08)
        + (min(100, linkage_pct + product_count * 5) * 0.08)
    )
    penalty = min(20, sum(profile.critical_open_incidents for profile in profiles) * 4)
    return int(max(0, min(100, score - penalty)))


def _product_maturity_score(profiles: list[TableProfile], links: list[SemanticLink]) -> int:
    if not profiles and not links:
        return 0
    quality, governance, coverage = _score_from_profiles(profiles)
    consumer_signal = min(100, max(len(links) * 8, 0))
    dashboard_signal = min(100, sum(1 for link in links if link.entity_kind == "dashboard") * 12)
    pipeline_signal = min(100, sum(1 for link in links if link.entity_kind == "pipeline") * 10)
    incident_penalty = min(20, sum(profile.critical_open_incidents for profile in profiles) * 4)
    score = round(
        (quality * 0.30)
        + (governance * 0.22)
        + (coverage * 0.20)
        + (consumer_signal * 0.12)
        + (dashboard_signal * 0.08)
        + (pipeline_signal * 0.08)
    )
    return int(max(0, min(100, score - incident_penalty)))


def _select_domain_profiles(session: Session, domain: SemanticDomain, current_user=None) -> list[TableProfile]:
    now = datetime.now(timezone.utc)
    profiles = load_table_profiles(session, now, current_user=current_user)
    normalized_domain = domain.name.strip().lower()
    linked_ids = {
        int(link.entity_id)
        for link in domain.links
        if link.entity_kind == "table" and link.entity_id is not None
    }
    return [
        profile
        for profile in profiles
        if profile.table_id in linked_ids or (profile.domain_name or "").strip().lower() == normalized_domain
    ]


def _select_product_profiles(session: Session, product: SemanticDataProduct, current_user=None) -> list[TableProfile]:
    now = datetime.now(timezone.utc)
    profiles = load_table_profiles(session, now, current_user=current_user)
    linked_ids = {
        int(link.entity_id)
        for link in product.links
        if link.entity_kind == "table" and link.entity_id is not None
    }
    return [profile for profile in profiles if profile.table_id in linked_ids]


def _collect_domain_assets(domain: SemanticDomain, profiles: list[TableProfile]) -> list[SemanticAssetOut]:
    linked_ids = {
        int(link.entity_id)
        for link in domain.links
        if link.entity_kind == "table" and link.entity_id is not None
    }
    items = [profile for profile in profiles if profile.table_id in linked_ids or (profile.domain_name or "").strip().lower() == domain.name.strip().lower()]
    return sorted([_asset_summary(item) for item in items], key=lambda item: item.label.lower())


def _collect_product_assets(product: SemanticDataProduct, profiles: list[TableProfile]) -> list[SemanticAssetOut]:
    linked_ids = {
        int(link.entity_id)
        for link in product.links
        if link.entity_kind == "table" and link.entity_id is not None
    }
    items = [profile for profile in profiles if profile.table_id in linked_ids]
    return sorted([_asset_summary(item) for item in items], key=lambda item: item.label.lower())


def _quality_status(score: int | None, has_assets: bool) -> str:
    if not has_assets or score is None:
        return "unknown"
    if score >= 90:
        return "excellent"
    if score >= 75:
        return "healthy"
    if score >= 60:
        return "attention"
    if score >= 40:
        return "degraded"
    return "critical"


def _freshness_status(profile: TableProfile) -> str:
    if profile.freshness_seconds is None:
        return "unknown"
    if profile.sla_hours:
        return "fresh" if profile.freshness_seconds <= profile.sla_hours * 3600 else "stale"
    if profile.freshness_seconds <= 24 * 3600:
        return "fresh"
    if profile.freshness_seconds <= 72 * 3600:
        return "attention"
    return "stale"


def _semantic_link_item(link: SemanticLink) -> dict[str, object]:
    return {
        "id": link.id,
        "relation_kind": link.relation_kind,
        "entity_kind": link.entity_kind,
        "entity_id": link.entity_id,
        "label": link.entity_label,
        "href": link.entity_href,
        "notes": link.notes,
        "is_primary": link.is_primary,
    }


def _profile_asset_item(profile: TableProfile, rules_by_table: dict[int, dict[str, int]]) -> dict[str, object]:
    rule_counts = rules_by_table.get(profile.table_id, {"total": 0, "active": 0, "failed": 0, "technical_error": 0})
    return {
        "id": profile.table_id,
        "entity_id": profile.table_id,
        "entity_kind": "table",
        "source": profile.datasource_name,
        "database": profile.database_name,
        "schema": profile.schema_name,
        "table": profile.table_name,
        "full_name": profile.table_fqn,
        "owner": profile.owner_name,
        "quality_score": profile.dq_score,
        "governance_score": profile.trust_score,
        "documentation_score": profile.documentation_score,
        "readiness_score": profile.readiness_score,
        "certification_status": profile.certification_status,
        "rules_total": rule_counts["total"],
        "rules_active": rule_counts["active"],
        "rules_failed": rule_counts["failed"],
        "rules_technical_error": rule_counts["technical_error"],
        "open_incidents": profile.open_incidents,
        "critical_incidents": profile.critical_open_incidents,
        "freshness_status": _freshness_status(profile),
        "last_profiled_at": profile.last_sync_at.isoformat() if profile.last_sync_at else None,
        "href": _asset_href(profile.table_id),
    }


def _rule_counts_by_table(session: Session, table_ids: list[int]) -> dict[int, dict[str, int]]:
    if not table_ids:
        return {}
    rules = list(session.scalars(select(DQRule).where(DQRule.table_id.in_(table_ids))).all())
    latest_runs: dict[int, DQRuleRun] = {}
    rule_ids = [rule.id for rule in rules]
    if rule_ids:
        runs = session.scalars(select(DQRuleRun).where(DQRuleRun.rule_id.in_(rule_ids)).order_by(desc(DQRuleRun.created_at))).all()
        for run in runs:
            latest_runs.setdefault(run.rule_id, run)
    counts: dict[int, dict[str, int]] = {
        table_id: {"total": 0, "active": 0, "failed": 0, "technical_error": 0} for table_id in table_ids
    }
    for rule in rules:
        table_id = int(rule.table_id) if rule.table_id is not None else None
        if table_id is None:
            continue
        item = counts.setdefault(table_id, {"total": 0, "active": 0, "failed": 0, "technical_error": 0})
        item["total"] += 1
        if rule.is_active:
            item["active"] += 1
        latest = latest_runs.get(rule.id)
        if latest:
            normalized_status = (latest.status or "").lower()
            if normalized_status in {"fail", "failed", "violation"} or int(latest.violations_count or 0) > 0:
                item["failed"] += 1
            if normalized_status in {"error", "technical_error"} or latest.error_message:
                item["technical_error"] += 1
    return counts


def _incidents_for_profiles(session: Session, profiles: list[TableProfile], direct_links: list[SemanticLink]) -> list[Incident]:
    lookup_keys = {profile.incident_lookup_key for profile in profiles}
    direct_ids = [link.entity_id for link in direct_links if link.entity_id is not None]
    clauses = []
    if lookup_keys:
        clauses.append(Incident.table_fqn.in_(lookup_keys))
    if direct_ids:
        clauses.append(Incident.id.in_(direct_ids))
    if not clauses:
        return []
    return list(session.scalars(select(Incident).where(or_(*clauses)).order_by(desc(Incident.detected_at)).limit(50)).all())


def _incident_item(incident: Incident) -> dict[str, object]:
    return {
        "id": incident.id,
        "title": incident.title,
        "severity": incident.severity,
        "status": incident.status,
        "asset": incident.table_fqn,
        "detected_at": incident.detected_at.isoformat() if incident.detected_at else None,
        "owner": incident.owner_team,
        "href": f"/incidents/tickets?incidentId={incident.id}",
    }


def _contract_summary_from_session(session: Session, product: SemanticDataProduct, profiles: list[TableProfile], contract_links: list[SemanticLink]) -> dict[str, object]:
    table_ids = [profile.table_id for profile in profiles]
    contracts: list[DataContract] = []
    if table_ids:
        contracts = list(session.scalars(select(DataContract).where(DataContract.table_id.in_(table_ids)).order_by(desc(DataContract.updated_at)).limit(20)).all())
    has_text = bool(product.contract_text)
    status = "none"
    if contracts:
        failed = sum(1 for contract in contracts if (contract.last_validation_status or "").lower() in {"failed", "broken", "error"})
        warnings = sum(int(contract.last_validation_issues or 0) for contract in contracts)
        status = "broken" if failed else "validated" if any(contract.last_validation_status for contract in contracts) else "active"
    elif has_text or contract_links:
        status = "draft"
        warnings = 0
        failed = 0
    else:
        warnings = 0
        failed = 0
    first_contract = contracts[0] if contracts else None
    return {
        "status": status,
        "name": product.contract_text or (f"Contrato v{first_contract.version}" if first_contract else None),
        "version": str(first_contract.version) if first_contract else None,
        "last_validated_at": first_contract.last_validation_at.isoformat() if first_contract and first_contract.last_validation_at else None,
        "breaks": failed,
        "warnings": warnings,
        "items": [
            {
                "id": contract.id,
                "table_id": contract.table_id,
                "version": contract.version,
                "status": contract.status,
                "last_validation_status": contract.last_validation_status,
                "last_validation_issues": contract.last_validation_issues,
            }
            for contract in contracts
        ]
        + [_semantic_link_item(link) for link in contract_links],
    }


def _lineage_summary(session: Session, profiles: list[TableProfile], links: list[SemanticLink]) -> dict[str, object]:
    table_ids = [profile.table_id for profile in profiles]
    if not table_ids:
        return {"upstreams": [], "downstreams": [], "pipelines": [], "consumers": []}
    lineage_assets = list(session.scalars(select(LineageAsset).where(LineageAsset.catalog_table_id.in_(table_ids), LineageAsset.is_active.is_(True))).all())
    lineage_ids = [asset.id for asset in lineage_assets]
    if not lineage_ids:
        return {
            "upstreams": [],
            "downstreams": [],
            "pipelines": [_semantic_link_item(link) for link in links if link.entity_kind == "pipeline"],
            "consumers": [_semantic_link_item(link) for link in links if link.entity_kind == "dashboard"],
        }
    upstream_rows = list(
        session.execute(
            select(LineageRelation, LineageAsset)
            .join(LineageAsset, LineageAsset.id == LineageRelation.source_asset_id)
            .where(LineageRelation.target_asset_id.in_(lineage_ids), LineageRelation.is_active.is_(True))
            .limit(50)
        ).all()
    )
    downstream_rows = list(
        session.execute(
            select(LineageRelation, LineageAsset)
            .join(LineageAsset, LineageAsset.id == LineageRelation.target_asset_id)
            .where(LineageRelation.source_asset_id.in_(lineage_ids), LineageRelation.is_active.is_(True))
            .limit(50)
        ).all()
    )
    pipelines = {
        relation.process_name
        for relation, _asset in upstream_rows + downstream_rows
        if relation.process_name
    }
    dashboards = {
        relation.dashboard_name
        for relation, _asset in upstream_rows + downstream_rows
        if relation.dashboard_name
    }
    return {
        "upstreams": [
            {
                "asset_id": asset.id,
                "asset_name": asset.asset_name,
                "asset_type": asset.asset_type,
                "relation_type": relation.relation_type,
                "process_name": relation.process_name,
            }
            for relation, asset in upstream_rows
        ],
        "downstreams": [
            {
                "asset_id": asset.id,
                "asset_name": asset.asset_name,
                "asset_type": asset.asset_type,
                "relation_type": relation.relation_type,
                "process_name": relation.process_name,
            }
            for relation, asset in downstream_rows
        ],
        "pipelines": [{"name": name} for name in sorted(pipelines)] + [_semantic_link_item(link) for link in links if link.entity_kind == "pipeline"],
        "consumers": [{"name": name, "kind": "dashboard"} for name in sorted(dashboards)] + [_semantic_link_item(link) for link in links if link.entity_kind == "dashboard"],
    }


def _certification_readiness(product: SemanticDataProduct, domain: SemanticDomain | None, profiles: list[TableProfile], quality: dict[str, object]) -> dict[str, object]:
    critical_incidents = sum(profile.critical_open_incidents for profile in profiles)
    checks = [
        ("domain", "Domínio vinculado", domain is not None, "Produto possui domínio associado." if domain else "Produto ainda não possui domínio associado."),
        ("owner", "Owner definido", bool(product.owner), "Owner informado." if product.owner else "Owner do produto ausente."),
        ("steward", "Steward definido", bool(product.steward), "Steward informado." if product.steward else "Steward do produto ausente."),
        ("consumers", "Consumidores informados", bool(product.consumers), "Consumidores definidos." if product.consumers else "Consumidores ainda não foram informados."),
        ("sla", "SLA definido", bool(product.sla_text), "SLA informado." if product.sla_text else "SLA ausente."),
        ("contract", "Contrato definido", bool(product.contract_text), "Contrato informado." if product.contract_text else "Contrato ausente."),
        ("assets", "Ativos associados", bool(profiles), "Produto possui ativos associados." if profiles else "Produto ainda não possui ativos associados."),
        ("quality", "Qualidade monitorada", int(quality.get("rules_active") or 0) > 0, "Há regras ativas." if int(quality.get("rules_active") or 0) > 0 else "Não há regras DQ ativas nos ativos associados."),
        ("critical_incidents", "Sem incidente crítico", critical_incidents == 0, "Nenhum incidente crítico aberto." if critical_incidents == 0 else "Há incidente crítico aberto."),
        ("quality_score", "Score de qualidade >= 85", int(quality.get("score") or 0) >= 85, f"Score atual: {quality.get('score') or 0}."),
        ("governance_score", "Score de governança >= 80", _score_from_profiles(profiles)[1] >= 80 if profiles else False, f"Score de governança atual: {_score_from_profiles(profiles)[1] if profiles else 0}."),
        ("asset_owners", "Ativos com owner", all(profile.owner_defined for profile in profiles) if profiles else False, "Todos os ativos possuem owner." if profiles and all(profile.owner_defined for profile in profiles) else "Há ativos sem owner."),
    ]
    checklist = [{"key": key, "label": label, "passed": passed, "reason": reason} for key, label, passed, reason in checks]
    score = round(sum(1 for item in checklist if item["passed"]) / len(checklist) * 100)
    blockers = [item for item in checklist if item["key"] in {"domain", "assets", "critical_incidents"} and not item["passed"]]
    warnings = [item for item in checklist if not item["passed"] and item not in blockers]
    if blockers:
        status = "blocked"
    elif score >= 90:
        status = "ready"
    elif score >= 70:
        status = "candidate"
    else:
        status = "not_ready"
    return {"status": status, "score": score, "checklist": checklist, "blockers": blockers, "warnings": warnings}


def _product_recommendations(
    product: SemanticDataProduct,
    profiles: list[TableProfile],
    quality: dict[str, object],
    incidents: dict[str, object],
    contract: dict[str, object],
    readiness: dict[str, object],
) -> list[dict[str, object]]:
    recommendations: list[dict[str, object]] = []

    def add(kind: str, severity: str, title: str, description: str, reason: str, action_label: str, action_target: str) -> None:
        recommendations.append(
            {
                "type": kind,
                "severity": severity,
                "title": title,
                "description": description,
                "reason": reason,
                "action_label": action_label,
                "action_target": action_target,
            }
        )

    if not profiles:
        add("asset", "high", "Associar ativos técnicos", "Este produto ainda não possui tabelas ou views vinculadas.", "Sem ativos, não há como consolidar qualidade, incidentes ou linhagem.", "Adicionar ativos", "assets")
    if not product.consumers:
        add("governance", "medium", "Informar consumidores", "Defina quem consome este produto para medir impacto e prioridade.", "Campo consumers vazio.", "Editar consumidores", "edit")
    if not product.sla_text:
        add("governance", "medium", "Definir SLA", "Formalize expectativa de atualização ou disponibilidade.", "Produto sem SLA definido.", "Definir SLA", "edit")
    if contract.get("status") == "none":
        add("contract", "medium", "Definir contrato do produto", "Crie ou associe um contrato para formalizar campos, regras e expectativas.", "Nenhum contrato encontrado.", "Criar contrato", "contract")
    if int(quality.get("assets_without_rules") or 0) > 0:
        add("quality", "high", "Configurar regras de qualidade", "Existem ativos associados sem validações ativas.", "Ativos sem regra DQ podem esconder problemas.", "Abrir Data Quality", "/data-quality")
    if int(incidents.get("critical") or 0) > 0:
        add("incident", "high", "Resolver incidentes críticos", "Há incidentes críticos relacionados ao produto ou ativos associados.", "Incidentes críticos bloqueiam certificação.", "Ver incidentes", "/incidents/tickets")
    elif int(incidents.get("open") or 0) > 0:
        add("incident", "medium", "Tratar incidentes abertos", "Há incidentes abertos que podem afetar consumidores ou SLA.", "Produto possui incidentes em aberto.", "Ver incidentes", "/incidents/tickets")
    if (quality.get("status") in {"excellent", "healthy"}) and readiness.get("status") in {"candidate", "ready"}:
        add("certification", "low", "Avaliar certificação", "O produto possui bons sinais para iniciar avaliação formal.", "Qualidade e governança estão em nível candidato.", "Revisar checklist", "certification")
    return recommendations[:8]


def get_product_summary(session: Session, *, slug: str, current_user=None) -> dict[str, object]:
    product = session.scalar(select(SemanticDataProduct).where(func.lower(SemanticDataProduct.slug) == _normalize_slug(slug)))
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data product not found")
    domain = session.get(SemanticDomain, product.domain_id)
    links = list(product.links)
    profiles = _select_product_profiles(session, product, current_user=current_user)
    table_ids = [profile.table_id for profile in profiles]
    rules_by_table = _rule_counts_by_table(session, table_ids)
    asset_items = [_profile_asset_item(profile, rules_by_table) for profile in profiles]
    quality_scores = [profile.dq_score for profile in profiles if profile.dq_score is not None]
    quality_score = round(sum(quality_scores) / len(quality_scores)) if quality_scores else None
    rules_total = sum(item["total"] for item in rules_by_table.values())
    rules_active = sum(item["active"] for item in rules_by_table.values())
    rules_failed = sum(item["failed"] for item in rules_by_table.values())
    rules_technical_error = sum(item["technical_error"] for item in rules_by_table.values())
    quality = {
        "score": quality_score,
        "status": _quality_status(quality_score, bool(profiles)),
        "rules_total": rules_total,
        "rules_active": rules_active,
        "rules_passed": max(0, rules_active - rules_failed - rules_technical_error),
        "rules_failed": rules_failed,
        "rules_technical_error": rules_technical_error,
        "assets_without_rules": sum(1 for profile in profiles if rules_by_table.get(profile.table_id, {}).get("active", 0) == 0),
        "critical_failures": sum(1 for profile in profiles if profile.critical_open_incidents > 0 or profile.recent_dq_failure_runs_30d > 0),
        "worst_assets": sorted(asset_items, key=lambda item: item["quality_score"] if item["quality_score"] is not None else -1)[:5],
    }
    direct_incident_links = [link for link in links if link.entity_kind == "incident"]
    incident_rows = _incidents_for_profiles(session, profiles, direct_incident_links)
    incidents = {
        "open": sum(1 for item in incident_rows if item.status in {"open", "reopened", "recurring"}),
        "critical": sum(1 for item in incident_rows if item.severity == "sev1" and item.status not in {"resolved", "closed"}),
        "in_progress": sum(1 for item in incident_rows if item.status in {"investigating", "mitigated"}),
        "resolved_recently": sum(1 for item in incident_rows if item.status in {"resolved", "closed"}),
        "items": [_incident_item(item) for item in incident_rows[:20]],
    }
    dashboard_links = [link for link in links if link.entity_kind == "dashboard"]
    dashboards = {"total": len(dashboard_links), "items": [_semantic_link_item(link) for link in dashboard_links]}
    contract = _contract_summary_from_session(session, product, profiles, [link for link in links if link.entity_kind == "contract"])
    lineage = _lineage_summary(session, profiles, links)
    readiness = _certification_readiness(product, domain, profiles, quality)
    recommendations = _product_recommendations(product, profiles, quality, incidents, contract, readiness)
    quality_fallback, governance_fallback, _coverage = _score_from_profiles(profiles)
    return {
        "product": {
            "id": product.id,
            "name": product.name,
            "slug": product.slug,
            "description": product.description,
            "domain_slug": domain.slug if domain else None,
            "domain_name": domain.name if domain else None,
            "owner": product.owner,
            "steward": product.steward,
            "consumers": list(product.consumers or []),
            "sla": product.sla_text,
            "contract": product.contract_text,
            "maturity": product.maturity_status,
            "quality_score": product.quality_score if product.quality_score is not None else quality_fallback,
            "governance_score": product.governance_score if product.governance_score is not None else governance_fallback,
            "certification_status": readiness["status"],
            "updated_at": product.updated_at.isoformat() if product.updated_at else None,
        },
        "domain": {
            "slug": domain.slug if domain else None,
            "name": domain.name if domain else None,
            "owner": domain.owner if domain else None,
            "steward": domain.steward if domain else None,
            "criticality": domain.criticality if domain else None,
            "maturity": domain.maturity_status if domain else None,
            "quality_score": domain.quality_score if domain else None,
            "governance_score": domain.governance_score if domain else None,
        },
        "assets": {
            "total": len(asset_items),
            "without_owner": sum(1 for profile in profiles if not profile.owner_defined),
            "without_quality_rules": int(quality["assets_without_rules"]),
            "with_incidents": sum(1 for profile in profiles if profile.open_incidents > 0),
            "certified": sum(1 for profile in profiles if profile.certification_status == "certified"),
            "items": asset_items,
        },
        "quality": quality,
        "incidents": incidents,
        "dashboards": dashboards,
        "contract": contract,
        "lineage": lineage,
        "certification_readiness": readiness,
        "recommendations": recommendations,
        "links": [_semantic_link_item(link) for link in links],
    }


def _domain_counts(domain: SemanticDomain, profiles: list[TableProfile]) -> dict[str, int]:
    profile_ids = {profile.table_id for profile in profiles}
    tables_linked = sum(1 for link in domain.links if link.entity_kind == "table" and (link.entity_id is None or link.entity_id in profile_ids))
    pipelines = sum(1 for link in domain.links if link.entity_kind == "pipeline")
    dq_rules = sum(1 for link in domain.links if link.entity_kind == "dq_rule")
    incidents = sum(1 for link in domain.links if link.entity_kind == "incident") + sum(profile.open_incidents for profile in profiles)
    dashboards = sum(1 for link in domain.links if link.entity_kind == "dashboard")
    contracts = sum(1 for link in domain.links if link.entity_kind == "contract")
    return {
        "assets_count": tables_linked,
        "pipelines_count": pipelines,
        "rules_count": dq_rules,
        "incidents_count": incidents,
        "dashboards_count": dashboards,
        "contracts_count": contracts,
    }


def _product_counts(product: SemanticDataProduct, profiles: list[TableProfile]) -> dict[str, int]:
    profile_ids = {profile.table_id for profile in profiles}
    tables_linked = sum(1 for link in product.links if link.entity_kind == "table" and (link.entity_id is None or link.entity_id in profile_ids))
    pipelines = sum(1 for link in product.links if link.entity_kind == "pipeline")
    dq_rules = sum(1 for link in product.links if link.entity_kind == "dq_rule")
    incidents = sum(1 for link in product.links if link.entity_kind == "incident") + sum(profile.open_incidents for profile in profiles)
    dashboards = sum(1 for link in product.links if link.entity_kind == "dashboard")
    contracts = sum(1 for link in product.links if link.entity_kind == "contract")
    return {
        "assets_count": tables_linked,
        "pipelines_count": pipelines,
        "rules_count": dq_rules,
        "incidents_count": incidents,
        "dashboards_count": dashboards,
        "contracts_count": contracts,
    }


def list_domain_suggestions(session: Session, *, current_user=None) -> list[SemanticDomainSuggestionOut]:
    now = datetime.now(timezone.utc)
    profiles = load_table_profiles(session, now, current_user=current_user)
    grouped: dict[str, list[TableProfile]] = defaultdict(list)
    for profile in profiles:
        if profile.domain_name and profile.domain_name.strip():
            grouped[profile.domain_name.strip()].append(profile)
    explicit = {
        (str(domain.name).strip().lower() if domain.name else "")
        for domain in session.scalars(select(SemanticDomain)).all()
        if domain.name and domain.name.strip()
    }
    suggestions: list[SemanticDomainSuggestionOut] = []
    for domain_name, group in sorted(grouped.items(), key=lambda item: item[0].lower()):
        if domain_name.lower() in explicit:
            continue
        quality, governance, _coverage = _score_from_profiles(group)
        maturity_score = _domain_maturity_score(group, [], 0)
        suggestions.append(
            SemanticDomainSuggestionOut(
                slug=_normalize_slug(domain_name),
                name=domain_name,
                assets_count=len(group),
                quality_score=quality,
                governance_score=governance,
                maturity_score=maturity_score,
                maturity_status=_maturity_label(maturity_score),
                open_incidents=sum(profile.open_incidents for profile in group),
                critical_open_incidents=sum(profile.critical_open_incidents for profile in group),
            )
        )
    return suggestions


def list_domains(session: Session, *, q: str | None = None, page: int = 1, page_size: int = 25, current_user=None):
    domains = list(session.scalars(select(SemanticDomain).order_by(SemanticDomain.name)).all())
    if q:
        needle = q.strip().lower()
        domains = [
            domain
            for domain in domains
            if needle in domain.name.lower() or needle in (domain.description or "").lower() or needle in (domain.owner or "").lower()
        ]
    results: list[SemanticDomainOut] = []
    for domain in domains:
        profiles = _select_domain_profiles(session, domain, current_user=current_user)
        counts = _domain_counts(domain, profiles)
        quality, governance, _ = _score_from_profiles(profiles)
        maturity_score = _domain_maturity_score(profiles, list(domain.links), len(domain.products))
        results.append(
            SemanticDomainOut(
                id=domain.id,
                slug=domain.slug,
                name=domain.name,
                description=domain.description,
                owner=domain.owner,
                steward=domain.steward,
                criticality=domain.criticality,
                maturity_status=domain.maturity_status,
                quality_score=domain.quality_score if domain.quality_score is not None else quality,
                governance_score=domain.governance_score if domain.governance_score is not None else governance,
                notes=domain.notes,
                is_active=domain.is_active,
                products_count=len(domain.products),
                maturity_score=maturity_score,
                maturity_label=_maturity_label(maturity_score),
                created_at=domain.created_at,
                updated_at=domain.updated_at,
                **counts,
            )
        )
    return paginate_items(results, page=page, page_size=page_size)


def create_domain(session: Session, payload: SemanticDomainCreate) -> SemanticDomain:
    slug = _normalize_slug(payload.slug or payload.name)
    domain = SemanticDomain(
        slug=slug,
        name=payload.name.strip(),
        description=(payload.description or "").strip() or None,
        owner=(payload.owner or "").strip() or None,
        steward=(payload.steward or "").strip() or None,
        criticality=(payload.criticality or "").strip() or None,
        maturity_status=(payload.maturity_status or "emerging").strip() or "emerging",
        quality_score=payload.quality_score,
        governance_score=payload.governance_score,
        notes=(payload.notes or "").strip() or None,
        is_active=payload.is_active,
    )
    session.add(domain)
    session.flush()
    return domain


def update_domain(session: Session, domain: SemanticDomain, payload) -> SemanticDomain:
    data = payload.model_dump(exclude_unset=True)
    if "slug" in data and data["slug"]:
        domain.slug = _normalize_slug(data["slug"])
    if "name" in data and data["name"]:
        domain.name = str(data["name"]).strip()
    if "description" in data:
        domain.description = (str(data["description"]).strip() or None) if data["description"] is not None else None
    if "owner" in data:
        domain.owner = (str(data["owner"]).strip() or None) if data["owner"] is not None else None
    if "steward" in data:
        domain.steward = (str(data["steward"]).strip() or None) if data["steward"] is not None else None
    if "criticality" in data:
        domain.criticality = (str(data["criticality"]).strip() or None) if data["criticality"] is not None else None
    if "maturity_status" in data and data["maturity_status"] is not None:
        domain.maturity_status = str(data["maturity_status"]).strip() or domain.maturity_status
    if "quality_score" in data:
        domain.quality_score = data["quality_score"]
    if "governance_score" in data:
        domain.governance_score = data["governance_score"]
    if "notes" in data:
        domain.notes = (str(data["notes"]).strip() or None) if data["notes"] is not None else None
    if "is_active" in data and data["is_active"] is not None:
        domain.is_active = bool(data["is_active"])
    session.flush()
    return domain


def delete_domain(session: Session, domain: SemanticDomain) -> None:
    session.delete(domain)
    session.flush()


def create_product(session: Session, payload: SemanticProductCreate) -> SemanticDataProduct:
    domain_slug = _normalize_slug(payload.domain_slug)
    domain = session.scalar(select(SemanticDomain).where(func.lower(SemanticDomain.slug) == domain_slug))
    if not domain:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
    product = SemanticDataProduct(
        domain_id=domain.id,
        slug=_normalize_slug(payload.slug or payload.name),
        name=payload.name.strip(),
        description=(payload.description or "").strip() or None,
        owner=(payload.owner or "").strip() or None,
        steward=(payload.steward or "").strip() or None,
        consumers=[value.strip() for value in payload.consumers if value.strip()],
        sla_text=(payload.sla_text or "").strip() or None,
        contract_text=(payload.contract_text or "").strip() or None,
        maturity_status=(payload.maturity_status or "emerging").strip() or "emerging",
        quality_score=payload.quality_score,
        governance_score=payload.governance_score,
        notes=(payload.notes or "").strip() or None,
        is_active=payload.is_active,
    )
    session.add(product)
    session.flush()
    return product


def update_product(session: Session, product: SemanticDataProduct, payload: SemanticProductUpdate) -> SemanticDataProduct:
    data = payload.model_dump(exclude_unset=True)
    if "domain_slug" in data and data["domain_slug"]:
        domain_slug = _normalize_slug(str(data["domain_slug"]))
        domain = session.scalar(select(SemanticDomain).where(func.lower(SemanticDomain.slug) == domain_slug))
        if not domain:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
        product.domain_id = domain.id
    if "slug" in data and data["slug"]:
        product.slug = _normalize_slug(data["slug"])
    if "name" in data and data["name"]:
        product.name = str(data["name"]).strip()
    if "description" in data:
        product.description = (str(data["description"]).strip() or None) if data["description"] is not None else None
    if "owner" in data:
        product.owner = (str(data["owner"]).strip() or None) if data["owner"] is not None else None
    if "steward" in data:
        product.steward = (str(data["steward"]).strip() or None) if data["steward"] is not None else None
    if "consumers" in data and data["consumers"] is not None:
        product.consumers = [str(value).strip() for value in data["consumers"] if str(value).strip()]
    if "sla_text" in data:
        product.sla_text = (str(data["sla_text"]).strip() or None) if data["sla_text"] is not None else None
    if "contract_text" in data:
        product.contract_text = (str(data["contract_text"]).strip() or None) if data["contract_text"] is not None else None
    if "maturity_status" in data and data["maturity_status"] is not None:
        product.maturity_status = str(data["maturity_status"]).strip() or product.maturity_status
    if "quality_score" in data:
        product.quality_score = data["quality_score"]
    if "governance_score" in data:
        product.governance_score = data["governance_score"]
    if "notes" in data:
        product.notes = (str(data["notes"]).strip() or None) if data["notes"] is not None else None
    if "is_active" in data and data["is_active"] is not None:
        product.is_active = bool(data["is_active"])
    session.flush()
    return product


def delete_product(session: Session, product: SemanticDataProduct) -> None:
    session.delete(product)
    session.flush()


def _resolve_semantic_scope(session: Session, scope_kind: str, scope_slug: str):
    normalized_slug = _normalize_slug(scope_slug)
    if scope_kind == "domain":
        scope = session.scalar(select(SemanticDomain).where(func.lower(SemanticDomain.slug) == normalized_slug))
        if not scope:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
        return scope
    if scope_kind == "product":
        scope = session.scalar(select(SemanticDataProduct).where(func.lower(SemanticDataProduct.slug) == normalized_slug))
        if not scope:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data product not found")
        return scope
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid semantic scope")


def list_domain_detail(session: Session, *, slug: str, current_user=None):
    domain = session.scalar(select(SemanticDomain).where(func.lower(SemanticDomain.slug) == _normalize_slug(slug)))
    if not domain:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
    profiles = _select_domain_profiles(session, domain, current_user=current_user)
    links = list(domain.links)
    counts = _domain_counts(domain, profiles)
    quality, governance, _ = _score_from_profiles(profiles)
    maturity_score = _domain_maturity_score(profiles, links, len(domain.products))
    domain_out = SemanticDomainOut(
        id=domain.id,
        slug=domain.slug,
        name=domain.name,
        description=domain.description,
        owner=domain.owner,
        steward=domain.steward,
        criticality=domain.criticality,
        maturity_status=domain.maturity_status,
        quality_score=domain.quality_score if domain.quality_score is not None else quality,
        governance_score=domain.governance_score if domain.governance_score is not None else governance,
        notes=domain.notes,
        is_active=domain.is_active,
        products_count=len(domain.products),
        maturity_score=maturity_score,
        maturity_label=_maturity_label(maturity_score),
        created_at=domain.created_at,
        updated_at=domain.updated_at,
        **counts,
    )
    products = [list_product_detail(session, slug=product.slug, current_user=current_user, include_domain=False) for product in domain.products]
    return SemanticDomainDetailOut(
        **domain_out.model_dump(),
        products=products,
        links=[SemanticLinkOut.model_validate(link) for link in links],
        assets=_collect_domain_assets(domain, profiles),
    )


def list_product_detail(
    session: Session,
    *,
    slug: str,
    current_user=None,
    include_domain: bool = True,
):
    product = session.scalar(select(SemanticDataProduct).where(func.lower(SemanticDataProduct.slug) == _normalize_slug(slug)))
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data product not found")
    domain = session.get(SemanticDomain, product.domain_id) if include_domain else None
    profiles = _select_product_profiles(session, product, current_user=current_user)
    links = list(product.links)
    counts = _product_counts(product, profiles)
    quality, governance, coverage = _score_from_profiles(profiles)
    maturity_score = _product_maturity_score(profiles, links)
    out = SemanticProductOut(
        id=product.id,
        domain_id=product.domain_id,
        domain_slug=domain.slug if domain else None,
        domain_name=domain.name if domain else None,
        slug=product.slug,
        name=product.name,
        description=product.description,
        owner=product.owner,
        steward=product.steward,
        consumers=list(product.consumers or []),
        sla_text=product.sla_text,
        contract_text=product.contract_text,
        maturity_status=product.maturity_status,
        quality_score=product.quality_score if product.quality_score is not None else quality,
        governance_score=product.governance_score if product.governance_score is not None else governance,
        notes=product.notes,
        is_active=product.is_active,
        assets_count=counts["assets_count"],
        pipelines_count=counts["pipelines_count"],
        rules_count=counts["rules_count"],
        incidents_count=counts["incidents_count"],
        dashboards_count=counts["dashboards_count"],
        contracts_count=counts["contracts_count"],
        maturity_score=maturity_score,
        maturity_label=_maturity_label(maturity_score),
        created_at=product.created_at,
        updated_at=product.updated_at,
    )
    if not include_domain:
        return out
    return SemanticProductDetailOut(
        **out.model_dump(),
        links=[SemanticLinkOut.model_validate(link) for link in links],
        assets=_collect_product_assets(product, profiles),
    )


def find_product_detail_for_table(session: Session, *, table_id: int, current_user=None):
    """Return the data product detail that contains the given catalog table, or None.

    Direct lookup via the semantic link index (entity_kind/entity_id) — avoids the
    client fanning out a detail request per product just to find the association.
    """
    link = session.scalar(
        select(SemanticLink)
        .where(
            SemanticLink.entity_kind == "table",
            SemanticLink.entity_id == table_id,
            SemanticLink.product_id.is_not(None),
        )
        .order_by(SemanticLink.product_id)
        .limit(1)
    )
    if link is None or link.product_id is None:
        return None
    product = session.get(SemanticDataProduct, link.product_id)
    if product is None:
        return None
    return list_product_detail(session, slug=product.slug, current_user=current_user, include_domain=True)


def list_products(session: Session, *, q: str | None = None, domain_slug: str | None = None, page: int = 1, page_size: int = 25, current_user=None):
    stmt = select(SemanticDataProduct)
    if domain_slug:
        stmt = stmt.join(SemanticDomain, SemanticDomain.id == SemanticDataProduct.domain_id).where(
            func.lower(SemanticDomain.slug) == _normalize_slug(domain_slug)
        )
    products = list(session.scalars(stmt.order_by(SemanticDataProduct.name)).all())
    if q:
        needle = q.strip().lower()
        products = [
            product
            for product in products
            if needle in product.name.lower() or needle in (product.description or "").lower() or needle in (product.owner or "").lower()
        ]
    results: list[SemanticProductOut] = []
    for product in products:
        results.append(list_product_detail(session, slug=product.slug, current_user=current_user))
    return paginate_items(results, page=page, page_size=page_size)


def list_domain_links(session: Session, *, domain_slug: str) -> list[SemanticLinkOut]:
    domain = session.scalar(select(SemanticDomain).where(func.lower(SemanticDomain.slug) == _normalize_slug(domain_slug)))
    if not domain:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
    return [SemanticLinkOut.model_validate(link) for link in domain.links]


def list_product_links(session: Session, *, product_slug: str) -> list[SemanticLinkOut]:
    product = session.scalar(select(SemanticDataProduct).where(func.lower(SemanticDataProduct.slug) == _normalize_slug(product_slug)))
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data product not found")
    return [SemanticLinkOut.model_validate(link) for link in product.links]


def add_domain_link(session: Session, *, domain_slug: str, payload: SemanticLinkCreate) -> SemanticLink:
    domain = session.scalar(select(SemanticDomain).where(func.lower(SemanticDomain.slug) == _normalize_slug(domain_slug)))
    if not domain:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
    link = SemanticLink(
        domain_id=domain.id,
        relation_kind=payload.relation_kind.strip(),
        entity_kind=payload.entity_kind.strip(),
        entity_id=payload.entity_id,
        entity_label=payload.entity_label.strip(),
        entity_href=(payload.entity_href or "").strip() or None,
        notes=(payload.notes or "").strip() or None,
        is_primary=payload.is_primary,
    )
    if link.entity_kind == "table" and link.entity_id is not None and not link.entity_href:
        link.entity_href = _asset_href(link.entity_id)
    session.add(link)
    session.flush()
    return link


def add_product_link(session: Session, *, product_slug: str, payload: SemanticLinkCreate) -> SemanticLink:
    product = session.scalar(select(SemanticDataProduct).where(func.lower(SemanticDataProduct.slug) == _normalize_slug(product_slug)))
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data product not found")
    link = SemanticLink(
        product_id=product.id,
        relation_kind=payload.relation_kind.strip(),
        entity_kind=payload.entity_kind.strip(),
        entity_id=payload.entity_id,
        entity_label=payload.entity_label.strip(),
        entity_href=(payload.entity_href or "").strip() or None,
        notes=(payload.notes or "").strip() or None,
        is_primary=payload.is_primary,
    )
    if link.entity_kind == "table" and link.entity_id is not None and not link.entity_href:
        link.entity_href = _asset_href(link.entity_id)
    session.add(link)
    session.flush()
    return link


def delete_link(session: Session, link_id: int) -> None:
    link = session.get(SemanticLink, link_id)
    if not link:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")
    session.delete(link)
    session.flush()


def resolve_table_link_defaults(db: Session, table_id: int) -> dict[str, object]:
    table = (
        db.execute(
            select(
                TableEntity.id,
                TableEntity.name,
                Schema.name.label("schema_name"),
                Database.name.label("database_name"),
                DataSource.name.label("datasource_name"),
            )
            .join(Schema, Schema.id == TableEntity.schema_id)
            .join(Database, Database.id == Schema.database_id)
            .join(DataSource, DataSource.id == Database.datasource_id)
            .where(TableEntity.id == table_id)
        )
        .mappings()
        .first()
    )
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return {
        "entity_label": f"{table['datasource_name']}.{table['database_name']}.{table['schema_name']}.{table['name']}",
        "entity_href": f"/explorer?tableId={table_id}",
    }
