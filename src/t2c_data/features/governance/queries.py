from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Lock
from urllib.parse import urlencode

from fastapi import HTTPException, status
from sqlalchemy import Integer, case, cast, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.certification.api_support import certification_status_label, resolve_certification_status_for_profile
from t2c_data.features.governance.active_governance import build_active_governance_findings
from t2c_data.features.dashboard.executive_scoring import compute_profile_priority_score
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.features.ingestion import load_ingestion_operational_overview_from_source
from t2c_data.features.ingestion.service import STALE_SUCCESS_THRESHOLD_HOURS
from t2c_data.features.governance.rules import (
    CRITICAL_CHANGE_LOOKBACK_DAYS,
    certification_review_due,
    owner_review_due,
    privacy_review_due,
)
from t2c_data.features.governance.scoring import build_governance_score_for_profile, governance_score_label
from t2c_data.features.governance.risk import build_risk_payload
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.privacy_access.policy import sensitivity_label
from t2c_data.features.stewardship.workflow import build_stewardship_inbox_summary
from t2c_data.features.privacy_access import can_view_table
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.metabase_impact import MetabaseImpactSnapshot
from t2c_data.models.search import SearchResultClick
from t2c_data.models.stewardship import StewardshipRequest

_PENDING_CENTER_CACHE_LOCK = Lock()
_PENDING_CENTER_CACHE_TTL_SECONDS = 30
_PENDING_CENTER_CACHE: dict[tuple[object, ...], tuple[datetime, dict[str, object]]] = {}


CAMPAIGN_CONFIG = {
    "no_owner": {
        "label": "Ativos sem owner",
        "hint": "Direcionar accountability antes de avançar em certificação, operação e revisão.",
        "href": "/data-owners",
        "tone": "warning",
        "responsible": "Stewardship e Data Owners",
    },
    "no_description": {
        "label": "Ativos sem descrição",
        "hint": "Completar o contexto de negócio reduz ambiguidade para consumo e suporte.",
        "href": "/explorer",
        "tone": "neutral",
        "responsible": "Stewardship e times de domínio",
    },
    "no_classification": {
        "label": "Ativos sem classificação",
        "hint": "Revisar sensibilidade e privacidade reduz exposição regulatória.",
        "href": "/privacy-access",
        "tone": "warning",
        "responsible": "Governança e Privacidade",
    },
    "no_sla": {
        "label": "Ativos sem SLA",
        "hint": "Definir SLA dá um limite formal para atualização, revisão e tratamento de falhas.",
        "href": "/governance/change-management",
        "tone": "warning",
        "responsible": "Governança contínua",
    },
    "no_certification": {
        "label": "Elegíveis sem certificação",
        "hint": "Levar ativos prontos para decisão reduz fila invisível de aprovação.",
        "href": "/certification",
        "tone": "accent",
        "responsible": "Governança e dono do ativo",
    },
    "no_dictionary": {
        "label": "Ativos sem dicionário",
        "hint": "O dicionário ainda impede entendimento seguro e uso assistido das colunas.",
        "href": "/explorer",
        "tone": "warning",
        "responsible": "Stewardship e times de engenharia",
    },
    "stale_reviews": {
        "label": "Revisões vencidas",
        "hint": "Owner, privacidade ou certificação sem confirmação recente precisam entrar na fila.",
        "href": "/dashboard?governanceCampaign=stale_reviews",
        "tone": "warning",
        "responsible": "Governança contínua",
    },
    "no_terms": {
        "label": "Sem termo associado",
        "hint": "Conectar o glossário melhora entendimento de negócio e encontrabilidade.",
        "href": "/glossary",
        "tone": "neutral",
        "responsible": "Stewardship semântico",
    },
    "no_tags": {
        "label": "Sem tags estratégicas",
        "hint": "Tags ainda faltam para descoberta, classificação e recortes executivos.",
        "href": "/tags",
        "tone": "neutral",
        "responsible": "Governança de metadados",
    },
}


def _links_for(table) -> dict[str, str]:
    return build_asset_links(
        table_id=table.table_id,
        datasource_id=table.datasource_id,
        database_id=table.database_id,
        schema_id=table.schema_id,
        data_owner_id=table.data_owner_id,
    )


def _display_value(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return str(value.get("label") or value.get("value") or value)
    if isinstance(value, list):
        return ", ".join(str(item) for item in value[:5])
    return str(value)


def _review_item(table, *, now: datetime, settings_snapshot) -> dict[str, object]:
    effective_status = resolve_certification_status_for_profile(table, now=now)
    return {
        "table_id": table.table_id,
        "table_name": table.table_name,
        "table_fqn": table.table_fqn,
        "datasource_name": table.datasource_name,
        "database_name": table.database_name,
        "schema_name": table.schema_name,
        "owner_name": table.owner_name or "Não definido",
        "certification_status": effective_status,
        "certification_status_label": certification_status_label(effective_status),
        "sensitivity_label": sensitivity_label(table.sensitivity_level),
        "owner_review_due": owner_review_due(table, now=now, settings_snapshot=settings_snapshot),
        "privacy_review_due": privacy_review_due(table, now=now, settings_snapshot=settings_snapshot),
        "certification_review_due": certification_review_due(table, now=now, settings_snapshot=settings_snapshot),
        "last_review_at": table.last_review_at.isoformat() if table.last_review_at else None,
        "links": _links_for(table),
    }


def _campaign_match(table, *, key: str, now: datetime, settings_snapshot) -> bool:
    if key == "no_owner":
        return not table.owner_defined
    if key == "no_description":
        return not table.description_complete
    if key == "no_classification":
        return not table.sensitivity_level
    if key == "no_sla":
        return not table.sla_defined
    if key == "no_certification":
        return table.eligible_for_certification and resolve_certification_status_for_profile(table, now=now) != "certified"
    if key == "no_dictionary":
        return not table.dictionary_complete
    if key == "stale_reviews":
        return (
            owner_review_due(table, now=now, settings_snapshot=settings_snapshot)
            or privacy_review_due(table, now=now, settings_snapshot=settings_snapshot)
            or certification_review_due(table, now=now, settings_snapshot=settings_snapshot)
        )
    if key == "no_terms":
        return table.terms_count <= 0
    if key == "no_tags":
        return table.tags_count <= 0
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Governance campaign not found")


def _campaign_item(
    key: str,
    *,
    tables,
    now: datetime,
    settings_snapshot,
) -> dict[str, object]:
    config = CAMPAIGN_CONFIG[key]
    pending = sum(1 for table in tables if _campaign_match(table, key=key, now=now, settings_snapshot=settings_snapshot))
    total_assets = len(tables)
    completed = max(total_assets - pending, 0)
    progress_pct = round((completed / total_assets) * 100, 1) if total_assets else 0.0
    return {
        "key": key,
        "label": config["label"],
        "count": pending,
        "completed_count": completed,
        "progress_pct": progress_pct,
        "responsible": config["responsible"],
        "hint": config["hint"],
        "href": config["href"],
        "export_csv_href": f"/api/v1/governance/campaigns/{key}/export.csv",
        "export_xlsx_href": f"/api/v1/governance/campaigns/{key}/export.xlsx",
        "tone": config["tone"],
    }


def _campaign_queue_items(tables, *, key: str, now: datetime, settings_snapshot) -> list[dict[str, object]]:
    matched = [table for table in tables if _campaign_match(table, key=key, now=now, settings_snapshot=settings_snapshot)]
    scores = {
        table.table_id: build_governance_score_for_profile(table, settings_snapshot=settings_snapshot)
        for table in matched
    }
    matched.sort(
        key=lambda table: (
            int(scores[table.table_id]["score"]),
            table.owner_defined,
            table.dictionary_complete,
            table.description_complete,
            table.table_fqn,
        )
    )
    return [
        {
            "table_id": table.table_id,
            "table_name": table.table_name,
            "table_fqn": table.table_fqn,
            "datasource_name": table.datasource_name,
            "database_name": table.database_name,
            "schema_name": table.schema_name,
            "owner_name": table.owner_name or "Não definido",
            "governance_score": scores[table.table_id],
            "certification_status": resolve_certification_status_for_profile(table, now=now),
            "certification_status_label": certification_status_label(
                resolve_certification_status_for_profile(table, now=now)
            ),
            "sensitivity_label": sensitivity_label(table.sensitivity_level),
            "last_review_at": table.last_review_at.isoformat() if table.last_review_at else None,
            "links": _links_for(table),
        }
        for table in matched
    ]


def get_governance_review_summary(session: Session, *, current_user=None) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    tables = load_table_profiles(session, now, current_user=current_user)
    due_items = []
    owner_due_count = 0
    privacy_due_count = 0
    certification_due_count = 0
    for table in tables:
        owner_due = owner_review_due(table, now=now, settings_snapshot=settings_snapshot)
        privacy_due = privacy_review_due(table, now=now, settings_snapshot=settings_snapshot)
        cert_due = certification_review_due(table, now=now, settings_snapshot=settings_snapshot)
        owner_due_count += int(owner_due)
        privacy_due_count += int(privacy_due)
        certification_due_count += int(cert_due)
        if owner_due or privacy_due or cert_due:
            due_items.append(table)

    sorted_items = sorted(
        due_items,
        key=lambda item: (
            -int(owner_review_due(item, now=now, settings_snapshot=settings_snapshot)),
            -int(privacy_review_due(item, now=now, settings_snapshot=settings_snapshot)),
            -int(certification_review_due(item, now=now, settings_snapshot=settings_snapshot)),
            item.last_review_at or datetime(1970, 1, 1, tzinfo=timezone.utc),
            item.table_fqn,
        ),
    )

    return {
        "generated_at": now.isoformat(),
        "owner_review_due": owner_due_count,
        "privacy_review_due": privacy_due_count,
        "certification_review_due": certification_due_count,
        "items": [_review_item(table, now=now, settings_snapshot=settings_snapshot) for table in sorted_items[:12]],
    }


def get_governance_campaigns(session: Session, *, current_user=None) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    tables = load_table_profiles(session, now, current_user=current_user)
    total_assets = len(tables)
    items = [_campaign_item(key, tables=tables, now=now, settings_snapshot=settings_snapshot) for key in CAMPAIGN_CONFIG]

    return {
        "generated_at": now.isoformat(),
        "total_assets": total_assets,
        "items": sorted(items, key=lambda item: (-item["count"], item["label"])),
    }


def get_governance_campaign_queue(
    session: Session,
    *,
    campaign_key: str,
    page: int = 1,
    page_size: int = 50,
    current_user=None,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    tables = load_table_profiles(session, now, current_user=current_user)
    campaign = _campaign_item(campaign_key, tables=tables, now=now, settings_snapshot=settings_snapshot)
    items = _campaign_queue_items(tables, key=campaign_key, now=now, settings_snapshot=settings_snapshot)
    total = len(items)
    start = max(page - 1, 0) * page_size
    end = start + page_size
    return {
        "generated_at": now.isoformat(),
        "campaign": campaign,
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": items[start:end],
    }


PENDING_SEVERITY_LABELS = {
    "critical": "Crítica",
    "high": "Alta",
    "medium": "Moderada",
    "low": "Baixa",
}

PENDING_ORIGIN_LABELS = {
    "governance": "Governança",
    "metadata": "Metadados",
    "glossary": "Glossário",
    "certification": "Certificação",
    "quality": "Qualidade",
    "operations": "Operação",
    "incidents": "Incidentes",
}

PENDING_STATUS_LABELS = {
    "open": "Aberta",
}

PENDING_GROUP_LABELS = {
    "owner": "Por owner",
    "datasource": "Por fonte",
    "severity": "Por severidade",
}

PIPELINE_MAPPING_SLA_DAYS = 7

SLA_STATUS_LABELS = {
    "within_sla": "Dentro do SLA",
    "due_soon": "Próximo do vencimento",
    "overdue": "Fora do SLA",
}


def _pending_priority(severity: str, *, base: int) -> int:
    severity_weight = {
        "critical": 400,
        "high": 300,
        "medium": 200,
        "low": 100,
    }.get(severity, 0)
    return severity_weight + base


def _metabase_dashboard_impact_map(session: Session, table_ids: list[int]) -> dict[int, int]:
    if not table_ids:
        return {}
    try:
        rows = session.scalars(
            select(MetabaseImpactSnapshot)
            .where(MetabaseImpactSnapshot.table_id.in_(table_ids))
            .order_by(
                MetabaseImpactSnapshot.table_id.asc(),
                MetabaseImpactSnapshot.last_verified_at.desc().nullslast(),
                MetabaseImpactSnapshot.created_at.desc(),
                MetabaseImpactSnapshot.id.desc(),
            )
        ).all()
    except SQLAlchemyError:
        return {}
    impact: dict[int, int] = {}
    for row in rows:
        table_id = int(row.table_id)
        if table_id not in impact:
            impact[table_id] = int(row.dashboard_count or 0)
    return impact


def _lineage_impact_map(session: Session, table_ids: list[int]) -> dict[int, dict[str, int]]:
    if not table_ids:
        return {}
    asset_rows = session.execute(
        select(LineageAsset.id, LineageAsset.catalog_table_id).where(
            LineageAsset.catalog_table_id.in_(table_ids),
            LineageAsset.is_active.is_(True),
        )
    ).all()
    lineage_to_table = {int(row.id): int(row.catalog_table_id) for row in asset_rows if row.catalog_table_id is not None}
    if not lineage_to_table:
        return {}
    lineage_ids = list(lineage_to_table.keys())
    impact = {table_id: {"upstream": 0, "downstream": 0} for table_id in table_ids}
    upstream_rows = session.execute(
        select(LineageRelation.target_asset_id, func.count(LineageRelation.id))
        .where(LineageRelation.target_asset_id.in_(lineage_ids), LineageRelation.is_active.is_(True))
        .group_by(LineageRelation.target_asset_id)
    ).all()
    downstream_rows = session.execute(
        select(LineageRelation.source_asset_id, func.count(LineageRelation.id))
        .where(LineageRelation.source_asset_id.in_(lineage_ids), LineageRelation.is_active.is_(True))
        .group_by(LineageRelation.source_asset_id)
    ).all()
    for lineage_asset_id, count in upstream_rows:
        table_id = lineage_to_table.get(int(lineage_asset_id))
        if table_id is not None:
            impact.setdefault(table_id, {"upstream": 0, "downstream": 0})["upstream"] = int(count or 0)
    for lineage_asset_id, count in downstream_rows:
        table_id = lineage_to_table.get(int(lineage_asset_id))
        if table_id is not None:
            impact.setdefault(table_id, {"upstream": 0, "downstream": 0})["downstream"] = int(count or 0)
    return impact


def _search_user_impact_map(session: Session, table_ids: list[int], now: datetime) -> dict[int, int]:
    if not table_ids:
        return {}
    rows = session.execute(
        select(SearchResultClick.entity_id, func.count(func.distinct(SearchResultClick.user_id)))
        .where(
            SearchResultClick.created_at >= now - timedelta(days=30),
            SearchResultClick.entity_type == "table",
            SearchResultClick.entity_id.in_(table_ids),
            SearchResultClick.user_id.is_not(None),
        )
        .group_by(SearchResultClick.entity_id)
    ).all()
    return {int(table_id): int(count or 0) for table_id, count in rows if table_id is not None}


def _build_pending_center_href(**params: object) -> str:
    query = urlencode({key: value for key, value in params.items() if value not in (None, "", [])})
    return f"/governance/pending-center?{query}" if query else "/governance/pending-center"


def _stewardship_request_href(
    *,
    table_id: int,
    request_type: str,
    origin: str = "pending_center",
    table_name: str | None = None,
    schema_name: str | None = None,
    database_name: str | None = None,
    datasource_name: str | None = None,
) -> str:
    params = {
        "tableId": table_id,
        "requestType": request_type,
        "create": 1,
        "origin": origin,
        "tableName": table_name,
        "schemaName": schema_name,
        "databaseName": database_name,
        "datasourceName": datasource_name,
    }
    query = urlencode({key: value for key, value in params.items() if value not in (None, "", [])})
    return f"/governance/stewardship?{query}"


def _sla_payload(
    *,
    detected_at: datetime,
    due_at: datetime | None = None,
    sla_days: int | None = None,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    detected = detected_at if detected_at.tzinfo is not None else detected_at.replace(tzinfo=timezone.utc)
    result: dict[str, object] = {
        "aging_days": max((now - detected).days, 0),
        "sla_days": sla_days,
        "due_at": due_at.isoformat() if due_at else None,
        "sla_status": None,
        "sla_status_label": None,
    }
    if due_at is None:
        return result
    due = due_at if due_at.tzinfo is not None else due_at.replace(tzinfo=timezone.utc)
    remaining_days = (due - now).total_seconds() / 86400
    if remaining_days < 0:
        status = "overdue"
    elif remaining_days <= 3:
        status = "due_soon"
    else:
        status = "within_sla"
    result["sla_status"] = status
    result["sla_status_label"] = SLA_STATUS_LABELS[status]
    return result


def _pending_item(
    table,
    *,
    governance_score: dict[str, object],
    trust_score: int | None = None,
    trust_label: str | None = None,
    trust_tone: str | None = None,
    key: str,
    title: str,
    description: str,
    severity: str,
    origin: str,
    action_label: str,
    action_href: str,
    detected_at: datetime,
    due_at: datetime | None = None,
    sla_days: int | None = None,
    context_value: str | None = None,
    base_priority: int = 0,
) -> dict[str, object]:
    sla_payload = _sla_payload(detected_at=detected_at, due_at=due_at, sla_days=sla_days)
    effective_trust_score = int(trust_score if trust_score is not None else getattr(table, "trust_score", 0) or 0)
    effective_trust_label = trust_label if trust_label is not None else getattr(table, "trust_label", None)
    effective_trust_tone = trust_tone if trust_tone is not None else getattr(table, "trust_tone", None)
    risk_payload = build_risk_payload(
        table=table,
        severity=severity,
        origin=origin,
        trust_score=effective_trust_score,
        sla_status=str(sla_payload["sla_status"] or ""),
        context_value=context_value,
    )
    final_priority_score = compute_profile_priority_score(
        table,
        int(risk_payload["risk_score"] or 0),
        dashboards=int(getattr(table, "metabase_dashboard_count", 0) or 0),
        users=int(getattr(table, "impacted_user_count", table.search_clicks_30d) or 0),
        upstream=int(getattr(table, "lineage_upstream_count", 0) or 0),
        downstream=int(getattr(table, "lineage_downstream_count", 0) or 0),
    )
    return {
        "key": key,
        "title": title,
        "description": description,
        "severity": severity,
        "severity_label": PENDING_SEVERITY_LABELS.get(severity, severity.title()),
        "priority": _pending_priority(severity, base=base_priority) + final_priority_score,
        "origin": origin,
        "origin_label": PENDING_ORIGIN_LABELS.get(origin, origin.title()),
        "status": "open",
        "status_label": PENDING_STATUS_LABELS["open"],
        "table_id": table.table_id,
        "table_name": table.table_name,
        "table_fqn": table.table_fqn,
        "datasource_name": table.datasource_name,
        "database_name": table.database_name,
        "schema_name": table.schema_name,
        "domain_name": table.domain_name,
        "owner_name": table.owner_name or "Não definido",
        "data_owner_id": table.data_owner_id,
        "detected_at": detected_at.isoformat(),
        "aging_days": sla_payload["aging_days"],
        "sla_days": sla_payload["sla_days"],
        "due_at": sla_payload["due_at"],
        "sla_status": sla_payload["sla_status"],
        "sla_status_label": sla_payload["sla_status_label"],
        "governance_score": governance_score,
        "trust_score": effective_trust_score,
        "trust_label": effective_trust_label,
        "trust_tone": effective_trust_tone,
        "risk_score": risk_payload["risk_score"],
        "risk_label": risk_payload["risk_label"],
        "risk_tone": risk_payload["risk_tone"],
        "risk_reason": risk_payload["risk_reason"],
        "risk_components": risk_payload["risk_components"],
        "context_value": context_value,
        "action_label": action_label,
        "action_href": action_href,
        "links": _links_for(table),
    }


def _pending_filters(items: list[dict[str, object]]) -> dict[str, object]:
    def _option_pairs(values: list[tuple[str, str]]) -> list[dict[str, str]]:
        return [{"value": value, "label": label} for value, label in values if value]

    severity_pairs = [(key, PENDING_SEVERITY_LABELS[key]) for key in ["critical", "high", "medium", "low"]]
    origin_pairs = [(key, label) for key, label in PENDING_ORIGIN_LABELS.items()]
    status_pairs = [("open", PENDING_STATUS_LABELS["open"])]
    owner_pairs = sorted(
        {(str(item["owner_name"]), str(item["owner_name"])) for item in items if item.get("owner_name")},
        key=lambda item: item[1].lower(),
    )
    datasource_pairs = sorted(
        {(str(item["datasource_name"]), str(item["datasource_name"])) for item in items if item.get("datasource_name")},
        key=lambda item: item[1].lower(),
    )
    schema_pairs = sorted(
        {(str(item["schema_name"]), str(item["schema_name"])) for item in items if item.get("schema_name")},
        key=lambda item: item[1].lower(),
    )
    domain_pairs = sorted(
        {(str(item["domain_name"]), str(item["domain_name"])) for item in items if item.get("domain_name")},
        key=lambda item: item[1].lower(),
    )
    return {
        "severities": _option_pairs(severity_pairs),
        "origins": _option_pairs(origin_pairs),
        "statuses": _option_pairs(status_pairs),
        "owners": _option_pairs(owner_pairs),
        "datasources": _option_pairs(datasource_pairs),
        "schemas": _option_pairs(schema_pairs),
        "domains": _option_pairs(domain_pairs),
    }


def _pending_breakdown(items: list[dict[str, object]], *, field: str, labels: dict[str, str], order: list[str]) -> list[dict[str, object]]:
    counts = {key: 0 for key in order}
    for item in items:
        value = str(item.get(field) or "")
        if value in counts:
            counts[value] += 1
    return [{"key": key, "label": labels.get(key, key.title()), "count": counts[key]} for key in order if counts[key] > 0]


def _pending_summary_cards(
    items: list[dict[str, object]],
    *,
    stewardship_summary: dict[str, object],
    notification_summary: dict[str, object],
) -> dict[str, int]:
    stewardship_keys = {
        "no_owner",
        "no_description",
        "no_classification",
        "no_sla",
        "no_dictionary",
        "no_tags",
        "no_terms",
        "no_certification",
        "owner_review_due",
        "privacy_review_due",
        "certification_review_due",
        "critical_without_dq",
        "classification_high_usage",
        "dictionary_high_usage",
        "recurring_dq_failure_critical",
    }
    reviews_keys = {"owner_review_due", "privacy_review_due", "certification_review_due"}
    certification_keys = {"no_certification", "certification_review_due"}
    origin_keys = {"governance", "metadata", "glossary", "certification"}
    stewardship_pending = sum(
        1
        for item in items
        if str(item.get("key") or "") in stewardship_keys or str(item.get("origin") or "") in origin_keys
    )
    without_approver = sum(1 for item in items if str(item.get("key") or "") == "no_owner")
    reviews = sum(1 for item in items if str(item.get("key") or "") in reviews_keys)
    certification = sum(1 for item in items if str(item.get("key") or "") in certification_keys)
    trust_at_risk = sum(1 for item in items if int(item.get("trust_score") or 0) < 60)
    return {
        "stewardship_pending": stewardship_pending,
        "without_approver": without_approver,
        "reviews": reviews,
        "certification": certification,
        "trust_at_risk": trust_at_risk,
        "my_approval": int(stewardship_summary.get("my_approvals_pending") or 0),
        "my_queue": int(stewardship_summary.get("my_owner_queue") or 0),
        "active_notifications": int(notification_summary.get("active_total") or 0),
        "ready_to_resend": int(notification_summary.get("due_now_total") or 0),
        "critical": int(notification_summary.get("critical_total") or 0),
        "operation": int(notification_summary.get("operational_total") or 0),
        "quality_incidents": int(notification_summary.get("quality_total") or 0) + int(notification_summary.get("incident_total") or 0),
    }


def _pending_campaigns(items: list[dict[str, object]]) -> list[dict[str, object]]:
    campaigns: list[dict[str, object]] = []

    def _group(
        *,
        group_by: str,
        label_getter,
        value_getter,
        href_builder,
        hint_builder,
        limit: int,
    ) -> None:
        counts: dict[str, int] = {}
        for item in items:
            value = value_getter(item)
            if not value:
                continue
            counts[value] = counts.get(value, 0) + 1
        top_values = sorted(counts.items(), key=lambda entry: (-entry[1], entry[0].lower()))[:limit]
        for value, count in top_values:
            grouped_items = [item for item in items if value_getter(item) == value]
            governance_scores = [int(item.get("governance_score", {}).get("score", 0)) for item in grouped_items]
            avg_governance_score = round(sum(governance_scores) / len(governance_scores), 1) if governance_scores else 0.0
            lowest_governance_score = min(governance_scores) if governance_scores else 0
            governance_label, governance_tone = governance_score_label(int(round(avg_governance_score))) if governance_scores else ("Sem leitura", "neutral")
            campaigns.append(
                {
                    "group_by": group_by,
                    "group_label": PENDING_GROUP_LABELS[group_by],
                    "value": value,
                    "label": label_getter(value),
                    "count": count,
                    "avg_governance_score": avg_governance_score,
                    "lowest_governance_score": lowest_governance_score,
                    "governance_label": governance_label,
                    "governance_tone": governance_tone,
                    "href": href_builder(value),
                    "hint": hint_builder(value, count),
                }
            )

    _group(
        group_by="owner",
        label_getter=lambda value: value,
        value_getter=lambda item: str(item.get("owner_name") or ""),
        href_builder=lambda value: _build_pending_center_href(owner=value),
        hint_builder=lambda value, count: f"{count} pendência(s) concentradas em ativos atribuídos a {value}.",
        limit=5,
    )
    _group(
        group_by="datasource",
        label_getter=lambda value: value,
        value_getter=lambda item: str(item.get("datasource_name") or ""),
        href_builder=lambda value: _build_pending_center_href(datasource=value),
        hint_builder=lambda value, count: f"{count} pendência(s) concentradas na fonte {value}.",
        limit=5,
    )
    _group(
        group_by="severity",
        label_getter=lambda value: PENDING_SEVERITY_LABELS.get(value, value.title()),
        value_getter=lambda item: str(item.get("severity") or ""),
        href_builder=lambda value: _build_pending_center_href(severity=value),
        hint_builder=lambda value, count: f"{count} pendência(s) com severidade {PENDING_SEVERITY_LABELS.get(value, value)}.",
        limit=4,
    )
    return campaigns


def _pending_center_cache_key(
    *,
    current_user,
    q: str | None,
    severity: str | None,
    origin: str | None,
    owner_id: int | None,
    owner: str | None,
    datasource: str | None,
    schema_name: str | None,
    domain: str | None,
    status_filter: str | None,
    include_ingestion: bool,
    include_campaigns: bool,
    include_queue: bool,
) -> tuple[object, ...]:
    return (
        getattr(current_user, "id", None),
        bool(include_ingestion),
        bool(include_campaigns),
        bool(include_queue),
        (q or "").strip().lower(),
        (severity or "").strip().lower(),
        (origin or "").strip().lower(),
        owner_id,
        (owner or "").strip().lower(),
        (datasource or "").strip().lower(),
        (schema_name or "").strip().lower(),
        (domain or "").strip().lower(),
        (status_filter or "").strip().lower(),
    )


def _build_pending_center_dataset(
    session: Session,
    *,
    q: str | None = None,
    severity: str | None = None,
    origin: str | None = None,
    owner_id: int | None = None,
    owner: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    status_filter: str | None = None,
    include_ingestion: bool = True,
    include_campaigns: bool = True,
    include_queue: bool = True,
    current_user=None,
) -> dict[str, object]:
    from t2c_data.features.governance.notifications import get_governance_notification_summary

    cache_key = _pending_center_cache_key(
        current_user=current_user,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        include_ingestion=include_ingestion,
        include_campaigns=include_campaigns,
        include_queue=include_queue,
    )
    now = datetime.now(timezone.utc)
    with _PENDING_CENTER_CACHE_LOCK:
        cached = _PENDING_CENTER_CACHE.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

    settings_snapshot = get_governance_settings_snapshot(session)
    tables, _read_model_source = load_dashboard_profiles_with_fallback(session, now, current_user=current_user)
    table_ids = [table.table_id for table in tables]
    metabase_dashboard_map = _metabase_dashboard_impact_map(session, table_ids)
    lineage_impact_map = _lineage_impact_map(session, table_ids)
    user_impact_map = _search_user_impact_map(session, table_ids, now)
    for table in tables:
        setattr(table, "metabase_dashboard_count", metabase_dashboard_map.get(table.table_id, 0))
        setattr(table, "impacted_user_count", user_impact_map.get(table.table_id, table.search_clicks_30d))
        setattr(table, "lineage_upstream_count", lineage_impact_map.get(table.table_id, {}).get("upstream", 0))
        setattr(table, "lineage_downstream_count", lineage_impact_map.get(table.table_id, {}).get("downstream", 0))
    ingestion = None
    unmapped_items: dict[str, dict[str, object]] = {}
    overview_items: dict[str, dict[str, object]] = {}
    degraded_items: dict[str, dict[str, object]] = {}
    failed_items: dict[str, dict[str, object]] = {}
    critical_stale_fqns: set[str] = set()
    if include_ingestion:
        table_refs = [
            {
                "table_id": table.table_id,
                "table_name": table.table_name,
                "table_fqn": table.table_fqn,
                "schema_name": table.schema_name,
                "criticality_score": 100 if table.critical_open_incidents > 0 else 80 if table.open_incidents > 0 else 50,
            }
            for table in tables
        ]
        ingestion = load_ingestion_operational_overview_from_source(
            session,
            table_refs=table_refs,
            limit=max(len(table_refs), 8),
            high_volume_threshold_rows=settings_snapshot.operational_high_volume_threshold_rows,
            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
        )
        unmapped_items = {
            str(item.get("table_fqn")): item
            for item in ingestion.get("unmapped_items", [])
            if item.get("table_fqn")
        }
        overview_items = {
            str(item.get("table_fqn")): item
            for item in ingestion.get("items", [])
            if item.get("table_fqn")
        }
        degraded_items = {
            str(item.get("table_fqn")): item
            for item in ingestion.get("degraded_items", [])
            if item.get("table_fqn")
        }
        failed_items = {
            str(item.get("table_fqn")): item
            for item in ingestion.get("failed_items", [])
            if item.get("table_fqn")
        }
        critical_stale_fqns = {
            str(item.get("table_fqn"))
            for item in ingestion.get("critical_stale_items", [])
            if item.get("table_fqn")
        }

    items: list[dict[str, object]] = []
    governance_scores = {
        table.table_id: build_governance_score_for_profile(table, settings_snapshot=settings_snapshot)
        for table in tables
    }
    for table in tables:
        links = _links_for(table)
        governance_score = governance_scores[table.table_id]
        trust_score = int(getattr(table, "trust_score", 0) or 0)
        trust_label = getattr(table, "trust_label", None)
        trust_tone = getattr(table, "trust_tone", None)
        owner_due = owner_review_due(table, now=now, settings_snapshot=settings_snapshot)
        privacy_due = privacy_review_due(table, now=now, settings_snapshot=settings_snapshot)
        cert_due = certification_review_due(table, now=now, settings_snapshot=settings_snapshot)
        effective_status = resolve_certification_status_for_profile(table, now=now)

        if not table.description_complete:
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="no_description",
                    title="Tabela sem descrição",
                    description="A descrição principal do ativo ainda não foi preenchida.",
                    severity="medium",
                    origin="metadata",
                    action_label="Solicitar descrição",
                    action_href=_stewardship_request_href(
                        table_id=table.table_id,
                        request_type="table_description",
                        table_name=table.table_name,
                        schema_name=table.schema_name,
                        database_name=table.database_name,
                        datasource_name=table.datasource_name,
                    ),
                    detected_at=now,
                    base_priority=80,
                )
            )
        if table.tags_count <= 0:
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="no_tags",
                    title="Sem tags estratégicas",
                    description="O ativo ainda não possui tags para descoberta, classificação ou recortes executivos.",
                    severity="low",
                    origin="metadata",
                    action_label="Aplicar tags",
                    action_href=links["explorer"],
                    detected_at=now,
                    base_priority=40,
                )
            )
        if table.terms_count <= 0:
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="no_terms",
                    title="Sem termo de glossário",
                    description="O ativo ainda não está conectado a nenhum termo de glossário.",
                    severity="low",
                    origin="glossary",
                    action_label="Solicitar associação",
                    action_href=_stewardship_request_href(
                        table_id=table.table_id,
                        request_type="glossary_terms",
                        table_name=table.table_name,
                        schema_name=table.schema_name,
                        database_name=table.database_name,
                        datasource_name=table.datasource_name,
                    ),
                    detected_at=now,
                    base_priority=35,
                )
            )
        active_governance_findings = build_active_governance_findings(
            table,
            settings_snapshot=settings_snapshot,
            links=links,
            now=now,
        )
        for finding in active_governance_findings:
            items.append(
                finding.as_pending_item(
                    table,
                    governance_score=governance_score,
                    links=links,
                )
            )
        if table.eligible_for_certification and effective_status != "certified":
            certification_detected_at = table.last_review_at or table.last_updated_at or now
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="no_certification",
                    title="Elegível sem certificação",
                    description="O ativo reúne sinais de prontidão, mas ainda não foi certificado.",
                    severity="medium",
                    origin="certification",
                    action_label="Solicitar revisão",
                    action_href=_stewardship_request_href(
                        table_id=table.table_id,
                        request_type="certification_review",
                        table_name=table.table_name,
                        schema_name=table.schema_name,
                        database_name=table.database_name,
                        datasource_name=table.datasource_name,
                    ),
                    detected_at=certification_detected_at,
                    due_at=certification_detected_at + timedelta(days=settings_snapshot.certification_review_sla_days),
                    sla_days=settings_snapshot.certification_review_sla_days,
                    context_value=certification_status_label(effective_status),
                    base_priority=70,
                )
            )
        if table.dq_score is not None and table.dq_score < 70:
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="low_dq",
                    title="Score de qualidade abaixo do mínimo",
                    description="A qualidade do ativo está abaixo do mínimo recomendado para consumo seguro.",
                    severity="high",
                    origin="quality",
                    action_label="Abrir Data Quality",
                    action_href=links["data_quality"],
                    detected_at=table.last_sync_at or now,
                    context_value=f"{round(table.dq_score, 1)} pts",
                    base_priority=95,
                )
            )
        if table.open_incidents > 0:
            severity_key = "critical" if table.critical_open_incidents > 0 else "high"
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="open_incident",
                    title="Incidente aberto",
                    description="Há incidente operacional ou de qualidade em aberto ligado a este ativo.",
                    severity=severity_key,
                    origin="incidents",
                    action_label="Abrir incidente",
                    action_href=links["incidents"],
                    detected_at=now,
                    context_value=f"{table.open_incidents} incidente(s) aberto(s)",
                    base_priority=100 if severity_key == "critical" else 85,
                )
            )
        if owner_due:
            owner_detected_at = table.owner_reviewed_at or now
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="owner_review_due",
                    title="Revisão de owner vencida",
                    description="A confirmação formal de ownership precisa ser revalidada.",
                    severity="medium",
                    origin="governance",
                    action_label="Abrir revisão",
                    action_href=_stewardship_request_href(
                        table_id=table.table_id,
                        request_type="owner_review",
                        table_name=table.table_name,
                        schema_name=table.schema_name,
                        database_name=table.database_name,
                        datasource_name=table.datasource_name,
                    ),
                    detected_at=owner_detected_at,
                    due_at=owner_detected_at + timedelta(days=settings_snapshot.owner_review_interval_days),
                    sla_days=settings_snapshot.owner_review_interval_days,
                    base_priority=60,
                )
            )
        if privacy_due:
            privacy_detected_at = table.privacy_reviewed_at or now
            privacy_interval = (
                settings_snapshot.sensitive_privacy_review_interval_days
                if table.sensitivity_level or table.has_personal_data or table.has_sensitive_personal_data
                else settings_snapshot.privacy_review_interval_days
            )
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="privacy_review_due",
                    title="Revisão de privacidade vencida",
                    description="A classificação e os controles de privacidade precisam de nova validação.",
                    severity="high",
                    origin="governance",
                    action_label="Abrir revisão",
                    action_href=_stewardship_request_href(
                        table_id=table.table_id,
                        request_type="privacy_review",
                        table_name=table.table_name,
                        schema_name=table.schema_name,
                        database_name=table.database_name,
                        datasource_name=table.datasource_name,
                    ),
                    detected_at=privacy_detected_at,
                    due_at=privacy_detected_at + timedelta(days=privacy_interval),
                    sla_days=privacy_interval,
                    base_priority=88,
                )
            )
        if cert_due:
            cert_detected_at = table.certification_review_at or table.certification_expires_at or table.certification_decided_at or now
            cert_due_at = table.certification_review_at or table.certification_expires_at
            if cert_due_at is None:
                cert_due_at = cert_detected_at + timedelta(days=settings_snapshot.certification_review_interval_days)
            items.append(
                _pending_item(
                    table,
                    governance_score=governance_score,
                    key="certification_review_due",
                    title="Revisão de certificação vencida",
                    description="A certificação do ativo exige revisão ou revalidação para continuar confiável.",
                    severity="high",
                    origin="certification",
                    action_label="Solicitar revalidação",
                    action_href=_stewardship_request_href(
                        table_id=table.table_id,
                        request_type="certification_review",
                        table_name=table.table_name,
                        schema_name=table.schema_name,
                        database_name=table.database_name,
                        datasource_name=table.datasource_name,
                    ),
                    detected_at=cert_detected_at,
                    due_at=cert_due_at,
                    sla_days=settings_snapshot.certification_review_interval_days,
                    base_priority=82,
                )
            )

        if include_ingestion:
            if table.table_fqn in unmapped_items:
                pipeline_detected_at = table.last_updated_at or now
                items.append(
                    _pending_item(
                        table,
                        governance_score=governance_score,
                        key="no_pipeline_mapped",
                        title="Sem pipeline mapeado",
                        description="O ativo está no catálogo, mas ainda não possui pipeline Airflow vinculado na camada operacional.",
                        severity="high",
                        origin="operations",
                        action_label="Abrir operação",
                        action_href="/ops/ingestion",
                        detected_at=pipeline_detected_at,
                        due_at=pipeline_detected_at + timedelta(days=PIPELINE_MAPPING_SLA_DAYS),
                        sla_days=PIPELINE_MAPPING_SLA_DAYS,
                        base_priority=92,
                    )
                )

            if table.table_fqn not in unmapped_items:
                operational_item = overview_items.get(table.table_fqn) or degraded_items.get(table.table_fqn)
                failed_item = failed_items.get(table.table_fqn)
                if operational_item is not None:
                    status_label = str(operational_item.get("latest_status_label") or operational_item.get("status_label") or "")
                    last_success_raw = operational_item.get("last_success_at")
                    last_success_at = None
                    if isinstance(last_success_raw, str) and last_success_raw:
                        try:
                            last_success_at = datetime.fromisoformat(last_success_raw.replace("Z", "+00:00"))
                        except ValueError:
                            last_success_at = None
                    threshold = timedelta(hours=STALE_SUCCESS_THRESHOLD_HOURS)
                    stale_detected = status_label not in {"Falha", "Em execução"} and (
                        last_success_at is None or last_success_at <= now - threshold
                    )
                    if stale_detected:
                        is_critical_stale = table.table_fqn in critical_stale_fqns
                        items.append(
                            _pending_item(
                                table,
                                governance_score=governance_score,
                                key="stale_update",
                                title="Sem atualização recente",
                                description=f"O ativo está sem sucesso operacional recente nas últimas {STALE_SUCCESS_THRESHOLD_HOURS} horas.",
                                severity="high" if is_critical_stale else "medium",
                                origin="operations",
                                action_label="Ver histórico operacional",
                                action_href=str(operational_item.get("pipeline_history_href") or "/ops/ingestion"),
                                detected_at=last_success_at or now,
                                due_at=(last_success_at + timedelta(hours=STALE_SUCCESS_THRESHOLD_HOURS)) if last_success_at else now,
                                sla_days=max(int(STALE_SUCCESS_THRESHOLD_HOURS / 24), 1),
                                context_value=status_label or "Sem sucesso recente",
                                base_priority=84 if is_critical_stale else 68,
                            )
                        )
                    has_governance_correlation = (
                        status_label == "Falha" or stale_detected
                    ) and ((table.dq_score is not None and table.dq_score < 90) or table.open_incidents > 0)
                    if has_governance_correlation:
                        detected_at = last_success_at or now
                        severity = "critical" if (table.open_incidents > 0 and (table.dq_score or 100) < 90) else "high"
                        context_parts = []
                        if table.dq_score is not None:
                            context_parts.append(f"DQ {round(table.dq_score, 1)} pts")
                        if table.open_incidents > 0:
                            context_parts.append(f"{table.open_incidents} incidente(s) aberto(s)")
                        context_parts.append(status_label or "Sem sucesso recente")
                        items.append(
                            _pending_item(
                                table,
                                governance_score=governance_score,
                                key="operational_governance_risk",
                                title="Falha operacional com impacto de governança",
                                description=(
                                    "A operação do ativo está degradada e já há reflexo em qualidade ou incidentes abertos. "
                                    "Esse recorte merece tratativa conjunta entre operação, DQ e governança."
                                ),
                                severity=severity,
                                origin="operations",
                                action_label="Abrir correlação do ativo",
                                action_href=f"/explorer?tableId={table.table_id}",
                                detected_at=detected_at,
                                due_at=detected_at + timedelta(days=1),
                                sla_days=1,
                                context_value=" · ".join(context_parts),
                                base_priority=110 if severity == "critical" else 96,
                            )
                        )
                    if failed_item is not None and table.owner_defined:
                        failure_detected_at = last_success_at or now
                        due_at = failure_detected_at + timedelta(hours=settings_snapshot.pipeline_failure_owner_sla_hours)
                        items.append(
                            _pending_item(
                                table,
                                governance_score=governance_score,
                                key="owner_pipeline_failure_followup",
                                title="Owner precisa coordenar falha de pipeline",
                                description=(
                                    "O pipeline do ativo falhou e a tratativa precisa ficar explícita na fila do responsável do ativo."
                                ),
                                severity="high",
                                origin="operations",
                                action_label="Abrir incidente e acompanhar",
                                action_href=links["incidents"],
                                detected_at=failure_detected_at,
                                due_at=due_at,
                                sla_days=max(int(settings_snapshot.pipeline_failure_owner_sla_hours / 24), 1),
                                context_value=f"Responsável: {table.owner_name or table.owner or 'Sem owner'}",
                                base_priority=98,
                            )
                        )

    all_items = items[:]
    filters = _pending_filters(all_items)
    normalized_q = (q or "").strip().lower()
    normalized_severity = (severity or "").strip().lower()
    normalized_origin = (origin or "").strip().lower()
    normalized_owner = (owner or "").strip().lower()
    normalized_datasource = (datasource or "").strip().lower()
    normalized_schema = (schema_name or "").strip().lower()
    normalized_domain = (domain or "").strip().lower()
    normalized_status = (status_filter or "").strip().lower()

    filtered: list[dict[str, object]] = []
    for item in all_items:
        searchable = " ".join(
            [
                str(item.get("title") or ""),
                str(item.get("table_name") or ""),
                str(item.get("table_fqn") or ""),
                str(item.get("owner_name") or ""),
                str(item.get("datasource_name") or ""),
                str(item.get("schema_name") or ""),
                str(item.get("description") or ""),
            ]
        ).lower()
        if normalized_q and normalized_q not in searchable:
            continue
        if normalized_severity and str(item.get("severity") or "").lower() != normalized_severity:
            continue
        if normalized_origin and str(item.get("origin") or "").lower() != normalized_origin:
            continue
        if owner_id is not None and item.get("data_owner_id") != owner_id:
            continue
        if normalized_owner and str(item.get("owner_name") or "").lower() != normalized_owner:
            continue
        if normalized_datasource and str(item.get("datasource_name") or "").lower() != normalized_datasource:
            continue
        if normalized_schema and str(item.get("schema_name") or "").lower() != normalized_schema:
            continue
        if normalized_domain and str(item.get("domain_name") or "").lower() != normalized_domain:
            continue
        if normalized_status and str(item.get("status") or "").lower() != normalized_status:
            continue
        filtered.append(item)

    filtered.sort(key=lambda item: (-int(item["priority"]), str(item["table_fqn"]).lower(), str(item["title"]).lower()))
    risk_candidates = [
        item
        for item in filtered
        if int(item.get("risk_score") or 0) >= 50
        or str(item.get("sla_status") or "") in {"overdue", "due_soon"}
        or str(item.get("severity") or "") in {"critical", "high"}
        or int(item.get("trust_score") or 0) < 60
    ]
    risk_queue = []
    if include_queue:
        risk_queue = sorted(
            risk_candidates,
            key=lambda item: (
                -int(item.get("priority") or 0),
                -int(item.get("risk_score") or 0),
                int(item.get("trust_score") or 0),
                str(item.get("table_fqn") or "").lower(),
                str(item.get("title") or "").lower(),
            ),
        )[:12]
    export_params = {
        "q": q,
        "severity": severity,
        "origin": origin,
        "owner_id": owner_id,
        "owner": owner,
        "datasource": datasource,
        "schema_name": schema_name,
        "domain": domain,
        "status": status_filter,
    }
    export_query = urlencode({key: value for key, value in export_params.items() if value not in (None, "")})
    stewardship_items = session.scalars(
        select(StewardshipRequest)
        .options(
            selectinload(StewardshipRequest.table).selectinload(TableEntity.data_owner),
            selectinload(StewardshipRequest.approver_user),
        )
    ).all()
    stewardship_summary = build_stewardship_inbox_summary(stewardship_items, current_user=current_user)
    notification_summary = get_governance_notification_summary(session)
    payload = {
        "generated_at": now.isoformat(),
        "total": len(filtered),
        "export_csv_href": f"/api/v1/governance/pending-center/export.csv{f'?{export_query}' if export_query else ''}",
        "export_xlsx_href": f"/api/v1/governance/pending-center/export.xlsx{f'?{export_query}' if export_query else ''}",
        "summary_cards": _pending_summary_cards(
            filtered,
            stewardship_summary=stewardship_summary,
            notification_summary=notification_summary,
        ),
        "summary": _pending_breakdown(
            filtered,
            field="severity",
            labels=PENDING_SEVERITY_LABELS,
            order=["critical", "high", "medium", "low"],
        ),
        "origins": _pending_breakdown(
            filtered,
            field="origin",
            labels=PENDING_ORIGIN_LABELS,
            order=["governance", "metadata", "glossary", "certification", "quality", "operations", "incidents"],
        ),
        "campaigns": _pending_campaigns(filtered) if include_campaigns else [],
        "stewardship": stewardship_summary,
        "notifications": notification_summary,
        "filters": filters,
    }
    if include_queue:
        payload["risk_queue"] = risk_queue
        payload["all_items"] = filtered
    with _PENDING_CENTER_CACHE_LOCK:
        _PENDING_CENTER_CACHE[cache_key] = (
            now + timedelta(seconds=_PENDING_CENTER_CACHE_TTL_SECONDS),
            payload,
        )
    return payload


def get_governance_pending_center_summary(
    session: Session,
    *,
    q: str | None = None,
    severity: str | None = None,
    origin: str | None = None,
    owner_id: int | None = None,
    owner: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    status_filter: str | None = None,
    current_user=None,
) -> dict[str, object]:
    payload = _build_pending_center_dataset(
        session,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        include_ingestion=True,
        include_campaigns=True,
        include_queue=False,
        current_user=current_user,
    )
    return {
        "generated_at": payload["generated_at"],
        "total": payload["total"],
        "export_csv_href": payload["export_csv_href"],
        "export_xlsx_href": payload["export_xlsx_href"],
        "summary_cards": payload["summary_cards"],
        "summary": payload["summary"],
        "origins": payload["origins"],
        "campaigns": payload["campaigns"],
        "stewardship": payload["stewardship"],
        "notifications": payload["notifications"],
        "filters": payload["filters"],
    }


def get_governance_pending_center_summary_light(
    session: Session,
    *,
    q: str | None = None,
    severity: str | None = None,
    origin: str | None = None,
    owner_id: int | None = None,
    owner: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    status_filter: str | None = None,
    current_user=None,
) -> dict[str, object]:
    payload = _build_pending_center_dataset(
        session,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        include_ingestion=False,
        include_campaigns=False,
        include_queue=False,
        current_user=current_user,
    )
    return {
        "generated_at": payload["generated_at"],
        "total": payload["total"],
        "export_csv_href": payload["export_csv_href"],
        "export_xlsx_href": payload["export_xlsx_href"],
        "summary_cards": payload["summary_cards"],
        "summary": payload["summary"],
        "origins": payload["origins"],
        "campaigns": [],
        "stewardship": payload["stewardship"],
        "notifications": payload["notifications"],
        "filters": payload["filters"],
    }


def get_governance_pending_center_campaigns(
    session: Session,
    *,
    q: str | None = None,
    severity: str | None = None,
    origin: str | None = None,
    owner_id: int | None = None,
    owner: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    status_filter: str | None = None,
    current_user=None,
) -> dict[str, object]:
    payload = _build_pending_center_dataset(
        session,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        include_ingestion=False,
        include_campaigns=True,
        include_queue=False,
        current_user=current_user,
    )
    return {
        "generated_at": payload["generated_at"],
        "total": payload["total"],
        "campaigns": payload["campaigns"],
    }


def get_governance_pending_center_queue(
    session: Session,
    *,
    q: str | None = None,
    severity: str | None = None,
    origin: str | None = None,
    owner_id: int | None = None,
    owner: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    status_filter: str | None = None,
    page: int = 1,
    page_size: int = 50,
    current_user=None,
) -> dict[str, object]:
    payload = _build_pending_center_dataset(
        session,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        include_ingestion=True,
        include_campaigns=False,
        include_queue=True,
        current_user=current_user,
    )
    start = max(page - 1, 0) * page_size
    end = start + page_size
    items = list(payload["all_items"])[start:end]
    return {
        "generated_at": payload["generated_at"],
        "total": payload["total"],
        "page": page,
        "page_size": page_size,
        "export_csv_href": payload["export_csv_href"],
        "export_xlsx_href": payload["export_xlsx_href"],
        "risk_queue": payload["risk_queue"],
        "items": items,
    }


def get_governance_pending_center(
    session: Session,
    *,
    q: str | None = None,
    severity: str | None = None,
    origin: str | None = None,
    owner_id: int | None = None,
    owner: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    status_filter: str | None = None,
    page: int = 1,
    page_size: int = 50,
    current_user=None,
) -> dict[str, object]:
    summary_payload = get_governance_pending_center_summary(
        session,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        current_user=current_user,
    )
    queue_payload = get_governance_pending_center_queue(
        session,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        page=page,
        page_size=page_size,
        current_user=current_user,
    )
    return {
        **summary_payload,
        "page": queue_payload["page"],
        "page_size": queue_payload["page_size"],
        "risk_queue": queue_payload["risk_queue"],
        "items": queue_payload["items"],
    }


def get_governance_critical_changes(session: Session, *, limit: int = 10, current_user=None) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    table_id_expr = func.coalesce(
        cast(
            func.nullif(
                case(
                    (AuditLog.entity_type == "table", AuditLog.entity_id),
                    else_=None,
                ),
                "",
            ),
            Integer,
        ),
        cast(
            func.nullif(
                case(
                    (AuditLog.parent_entity_type == "table", AuditLog.parent_entity_id),
                    else_=None,
                ),
                "",
            ),
            Integer,
        ),
    )
    stmt = (
        select(
            AuditLog,
            table_id_expr.label("table_id"),
            TableEntity.name.label("table_name"),
            Schema.name.label("schema_name"),
            Database.name.label("database_name"),
            DataSource.name.label("datasource_name"),
        )
        .outerjoin(TableEntity, TableEntity.id == table_id_expr)
        .outerjoin(Schema, Schema.id == TableEntity.schema_id)
        .outerjoin(Database, Database.id == Schema.database_id)
        .outerjoin(DataSource, DataSource.id == Database.datasource_id)
        .where(
            AuditLog.is_sensitive_change.is_(True),
            AuditLog.created_at >= now.replace(microsecond=0) - timedelta(days=CRITICAL_CHANGE_LOOKBACK_DAYS),
        )
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
        .limit(limit)
    )
    rows = session.execute(stmt).all()
    if current_user is not None:
        filtered_rows = []
        for row in rows:
            if row.table_id is None:
                filtered_rows.append(row)
                continue
            table = session.get(TableEntity, int(row.table_id))
            if table is None or can_view_table(current_user, table):
                filtered_rows.append(row)
        rows = filtered_rows
    return {
        "generated_at": now.isoformat(),
        "total": len(rows),
        "items": [
            {
                "id": row.AuditLog.id,
                "changed_at": row.AuditLog.created_at,
                "actor_name": row.AuditLog.actor_name,
                "actor_email": row.AuditLog.user_email,
                "field_name": row.AuditLog.field_name,
                "change_type": row.AuditLog.change_type,
                "sensitive_category": row.AuditLog.sensitive_category,
                "table_id": int(row.table_id) if row.table_id is not None else None,
                "table_name": row.table_name,
                "schema_name": row.schema_name,
                "database_name": row.database_name,
                "datasource_name": row.datasource_name,
                "before_value": _display_value(row.AuditLog.before_json),
                "after_value": _display_value(row.AuditLog.after_json),
                "href": f"/audit?entity_type=table&entity_id={int(row.table_id)}&sensitive_only=true"
                if row.table_id is not None
                else "/audit?sensitive_only=true",
            }
            for row in rows
        ],
    }


def mark_owner_review(session: Session, *, table_id: int, user) -> dict[str, object]:
    table = session.get(TableEntity, table_id)
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    reviewed_at = datetime.now(timezone.utc)
    table.owner_reviewed_by_user_id = user.id
    table.owner_reviewed_at = reviewed_at
    session.flush()
    return {
        "table_id": table.id,
        "review_type": "owner",
        "reviewed_at": reviewed_at.isoformat(),
        "reviewed_by_user_id": user.id,
        "reviewed_by_name": getattr(user, "name", None) or getattr(user, "full_name", None),
    }


def mark_privacy_review(session: Session, *, table_id: int, user) -> dict[str, object]:
    table = session.get(TableEntity, table_id)
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    reviewed_at = datetime.now(timezone.utc)
    table.privacy_reviewed_by_user_id = user.id
    table.privacy_reviewed_at = reviewed_at
    session.flush()
    return {
        "table_id": table.id,
        "review_type": "privacy",
        "reviewed_at": reviewed_at.isoformat(),
        "reviewed_by_user_id": user.id,
        "reviewed_by_name": getattr(user, "name", None) or getattr(user, "full_name", None),
    }
