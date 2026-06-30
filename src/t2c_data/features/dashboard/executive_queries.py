from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from threading import Lock

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.operational_context import build_asset_links, build_contextual_actions
from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.executive_scoring import (
    ExecutiveScoreFactor,
    compute_priority_score,
    compute_profile_priority_score,
    recommended_actions,
    risk_label,
    risk_tone,
)
from t2c_data.features.dashboard.operational_intelligence import build_operational_intelligence
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.dashboard.support import TableProfile, normalize_dt, round_pct
from t2c_data.features.governance import get_governance_critical_changes
from t2c_data.features.governance.rules import certification_review_due, owner_review_due, privacy_review_due
from t2c_data.features.governance.score_history import summarize_governance_score_trend
from t2c_data.features.governance.scoring import build_governance_score_for_profile, governance_score_label
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.ingestion import load_ingestion_operational_overview_from_source
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.features.platform.visibility import (
    is_table_visible,
    mask_dashboard_asset_payload,
    visibility_for_profiles,
)
from t2c_data.features.privacy_access.policy import sensitivity_label
from t2c_data.features.stewardship.workflow import build_stewardship_inbox_summary
from t2c_data.models.catalog import TableEntity
from t2c_data.models.dq import DQRun, DQTableMetric
from t2c_data.models.incident import Incident
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.metabase_impact import MetabaseImpactSnapshot
from t2c_data.models.search import SearchResultClick
from t2c_data.models.stewardship import StewardshipRequest

_EXECUTIVE_SUMMARY_CACHE_LOCK = Lock()
_EXECUTIVE_SUMMARY_CACHE_TTL_SECONDS = 30
_EXECUTIVE_SUMMARY_CACHE: dict[tuple[object, ...], tuple[datetime, dict[str, object]]] = {}

CERTIFICATION_STATUS_LABELS = {
    "not_eligible": "Não elegível",
    "eligible": "Elegível",
    "not_assessed": "Não elegível",
    "in_review": "Em revisão",
    "certified": "Certificado",
    "rejected": "Recusado",
    "expired": "Vencido",
    "revalidation_pending": "Pendente de revalidação",
}

DQ_BANDS = {
    "unknown": "Não avaliado",
    "critical": "Abaixo de 70",
    "warning": "70 a 89",
    "healthy": "90 ou mais",
}

INCIDENT_OPTIONS = {
    "all": "Todos",
    "with_open": "Com incidentes",
    "without_open": "Sem incidentes",
    "critical_only": "Somente críticos",
}

GOVERNANCE_MATURITY_ORDER = [
    ("strong", "Forte", "success"),
    ("good", "Boa", "accent"),
    ("evolving", "Em evolução", "warning"),
    ("critical", "Crítica", "danger"),
]


class DashboardExecutiveFilters(dict):
    domain: str | None
    data_source_id: int | None
    source: str | None
    database: str | None
    schema_key: str | None
    schema: str | None
    owner: str | None
    certification_status: str | None
    dq_band: str | None
    incidents: str | None
    q: str | None


def _normalized(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


def _normalized_int(value: int | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def normalize_filters(
    domain: str | None = None,
    data_source_id: int | str | None = None,
    source: str | None = None,
    database: str | None = None,
    schema_key: str | None = None,
    schema: str | None = None,
    owner: str | None = None,
    certification_status: str | None = None,
    dq_band: str | None = None,
    incidents: str | None = None,
    q: str | None = None,
) -> DashboardExecutiveFilters:
    return DashboardExecutiveFilters(
        domain=_normalized(domain),
        data_source_id=_normalized_int(data_source_id),
        source=_normalized(source),
        database=_normalized(database),
        schema_key=_normalized(schema_key),
        schema=_normalized(schema),
        owner=_normalized(owner),
        certification_status=_normalized(certification_status),
        dq_band=_normalized(dq_band),
        incidents=_normalized(incidents),
        q=_normalized(q),
    )


def _executive_cache_key(filters: DashboardExecutiveFilters, *, include_secondary: bool, current_user=None) -> tuple[object, ...]:
    return (
        getattr(current_user, "id", None),
        bool(include_secondary),
        filters.get("domain"),
        filters.get("data_source_id"),
        filters.get("source"),
        filters.get("database"),
        filters.get("schema_key"),
        filters.get("schema"),
        filters.get("owner"),
        filters.get("certification_status"),
        filters.get("dq_band"),
        filters.get("incidents"),
        filters.get("q"),
    )


def _dq_band_for(table: TableProfile) -> str:
    if table.dq_score is None:
        return "unknown"
    if table.dq_score < 70:
        return "critical"
    if table.dq_score < 90:
        return "warning"
    return "healthy"


def _certification_label(status: str) -> str:
    return CERTIFICATION_STATUS_LABELS.get(status, status)


def _dictionary_status_label(table: TableProfile) -> str:
    return "Completo" if table.dictionary_complete else "Pendente"


def _owner_status_label(table: TableProfile) -> str:
    return table.owner_name or "Não definido"


def _dq_status_label(table: TableProfile) -> str:
    if table.dq_score is None:
        return "Não avaliado"
    return f"{round(table.dq_score, 1)}"


def _domain_label(table: TableProfile) -> str:
    return table.domain_name or "Sem dados suficientes"


def _source_filter_label(table: TableProfile) -> str:
    return table.datasource_name or "Sem fonte"


def _source_filter_key(table: TableProfile) -> str:
    return str(table.datasource_id)


def _schema_filter_label(table: TableProfile) -> str:
    return f"{table.datasource_name} / {table.database_name}.{table.schema_name}"


def _schema_filter_key(table: TableProfile) -> str:
    database_id = table.database_id if table.database_id is not None else 0
    return f"{table.datasource_id}:{database_id}:{table.schema_id}"


def _governance_maturity_key(score: int) -> str:
    if score >= 85:
        return "strong"
    if score >= 70:
        return "good"
    if score >= 50:
        return "evolving"
    return "critical"


def _governance_maturity_tone(score: float) -> str:
    key = _governance_maturity_key(int(round(score)))
    if key == "strong":
        return "success"
    if key == "good":
        return "accent"
    if key == "evolving":
        return "warning"
    return "danger"


def _filter_matches(table: TableProfile, filters: DashboardExecutiveFilters) -> bool:
    if filters.get("domain") and _domain_label(table) != filters["domain"]:
        return False
    if filters.get("data_source_id") is not None and table.datasource_id != filters["data_source_id"]:
        return False
    if filters.get("source") and table.datasource_name != filters["source"]:
        return False
    if filters.get("database") and table.database_name != filters["database"]:
        return False
    if filters.get("schema_key") and _schema_filter_key(table) != filters["schema_key"]:
        return False
    if filters.get("schema") and table.schema_name != filters["schema"]:
        return False
    if filters.get("owner") and _owner_status_label(table) != filters["owner"]:
        return False
    if filters.get("certification_status") and resolve_certification_status_for_profile(table) != filters["certification_status"]:
        return False
    if filters.get("dq_band") and _dq_band_for(table) != filters["dq_band"]:
        return False
    if filters.get("incidents") == "with_open" and table.open_incidents <= 0:
        return False
    if filters.get("incidents") == "without_open" and table.open_incidents > 0:
        return False
    if filters.get("incidents") == "critical_only" and table.critical_open_incidents <= 0:
        return False
    if filters.get("q"):
        needle = filters["q"].lower()
        haystacks = [
            table.table_name,
            table.table_fqn,
            table.datasource_name,
            table.database_name,
            table.schema_name,
            table.owner_name or "",
        ]
        if not any(needle in value.lower() for value in haystacks):
            return False
    return True


def filter_profiles(tables: list[TableProfile], filters: DashboardExecutiveFilters) -> list[TableProfile]:
    return [table for table in tables if _filter_matches(table, filters)]


def _recent_incident_maps(session: Session, now: datetime) -> tuple[dict[str, int], dict[str, int]]:
    rows = session.execute(
        select(
            Incident.table_fqn,
            func.count(Incident.id).label("recent_count"),
            func.sum(Incident.occurrences).label("recent_occurrences"),
        )
        .where(
            Incident.entity_type == "table",
            Incident.detected_at >= now - timedelta(days=30),
        )
        .group_by(Incident.table_fqn)
    ).all()
    count_map = {str(row.table_fqn): int(row.recent_count or 0) for row in rows if row.table_fqn}
    occurrence_map = {str(row.table_fqn): int(row.recent_occurrences or 0) for row in rows if row.table_fqn}
    return count_map, occurrence_map


def _metabase_dashboard_impact_map(session: Session, table_ids: list[int]) -> dict[int, int]:
    if not table_ids:
        return {}
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


def _score_table_with_impact(
    table: TableProfile,
    *,
    recent_incident_count: int,
    recent_occurrences: int,
    metabase_dashboards: int = 0,
    impacted_users: int | None = None,
    lineage_upstream: int = 0,
    lineage_downstream: int = 0,
) -> tuple[int, list[ExecutiveScoreFactor]]:
    risk_score, factors = compute_priority_score(
        table,
        recent_incident_count=recent_incident_count,
        recent_occurrences=recent_occurrences,
    )
    priority_score = compute_profile_priority_score(
        table,
        risk_score,
        dashboards=metabase_dashboards,
        users=impacted_users,
        upstream=lineage_upstream,
        downstream=lineage_downstream,
    )
    return priority_score, factors


def _trend_for_tables(session: Session, table_ids: list[int]) -> list[dict[str, float | str]]:
    if not table_ids:
        return []
    rows = session.execute(
        select(
            func.date(DQRun.created_at).label("day"),
            func.avg(DQTableMetric.dq_score).label("avg_score"),
        )
        .join(DQTableMetric, DQTableMetric.run_id == DQRun.id)
        .where(DQRun.status == "success", DQTableMetric.table_id.in_(table_ids))
        .group_by(func.date(DQRun.created_at))
        .order_by(desc(func.date(DQRun.created_at)))
        .limit(8)
    ).all()
    return [
        {
            "label": str(row.day)[5:] if row.day else "-",
            "value": round(float(row.avg_score or 0.0), 1),
        }
        for row in reversed(rows)
    ]


def _serialize_score_factors(factors: list[ExecutiveScoreFactor]) -> list[dict[str, object]]:
    return [
        {
            "key": factor.key,
            "label": factor.label,
            "points": factor.points,
            "applied": factor.applied,
            "detail": factor.detail,
        }
        for factor in factors
    ]


def _serialize_asset(
    table: TableProfile,
    score: int,
    factors: list[ExecutiveScoreFactor],
    recent_incident_count: int = 0,
    settings_snapshot=None,
) -> dict[str, object]:
    certification_status = resolve_certification_status_for_profile(table)
    links = build_asset_links(
        table_id=table.table_id,
        datasource_id=table.datasource_id,
        database_id=table.database_id,
        schema_id=table.schema_id,
        data_owner_id=table.data_owner_id,
    )
    return {
        "table_id": table.table_id,
        "table_name": table.table_name,
        "table_fqn": table.table_fqn,
        "domain_name": _domain_label(table),
        "datasource_name": table.datasource_name,
        "database_name": table.database_name,
        "schema_name": table.schema_name,
        "owner_name": _owner_status_label(table),
        "owner_defined": table.owner_defined,
        "data_owner_is_active": table.data_owner_is_active,
        "governance_score": build_governance_score_for_profile(table, settings_snapshot=settings_snapshot),
        "criticality_score": score,
        "criticality_label": risk_label(score),
        "criticality_tone": risk_tone(score),
        "dq_score": round(table.dq_score, 1) if table.dq_score is not None else None,
        "dq_status_label": _dq_status_label(table),
        "certification_status": certification_status,
        "certification_status_label": _certification_label(certification_status),
        "dictionary_complete": table.dictionary_complete,
        "dictionary_status_label": _dictionary_status_label(table),
        "tags_count": table.tags_count,
        "terms_count": table.terms_count,
        "open_incidents": table.open_incidents,
        "critical_open_incidents": table.critical_open_incidents,
        "last_review_at": table.last_review_at.isoformat() if table.last_review_at else None,
        "last_updated_at": table.last_updated_at.isoformat() if table.last_updated_at else None,
        "last_sync_at": table.last_sync_at.isoformat() if table.last_sync_at else None,
        "sensitivity_level": table.sensitivity_level,
        "sensitivity_label": sensitivity_label(table.sensitivity_level),
        "eligible_for_certification": table.eligible_for_certification,
        "owner_review_due": owner_review_due(table, settings_snapshot=settings_snapshot),
        "privacy_review_due": privacy_review_due(table, settings_snapshot=settings_snapshot),
        "certification_review_due": certification_review_due(table, settings_snapshot=settings_snapshot),
        "score_factors": _serialize_score_factors(factors),
        "recommended_actions": recommended_actions(table, recent_incident_count=recent_incident_count),
        "actions": build_contextual_actions(table, links, settings_snapshot=settings_snapshot),
        "links": links,
    }


def _serialize_asset_with_visibility(
    table: TableProfile,
    score: int,
    factors: list[ExecutiveScoreFactor],
    *,
    recent_incident_count: int = 0,
    settings_snapshot=None,
    masked: bool = False,
) -> dict[str, object]:
    payload = _serialize_asset(
        table,
        score,
        factors,
        recent_incident_count=recent_incident_count,
        settings_snapshot=settings_snapshot,
    )
    return mask_dashboard_asset_payload(payload) if masked else payload


def _sort_top_assets(items: list[tuple[TableProfile, int, list[ExecutiveScoreFactor], int]]) -> list[tuple[TableProfile, int, list[ExecutiveScoreFactor], int]]:
    return sorted(
        items,
        key=lambda item: (
            -item[1],
            -item[0].critical_open_incidents,
            -item[0].open_incidents,
            item[0].dq_score if item[0].dq_score is not None else 101,
            item[0].table_fqn,
        ),
    )


def _filter_options(
    tables: list[TableProfile],
    *,
    schema_tables: list[TableProfile] | None = None,
) -> dict[str, list[dict[str, object]]]:
    def build(values: list[dict[str, object]], *, sort_key: str = "label") -> list[dict[str, object]]:
        deduped: dict[str, dict[str, object]] = {}
        for item in values:
            deduped[str(item["value"])] = item
        return sorted(deduped.values(), key=lambda item: str(item.get(sort_key, "")).lower())

    domains = build(
        [{"value": value, "label": value} for value in sorted(dict.fromkeys(_domain_label(table) for table in tables))]
    )
    sources = build(
        [
            {
                "value": _source_filter_key(table),
                "label": _source_filter_label(table),
                "datasource_id": table.datasource_id,
            }
            for table in tables
        ]
    )
    databases = build(
        [
            {
                "value": table.database_name,
                "label": table.database_name,
                "datasource_id": table.datasource_id,
                "database_id": table.database_id,
            }
            for table in tables
        ]
    )
    schema_source = tables if schema_tables is None else schema_tables
    schemas = build(
        [
            {
                "value": _schema_filter_key(table),
                "label": _schema_filter_label(table),
                "datasource_id": table.datasource_id,
                "database_id": table.database_id,
                "schema_id": table.schema_id,
            }
            for table in schema_source
        ]
    )
    owners = build([{"value": value, "label": value} for value in sorted(dict.fromkeys(_owner_status_label(table) for table in tables))])
    certification_statuses = build(
        [{"value": key, "label": label} for key, label in CERTIFICATION_STATUS_LABELS.items()]
    )
    dq_bands = build([{"value": key, "label": label} for key, label in DQ_BANDS.items()])
    incident_options = build([{"value": key, "label": label} for key, label in INCIDENT_OPTIONS.items()])
    return {
        "domains": domains,
        "sources": sources,
        "databases": databases,
        "schemas": schemas,
        "owners": owners,
        "certification_statuses": certification_statuses,
        "dq_bands": dq_bands,
        "incident_options": incident_options,
    }


def _governance_items(tables: list[TableProfile]) -> list[dict[str, object]]:
    total = len(tables)
    items = [
        ("no_owner", "Sem owner", sum(1 for table in tables if not table.owner_defined), "Sem responsável definido para condução do ativo."),
        ("no_dictionary", "Sem dicionário completo", sum(1 for table in tables if not table.dictionary_complete), "Ainda existem colunas sem descrição de negócio."),
        ("no_tags", "Sem tags relevantes", sum(1 for table in tables if table.tags_count <= 0), "Ativos ainda sem classificação mínima."),
        ("no_recent_review", "Sem revisão recente", sum(1 for table in tables if not table.review_recent), "Ativos sem revisão de governança nos últimos 90 dias."),
    ]
    return [
        {
            "key": key,
            "label": label,
            "count": count,
            "pct": round_pct(count, total),
            "hint": hint,
        }
        for key, label, count, hint in items
    ]


def _governance_review_items(tables: list[TableProfile], settings_snapshot) -> list[dict[str, object]]:
    total = len(tables)
    items = [
        ("owner_review_due", "Owner sem revisão recente", sum(1 for table in tables if owner_review_due(table, settings_snapshot=settings_snapshot)), "Owners que precisam de confirmação formal."),
        (
            "privacy_review_due",
            "Privacidade sem revisão recente",
            sum(1 for table in tables if privacy_review_due(table, settings_snapshot=settings_snapshot)),
            "Classificação e privacidade precisam ser reconfirmadas.",
        ),
        (
            "certification_review_due",
            "Certificação vencida ou pendente",
            sum(1 for table in tables if certification_review_due(table, settings_snapshot=settings_snapshot)),
            "Ativos certificados que precisam de revalidação formal.",
        ),
    ]
    return [
        {
            "key": key,
            "label": label,
            "count": count,
            "pct": round_pct(count, total),
            "hint": hint,
        }
        for key, label, count, hint in items
    ]


def _campaign_items(tables: list[TableProfile], settings_snapshot) -> list[dict[str, object]]:
    total = len(tables)
    items = [
        {
            "key": "no_owner",
            "label": "Ativos sem owner",
            "count": sum(1 for table in tables if not table.owner_defined),
            "responsible": "Stewardship e Data Owners",
            "hint": "Definir responsável é a primeira ação para fechar accountability.",
            "href": "/dashboard/campaigns/no_owner",
            "export_csv_href": "/api/v1/dashboard/executive/campaigns/no_owner/export.csv",
            "export_xlsx_href": "/api/v1/dashboard/executive/campaigns/no_owner/export.xlsx",
            "tone": "warning",
        },
        {
            "key": "no_dictionary",
            "label": "Ativos sem dicionário",
            "count": sum(1 for table in tables if not table.dictionary_complete),
            "responsible": "Stewardship e times de engenharia",
            "hint": "Completar o dicionário reduz ambiguidade e acelera adoção.",
            "href": "/dashboard/campaigns/no_dictionary",
            "export_csv_href": "/api/v1/dashboard/executive/campaigns/no_dictionary/export.csv",
            "export_xlsx_href": "/api/v1/dashboard/executive/campaigns/no_dictionary/export.xlsx",
            "tone": "warning",
        },
        {
            "key": "no_classification",
            "label": "Ativos sem classificação",
            "count": sum(1 for table in tables if not table.sensitivity_level),
            "responsible": "Governança e Privacidade",
            "hint": "A sensibilidade ainda não foi confirmada para parte do recorte.",
            "href": "/dashboard/campaigns/no_classification",
            "export_csv_href": "/api/v1/dashboard/executive/campaigns/no_classification/export.csv",
            "export_xlsx_href": "/api/v1/dashboard/executive/campaigns/no_classification/export.xlsx",
            "tone": "warning",
        },
        {
            "key": "no_certification",
            "label": "Elegíveis sem certificação",
            "count": sum(
                1
                for table in tables
                if table.eligible_for_certification
                and resolve_certification_status_for_profile(table) != "certified"
            ),
            "responsible": "Governança e dono do ativo",
            "hint": "Leve ativos prontos para a etapa de decisão ou revalidação.",
            "href": "/dashboard/campaigns/no_certification",
            "export_csv_href": "/api/v1/dashboard/executive/campaigns/no_certification/export.csv",
            "export_xlsx_href": "/api/v1/dashboard/executive/campaigns/no_certification/export.xlsx",
            "tone": "accent",
        },
        {
            "key": "stale_reviews",
            "label": "Revisões vencidas",
            "count": sum(
                1
                for table in tables
                if owner_review_due(table, settings_snapshot=settings_snapshot)
                or privacy_review_due(table, settings_snapshot=settings_snapshot)
                or certification_review_due(table, settings_snapshot=settings_snapshot)
            ),
            "responsible": "Governança contínua",
            "hint": "Há owner, privacidade ou certificação sem confirmação recente.",
            "href": "/dashboard/campaigns/stale_reviews",
            "export_csv_href": "/api/v1/dashboard/executive/campaigns/stale_reviews/export.csv",
            "export_xlsx_href": "/api/v1/dashboard/executive/campaigns/stale_reviews/export.xlsx",
            "tone": "warning",
        },
        {
            "key": "no_terms",
            "label": "Sem termos associados",
            "count": sum(1 for table in tables if table.terms_count <= 0),
            "responsible": "Stewardship semântico",
            "hint": "Conectar glossário melhora o contexto semântico do ativo.",
            "href": "/dashboard/campaigns/no_terms",
            "export_csv_href": "/api/v1/dashboard/executive/campaigns/no_terms/export.csv",
            "export_xlsx_href": "/api/v1/dashboard/executive/campaigns/no_terms/export.xlsx",
            "tone": "neutral",
        },
    ]
    for item in items:
        item["completed_count"] = max(total - int(item["count"]), 0)
        item["progress_pct"] = round_pct(int(item["completed_count"]), total)
    return sorted(items, key=lambda item: (-item["count"], item["label"]))


def _campaign_matches(table: TableProfile, campaign_key: str, settings_snapshot) -> bool:
    if campaign_key == "no_owner":
        return not table.owner_defined
    if campaign_key == "no_dictionary":
        return not table.dictionary_complete
    if campaign_key == "no_classification":
        return not table.sensitivity_level
    if campaign_key == "no_certification":
        return table.eligible_for_certification and resolve_certification_status_for_profile(table) != "certified"
    if campaign_key == "stale_reviews":
        return (
            owner_review_due(table, settings_snapshot=settings_snapshot)
            or privacy_review_due(table, settings_snapshot=settings_snapshot)
            or certification_review_due(table, settings_snapshot=settings_snapshot)
        )
    if campaign_key == "no_terms":
        return table.terms_count <= 0
    raise ValueError(f"Unsupported campaign key: {campaign_key}")


def get_dashboard_executive_campaign_queue(
    session: Session,
    campaign_key: str,
    filters: DashboardExecutiveFilters | None = None,
    *,
    page: int = 1,
    page_size: int = 50,
    current_user=None,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    payload = get_dashboard_executive_summary(session, filters, current_user=current_user)
    all_tables, _source = load_dashboard_profiles_with_fallback(session, now, current_user=current_user)
    visibility_states = visibility_for_profiles(session, all_tables, user=current_user)
    tables = filter_profiles(
        [table for table in all_tables if visibility_states.get(table.table_id, None) is None or visibility_states[table.table_id].visible],
        filters or normalize_filters(),
    )
    items = [table for table in tables if _campaign_matches(table, campaign_key, settings_snapshot)]
    governance_scores = {
        table.table_id: build_governance_score_for_profile(table, settings_snapshot=settings_snapshot)
        for table in items
    }
    items.sort(
        key=lambda table: (
            int(governance_scores[table.table_id]["score"]),
            table.owner_defined,
            table.dictionary_complete,
            table.description_complete,
            table.table_fqn,
        )
    )
    campaign = next((item for item in payload["campaigns"] if item["key"] == campaign_key), None)
    if campaign is None:
        raise ValueError(f"Unsupported campaign key: {campaign_key}")
    start = max(page - 1, 0) * page_size
    end = start + page_size
    return {
        "generated_at": payload["generated_at"],
        "campaign": campaign,
        "total": len(items),
        "page": page,
        "page_size": page_size,
        "items": [
            {
                "table_id": table.table_id,
                "table_name": table.table_name,
                "table_fqn": table.table_fqn,
                "datasource_name": table.datasource_name,
                "database_name": table.database_name,
                "schema_name": table.schema_name,
                "owner_name": _owner_status_label(table),
                "governance_score": governance_scores[table.table_id],
                "certification_status": resolve_certification_status_for_profile(table, now=now),
                "certification_status_label": _certification_label(resolve_certification_status_for_profile(table, now=now)),
                "sensitivity_label": sensitivity_label(table.sensitivity_level),
                "last_review_at": table.last_review_at.isoformat() if table.last_review_at else None,
                "masked_sensitive_fields": bool(visibility_states.get(table.table_id) and visibility_states[table.table_id].masked),
                "links": build_asset_links(
                    table_id=table.table_id,
                    datasource_id=table.datasource_id,
                    database_id=table.database_id,
                    schema_id=table.schema_id,
                    data_owner_id=table.data_owner_id,
                ),
            }
            for table in items[start:end]
        ],
    }


def _risk_ranking(
    label_counts: dict[str, list[tuple[int, TableProfile]]],
    *,
    labels: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    ranking = []
    for label, items in label_counts.items():
        scores = [score for score, _table in items]
        assets = [table for _score, table in items]
        ranking.append(
            {
                "label": labels.get(label, label) if labels else label,
                "asset_count": len(assets),
                "avg_score": round(sum(scores) / max(1, len(scores)), 1),
                "max_score": max(scores) if scores else 0,
                "critical_assets": sum(1 for score in scores if score >= 75),
                "open_incidents": sum(table.open_incidents for table in assets),
            }
        )
    return sorted(ranking, key=lambda item: (-item["avg_score"], -item["critical_assets"], item["label"]))[:8]


def _maturity_panels(
    groups: dict[str, list[TableProfile]],
    *,
    settings_snapshot,
    linked_table_ids: set[int],
    labels: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for raw_label, tables in groups.items():
        label = (labels.get(raw_label, raw_label) if labels else raw_label).strip() or "Sem dados suficientes"
        if not tables:
            continue
        governance_scores = [build_governance_score_for_profile(table, settings_snapshot=settings_snapshot)["score"] for table in tables]
        dq_values = [table.dq_score for table in tables if table.dq_score is not None]
        governance_avg = round(sum(governance_scores) / len(governance_scores), 1) if governance_scores else 0.0
        dq_avg = round(sum(dq_values) / len(dq_values), 1) if dq_values else 0.0
        items.append(
            {
                "key": label.lower().replace(" ", "-"),
                "label": label,
                "asset_count": len(tables),
                "owner_pct": round_pct(sum(1 for table in tables if table.owner_defined), len(tables)),
                "description_pct": round_pct(sum(1 for table in tables if table.description_complete), len(tables)),
                "tags_pct": round_pct(sum(1 for table in tables if table.tags_count > 0), len(tables)),
                "glossary_pct": round_pct(sum(1 for table in tables if table.terms_count > 0), len(tables)),
                "pipeline_mapped_pct": round_pct(sum(1 for table in tables if table.table_id in linked_table_ids), len(tables)),
                "dq_avg_score": dq_avg,
                "governance_avg_score": governance_avg,
                "open_incidents": sum(table.open_incidents for table in tables),
                "critical_open_incidents": sum(table.critical_open_incidents for table in tables),
                "governance_label": governance_score_label(int(round(governance_avg or 0)))[0],
                "governance_tone": _governance_maturity_tone(governance_avg),
            }
        )
    return sorted(
        items,
        key=lambda item: (
            item["governance_avg_score"],
            -item["critical_open_incidents"],
            -item["open_incidents"],
            -item["asset_count"],
            item["label"],
        ),
    )[:6]


def get_dashboard_executive_summary(
    session: Session,
    filters: DashboardExecutiveFilters | None = None,
    *,
    include_secondary: bool = True,
    current_user=None,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    normalized_filters = filters or normalize_filters()
    cache_key = _executive_cache_key(normalized_filters, include_secondary=include_secondary, current_user=current_user)
    with _EXECUTIVE_SUMMARY_CACHE_LOCK:
        cached = _EXECUTIVE_SUMMARY_CACHE.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

    settings_snapshot = get_governance_settings_snapshot(session)
    all_tables, read_model_source = load_dashboard_profiles_with_fallback(session, now, current_user=current_user)
    visibility_states = visibility_for_profiles(session, all_tables, user=current_user)
    all_tables = [table for table in all_tables if visibility_states.get(table.table_id, None) is None or visibility_states[table.table_id].visible]
    filtered_tables = filter_profiles(all_tables, normalized_filters)
    schema_scope_tables = (
        filter_profiles(
            all_tables,
            normalize_filters(
                data_source_id=normalized_filters.get("data_source_id"),
                source=normalized_filters.get("source"),
            ),
        )
        if normalized_filters.get("data_source_id") is not None or normalized_filters.get("source")
        else all_tables
    )
    recent_count_map, recent_occurrence_map = _recent_incident_maps(session, now)
    filtered_table_ids = [table.table_id for table in filtered_tables]
    metabase_dashboard_map = _metabase_dashboard_impact_map(session, filtered_table_ids)
    lineage_impact_map = _lineage_impact_map(session, filtered_table_ids)
    user_impact_map = _search_user_impact_map(session, filtered_table_ids, now)

    scored_items = _sort_top_assets(
        [
            (
                table,
                *_score_table_with_impact(
                    table,
                    recent_incident_count=recent_count_map.get(table.incident_lookup_key, 0),
                    recent_occurrences=recent_occurrence_map.get(table.incident_lookup_key, 0),
                    metabase_dashboards=metabase_dashboard_map.get(table.table_id, 0),
                    impacted_users=user_impact_map.get(table.table_id),
                    lineage_upstream=lineage_impact_map.get(table.table_id, {}).get("upstream", 0),
                    lineage_downstream=lineage_impact_map.get(table.table_id, {}).get("downstream", 0),
                ),
                recent_count_map.get(table.incident_lookup_key, 0),
            )
            for table in filtered_tables
        ]
    )

    total_assets = len(filtered_tables)
    dq_tables = [table for table in filtered_tables if table.dq_score is not None]
    governance_scores = [build_governance_score_for_profile(table, settings_snapshot=settings_snapshot)["score"] for table in filtered_tables]
    critical_assets = [item for item in scored_items if item[1] >= 75]
    certification_statuses = [resolve_certification_status_for_profile(table, now=now) for table in filtered_tables]
    certified_count = sum(1 for status in certification_statuses if status == "certified")
    eligible_count = sum(1 for table in filtered_tables if table.eligible_for_certification)
    eligible_not_certified = sum(
        1
        for table, status in zip(filtered_tables, certification_statuses, strict=False)
        if table.eligible_for_certification and status != "certified"
    )
    no_owner_count = sum(1 for table in filtered_tables if not table.owner_defined)
    no_dictionary_count = sum(1 for table in filtered_tables if not table.dictionary_complete)
    assets_with_incidents = sum(1 for table in filtered_tables if table.open_incidents > 0)
    dq_avg = round(sum(table.dq_score or 0 for table in dq_tables) / len(dq_tables), 1) if dq_tables else 0.0
    governance_avg = round(sum(governance_scores) / len(governance_scores), 1) if governance_scores else 0.0
    governance_maturity_counts = Counter(_governance_maturity_key(int(score)) for score in governance_scores)
    governance_trend = summarize_governance_score_trend(session, days=14)

    score_band_counter = Counter(_dq_band_for(table) for table in filtered_tables)
    dq_trend = None
    incident_severity_rows = []
    by_domain: defaultdict[str, list[tuple[int, TableProfile]]] = defaultdict(list)
    by_source: defaultdict[str, list[tuple[int, TableProfile]]] = defaultdict(list)
    by_schema: defaultdict[str, list[tuple[int, TableProfile]]] = defaultdict(list)
    source_labels: dict[str, str] = {}
    schema_labels: dict[str, str] = {}
    maturity_by_domain: defaultdict[str, list[TableProfile]] = defaultdict(list)
    maturity_by_source: defaultdict[str, list[TableProfile]] = defaultdict(list)
    maturity_by_owner: defaultdict[str, list[TableProfile]] = defaultdict(list)
    maturity_by_schema: defaultdict[str, list[TableProfile]] = defaultdict(list)
    if include_secondary:
        dq_trend = _trend_for_tables(session, [table.table_id for table in filtered_tables])
        incident_severity_rows = session.execute(
            select(Incident.severity, func.count(Incident.id))
            .where(
                Incident.entity_type == "table",
                Incident.status.in_(["open", "investigating"]),
                Incident.table_fqn.in_([table.incident_lookup_key for table in filtered_tables] or ["__none__"]),
            )
            .group_by(Incident.severity)
        ).all()
        for table, score, _factors, _recent_count in scored_items:
            by_domain[_domain_label(table)].append((score, table))
            source_key = _source_filter_key(table)
            schema_key = _schema_filter_key(table)
            source_labels[source_key] = _source_filter_label(table)
            schema_labels[schema_key] = _schema_filter_label(table)
            by_source[source_key].append((score, table))
            by_schema[schema_key].append((score, table))
            maturity_by_domain[_domain_label(table)].append(table)
            maturity_by_source[source_key].append(table)
            maturity_by_owner[_owner_status_label(table)].append(table)
            maturity_by_schema[schema_key].append(table)

    worst_assets = [
        _serialize_asset_with_visibility(
            table,
            score,
            factors,
            recent_incident_count=recent_count,
            settings_snapshot=settings_snapshot,
            masked=bool(visibility_states.get(table.table_id) and visibility_states[table.table_id].masked),
        )
        for table, score, factors, recent_count in sorted(
            [item for item in scored_items if item[0].dq_score is not None],
            key=lambda item: ((item[0].dq_score if item[0].dq_score is not None else 101), -item[1], item[0].table_fqn),
        )[:5]
    ]

    top_assets = [
        _serialize_asset_with_visibility(
            table,
            score,
            factors,
            recent_incident_count=recent_count,
            settings_snapshot=settings_snapshot,
            masked=bool(visibility_states.get(table.table_id) and visibility_states[table.table_id].masked),
        )
        for table, score, factors, recent_count in scored_items[:10]
    ]
    critical_assets_items = [
        _serialize_asset_with_visibility(
            table,
            score,
            factors,
            recent_incident_count=recent_count,
            settings_snapshot=settings_snapshot,
            masked=bool(visibility_states.get(table.table_id) and visibility_states[table.table_id].masked),
        )
        for table, score, factors, recent_count in critical_assets[:10]
    ]

    recurring_assets = [
        _serialize_asset_with_visibility(
            table,
            score,
            factors,
            recent_incident_count=recent_count,
            settings_snapshot=settings_snapshot,
            masked=bool(visibility_states.get(table.table_id) and visibility_states[table.table_id].masked),
        )
        for table, score, factors, recent_count in scored_items
        if recent_count >= 2
    ][:5]
    impact_assets = [
        _serialize_asset_with_visibility(
            table,
            score,
            factors,
            recent_incident_count=recent_count,
            settings_snapshot=settings_snapshot,
            masked=bool(visibility_states.get(table.table_id) and visibility_states[table.table_id].masked),
        )
        for table, score, factors, recent_count in sorted(
            scored_items,
            key=lambda item: (-item[0].critical_open_incidents, -item[0].open_incidents, -item[1], item[0].table_fqn),
        )[:5]
        if table.open_incidents > 0
    ]
    ingestion_summary = None
    linked_table_ids: set[int] = set()
    stewardship_summary = None
    if include_secondary:
        ingestion_summary = load_ingestion_operational_overview_from_source(
            session,
            table_refs=[
                {
                    "table_id": table.table_id,
                    "table_name": table.table_name,
                    "table_fqn": table.table_fqn,
                    "schema_name": table.schema_name,
                    "criticality_score": score,
                }
                for table, score, _, _ in scored_items
            ],
            limit=max(total_assets, 5),
            high_volume_threshold_rows=settings_snapshot.operational_high_volume_threshold_rows,
            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
        )
        linked_table_ids = {
            int(item["table_id"])
            for item in ingestion_summary.get("items", [])
            if item.get("table_id") is not None
        }
        ingestion_summary["items"] = list(ingestion_summary.get("items", []))[:5]
        ingestion_summary["unmapped_items"] = list(ingestion_summary.get("unmapped_items", []))[:5]
        ingestion_summary["degraded_items"] = list(ingestion_summary.get("degraded_items", []))[:5]
        ingestion_summary["failed_items"] = list(ingestion_summary.get("failed_items", []))[:5]
        ingestion_summary["critical_stale_items"] = list(ingestion_summary.get("critical_stale_items", []))[:5]
        ingestion_summary["high_volume_failed_items"] = list(ingestion_summary.get("high_volume_failed_items", []))[:5]
        filtered_table_ids = {table.table_id for table in filtered_tables}
        stewardship_requests = session.scalars(
            select(StewardshipRequest)
            .options(
                selectinload(StewardshipRequest.table).selectinload(TableEntity.data_owner),
                selectinload(StewardshipRequest.approver_user),
            )
        ).all()
        stewardship_summary = build_stewardship_inbox_summary(
            [item for item in stewardship_requests if item.table_id is None or item.table_id in filtered_table_ids],
            current_user=current_user,
        )
    critical_changes_items: list[dict[str, object]] = []
    operational_intelligence: dict[str, object] | None = None
    if include_secondary:
        critical_changes_payload = get_governance_critical_changes(session, limit=200, current_user=current_user)
        critical_changes_items = list(critical_changes_payload.get("items", []))
        operational_intelligence = build_operational_intelligence(
            session,
            profiles=filtered_tables,
            recent_incident_map=recent_count_map,
            recent_occurrence_map=recent_occurrence_map,
            ingestion_summary=ingestion_summary,
            critical_changes=critical_changes_items,
            current_user=current_user,
        )

    payload = {
        "generated_at": now.isoformat(),
        "read_model_source": read_model_source,
        "available_filters": _filter_options(
            all_tables,
            schema_tables=schema_scope_tables,
        ),
        "applied_filters": dict(normalized_filters),
        "kpis": [
            {"key": "total_assets", "label": "Ativos monitorados", "value": total_assets, "hint": "Escopo atual do dashboard executivo.", "tone": "neutral"},
            {"key": "critical_assets", "label": "Ativos críticos", "value": len(critical_assets), "hint": "Score entre 75 e 100.", "tone": "warning"},
            {"key": "certified_assets", "label": "Ativos certificados", "value": certified_count, "hint": "Ativos já certificados dentro do recorte atual.", "tone": "success"},
            {"key": "eligible_assets", "label": "Elegíveis para certificação", "value": eligible_count, "hint": "Ativos que já cumprem os critérios mínimos.", "tone": "accent"},
            {"key": "assets_without_owner", "label": "Sem owner", "value": no_owner_count, "hint": "Precisam de responsável definido.", "tone": "warning"},
            {"key": "assets_without_dictionary", "label": "Sem dicionário", "value": no_dictionary_count, "hint": "Ainda com lacunas de documentação.", "tone": "warning"},
            {"key": "assets_with_open_incidents", "label": "Com incidente aberto", "value": assets_with_incidents, "hint": "Ativos impactados operacionalmente agora.", "tone": "warning"},
            {"key": "dq_avg", "label": "DQ médio", "value": dq_avg, "hint": "Média de DQ apenas entre ativos avaliados.", "tone": "accent", "unit": "pts"},
            {"key": "governance_avg", "label": "Governança média", "value": governance_avg, "hint": "Média do score geral de governança do recorte.", "tone": "accent", "unit": "pts"},
        ],
        "top_critical": {"total": len(critical_assets), "items": critical_assets_items},
        "certification": {
            "certified": certified_count,
            "eligible_not_certified": eligible_not_certified,
            "not_eligible": max(total_assets - certified_count - eligible_not_certified, 0),
            "certified_pct": round_pct(certified_count, total_assets),
            "eligible_not_certified_pct": round_pct(eligible_not_certified, total_assets),
            "not_eligible_pct": round_pct(max(total_assets - certified_count - eligible_not_certified, 0), total_assets),
        },
        "governance_gaps": {
            "total_assets": total_assets,
            "items": _governance_items(filtered_tables),
        },
        "governance_reviews": {
            "total_assets": total_assets,
            "items": _governance_review_items(filtered_tables, settings_snapshot),
        },
        "governance_maturity": {
            "avg_score": governance_avg,
            "bands": [
                {
                    "key": key,
                    "label": label,
                    "count": int(governance_maturity_counts.get(key, 0)),
                    "pct": round_pct(int(governance_maturity_counts.get(key, 0)), total_assets),
                    "tone": tone,
                }
                for key, label, tone in GOVERNANCE_MATURITY_ORDER
            ],
        },
        "governance_trend": governance_trend,
    }
    if include_secondary:
        payload.update(
            {
                "stewardship": stewardship_summary,
                "campaigns": _campaign_items(filtered_tables, settings_snapshot),
                "critical_changes": critical_changes_items[:8],
                "ingestion": ingestion_summary,
                "dq": {
                    "avg_score": dq_avg,
                    "not_evaluated": sum(1 for table in filtered_tables if table.dq_score is None),
                    "score_bands": [
                        {"key": key, "label": DQ_BANDS[key], "value": int(score_band_counter.get(key, 0))}
                        for key in ["critical", "warning", "healthy", "unknown"]
                    ],
                    "worst_assets": worst_assets,
                    "trend": dq_trend,
                },
                "incidents": {
                    "open_total": sum(table.open_incidents for table in filtered_tables),
                    "critical_open_total": sum(table.critical_open_incidents for table in filtered_tables),
                    "by_severity": [
                        {"key": str(row.severity), "label": str(row.severity).upper(), "value": int(row[1] or 0)}
                        for row in incident_severity_rows
                    ],
                    "top_assets": [asset for asset in top_assets if asset["open_incidents"]],
                    "recurring_assets": recurring_assets,
                    "impact_assets": impact_assets,
                },
                "risk": {
                    "by_domain": _risk_ranking(by_domain),
                    "by_source": _risk_ranking(by_source, labels=source_labels),
                    "by_schema": _risk_ranking(by_schema, labels=schema_labels),
                },
                "maturity_panels": {
                    "by_domain": _maturity_panels(
                        maturity_by_domain,
                        settings_snapshot=settings_snapshot,
                        linked_table_ids=linked_table_ids,
                    ),
                    "by_source": _maturity_panels(
                        maturity_by_source,
                        settings_snapshot=settings_snapshot,
                        linked_table_ids=linked_table_ids,
                        labels=source_labels,
                    ),
                    "by_owner": _maturity_panels(
                        maturity_by_owner,
                        settings_snapshot=settings_snapshot,
                        linked_table_ids=linked_table_ids,
                    ),
                    "by_schema": _maturity_panels(
                        maturity_by_schema,
                        settings_snapshot=settings_snapshot,
                        linked_table_ids=linked_table_ids,
                        labels=schema_labels,
                    ),
                },
                "operational_intelligence": operational_intelligence,
            }
        )
    with _EXECUTIVE_SUMMARY_CACHE_LOCK:
        _EXECUTIVE_SUMMARY_CACHE[cache_key] = (
            now + timedelta(seconds=_EXECUTIVE_SUMMARY_CACHE_TTL_SECONDS),
            payload,
        )
    return payload


def get_dashboard_executive_overview(session: Session, filters: DashboardExecutiveFilters | None = None, current_user=None) -> dict[str, object]:
    payload = get_dashboard_executive_summary(session, filters, include_secondary=False, current_user=current_user)
    return {
        "generated_at": payload["generated_at"],
        "available_filters": payload["available_filters"],
        "applied_filters": payload["applied_filters"],
        "kpis": payload["kpis"],
        "top_critical": payload["top_critical"],
        "certification": payload["certification"],
        "governance_gaps": payload["governance_gaps"],
        "governance_reviews": payload["governance_reviews"],
        "governance_maturity": payload["governance_maturity"],
        "governance_trend": payload["governance_trend"],
    }


def get_dashboard_executive_secondary(session: Session, filters: DashboardExecutiveFilters | None = None, current_user=None) -> dict[str, object]:
    payload = get_dashboard_executive_summary(session, filters, include_secondary=True, current_user=current_user)
    return {
        "generated_at": payload["generated_at"],
        "stewardship": payload["stewardship"],
        "campaigns": payload["campaigns"],
        "critical_changes": payload["critical_changes"],
        "ingestion": payload["ingestion"],
        "dq": payload["dq"],
        "incidents": payload["incidents"],
        "risk": payload["risk"],
        "maturity_panels": payload["maturity_panels"],
        "operational_intelligence": payload.get(
            "operational_intelligence",
            {
                "generated_at": payload["generated_at"],
                "window_days": 30,
                "evaluated_assets": 0,
                "priority_queue_size": 0,
                "high_risk_assets": 0,
                "high_risk_domains": 0,
                "high_risk_products": 0,
                "unstable_pipelines": 0,
                "deteriorating_assets": 0,
                "recurring_instability": 0,
                "suggested_incidents": 0,
                "by_asset": [],
                "by_domain": [],
                "by_product": [],
                "by_pipeline": [],
                "alerts": [],
                "trend": [],
            },
        ),
    }


def get_dashboard_executive_top_critical(session: Session, filters: DashboardExecutiveFilters | None = None, limit: int = 10, offset: int = 0, current_user=None) -> dict[str, object]:
    payload = get_dashboard_executive_summary(session, filters, include_secondary=False, current_user=current_user)
    items = list(payload["top_critical"]["items"])
    return {"total": int(payload["top_critical"]["total"]), "items": items[offset : offset + limit]}


def get_dashboard_executive_certification(session: Session, filters: DashboardExecutiveFilters | None = None, current_user=None) -> dict[str, object]:
    return get_dashboard_executive_summary(session, filters, include_secondary=False, current_user=current_user)["certification"]


def get_dashboard_executive_governance_gaps(session: Session, filters: DashboardExecutiveFilters | None = None, current_user=None) -> dict[str, object]:
    return get_dashboard_executive_summary(session, filters, include_secondary=False, current_user=current_user)["governance_gaps"]


def get_dashboard_executive_dq(session: Session, filters: DashboardExecutiveFilters | None = None, current_user=None) -> dict[str, object]:
    return get_dashboard_executive_summary(session, filters, include_secondary=True, current_user=current_user)["dq"]


def get_dashboard_executive_incidents(session: Session, filters: DashboardExecutiveFilters | None = None, current_user=None) -> dict[str, object]:
    return get_dashboard_executive_summary(session, filters, include_secondary=True, current_user=current_user)["incidents"]


def get_dashboard_executive_risk(session: Session, filters: DashboardExecutiveFilters | None = None, current_user=None) -> dict[str, object]:
    return get_dashboard_executive_summary(session, filters, include_secondary=True, current_user=current_user)["risk"]


def get_dashboard_executive_asset_details(session: Session, table_id: int, current_user=None) -> dict[str, object] | None:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    if not is_table_visible(session, table_id, user=current_user):
        return None
    tables, _source = load_dashboard_profiles_with_fallback(session, now, table_ids=[table_id], current_user=current_user)
    table = next((item for item in tables if item.table_id == table_id), None)
    if table is None:
        return None

    recent_count_map, recent_occurrence_map = _recent_incident_maps(session, now)
    recent_count = recent_count_map.get(table.incident_lookup_key, 0)
    recent_occurrences = recent_occurrence_map.get(table.incident_lookup_key, 0)
    metabase_dashboard_map = _metabase_dashboard_impact_map(session, [table.table_id])
    lineage_impact_map = _lineage_impact_map(session, [table.table_id])
    user_impact_map = _search_user_impact_map(session, [table.table_id], now)
    score, factors = _score_table_with_impact(
        table,
        recent_incident_count=recent_count,
        recent_occurrences=recent_occurrences,
        metabase_dashboards=metabase_dashboard_map.get(table.table_id, 0),
        impacted_users=user_impact_map.get(table.table_id),
        lineage_upstream=lineage_impact_map.get(table.table_id, {}).get("upstream", 0),
        lineage_downstream=lineage_impact_map.get(table.table_id, {}).get("downstream", 0),
    )

    incidents = session.scalars(
        select(Incident)
        .where(Incident.entity_type == "table", Incident.table_fqn == table.incident_lookup_key)
        .order_by(
            desc(Incident.status.in_(["open", "investigating"])),
            desc(Incident.detected_at),
        )
        .limit(8)
    ).all()
    detail_visibility = visibility_for_profiles(session, [table], user=current_user).get(table.table_id)

    return {
        "generated_at": now.isoformat(),
        "asset": _serialize_asset_with_visibility(
            table,
            score,
            factors,
            recent_incident_count=recent_count,
            settings_snapshot=settings_snapshot,
            masked=bool(detail_visibility and detail_visibility.masked),
        ),
        "incidents": [
            {
                "id": incident.id,
                "title": incident.title,
                "severity": incident.severity,
                "status": incident.status,
                "detected_at": normalize_dt(incident.detected_at).isoformat() if normalize_dt(incident.detected_at) else None,
                "occurrences": incident.occurrences,
            }
            for incident in incidents
        ],
        "next_actions": recommended_actions(table, recent_incident_count=recent_count),
        "data_notes": {
            "domain": _domain_label(table),
            "dq_status": _dq_status_label(table),
            "eligibility": "Elegível" if table.eligible_for_certification else "Não elegível",
        },
    }
