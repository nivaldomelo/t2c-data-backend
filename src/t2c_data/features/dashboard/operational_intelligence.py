from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import log1p
from urllib.parse import quote_plus

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.dashboard.executive_scoring import compute_priority_score, compute_profile_priority_score, recommended_actions, risk_label, risk_tone
from t2c_data.features.dashboard.support import TableProfile, normalize_dt
from t2c_data.models.dq import DQRun
from t2c_data.models.incident import Incident
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.metabase_impact import MetabaseImpactSnapshot
from t2c_data.models.search import SearchResultClick
from t2c_data.models.semantic import SemanticDataProduct, SemanticDomain

_ENTITY_LIMIT = 6
_ALERT_LIMIT = 8
_TREND_WINDOW_DAYS = 14


@dataclass(frozen=True)
class _PipelineItem:
    key: str
    label: str
    href: str
    score: int
    priority_score: int
    risk_label: str
    risk_tone: str
    asset_count: int
    open_incidents: int
    critical_open_incidents: int
    reasons: list[str]
    suggested_actions: list[str]
    suggested_incident: bool


def _value_text(value: object | None) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _parse_dt(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return normalize_dt(value)
    text = _value_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return normalize_dt(parsed)


def _freshness_hours(table: TableProfile) -> int | None:
    freshness_seconds = table.freshness_seconds
    if freshness_seconds is None or freshness_seconds < 0:
        return None
    return int(round(freshness_seconds / 3600))


def _pipe_href(item: dict[str, object]) -> str:
    dag_id = _value_text(item.get("dag_id"))
    table_id = item.get("table_id")
    table_fqn = _value_text(item.get("table_fqn"))
    if dag_id:
        return f"/integrations/airflow?dagId={dag_id}"
    if table_id is not None:
        return f"/ops/ingestion?tableId={table_id}"
    if table_fqn:
        return f"/ops/ingestion?q={table_fqn}"
    return "/integrations/airflow"


def _pipeline_identity(item: dict[str, object]) -> str:
    dag_id = _value_text(item.get("dag_id"))
    pipeline_name = _value_text(item.get("pipeline_name"))
    table_id = item.get("table_id")
    table_fqn = _value_text(item.get("table_fqn"))
    if dag_id:
        return f"dag:{dag_id}"
    if pipeline_name:
        return f"pipeline:{pipeline_name}"
    if table_id is not None:
        return f"table:{table_id}"
    return f"fqn:{table_fqn or 'unknown'}"


def _pipeline_label(item: dict[str, object]) -> str:
    pipeline_name = _value_text(item.get("pipeline_name"))
    dag_id = _value_text(item.get("dag_id"))
    table_fqn = _value_text(item.get("table_fqn"))
    if pipeline_name and dag_id:
        return f"{pipeline_name} · {dag_id}"
    if pipeline_name:
        return pipeline_name
    if dag_id:
        return dag_id
    if table_fqn:
        return table_fqn
    return "Pipeline sem identificação"


def _severity_tone(score: int) -> str:
    if score >= 80:
        return "danger"
    if score >= 60:
        return "warning"
    if score >= 40:
        return "accent"
    return "neutral"


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
        session.rollback()
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
    try:
        asset_rows = session.execute(
            select(LineageAsset.id, LineageAsset.catalog_table_id).where(
                LineageAsset.catalog_table_id.in_(table_ids),
                LineageAsset.is_active.is_(True),
            )
        ).all()
    except SQLAlchemyError:
        session.rollback()
        return {}
    lineage_to_table = {int(row.id): int(row.catalog_table_id) for row in asset_rows if row.catalog_table_id is not None}
    if not lineage_to_table:
        return {}
    lineage_ids = list(lineage_to_table.keys())
    impact = {table_id: {"upstream": 0, "downstream": 0} for table_id in table_ids}
    try:
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
    except SQLAlchemyError:
        session.rollback()
        return {}
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
    try:
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
    except SQLAlchemyError:
        session.rollback()
        return {}
    return {int(table_id): int(count or 0) for table_id, count in rows if table_id is not None}


def _priority_score(score: int, impact: int) -> int:
    return max(0, min(100, score + impact))


def _change_counts(critical_changes: list[dict[str, object]], *, since: datetime) -> Counter[int]:
    counts: Counter[int] = Counter()
    for item in critical_changes:
        changed_at = _parse_dt(item.get("changed_at"))
        if changed_at is None or changed_at < since:
            continue
        table_id = item.get("table_id")
        if table_id is None:
            continue
        try:
            counts[int(table_id)] += 1
        except (TypeError, ValueError):
            continue
    return counts


def _trend_counts(critical_changes: list[dict[str, object]]) -> dict[str, Counter[str]]:
    trend: dict[str, Counter[str]] = {
        "incidents": Counter(),
        "dq_failures": Counter(),
        "changes": Counter(),
    }
    for item in critical_changes:
        changed_at = _value_text(item.get("changed_at"))
        if not changed_at:
            continue
        day = changed_at[:10]
        trend["changes"][day] += 1
    return trend


def _trend_series(
    *,
    since: datetime,
    incident_rows: list[tuple[object, object]],
    dq_failure_rows: list[tuple[object, object]],
    critical_changes: list[dict[str, object]],
) -> list[dict[str, object]]:
    days = max((datetime.now(timezone.utc).date() - since.date()).days, 0)
    buckets: dict[str, dict[str, object]] = {}
    for offset in range(days + 1):
        day = since.date() + timedelta(days=offset)
        buckets[day.isoformat()] = {
            "label": day.isoformat()[5:],
            "value": 0,
            "incidents": 0,
            "dq_failures": 0,
            "changes": 0,
        }

    for day_value, value in incident_rows:
        if day_value is None:
            continue
        day = str(day_value)
        if day not in buckets:
            continue
        count = int(value or 0)
        buckets[day]["incidents"] = count
        buckets[day]["value"] += count

    for day_value, value in dq_failure_rows:
        if day_value is None:
            continue
        day = str(day_value)
        if day not in buckets:
            continue
        count = int(value or 0)
        buckets[day]["dq_failures"] = count
        buckets[day]["value"] += count

    for item in critical_changes:
        changed_at = _parse_dt(item.get("changed_at"))
        if changed_at is None or changed_at < since:
            continue
        day = changed_at.date().isoformat()
        if day not in buckets:
            continue
        buckets[day]["changes"] = int(buckets[day]["changes"]) + 1
        buckets[day]["value"] = int(buckets[day]["value"]) + 1

    return [buckets[key] for key in sorted(buckets.keys())]


def _asset_item(
    table: TableProfile,
    *,
    recent_incident_count: int,
    recent_occurrences: int,
    change_count: int,
    ingestion_item: dict[str, object] | None,
    metabase_dashboards: int = 0,
    impacted_users: int | None = None,
    lineage_upstream: int = 0,
    lineage_downstream: int = 0,
) -> dict[str, object]:
    base_score, factors = compute_priority_score(table, recent_incident_count=recent_incident_count, recent_occurrences=recent_occurrences)
    reasons = [factor.detail for factor in factors if factor.applied]
    event_penalty = 0
    if table.recent_dq_failure_runs_30d:
        dq_penalty = min(18, table.recent_dq_failure_runs_30d * 5)
        event_penalty += dq_penalty
        reasons.append(f"{table.recent_dq_failure_runs_30d} falha(s) recente(s) de DQ")
    if change_count > 0:
        change_penalty = min(12, change_count * 3)
        event_penalty += change_penalty
        reasons.append(f"{change_count} mudança(s) crítica(s) recente(s)")

    stale_hours = _freshness_hours(table)
    if stale_hours is not None and stale_hours >= 72:
        stale_penalty = min(15, max(4, int((stale_hours - 72) / 6) + 4))
        event_penalty += stale_penalty
        reasons.append(f"{stale_hours}h sem frescor suficiente")
    elif stale_hours is not None and stale_hours >= 24:
        event_penalty += 4
        reasons.append(f"Frescor acima de 24h ({stale_hours}h)")

    ingestion_status = _value_text(ingestion_item.get("latest_status_label")) if ingestion_item else None
    if ingestion_status:
        normalized_status = ingestion_status.lower()
        if "falh" in normalized_status or "failed" in normalized_status:
            event_penalty += 12
            reasons.append("Pipeline com falha recente")
        elif "degrad" in normalized_status:
            event_penalty += 8
            reasons.append("Pipeline degradado")
        elif "pend" in normalized_status:
            event_penalty += 5
            reasons.append("Pipeline pendente de execução")

    risk_score = max(0, min(100, base_score + event_penalty))
    if table.search_clicks_30d > 0:
        reasons.append(f"{table.search_clicks_30d} clique(s) de busca em 30 dias")
    if metabase_dashboards > 0:
        reasons.append(f"{metabase_dashboards} dashboard(s) Metabase impactado(s)")
    if lineage_upstream + lineage_downstream > 0:
        reasons.append(f"{lineage_upstream} dependência(s) upstream e {lineage_downstream} downstream")
    priority_score = compute_profile_priority_score(
        table,
        risk_score,
        dashboards=metabase_dashboards,
        users=impacted_users,
        upstream=lineage_upstream,
        downstream=lineage_downstream,
    )
    suggested_incident = priority_score >= 60 and (
        table.open_incidents <= 0
        or table.recent_dq_failure_runs_30d >= 2
        or recent_incident_count >= 2
        or event_penalty >= 12
    )
    suggested_actions = recommended_actions(table, recent_incident_count=recent_incident_count)
    if suggested_incident:
        suggested_actions = ["Abrir incidente preventivo para o ativo", *suggested_actions]
    links = build_asset_links(
        table_id=table.table_id,
        datasource_id=table.datasource_id,
        database_id=table.database_id,
        schema_id=table.schema_id,
        data_owner_id=table.data_owner_id,
    )
    return {
        "entity_kind": "asset",
        "key": f"asset:{table.table_id}",
        "label": table.table_fqn,
        "href": links["explorer"],
        "table_id": table.table_id,
        "domain_name": table.domain_name,
        "owner_name": table.owner_name,
        "score": risk_score,
        "priority_score": priority_score,
        "risk_label": risk_label(priority_score),
        "risk_tone": risk_tone(priority_score),
        "asset_count": 1,
        "open_incidents": table.open_incidents,
        "critical_open_incidents": table.critical_open_incidents,
        "recent_incidents_30d": recent_incident_count,
        "recent_dq_failure_runs_30d": table.recent_dq_failure_runs_30d,
        "change_events_30d": change_count,
        "search_clicks_30d": table.search_clicks_30d,
        "stale_hours": stale_hours,
        "degraded_pipelines": 1 if ingestion_item and _value_text(ingestion_item.get("latest_status_label")) and "degrad" in _value_text(ingestion_item.get("latest_status_label")).lower() else 0,
        "failed_pipelines": 1 if ingestion_item and _value_text(ingestion_item.get("latest_status_label")) and ("falh" in _value_text(ingestion_item.get("latest_status_label")).lower() or "failed" in _value_text(ingestion_item.get("latest_status_label")).lower()) else 0,
        "reasons": reasons,
        "suggested_actions": suggested_actions,
        "suggested_incident": suggested_incident,
        "incident_hint": "Abrir incidente preventivo no ativo" if suggested_incident else None,
    }


def _aggregate_scores(items: list[dict[str, object]]) -> tuple[int, int, int]:
    if not items:
        return 0, 0, 0
    scores = [int(item["score"]) for item in items]
    priorities = [int(item["priority_score"]) for item in items]
    incidents = [int(item["open_incidents"]) for item in items]
    score = int(round(sum(scores) / len(scores)))
    priority = int(round(sum(priorities) / len(priorities)))
    incident_penalty = min(25, sum(1 for value in incidents if value > 0) * 4 + sum(min(5, value) for value in incidents))
    return score, priority, incident_penalty


def _group_profiles_by_domain(profiles: list[TableProfile]) -> dict[str, list[TableProfile]]:
    grouped: dict[str, list[TableProfile]] = defaultdict(list)
    for profile in profiles:
        label = (profile.domain_name or "").strip() or "Sem domínio"
        grouped[label].append(profile)
    return grouped


def _domain_item(
    label: str,
    profiles: list[TableProfile],
    *,
    linked_table_ids: set[int],
    domain_slug: str | None = None,
    product_count: int = 0,
    recent_incident_map: dict[str, int],
    recent_occurrence_map: dict[str, int],
    change_counts: Counter[int],
) -> dict[str, object]:
    asset_items: list[dict[str, object]] = []
    for table in profiles:
        asset_items.append(
            _asset_item(
                table,
                recent_incident_count=recent_incident_map.get(table.incident_lookup_key, 0),
                recent_occurrences=recent_occurrence_map.get(table.incident_lookup_key, 0),
                change_count=change_counts.get(table.table_id, 0),
                ingestion_item=None,
            )
        )
    score, priority, incident_penalty = _aggregate_scores(asset_items)
    search_clicks_30d = sum(table.search_clicks_30d for table in profiles)
    avg_dq = round(sum((table.dq_score or 0) for table in profiles if table.dq_score is not None) / max(1, sum(1 for table in profiles if table.dq_score is not None)), 1)
    avg_trust = round(sum(table.trust_score for table in profiles) / len(profiles), 1)
    reasons = []
    if any(table.critical_open_incidents > 0 for table in profiles):
        reasons.append(f"{sum(table.critical_open_incidents for table in profiles)} incidente(s) crítico(s) aberto(s)")
    if any(table.recent_dq_failure_runs_30d > 0 for table in profiles):
        reasons.append(f"{sum(table.recent_dq_failure_runs_30d for table in profiles)} falha(s) recente(s) de DQ")
    if any(not table.owner_defined for table in profiles):
        reasons.append(f"{sum(1 for table in profiles if not table.owner_defined)} ativo(s) sem owner")
    if any(change_counts.get(table.table_id, 0) > 0 for table in profiles):
        reasons.append("Mudanças recentes no domínio")
    if search_clicks_30d > 50:
        reasons.append(f"{search_clicks_30d} clique(s) de busca em 30 dias")
    if avg_dq < 75:
        reasons.append(f"DQ médio em {avg_dq}")
    if avg_trust < 70:
        reasons.append(f"Trust médio em {avg_trust}")

    impact_bonus = min(15, int(round(log1p(max(search_clicks_30d, 0)) * 2.5)))
    if linked_table_ids:
        impact_bonus = min(15, impact_bonus + min(5, len(linked_table_ids)))
    priority_score = _priority_score(score + incident_penalty, impact_bonus)
    suggested_incident = priority_score >= 70 and (sum(table.open_incidents for table in profiles) > 0 or sum(table.recent_dq_failure_runs_30d for table in profiles) > 0)
    href = f"/governance/domains/{domain_slug}" if domain_slug else f"/search?domain={quote_plus(label)}"
    suggested_actions = [
        "Revisar qualidade e cobertura do domínio",
        "Abrir domínio semântico para investigação",
    ]
    if suggested_incident:
        suggested_actions.insert(0, "Abrir incidente preventivo para o domínio")
    return {
        "entity_kind": "domain",
        "key": f"domain:{domain_slug or label.lower().replace(' ', '-')}",
        "label": label,
        "href": href,
        "table_id": profiles[0].table_id if profiles else None,
        "domain_name": label,
        "owner_name": next((table.owner_name for table in profiles if table.owner_name), None),
        "score": score,
        "priority_score": priority_score,
        "risk_label": risk_label(priority_score),
        "risk_tone": risk_tone(priority_score),
        "asset_count": len(profiles),
        "open_incidents": sum(table.open_incidents for table in profiles),
        "critical_open_incidents": sum(table.critical_open_incidents for table in profiles),
        "recent_incidents_30d": sum(recent_incident_map.get(table.incident_lookup_key, 0) for table in profiles),
        "recent_dq_failure_runs_30d": sum(table.recent_dq_failure_runs_30d for table in profiles),
        "change_events_30d": sum(change_counts.get(table.table_id, 0) for table in profiles),
        "search_clicks_30d": search_clicks_30d,
        "stale_hours": max(((_freshness_hours(table) or 0) for table in profiles), default=None),
        "degraded_pipelines": sum(1 for table in profiles if table.dq_score is not None and table.dq_score < 70),
        "failed_pipelines": sum(1 for table in profiles if table.critical_open_incidents > 0),
        "reasons": reasons[:5],
        "suggested_actions": suggested_actions,
        "suggested_incident": suggested_incident,
        "incident_hint": "Abrir incidente preventivo no domínio" if suggested_incident else None,
    }


def _product_item(
    product: SemanticDataProduct,
    *,
    domain_label: str,
    domain_slug: str | None,
    domain_profiles: list[TableProfile],
    product_profiles: list[TableProfile],
    recent_incident_map: dict[str, int],
    recent_occurrence_map: dict[str, int],
    change_counts: Counter[int],
) -> dict[str, object]:
    linked_ids = {int(link.entity_id) for link in product.links if link.entity_kind == "table" and link.entity_id is not None}
    profiles = product_profiles or domain_profiles
    asset_items: list[dict[str, object]] = []
    for table in profiles:
        asset_items.append(
            _asset_item(
                table,
                recent_incident_count=recent_incident_map.get(table.incident_lookup_key, 0),
                recent_occurrences=recent_occurrence_map.get(table.incident_lookup_key, 0),
                change_count=change_counts.get(table.table_id, 0),
                ingestion_item=None,
            )
        )
    score, priority, incident_penalty = _aggregate_scores(asset_items)
    consumer_count = len([value for value in (product.consumers or []) if value])
    reasons = []
    if not linked_ids:
        reasons.append("Produto sem ativos vinculados explicitamente")
    if consumer_count <= 0:
        reasons.append("Sem consumidores declarados")
    if not product.sla_text:
        reasons.append("Sem SLA registrado")
    if not product.contract_text:
        reasons.append("Sem contrato registrado")
    if not product.owner:
        reasons.append("Owner do produto não definido")
    if any(table.open_incidents > 0 for table in profiles):
        reasons.append(f"{sum(table.open_incidents for table in profiles)} incidente(s) relacionado(s)")
    if any(table.recent_dq_failure_runs_30d > 0 for table in profiles):
        reasons.append("DQ do produto em deterioração")

    metadata_penalty = 0
    metadata_penalty += 10 if not linked_ids else 0
    metadata_penalty += 10 if consumer_count <= 0 else 0
    metadata_penalty += 8 if not product.sla_text else 0
    metadata_penalty += 8 if not product.contract_text else 0
    metadata_penalty += 5 if not product.owner else 0
    metadata_penalty += 5 if not product.steward else 0
    metadata_penalty += min(12, incident_penalty)
    impact_bonus = min(15, int(round(log1p(sum(table.search_clicks_30d for table in profiles)) * 2.5)))
    priority_score = _priority_score(score + metadata_penalty, impact_bonus)
    suggested_incident = priority_score >= 65 and (
        any(table.open_incidents > 0 for table in profiles)
        or any(table.recent_dq_failure_runs_30d > 0 for table in profiles)
        or not linked_ids
    )
    href = f"/governance/data-products/{product.slug}"
    suggested_actions = [
        "Revisar contrato e SLA do produto",
        "Abrir produto semântico para investigação",
    ]
    if suggested_incident:
        suggested_actions.insert(0, "Abrir incidente preventivo para o produto")
    return {
        "entity_kind": "product",
        "key": f"product:{product.slug}",
        "label": product.name,
        "href": href,
        "table_id": product_profiles[0].table_id if product_profiles else (domain_profiles[0].table_id if domain_profiles else None),
        "domain_name": domain_label,
        "owner_name": product.owner or next((table.owner_name for table in profiles if table.owner_name), None),
        "score": score,
        "priority_score": priority_score,
        "risk_label": risk_label(priority_score),
        "risk_tone": risk_tone(priority_score),
        "asset_count": len(linked_ids) if linked_ids else len(profiles),
        "open_incidents": sum(table.open_incidents for table in profiles),
        "critical_open_incidents": sum(table.critical_open_incidents for table in profiles),
        "recent_incidents_30d": sum(recent_incident_map.get(table.incident_lookup_key, 0) for table in profiles),
        "recent_dq_failure_runs_30d": sum(table.recent_dq_failure_runs_30d for table in profiles),
        "change_events_30d": sum(change_counts.get(table.table_id, 0) for table in profiles),
        "search_clicks_30d": sum(table.search_clicks_30d for table in profiles),
        "stale_hours": max(((_freshness_hours(table) or 0) for table in profiles), default=None),
        "degraded_pipelines": sum(1 for table in profiles if table.dq_score is not None and table.dq_score < 70),
        "failed_pipelines": sum(1 for table in profiles if table.critical_open_incidents > 0),
        "reasons": reasons[:5],
        "suggested_actions": suggested_actions,
        "suggested_incident": suggested_incident,
        "incident_hint": "Abrir incidente preventivo para o produto" if suggested_incident else None,
    }


def _pipeline_item(
    item: dict[str, object],
    *,
    table_profile: TableProfile | None,
    recent_incident_count: int,
    recent_occurrences: int,
    search_clicks_30d: int,
) -> dict[str, object]:
    status = _value_text(item.get("latest_status_label")) or _value_text(item.get("latest_status")) or "unknown"
    status_lower = status.lower()
    base_score = 0
    reasons: list[str] = []
    if "falh" in status_lower or "failed" in status_lower:
        base_score += 65
        reasons.append("Falha recente na execução")
    elif "degrad" in status_lower:
        base_score += 50
        reasons.append("Execução degradada")
    elif "pend" in status_lower:
        base_score += 35
        reasons.append("Execução pendente")
    elif "run" in status_lower or "exec" in status_lower:
        base_score += 20
        reasons.append("Execução em andamento")
    else:
        base_score += 10

    last_success_at = _parse_dt(item.get("last_success_at"))
    if last_success_at is not None:
        stale_hours = int(round((datetime.now(timezone.utc) - last_success_at).total_seconds() / 3600))
        if stale_hours >= 72:
            base_score += min(18, 8 + int((stale_hours - 72) / 12))
            reasons.append(f"{stale_hours}h sem sucesso")
        elif stale_hours >= 24:
            base_score += 6
            reasons.append(f"{stale_hours}h desde o último sucesso")
    else:
        stale_hours = None
        base_score += 12
        reasons.append("Sem sucesso recente registrado")

    last_error = _value_text(item.get("last_error"))
    if last_error:
        base_score += 8
        reasons.append("Última execução com erro")

    if item.get("table_id") is not None and table_profile is not None:
        base_score += min(15, table_profile.recent_dq_failure_runs_30d * 4 + table_profile.critical_open_incidents * 3)
        if table_profile.open_incidents > 0:
            reasons.append(f"{table_profile.open_incidents} incidente(s) no ativo vinculado")

    impact_bonus = min(15, int(round(log1p(max(search_clicks_30d, 0)) * 2.5)))
    if search_clicks_30d > 0:
        reasons.append(f"{search_clicks_30d} clique(s) de busca vinculados")
    priority_score = _priority_score(base_score, impact_bonus)
    suggested_incident = priority_score >= 65 and (status_lower.startswith("fail") or "degrad" in status_lower or stale_hours is None or stale_hours >= 72)
    suggested_actions = [
        "Abrir Airflow e revisar DAG/task",
        "Reprocessar a execução após correção",
    ]
    if suggested_incident:
        suggested_actions.insert(0, "Abrir incidente preventivo da integração")
    return {
        "entity_kind": "pipeline",
        "key": _pipeline_identity(item),
        "label": _pipeline_label(item),
        "href": _pipe_href(item),
        "table_id": int(item["table_id"]) if item.get("table_id") is not None else (table_profile.table_id if table_profile else None),
        "domain_name": _value_text(item.get("schema_name")),
        "owner_name": None,
        "score": base_score,
        "priority_score": priority_score,
        "risk_label": risk_label(priority_score),
        "risk_tone": risk_tone(priority_score),
        "asset_count": 1 if item.get("table_id") is not None else 0,
        "open_incidents": table_profile.open_incidents if table_profile else 0,
        "critical_open_incidents": table_profile.critical_open_incidents if table_profile else 0,
        "recent_incidents_30d": recent_incident_count,
        "recent_dq_failure_runs_30d": table_profile.recent_dq_failure_runs_30d if table_profile else 0,
        "change_events_30d": 0,
        "search_clicks_30d": search_clicks_30d,
        "stale_hours": stale_hours,
        "degraded_pipelines": 1 if "degrad" in status_lower else 0,
        "failed_pipelines": 1 if ("falh" in status_lower or "failed" in status_lower) else 0,
        "reasons": reasons[:5],
        "suggested_actions": suggested_actions,
        "suggested_incident": suggested_incident,
        "incident_hint": "Abrir incidente preventivo da integração" if suggested_incident else None,
    }


def _alert_from_item(item: dict[str, object]) -> dict[str, object] | None:
    priority_score = int(item["priority_score"])
    if priority_score < 55:
        return None
    entity_kind = str(item["entity_kind"])
    label = str(item["label"])
    if entity_kind == "asset":
        title = f"Ativo com risco crescente: {label}"
        description = "Falhas de DQ, incidentes e mudanças recentes sugerem ação antes da degradação virar incidente grave."
    elif entity_kind == "domain":
        title = f"Domínio com deterioração de qualidade: {label}"
        description = "A cobertura e a confiabilidade do domínio estão caindo e pedem priorização."
    elif entity_kind == "product":
        title = f"Produto de dados instável: {label}"
        description = "O produto reúne sinais de risco, baixa cobertura ou dependências frágeis."
    else:
        title = f"Pipeline com risco crescente: {label}"
        description = "A integração/orquestração mostra falhas, atraso de sucesso ou degradação recorrente."
    severity = "critical" if priority_score >= 80 else "high" if priority_score >= 65 else "medium"
    return {
        "key": str(item["key"]),
        "title": title,
        "description": description,
        "severity": severity,
        "tone": _severity_tone(priority_score),
        "entity_kind": entity_kind,
        "href": str(item["href"]),
        "table_id": item.get("table_id"),
        "suggested_incident": bool(item["suggested_incident"]),
    }


def _semantic_groups(session: Session, profiles: list[TableProfile]) -> tuple[list[SemanticDomain], list[SemanticDataProduct]]:
    try:
        domains = list(
            session.scalars(
                select(SemanticDomain)
                .options(selectinload(SemanticDomain.links), selectinload(SemanticDomain.products))
                .order_by(SemanticDomain.name)
            ).all()
        )
    except Exception:  # noqa: BLE001
        domains = []
    try:
        products = list(
            session.scalars(
                select(SemanticDataProduct)
                .options(selectinload(SemanticDataProduct.links), selectinload(SemanticDataProduct.domain))
                .order_by(SemanticDataProduct.name)
            ).all()
        )
    except Exception:  # noqa: BLE001
        products = []
    if not domains:
        grouped = _group_profiles_by_domain(profiles)
        domains = []
        for label in grouped.keys():
            domains.append(
                SemanticDomain(
                    slug=label.lower().replace(" ", "-"),
                    name=label,
                    description=None,
                    owner=None,
                    steward=None,
                    criticality=None,
                    maturity_status="emerging",
                )
            )
    return domains, products


def build_operational_intelligence(
    session: Session,
    *,
    profiles: list[TableProfile],
    recent_incident_map: dict[str, int],
    recent_occurrence_map: dict[str, int],
    ingestion_summary: dict[str, object] | None,
    critical_changes: list[dict[str, object]],
    current_user=None,
    window_days: int = 30,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    change_window_start = now - timedelta(days=max(window_days, _TREND_WINDOW_DAYS))
    change_counts = _change_counts(critical_changes, since=change_window_start)
    domains, products = _semantic_groups(session, profiles)
    profile_by_id = {profile.table_id: profile for profile in profiles}
    table_ids = [profile.table_id for profile in profiles]
    metabase_dashboard_map = _metabase_dashboard_impact_map(session, table_ids)
    lineage_impact_map = _lineage_impact_map(session, table_ids)
    user_impact_map = _search_user_impact_map(session, table_ids, now)
    ingestion_items = list((ingestion_summary or {}).get("items", []))
    ingestion_by_table_id = {
        int(item["table_id"]): item
        for item in ingestion_items
        if item.get("table_id") is not None
    }

    asset_items = [
        _asset_item(
            profile,
            recent_incident_count=recent_incident_map.get(profile.incident_lookup_key, 0),
            recent_occurrences=recent_occurrence_map.get(profile.incident_lookup_key, 0),
            change_count=change_counts.get(profile.table_id, 0),
            ingestion_item=ingestion_by_table_id.get(profile.table_id),
            metabase_dashboards=metabase_dashboard_map.get(profile.table_id, 0),
            impacted_users=user_impact_map.get(profile.table_id),
            lineage_upstream=lineage_impact_map.get(profile.table_id, {}).get("upstream", 0),
            lineage_downstream=lineage_impact_map.get(profile.table_id, {}).get("downstream", 0),
        )
        for profile in profiles
    ]
    asset_items = sorted(
        asset_items,
        key=lambda item: (
            -int(item["priority_score"]),
            -int(item["critical_open_incidents"]),
            -int(item["open_incidents"]),
            -int(item["search_clicks_30d"]),
            str(item["label"]),
        ),
    )

    grouped_profiles = _group_profiles_by_domain(profiles)
    domain_items: list[dict[str, object]] = []
    used_domain_labels: set[str] = set()
    for domain in domains:
        profiles_for_domain = [
            profile
            for profile in profiles
            if profile.table_id in {int(link.entity_id) for link in domain.links if link.entity_kind == "table" and link.entity_id is not None}
            or (profile.domain_name or "").strip().lower() == domain.name.strip().lower()
        ]
        if not profiles_for_domain:
            continue
        domain_items.append(
            _domain_item(
                domain.name,
                profiles_for_domain,
                linked_table_ids={
                    int(link.entity_id)
                    for link in domain.links
                    if link.entity_kind == "table" and link.entity_id is not None
                },
                domain_slug=domain.slug,
                product_count=len(domain.products),
                recent_incident_map=recent_incident_map,
                recent_occurrence_map=recent_occurrence_map,
                change_counts=change_counts,
            )
        )
        used_domain_labels.add(domain.name.strip().lower())

    for label, grouped in grouped_profiles.items():
        normalized = label.strip().lower()
        if normalized in used_domain_labels:
            continue
        domain_items.append(
            _domain_item(
                label,
                grouped,
                linked_table_ids=set(),
                domain_slug=None,
                product_count=0,
                recent_incident_map=recent_incident_map,
                recent_occurrence_map=recent_occurrence_map,
                change_counts=change_counts,
            )
        )

    domain_items = sorted(domain_items, key=lambda item: (-int(item["priority_score"]), -int(item["open_incidents"]), str(item["label"])))[:_ENTITY_LIMIT]

    product_items: list[dict[str, object]] = []
    for product in products:
        domain = product.domain
        domain_label = domain.name if domain else "Sem domínio"
        domain_slug = domain.slug if domain else None
        domain_profiles = grouped_profiles.get(domain_label, [])
        product_profile_ids = {
            int(link.entity_id)
            for link in product.links
            if link.entity_kind == "table" and link.entity_id is not None
        }
        product_profiles = [profile for profile in profiles if profile.table_id in product_profile_ids]
        product_items.append(
            _product_item(
                product,
                domain_label=domain_label,
                domain_slug=domain_slug,
                domain_profiles=domain_profiles,
                product_profiles=product_profiles,
                recent_incident_map=recent_incident_map,
                recent_occurrence_map=recent_occurrence_map,
                change_counts=change_counts,
            )
        )
    product_items = sorted(product_items, key=lambda item: (-int(item["priority_score"]), -int(item["open_incidents"]), str(item["label"])))[:_ENTITY_LIMIT]

    pipeline_items_raw = list(ingestion_summary.get("items", []) if ingestion_summary else [])
    pipeline_items: list[dict[str, object]] = []
    for raw_item in pipeline_items_raw:
        table_id = raw_item.get("table_id")
        profile = profile_by_id.get(int(table_id)) if table_id is not None else None
        pipeline_items.append(
            _pipeline_item(
                raw_item,
                table_profile=profile,
                recent_incident_count=recent_incident_map.get(str(raw_item.get("table_fqn") or ""), 0),
                recent_occurrences=recent_occurrence_map.get(str(raw_item.get("table_fqn") or ""), 0),
                search_clicks_30d=profile.search_clicks_30d if profile else 0,
            )
        )
    pipeline_items = sorted(pipeline_items, key=lambda item: (-int(item["priority_score"]), -int(item["open_incidents"]), str(item["label"])))[:_ENTITY_LIMIT]

    top_asset_items = asset_items[:_ENTITY_LIMIT]
    alert_candidates = [_alert_from_item(item) for item in [*top_asset_items, *domain_items, *product_items, *pipeline_items]]
    alerts = [item for item in alert_candidates if item is not None][: _ALERT_LIMIT]

    trend_since = change_window_start
    incident_rows = session.execute(
        select(func.date(Incident.detected_at).label("day"), func.count(Incident.id).label("value"))
        .where(
            Incident.entity_type == "table",
            Incident.detected_at >= trend_since,
        )
        .group_by(func.date(Incident.detected_at))
        .order_by(func.date(Incident.detected_at))
    ).all()
    dq_failure_rows = session.execute(
        select(func.date(DQRun.created_at).label("day"), func.count(DQRun.id).label("value"))
        .where(
            DQRun.created_at >= trend_since,
            DQRun.status != "success",
            DQRun.table_id.in_([profile.table_id for profile in profiles] or [0]),
        )
        .group_by(func.date(DQRun.created_at))
        .order_by(func.date(DQRun.created_at))
    ).all()
    trend = _trend_series(
        since=trend_since,
        incident_rows=incident_rows,
        dq_failure_rows=dq_failure_rows,
        critical_changes=critical_changes,
    )

    high_risk_assets = sum(1 for item in asset_items if int(item["priority_score"]) >= 70)
    high_risk_domains = sum(1 for item in domain_items if int(item["priority_score"]) >= 70)
    high_risk_products = sum(1 for item in product_items if int(item["priority_score"]) >= 70)
    unstable_pipelines = sum(1 for item in pipeline_items if int(item["priority_score"]) >= 70)
    deteriorating_assets = sum(
        1
        for item in asset_items
        if int(item["recent_dq_failure_runs_30d"]) > 0
        or int(item["change_events_30d"]) > 0
        or int(item["priority_score"]) >= 60
    )
    recurring_instability = sum(
        1
        for item in [*asset_items, *domain_items, *product_items, *pipeline_items]
        if int(item["recent_incidents_30d"]) >= 2 or int(item["recent_dq_failure_runs_30d"]) >= 2 or int(item["failed_pipelines"]) >= 2
    )
    suggested_incidents = sum(1 for item in [*asset_items, *domain_items, *product_items, *pipeline_items] if item["suggested_incident"])
    priority_queue_size = sum(1 for item in [*asset_items, *domain_items, *product_items, *pipeline_items] if int(item["priority_score"]) >= 60)

    return {
        "generated_at": now.isoformat(),
        "window_days": window_days,
        "evaluated_assets": len(profiles),
        "priority_queue_size": priority_queue_size,
        "high_risk_assets": high_risk_assets,
        "high_risk_domains": high_risk_domains,
        "high_risk_products": high_risk_products,
        "unstable_pipelines": unstable_pipelines,
        "deteriorating_assets": deteriorating_assets,
        "recurring_instability": recurring_instability,
        "suggested_incidents": suggested_incidents,
        "by_asset": top_asset_items,
        "by_domain": domain_items,
        "by_product": product_items,
        "by_pipeline": pipeline_items,
        "alerts": alerts,
        "trend": trend,
    }
