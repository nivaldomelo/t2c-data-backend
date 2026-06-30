from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean
from threading import Lock

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.dashboard.support import TableProfile, round_pct
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.models.auth import User
from t2c_data.models.catalog import TableEntity
from t2c_data.models.dq import DQRun
from t2c_data.models.governance import GovernanceTrustSnapshot
from t2c_data.models.incident import Incident
from t2c_data.models.platform import PlatformUsageEvent
from t2c_data.models.search import SearchQueryHistory, SearchResultClick
from t2c_data.models.semantic import SemanticDataProduct, SemanticDomain, SemanticLink

_STRATEGIC_SUMMARY_CACHE_LOCK = Lock()
_STRATEGIC_SUMMARY_CACHE_TTL_SECONDS = 60
_STRATEGIC_SUMMARY_CACHE: dict[tuple[object, ...], tuple[datetime, dict[str, object]]] = {}

_DOC_PENALTY_KEYS = {"no_description", "no_dictionary", "no_terms", "no_tags"}


def _clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def _normalized(value: str | None) -> str:
    return str(value or "").strip()


def _maturity_label(score: float) -> str:
    if score >= 85:
        return "Líder"
    if score >= 70:
        return "Governado"
    if score >= 55:
        return "Confiável"
    if score >= 40:
        return "Gerenciado"
    return "Inicial"


def _tone(score: float) -> str:
    if score >= 85:
        return "success"
    if score >= 70:
        return "accent"
    if score >= 50:
        return "warning"
    return "danger"


def _delta_tone(current: float, previous: float, *, reverse: bool = False) -> str:
    if current == previous:
        return "neutral"
    improved = current > previous if not reverse else current < previous
    return "success" if improved else "danger"


def _score_from_components(*, quality: float, governance: float, coverage: float, reliability: float, adoption: float, incidents: float) -> float:
    return _clamp(
        (quality * 0.22)
        + (governance * 0.20)
        + (coverage * 0.18)
        + (reliability * 0.16)
        + (adoption * 0.14)
        - min(18.0, incidents * 1.5),
    )


def _window_bounds(days: int) -> tuple[datetime, datetime, datetime]:
    now = datetime.now(timezone.utc)
    current_since = now - timedelta(days=max(days, 1))
    previous_since = current_since - timedelta(days=max(days, 1))
    return now, current_since, previous_since


def _latest_snapshots(session: Session, *, since: datetime, until: datetime, table_ids: set[int] | None = None) -> list[GovernanceTrustSnapshot]:
    if table_ids is not None and not table_ids:
        return []
    query = (
        select(GovernanceTrustSnapshot)
        .where(GovernanceTrustSnapshot.bucket_date >= since, GovernanceTrustSnapshot.bucket_date < until)
        .order_by(GovernanceTrustSnapshot.bucket_date.asc())
    )
    if table_ids:
        query = query.where(GovernanceTrustSnapshot.table_id.in_(sorted(table_ids)))
    rows = session.scalars(query).all()
    latest: dict[int, GovernanceTrustSnapshot] = {}
    for row in rows:
        current = latest.get(row.table_id)
        if current is None or (row.bucket_date and current.bucket_date and row.bucket_date > current.bucket_date):
            latest[row.table_id] = row
    return list(latest.values())


def _snapshot_metrics(rows: list[GovernanceTrustSnapshot]) -> dict[str, float]:
    if not rows:
        return {
            "value_score": 0.0,
            "trust_score": 0.0,
            "quality_score": 0.0,
            "governance_score": 0.0,
            "coverage_score": 0.0,
            "reliability_score": 0.0,
            "owner_coverage": 0.0,
            "documentation_coverage": 0.0,
            "incident_total": 0.0,
            "critical_incident_total": 0.0,
            "dq_failure_runs": 0.0,
        }

    def penalties(row: GovernanceTrustSnapshot) -> set[str]:
        context = row.trust_context_json or {}
        items = context.get("penalties") or []
        return {
            str(item.get("key") or "").strip()
            for item in items
            if isinstance(item, dict) and str(item.get("key") or "").strip()
        }

    def documentation_covered(row: GovernanceTrustSnapshot) -> bool:
        keys = penalties(row)
        return not bool(keys.intersection(_DOC_PENALTY_KEYS))

    def owner_covered(row: GovernanceTrustSnapshot) -> bool:
        return "no_owner" not in penalties(row)

    trust_score = round(mean(float(row.score or 0) for row in rows), 1)
    quality_score = round(mean(float(row.dq_score or 0) for row in rows), 1)
    governance_score = round(mean(float(row.governance_score or 0) for row in rows), 1)
    coverage_score = round(mean(float(row.readiness_score or 0) for row in rows), 1)
    reliability_score = round(mean(float(row.operational_score or 0) for row in rows), 1)
    owner_coverage = round_pct(sum(1 for row in rows if owner_covered(row)), len(rows))
    documentation_coverage = round_pct(sum(1 for row in rows if documentation_covered(row)), len(rows))
    incident_total = float(sum(int(row.open_incidents or 0) for row in rows))
    critical_incident_total = float(sum(int(row.critical_open_incidents or 0) for row in rows))
    dq_failure_runs = round(mean(float(row.recent_dq_failure_runs_30d or 0) for row in rows), 1)
    adoption_score = _clamp(100 - min(100.0, dq_failure_runs * 6 + incident_total * 1.2))
    value_score = round(
        _score_from_components(
            quality=quality_score,
            governance=governance_score,
            coverage=coverage_score,
            reliability=reliability_score,
            adoption=adoption_score,
            incidents=min(100.0, incident_total * 2.0 + critical_incident_total * 4.0),
        ),
        1,
    )
    return {
        "value_score": value_score,
        "trust_score": trust_score,
        "quality_score": quality_score,
        "governance_score": governance_score,
        "coverage_score": coverage_score,
        "reliability_score": reliability_score,
        "owner_coverage": owner_coverage,
        "documentation_coverage": documentation_coverage,
        "incident_total": incident_total,
        "critical_incident_total": critical_incident_total,
        "dq_failure_runs": dq_failure_runs,
    }


def _trend_from_rows(rows: list[GovernanceTrustSnapshot], *, selector) -> list[dict[str, object]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        if row.bucket_date is None:
            continue
        grouped[row.bucket_date.date().isoformat()].append(float(selector(row)))
    return [
        {"label": day[5:] if len(day) >= 10 else day, "value": round(mean(values), 1)}
        for day, values in sorted(grouped.items())
        if values
    ]


def _daily_usage_trend(session: Session, *, since: datetime) -> list[dict[str, object]]:
    rows = session.execute(
        select(
            func.date(PlatformUsageEvent.created_at).label("day"),
            func.count(PlatformUsageEvent.id).label("usage_events"),
            func.count(func.distinct(PlatformUsageEvent.user_id)).label("active_users"),
        )
        .where(PlatformUsageEvent.created_at >= since)
        .group_by(func.date(PlatformUsageEvent.created_at))
    ).all()
    search_rows = session.execute(
        select(
            func.date(SearchQueryHistory.last_searched_at).label("day"),
            func.count(SearchQueryHistory.id).label("search_queries"),
            func.sum(SearchQueryHistory.search_count).label("search_count"),
        )
        .where(SearchQueryHistory.last_searched_at >= since)
        .group_by(func.date(SearchQueryHistory.last_searched_at))
    ).all()
    clicks_rows = session.execute(
        select(
            func.date(SearchResultClick.created_at).label("day"),
            func.count(SearchResultClick.id).label("search_clicks"),
        )
        .where(SearchResultClick.created_at >= since)
        .group_by(func.date(SearchResultClick.created_at))
    ).all()

    buckets: dict[str, dict[str, float]] = {}
    for day, usage_events, active_users in rows:
        if day is None:
            continue
        bucket = buckets.setdefault(str(day), {"label": str(day)[5:], "usage_events": 0.0, "search_queries": 0.0, "search_clicks": 0.0, "active_users": 0.0})
        bucket["usage_events"] += float(usage_events or 0)
        bucket["active_users"] = max(bucket["active_users"], float(active_users or 0))
    for day, search_queries, search_count in search_rows:
        if day is None:
            continue
        bucket = buckets.setdefault(str(day), {"label": str(day)[5:], "usage_events": 0.0, "search_queries": 0.0, "search_clicks": 0.0, "active_users": 0.0})
        bucket["search_queries"] += float(search_count or search_queries or 0)
    for day, search_clicks in clicks_rows:
        if day is None:
            continue
        bucket = buckets.setdefault(str(day), {"label": str(day)[5:], "usage_events": 0.0, "search_queries": 0.0, "search_clicks": 0.0, "active_users": 0.0})
        bucket["search_clicks"] += float(search_clicks or 0)

    return [
        {
            "label": bucket["label"],
            "value": round(bucket["usage_events"] + bucket["search_queries"] + bucket["search_clicks"], 1),
        }
        for _day, bucket in sorted(buckets.items())
    ]


def _platform_activity(session: Session, *, since: datetime, until: datetime) -> dict[str, object]:
    usage_rows = session.execute(
        select(PlatformUsageEvent.user_id, func.count(PlatformUsageEvent.id))
        .where(PlatformUsageEvent.created_at >= since, PlatformUsageEvent.created_at < until)
        .group_by(PlatformUsageEvent.user_id)
    ).all()
    query_rows = session.execute(
        select(SearchQueryHistory.user_id, func.sum(SearchQueryHistory.search_count))
        .where(SearchQueryHistory.last_searched_at >= since, SearchQueryHistory.last_searched_at < until)
        .group_by(SearchQueryHistory.user_id)
    ).all()
    user_activity: dict[int, int] = defaultdict(int)
    for user_id, count in usage_rows:
        if user_id is not None:
            user_activity[int(user_id)] += int(count or 0)
    for user_id, count in query_rows:
        if user_id is not None:
            user_activity[int(user_id)] += int(count or 0)

    users = (
        session.execute(select(User.id, User.name, User.full_name, User.email).where(User.id.in_(sorted(user_activity.keys()) or [-1]))).all()
        if user_activity
        else []
    )
    user_lookup = {
        int(row.id): str(row.name or row.full_name or row.email or f"Usuário {row.id}")
        for row in users
    }
    top_users = sorted(
        (
            {
                "user_id": user_id,
                "label": user_lookup.get(user_id, f"Usuário {user_id}"),
                "total_count": total,
                "usage_count": total,
                "search_count": next((int(count or 0) for query_user_id, count in query_rows if query_user_id == user_id), 0),
            }
            for user_id, total in user_activity.items()
        ),
        key=lambda item: (-int(item["total_count"]), str(item["label"])),
    )[:6]
    return {
        "active_users": len(user_activity),
        "top_users": top_users,
    }


def _group_profiles_by_label(profiles: list[TableProfile], *, label_getter) -> dict[str, list[TableProfile]]:
    grouped: dict[str, list[TableProfile]] = defaultdict(list)
    for profile in profiles:
        label = _normalized(label_getter(profile))
        if not label:
            continue
        grouped[label].append(profile)
    return grouped


def _compute_group_item(
    label: str,
    profiles: list[TableProfile],
    *,
    href: str | None = None,
    adoption_count: float = 0.0,
) -> dict[str, object]:
    if not profiles:
        return {
            "key": label.lower().replace(" ", "-"),
            "label": label,
            "href": href,
            "asset_count": 0,
            "quality_score": 0.0,
            "governance_score": 0.0,
            "coverage_score": 0.0,
            "reliability_score": 0.0,
            "adoption_count": round(adoption_count, 1),
            "adoption_score": 0.0,
            "open_incidents": 0,
            "critical_open_incidents": 0,
            "maturity_score": 0.0,
            "maturity_label": "Inicial",
            "tone": "danger",
        }

    quality = round(mean(float(profile.dq_score or 0) for profile in profiles), 1)
    governance = round(mean(float(profile.trust_score or 0) for profile in profiles), 1)
    coverage = round(mean(float(profile.readiness_score or 0) for profile in profiles), 1)
    reliability = round(mean(float(profile.documentation_score or 0) for profile in profiles), 1)
    incidents = round(sum(float(profile.open_incidents or 0) for profile in profiles), 1)
    critical_incidents = round(sum(float(profile.critical_open_incidents or 0) for profile in profiles), 1)
    adoption_score = _clamp(adoption_count * 6.0)
    maturity = round(
        _score_from_components(
            quality=quality,
            governance=governance,
            coverage=coverage,
            reliability=reliability,
            adoption=adoption_score,
            incidents=min(100.0, incidents * 1.5 + critical_incidents * 4),
        ),
        1,
    )
    return {
        "key": label.lower().replace(" ", "-"),
        "label": label,
        "href": href,
        "asset_count": len(profiles),
        "quality_score": quality,
        "governance_score": governance,
        "coverage_score": coverage,
        "reliability_score": reliability,
        "adoption_count": round(adoption_count, 1),
        "adoption_score": round(adoption_score, 1),
        "open_incidents": int(round(incidents)),
        "critical_open_incidents": int(round(critical_incidents)),
        "maturity_score": maturity,
        "maturity_label": _maturity_label(maturity),
        "tone": _tone(maturity),
    }


def _domain_href_map(session: Session) -> dict[str, str]:
    return {
        str(domain.name).strip().lower(): f"/governance/domains/{domain.slug}"
        for domain in session.scalars(select(SemanticDomain)).all()
        if domain.name and domain.slug
    }


def _product_benchmark_items(
    session: Session,
    profiles_by_table_id: dict[int, TableProfile],
    *,
    visible_table_ids: set[int],
) -> list[dict[str, object]]:
    domain_href_map = _domain_href_map(session)
    products = session.scalars(
        select(SemanticDataProduct)
        .options(selectinload(SemanticDataProduct.domain), selectinload(SemanticDataProduct.links))
        .order_by(SemanticDataProduct.name)
    ).all()
    items: list[dict[str, object]] = []
    for product in products:
        linked_ids = {
            int(link.entity_id)
            for link in product.links
            if link.entity_kind == "table" and link.entity_id is not None and int(link.entity_id) in visible_table_ids
        }
        linked_profiles = [profiles_by_table_id[table_id] for table_id in linked_ids if table_id in profiles_by_table_id]
        adoption_count = sum(float(profile.search_clicks_30d or 0) for profile in linked_profiles)
        current_quality = float(product.quality_score or (mean([profile.dq_score or 0 for profile in linked_profiles]) if linked_profiles else 0))
        current_governance = float(product.governance_score or (mean([profile.trust_score or 0 for profile in linked_profiles]) if linked_profiles else 0))
        current_coverage = float(mean([profile.readiness_score or 0 for profile in linked_profiles])) if linked_profiles else float(
            product.quality_score or 0
        )
        current_reliability = float(mean([profile.documentation_score or 0 for profile in linked_profiles])) if linked_profiles else 0
        incident_total = sum(float(profile.open_incidents or 0) for profile in linked_profiles)
        critical_total = sum(float(profile.critical_open_incidents or 0) for profile in linked_profiles)
        adoption_score = _clamp(adoption_count * 4.0)
        maturity = round(
            _score_from_components(
                quality=current_quality,
                governance=current_governance,
                coverage=current_coverage,
                reliability=current_reliability,
                adoption=adoption_score,
                incidents=min(100.0, incident_total * 1.5 + critical_total * 4),
            ),
            1,
        )
        items.append(
            {
                "key": product.slug,
                "label": product.name,
                "href": f"/governance/data-products/{product.slug}",
                "asset_count": len(linked_profiles),
                "quality_score": round(current_quality, 1),
                "governance_score": round(current_governance, 1),
                "coverage_score": round(current_coverage, 1),
                "reliability_score": round(current_reliability, 1),
                "adoption_count": round(adoption_count, 1),
                "adoption_score": round(adoption_score, 1),
                "open_incidents": int(round(incident_total)),
                "critical_open_incidents": int(round(critical_total)),
                "maturity_score": maturity,
                "maturity_label": _maturity_label(maturity),
                "tone": _tone(maturity),
                "domain_name": product.domain.name if product.domain else None,
                "domain_href": domain_href_map.get((product.domain.name or "").strip().lower()) if product.domain else None,
            }
        )
    return sorted(items, key=lambda item: (-float(item["maturity_score"]), -float(item["adoption_score"]), str(item["label"])))


def _domain_benchmark_items(
    profiles: list[TableProfile],
    *,
    domain_href_map: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    grouped = _group_profiles_by_label(profiles, label_getter=lambda profile: profile.domain_name)
    items: list[dict[str, object]] = []
    for label, group in grouped.items():
        href = domain_href_map.get(label.lower()) if domain_href_map else None
        items.append(
            _compute_group_item(
                label,
                group,
                href=href,
                adoption_count=sum(float(profile.search_clicks_30d or 0) for profile in group),
            )
        )
    return sorted(items, key=lambda item: (-float(item["maturity_score"]), -float(item["adoption_score"]), str(item["label"])))


def _area_benchmark_items(
    session: Session,
    profiles: list[TableProfile],
    *,
    visible_table_ids: set[int],
) -> list[dict[str, object]]:
    tables = session.scalars(
        select(TableEntity)
        .options(selectinload(TableEntity.data_owner))
        .where(TableEntity.id.in_(sorted(visible_table_ids) or [-1]))
    ).all()
    profiles_by_id = {profile.table_id: profile for profile in profiles}
    grouped: dict[str, list[TableProfile]] = defaultdict(list)
    for table in tables:
        profile = profiles_by_id.get(table.id)
        if profile is None:
            continue
        area = _normalized(table.data_owner.area if table.data_owner else None) or "Sem área"
        grouped[area].append(profile)
    items = [
        _compute_group_item(area, group, href="/data-owners", adoption_count=sum(float(profile.search_clicks_30d or 0) for profile in group))
        for area, group in grouped.items()
    ]
    return sorted(items, key=lambda item: (-float(item["maturity_score"]), -float(item["adoption_score"]), str(item["label"])))


def _latest_window_rows(session: Session, *, since: datetime, until: datetime, table_ids: set[int]) -> list[GovernanceTrustSnapshot]:
    rows = _latest_snapshots(session, since=since, until=until, table_ids=table_ids)
    return rows


def _metric_payload(
    *,
    key: str,
    label: str,
    current: float,
    previous: float,
    unit: str | None,
    hint: str,
    reverse_trend: bool = False,
) -> dict[str, object]:
    delta = round(current - previous, 1)
    return {
        "key": key,
        "label": label,
        "current": round(current, 1),
        "previous": round(previous, 1),
        "delta": delta,
        "unit": unit,
        "hint": hint,
        "reverse_trend": reverse_trend,
        "tone": _delta_tone(current, previous, reverse=reverse_trend),
    }


def build_platform_strategic_summary(session: Session, *, days: int = 30, current_user: User | None = None) -> dict[str, object]:
    now, current_since, previous_since = _window_bounds(days)
    cacheable = True
    cache_key = (
        getattr(current_user, "id", None),
        days,
    )
    if cacheable:
        with _STRATEGIC_SUMMARY_CACHE_LOCK:
            cached = _STRATEGIC_SUMMARY_CACHE.get(cache_key)
            if cached and cached[0] > now:
                return cached[1]

    tables, _source = load_dashboard_profiles_with_fallback(session, now, current_user=current_user)
    visible_table_ids = {table.table_id for table in tables}
    profiles_by_id = {table.table_id: table for table in tables}
    domain_href_map = _domain_href_map(session)

    current_rows = _latest_window_rows(session, since=current_since, until=now, table_ids=visible_table_ids)
    previous_rows = _latest_window_rows(session, since=previous_since, until=current_since, table_ids=visible_table_ids)
    current_metrics = _snapshot_metrics(current_rows)
    previous_metrics = _snapshot_metrics(previous_rows)

    incident_current = int(
        session.scalar(
            select(func.count(Incident.id)).where(Incident.detected_at >= current_since, Incident.detected_at < now)
        )
        or 0
    )
    incident_previous = int(
        session.scalar(
            select(func.count(Incident.id)).where(Incident.detected_at >= previous_since, Incident.detected_at < current_since)
        )
        or 0
    )
    dq_failure_current = int(
        session.scalar(
            select(func.count(DQRun.id)).where(DQRun.created_at >= current_since, DQRun.created_at < now, DQRun.status != "success")
        )
        or 0
    )
    dq_failure_previous = int(
        session.scalar(
            select(func.count(DQRun.id)).where(
                DQRun.created_at >= previous_since, DQRun.created_at < current_since, DQRun.status != "success"
            )
        )
        or 0
    )

    usage_current = _platform_activity(session, since=current_since, until=now)
    usage_previous = _platform_activity(session, since=previous_since, until=current_since)

    adoption_trend = _daily_usage_trend(session, since=current_since)
    value_trend = _trend_from_rows(current_rows, selector=lambda row: row.score)
    quality_trend = _trend_from_rows(current_rows, selector=lambda row: row.dq_score or 0.0)
    governance_trend = _trend_from_rows(current_rows, selector=lambda row: row.governance_score or 0.0)

    top_domains = _domain_benchmark_items(tables, domain_href_map=domain_href_map)
    top_areas = _area_benchmark_items(session, tables, visible_table_ids=visible_table_ids)
    top_products = _product_benchmark_items(session, profiles_by_id, visible_table_ids=visible_table_ids)

    total_asset_clicks = sum(float(profile.search_clicks_30d or 0) for profile in tables)
    active_domains = sum(1 for item in top_domains if float(item["adoption_count"]) > 0)
    active_areas = sum(1 for item in top_areas if float(item["adoption_count"]) > 0)
    active_products = sum(1 for item in top_products if float(item["adoption_count"]) > 0)
    active_users = int(usage_current["active_users"])

    adoption_sorted_domains = sorted(top_domains, key=lambda item: (-float(item["adoption_score"]), -float(item["asset_count"]), str(item["label"])))
    adoption_sorted_areas = sorted(top_areas, key=lambda item: (-float(item["adoption_score"]), -float(item["asset_count"]), str(item["label"])))
    adoption_sorted_products = sorted(top_products, key=lambda item: (-float(item["adoption_score"]), -float(item["asset_count"]), str(item["label"])))
    low_adoption_areas = list(reversed(adoption_sorted_areas))[:5]

    current_value_score = _clamp(
        mean(
            [
                current_metrics["quality_score"],
                current_metrics["owner_coverage"],
                current_metrics["documentation_coverage"],
                current_metrics["reliability_score"],
                current_metrics["trust_score"],
                max(0.0, 100.0 - min(100.0, current_metrics["incident_total"] * 2.0 + current_metrics["critical_incident_total"] * 4.0)),
            ]
        )
    )
    previous_value_score = _clamp(
        mean(
            [
                previous_metrics["quality_score"],
                previous_metrics["owner_coverage"],
                previous_metrics["documentation_coverage"],
                previous_metrics["reliability_score"],
                previous_metrics["trust_score"],
                max(0.0, 100.0 - min(100.0, previous_metrics["incident_total"] * 2.0 + previous_metrics["critical_incident_total"] * 4.0)),
            ]
        )
    )

    value_metrics = [
        _metric_payload(
            key="incidents",
            label="Redução de incidentes",
            current=current_metrics["incident_total"],
            previous=previous_metrics["incident_total"],
            unit="incidentes",
            hint="Ocorrências registradas nas janelas comparadas.",
            reverse_trend=True,
        ),
        _metric_payload(
            key="quality",
            label="Cobertura de qualidade",
            current=current_metrics["quality_score"],
            previous=previous_metrics["quality_score"],
            unit="pts",
            hint="Média de DQ das tabelas com leitura operacional.",
        ),
        _metric_payload(
            key="ownership",
            label="Ownership",
            current=current_metrics["owner_coverage"],
            previous=previous_metrics["owner_coverage"],
            unit="%",
            hint="Proporção de ativos com owner definido.",
        ),
        _metric_payload(
            key="documentation",
            label="Documentação",
            current=current_metrics["documentation_coverage"],
            previous=previous_metrics["documentation_coverage"],
            unit="%",
            hint="Ativos sem penalidades de documentação na janela.",
        ),
        _metric_payload(
            key="dq_failures",
            label="Falhas recorrentes",
            current=current_metrics["dq_failure_runs"],
            previous=previous_metrics["dq_failure_runs"],
            unit="runs",
            hint="Média de falhas DQ por ativo.",
            reverse_trend=True,
        ),
        _metric_payload(
            key="reliability",
            label="Confiabilidade",
            current=current_metrics["trust_score"],
            previous=previous_metrics["trust_score"],
            unit="pts",
            hint="Média do trust score da plataforma.",
        ),
    ]

    reports = {
        "maturity_by_domain": sorted(top_domains, key=lambda item: (-float(item["maturity_score"]), -float(item["adoption_score"]), str(item["label"]))),
        "reliability_by_domain": sorted(top_domains, key=lambda item: (-float(item["reliability_score"]), -float(item["adoption_score"]), str(item["label"]))),
        "quality_by_domain": sorted(top_domains, key=lambda item: (-float(item["quality_score"]), -float(item["adoption_score"]), str(item["label"]))),
        "governance_by_domain": sorted(top_domains, key=lambda item: (-float(item["governance_score"]), -float(item["adoption_score"]), str(item["label"]))),
        "coverage_by_domain": sorted(top_domains, key=lambda item: (-float(item["coverage_score"]), -float(item["adoption_score"]), str(item["label"]))),
        "value_trend": value_trend,
        "quality_trend": quality_trend,
        "governance_trend": governance_trend,
        "adoption_trend": adoption_trend,
    }

    roadmap_sources = [
        ("initial", "Inicial", "A plataforma já existe, mas com cobertura e responsabilização ainda fragmentadas."),
        ("managed", "Gerenciado", "Há owner, documentação e qualidade básicos para os principais ativos."),
        ("reliable", "Confiável", "Os domínios críticos têm qualidade, governança e incidentes sob controle."),
        ("governed", "Governado", "As mudanças são rastreáveis e os produtos de dados têm contratos e impacto visível."),
        ("product", "Orientado a produto", "A plataforma opera por domínios e produtos com métricas de valor e adoção contínuas."),
    ]
    roadmap: list[dict[str, object]] = []
    domain_scores = [float(item["maturity_score"]) for item in top_domains]
    for key, label, description in roadmap_sources:
        if key == "initial":
            count = sum(1 for score in domain_scores if score < 40)
            minimum_score = 0
            criteria = ["Owner e documentação ainda incompletos", "Baixa cobertura de DQ", "Pouco vínculo com produtos"]
        elif key == "managed":
            count = sum(1 for score in domain_scores if 40 <= score < 55)
            minimum_score = 40
            criteria = ["Ownership básico definido", "Cobertura mínima de documentação", "Primeiros vínculos de produto"]
        elif key == "reliable":
            count = sum(1 for score in domain_scores if 55 <= score < 70)
            minimum_score = 55
            criteria = ["Qualidade e confiabilidade consistentes", "Incidentes concentrados sob controle", "Maturidade operacional recorrente"]
        elif key == "governed":
            count = sum(1 for score in domain_scores if 70 <= score < 85)
            minimum_score = 70
            criteria = ["Contratos e lineage ativos", "Mudança e impacto visíveis", "Governança distribuída"]
        else:
            count = sum(1 for score in domain_scores if score >= 85)
            minimum_score = 85
            criteria = ["Produto de dados com operação autônoma", "Benchmark acima da média", "Uso e valor organizacional explícitos"]
        roadmap.append(
            {
                "key": key,
                "label": label,
                "description": description,
                "criteria": criteria,
                "minimum_score": minimum_score,
                "current_count": count,
                "current_pct": round_pct(count, max(len(domain_scores), 1)),
                "tone": _tone(minimum_score or 0),
            }
        )

    narrative = [
        f"Valor estratégico da plataforma: {current_value_score:.1f} pontos, contra {previous_value_score:.1f} no período anterior.",
        f"Domínio mais maduro: {top_domains[0]['label'] if top_domains else 'Sem dados suficientes'}.",
        f"Área com menor adoção: {low_adoption_areas[0]['label'] if low_adoption_areas else 'Sem dados suficientes'}.",
        f"Usuários mais ativos na janela: {active_users}.",
        f"Produtos de dados monitorados: {len(top_products)}.",
    ]

    payload = {
        "generated_at": now,
        "window_days": days,
        "value_score": round(current_value_score, 1),
        "value_score_previous": round(previous_value_score, 1),
        "value_score_delta": round(current_value_score - previous_value_score, 1),
        "value_metrics": value_metrics,
        "adoption": {
            "active_users": active_users,
            "active_domains": active_domains,
            "active_areas": active_areas,
            "active_products": active_products,
            "top_users": sorted(usage_current["top_users"], key=lambda item: (-int(item["total_count"]), str(item["label"]))),
            "top_domains": adoption_sorted_domains[:6],
            "top_areas": adoption_sorted_areas[:6],
            "top_products": adoption_sorted_products[:6],
            "low_adoption_areas": low_adoption_areas,
            "top_assets": [
                {
                    "table_id": profile.table_id,
                    "table_name": profile.table_name,
                    "table_fqn": profile.table_fqn,
                    "domain_name": profile.domain_name or "Sem domínio",
                    "owner_name": profile.owner_name or "Sem owner",
                    "adoption_score": round(min(100.0, float(profile.search_clicks_30d or 0) * 5.0), 1),
                    "search_clicks_30d": int(profile.search_clicks_30d or 0),
                    "dq_score": round(float(profile.dq_score or 0), 1),
                    "trust_score": int(profile.trust_score or 0),
                }
                for profile in sorted(
                    tables,
                    key=lambda item: (-int(item.search_clicks_30d or 0), -int(item.trust_score or 0), item.table_fqn),
                )[:8]
            ],
        },
        "reports": reports,
        "benchmark": {
            "by_domain": top_domains,
            "by_area": top_areas,
            "by_product": top_products,
        },
        "roadmap": roadmap,
        "narrative": narrative,
    }

    if cacheable:
        with _STRATEGIC_SUMMARY_CACHE_LOCK:
            _STRATEGIC_SUMMARY_CACHE[cache_key] = (
                now + timedelta(seconds=_STRATEGIC_SUMMARY_CACHE_TTL_SECONDS),
                payload,
            )
    return payload
