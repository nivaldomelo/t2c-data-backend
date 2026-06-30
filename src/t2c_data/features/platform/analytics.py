from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.json_utils import to_jsonable
from t2c_data.features.privacy_access import can_view_table
from t2c_data.features.platform.events import record_platform_domain_event_from_usage, should_emit_platform_usage_domain_event
from t2c_data.models.auth import User
from t2c_data.models.audit import AccessLog, AccessLogArchive
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.platform import PlatformUsageEvent, SearchReadModel
from t2c_data.models.search import SearchQueryHistory, SearchResultClick


def legacy_api_usage_stats_by_module(session: Session, *, days: int = 30) -> dict[str, dict[str, object]]:
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=max(days, 1))

    def _merge_counts(rows: list[tuple[object, object]], key: str, target: dict[str, dict[str, object]]) -> None:
        for label, value in rows:
            if not label:
                continue
            module = str(label)
            bucket = target.setdefault(module, {"hits_total": 0, "hits_in_window": 0, "last_hit_at": None})
            bucket[key] = int(bucket.get(key, 0)) + int(value or 0)

    def _merge_last_seen(rows: list[tuple[object, object]], target: dict[str, dict[str, object]]) -> None:
        for label, value in rows:
            if not label:
                continue
            module = str(label)
            bucket = target.setdefault(module, {"hits_total": 0, "hits_in_window": 0, "last_hit_at": None})
            current = bucket.get("last_hit_at")
            if value is not None and (current is None or value > current):
                bucket["last_hit_at"] = value

    stats: dict[str, dict[str, object]] = {}
    _merge_counts(
        session.execute(
            select(AccessLog.module_name, func.count(AccessLog.id))
            .where(AccessLog.api_version == "legacy")
            .group_by(AccessLog.module_name)
        ).all(),
        "hits_total",
        stats,
    )
    _merge_counts(
        session.execute(
            select(AccessLogArchive.module_name, func.count(AccessLogArchive.id))
            .where(AccessLogArchive.api_version == "legacy")
            .group_by(AccessLogArchive.module_name)
        ).all(),
        "hits_total",
        stats,
    )
    _merge_counts(
        session.execute(
            select(AccessLog.module_name, func.count(AccessLog.id))
            .where(AccessLog.created_at >= since, AccessLog.api_version == "legacy")
            .group_by(AccessLog.module_name)
        ).all(),
        "hits_in_window",
        stats,
    )
    _merge_counts(
        session.execute(
            select(AccessLogArchive.module_name, func.count(AccessLogArchive.id))
            .where(AccessLogArchive.created_at >= since, AccessLogArchive.api_version == "legacy")
            .group_by(AccessLogArchive.module_name)
        ).all(),
        "hits_in_window",
        stats,
    )
    _merge_last_seen(
        session.execute(
            select(AccessLog.module_name, func.max(AccessLog.created_at))
            .where(AccessLog.api_version == "legacy")
            .group_by(AccessLog.module_name)
        ).all(),
        stats,
    )
    _merge_last_seen(
        session.execute(
            select(AccessLogArchive.module_name, func.max(AccessLogArchive.created_at))
            .where(AccessLogArchive.api_version == "legacy")
            .group_by(AccessLogArchive.module_name)
        ).all(),
        stats,
    )
    return stats


def legacy_api_usage_by_module(session: Session, *, days: int = 30) -> dict[str, int]:
    stats = legacy_api_usage_stats_by_module(session, days=days)
    return {module: int(payload.get("hits_in_window", 0)) for module, payload in stats.items()}


def track_usage_event(
    session: Session,
    *,
    user: User | None,
    event_name: str,
    module_name: str,
    page_path: str | None = None,
    entity_type: str | None = None,
    entity_id: int | None = None,
    target_url: str | None = None,
    metadata: dict | None = None,
) -> None:
    session.add(
        PlatformUsageEvent(
            user_id=getattr(user, "id", None),
            event_name=event_name,
            module_name=module_name,
            page_path=page_path,
            entity_type=entity_type,
            entity_id=entity_id,
            target_url=target_url,
            metadata_json=to_jsonable(metadata) if metadata else None,
        )
    )
    if should_emit_platform_usage_domain_event(event_name, module_name):
        try:
            record_platform_domain_event_from_usage(
                session,
                event_name=event_name,
                module_name=module_name,
                page_path=page_path,
                entity_type=entity_type,
                entity_id=entity_id,
                target_url=target_url,
                metadata=metadata,
            )
        except Exception:  # noqa: BLE001
            # Usage tracking remains best-effort; domain event emission must not block.
            pass


def _load_top_asset_rows(session: Session, *, since: datetime, limit: int = 10, current_user: User | None = None) -> list[dict[str, object]]:
    click_rows = session.execute(
        select(
            SearchResultClick.entity_type,
            SearchResultClick.entity_id,
            func.count(SearchResultClick.id).label("click_count"),
        )
        .where(SearchResultClick.created_at >= since)
        .group_by(SearchResultClick.entity_type, SearchResultClick.entity_id)
        .order_by(desc(func.count(SearchResultClick.id)))
        .limit(limit)
    ).all()
    if not click_rows:
        return []

    table_ids = [int(row.entity_id) for row in click_rows if str(row.entity_type) == "table" and row.entity_id is not None]
    table_meta: dict[int, dict[str, object]] = {}
    if table_ids:
        table_rows = session.execute(
            select(
                TableEntity.id,
                TableEntity.name,
                Schema.name.label("schema_name"),
                Database.name.label("database_name"),
                DataSource.name.label("source_name"),
            )
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .join(DataSource, Database.datasource_id == DataSource.id)
            .where(TableEntity.id.in_(table_ids))
        ).all()
        table_meta = {
            int(row.id): {
                "asset_name": str(row.name),
                "schema_name": str(row.schema_name),
                "database_name": str(row.database_name),
                "source_name": str(row.source_name),
                "qualified_name": f"{row.source_name}.{row.database_name}.{row.schema_name}.{row.name}",
            }
            for row in table_rows
        }
        if current_user is not None:
            visible_table_rows = session.scalars(
                select(TableEntity)
                .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
                .where(TableEntity.id.in_(table_ids))
            ).all()
            visible_table_ids = {table.id for table in visible_table_rows if can_view_table(current_user, table)}
            table_meta = {table_id: meta for table_id, meta in table_meta.items() if table_id in visible_table_ids}

    entity_types = sorted({str(row.entity_type) for row in click_rows if row.entity_type})
    entity_ids = sorted({int(row.entity_id) for row in click_rows if row.entity_id is not None})
    search_meta: dict[tuple[str, int], dict[str, object]] = {}
    if entity_types and entity_ids:
        search_rows = session.execute(
            select(
                SearchReadModel.entity_type,
                SearchReadModel.entity_id,
                SearchReadModel.title,
                SearchReadModel.source_name,
                SearchReadModel.database_name,
                SearchReadModel.schema_name,
            )
            .where(
                SearchReadModel.entity_type.in_(entity_types),
                SearchReadModel.entity_id.in_(entity_ids),
            )
        ).all()
        search_meta = {
            (str(row.entity_type), int(row.entity_id)): {
                "asset_name": str(row.title or "Ativo sem metadados"),
                "schema_name": row.schema_name,
                "database_name": row.database_name,
                "source_name": row.source_name,
                "qualified_name": ".".join(
                    part for part in [row.source_name, row.database_name, row.schema_name, row.title] if part
                )
                or str(row.title or "Ativo sem metadados"),
            }
            for row in search_rows
            if row.entity_type and row.entity_id is not None
        }
        if current_user is not None:
            search_meta = {
                key: value
                for key, value in search_meta.items()
                if key[0] == "table" and key[1] in table_meta
            }

    items: list[dict[str, object]] = []
    for row in click_rows[:limit]:
        asset_type = str(row.entity_type or "asset")
        asset_id = int(row.entity_id)
        meta = table_meta.get(asset_id) if asset_type == "table" else None
        if meta is None:
            meta = search_meta.get((asset_type, asset_id))
        if current_user is not None and meta is None:
            continue
        asset_name = str(meta.get("asset_name")) if meta else "Ativo sem metadados"
        schema_name = meta.get("schema_name") if meta else None
        source_name = meta.get("source_name") if meta else None
        qualified_name = str(meta.get("qualified_name")) if meta else asset_name
        if not qualified_name:
            qualified_name = asset_name
        items.append(
            {
                "asset_id": asset_id,
                "asset_type": asset_type,
                "asset_name": asset_name,
                "schema_name": str(schema_name) if schema_name else None,
                "qualified_name": qualified_name,
                "source_name": str(source_name) if source_name else None,
                "total_clicks": int(row.click_count or 0),
                "entity_type": asset_type,
                "entity_id": asset_id,
                "count": int(row.click_count or 0),
            }
        )
    return items


def _build_trend_series(session: Session, *, since: datetime) -> list[dict[str, object]]:
    trend_by_day: dict[str, dict[str, int]] = {}
    default_bucket = {
        "search_queries": 0,
        "search_clicks": 0,
        "usage_events": 0,
        "explorer_page_views": 0,
        "incidents_page_views": 0,
        "certification_page_views": 0,
        "privacy_page_views": 0,
        "legacy_api_hits": 0,
    }

    def _bucket(day_value: object) -> dict[str, int]:
        day_key = str(day_value)
        return trend_by_day.setdefault(
            day_key,
            {"label": day_key[5:] if len(day_key) >= 10 else day_key, **default_bucket},
        )

    for offset in range((datetime.now(timezone.utc).date() - since.date()).days + 1):
        day = since.date() + timedelta(days=offset)
        trend_by_day[day.isoformat()] = {"label": day.isoformat()[5:], **default_bucket}

    query_rows = session.execute(
        select(
            func.date(SearchQueryHistory.last_searched_at).label("day"),
            func.coalesce(func.sum(SearchQueryHistory.search_count), 0).label("value"),
        )
        .where(SearchQueryHistory.last_searched_at >= since)
        .group_by(func.date(SearchQueryHistory.last_searched_at))
    ).all()
    for row in query_rows:
        if row.day is None:
            continue
        bucket = _bucket(row.day)
        bucket["search_queries"] = int(row.value or 0)

    click_rows = session.execute(
        select(func.date(SearchResultClick.created_at).label("day"), func.count(SearchResultClick.id).label("value"))
        .where(SearchResultClick.created_at >= since)
        .group_by(func.date(SearchResultClick.created_at))
    ).all()
    for row in click_rows:
        if row.day is None:
            continue
        bucket = _bucket(row.day)
        bucket["search_clicks"] = int(row.value or 0)

    usage_rows = session.execute(
        select(
            func.date(PlatformUsageEvent.created_at).label("day"),
            PlatformUsageEvent.module_name,
            PlatformUsageEvent.event_name,
            func.count(PlatformUsageEvent.id).label("value"),
        )
        .where(PlatformUsageEvent.created_at >= since)
        .group_by(func.date(PlatformUsageEvent.created_at), PlatformUsageEvent.module_name, PlatformUsageEvent.event_name)
    ).all()
    for row in usage_rows:
        if row.day is None:
            continue
        bucket = _bucket(row.day)
        value = int(row.value or 0)
        bucket["usage_events"] += value
        module_name = str(row.module_name or "")
        event_name = str(row.event_name or "")
        if event_name == "page_view":
            if module_name == "explorer":
                bucket["explorer_page_views"] += value
            elif module_name == "incidents":
                bucket["incidents_page_views"] += value
            elif module_name == "certification":
                bucket["certification_page_views"] += value
            elif module_name == "privacy_access":
                bucket["privacy_page_views"] += value

    legacy_rows = session.execute(
        select(func.date(AccessLog.created_at).label("day"), func.count(AccessLog.id).label("value"))
        .where(AccessLog.created_at >= since, AccessLog.api_version == "legacy")
        .group_by(func.date(AccessLog.created_at))
    ).all()
    for row in legacy_rows:
        if row.day is None:
            continue
        bucket = _bucket(row.day)
        bucket["legacy_api_hits"] = int(row.value or 0)

    return [trend_by_day[key] for key in sorted(trend_by_day.keys())]


def analytics_summary(session: Session, *, days: int = 30, current_user: User | None = None) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=max(days, 1))
    from t2c_data.features.governance.settings import (
        LEGACY_API_AUTO_CUTOFF_MODULES,
        LEGACY_API_MANAGED_MODULES,
        get_effective_legacy_api_disabled_modules,
        get_governance_settings_snapshot,
    )

    governance = get_governance_settings_snapshot(session)

    search_query_total = int(
        session.scalar(
            select(func.coalesce(func.sum(SearchQueryHistory.search_count), 0)).where(SearchQueryHistory.last_searched_at >= since)
        )
        or 0
    )
    search_click_total = int(
        session.scalar(select(func.count(SearchResultClick.id)).where(SearchResultClick.created_at >= since)) or 0
    )
    usage_total = int(
        session.scalar(select(func.count(PlatformUsageEvent.id)).where(PlatformUsageEvent.created_at >= since)) or 0
    )
    module_rows = session.execute(
        select(PlatformUsageEvent.module_name, func.count(PlatformUsageEvent.id))
        .where(PlatformUsageEvent.created_at >= since)
        .group_by(PlatformUsageEvent.module_name)
        .order_by(desc(func.count(PlatformUsageEvent.id)))
        .limit(8)
    ).all()
    event_rows = session.execute(
        select(PlatformUsageEvent.event_name, func.count(PlatformUsageEvent.id))
        .where(PlatformUsageEvent.created_at >= since)
        .group_by(PlatformUsageEvent.event_name)
        .order_by(desc(func.count(PlatformUsageEvent.id)))
        .limit(8)
    ).all()
    dashboard_actions = int(
        session.scalar(
            select(func.count(PlatformUsageEvent.id)).where(
                PlatformUsageEvent.created_at >= since,
                PlatformUsageEvent.module_name == "dashboard",
                PlatformUsageEvent.event_name != "page_view",
            )
        )
        or 0
    )
    campaign_actions = int(
        session.scalar(
            select(func.count(PlatformUsageEvent.id)).where(
                PlatformUsageEvent.created_at >= since,
                PlatformUsageEvent.module_name == "governance_campaign",
                PlatformUsageEvent.event_name.in_(["review_confirmed", "metadata_updated", "item_completed"]),
            )
        )
        or 0
    )
    explorer_page_views = int(
        session.scalar(
            select(func.count(PlatformUsageEvent.id)).where(
                PlatformUsageEvent.created_at >= since,
                PlatformUsageEvent.module_name == "explorer",
                PlatformUsageEvent.event_name == "page_view",
            )
        )
        or 0
    )
    incidents_page_views = int(
        session.scalar(
            select(func.count(PlatformUsageEvent.id)).where(
                PlatformUsageEvent.created_at >= since,
                PlatformUsageEvent.module_name == "incidents",
                PlatformUsageEvent.event_name == "page_view",
            )
        )
        or 0
    )
    certification_page_views = int(
        session.scalar(
            select(func.count(PlatformUsageEvent.id)).where(
                PlatformUsageEvent.created_at >= since,
                PlatformUsageEvent.module_name == "certification",
                PlatformUsageEvent.event_name == "page_view",
            )
        )
        or 0
    )
    privacy_page_views = int(
        session.scalar(
            select(func.count(PlatformUsageEvent.id)).where(
                PlatformUsageEvent.created_at >= since,
                PlatformUsageEvent.module_name == "privacy_access",
                PlatformUsageEvent.event_name == "page_view",
            )
        )
        or 0
    )
    legacy_by_module = legacy_api_usage_by_module(session, days=days)
    legacy_stats_by_module = legacy_api_usage_stats_by_module(session, days=days)
    legacy_access_total = sum(legacy_by_module.values())
    top_legacy_modules = [
        {"label": label, "value": value}
        for label, value in sorted(legacy_by_module.items(), key=lambda item: item[1], reverse=True)[:8]
    ]
    trend = _build_trend_series(session, since=since)
    eligible_legacy_modules_to_disable = sorted(
        module
        for module in LEGACY_API_MANAGED_MODULES
        if (
            (
                module in LEGACY_API_AUTO_CUTOFF_MODULES
                and legacy_by_module.get(module, 0) <= 0
            )
            or (
                module not in LEGACY_API_AUTO_CUTOFF_MODULES
                and legacy_stats_by_module.get(module, {}).get("last_hit_at") is not None
                and legacy_by_module.get(module, 0) <= 0
            )
            and module not in governance.legacy_api_force_enabled_modules
        )
    )

    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "search_queries": search_query_total,
        "search_clicks": search_click_total,
        "usage_events": usage_total,
        "search_to_asset_conversion_pct": round((search_click_total / search_query_total) * 100, 1) if search_query_total else 0.0,
        "dashboard_to_action_count": dashboard_actions,
        "campaign_to_update_count": campaign_actions,
        "explorer_page_views": explorer_page_views,
        "incidents_page_views": incidents_page_views,
        "certification_page_views": certification_page_views,
        "privacy_page_views": privacy_page_views,
        "legacy_api_hits": legacy_access_total,
        "legacy_api_cutoff_window_days": governance.legacy_api_cutoff_window_days,
        "managed_legacy_modules": list(LEGACY_API_MANAGED_MODULES),
        "disabled_legacy_modules": list(get_effective_legacy_api_disabled_modules(session)),
        "force_enabled_legacy_modules": list(governance.legacy_api_force_enabled_modules),
        "eligible_legacy_modules_to_disable": eligible_legacy_modules_to_disable,
        "top_modules": [{"label": str(label), "value": int(value or 0)} for label, value in module_rows if label],
        "top_events": [{"label": str(label), "value": int(value or 0)} for label, value in event_rows if label],
        "top_legacy_modules": top_legacy_modules,
        "top_assets": _load_top_asset_rows(session, since=since, limit=10, current_user=current_user),
        "trend": trend,
    }
