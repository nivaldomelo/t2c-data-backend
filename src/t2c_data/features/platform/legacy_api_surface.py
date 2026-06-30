from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from t2c_data.core.legacy_api_surface import LEGACY_API_SURFACE, REMOVED_LEGACY_API_MODULES, legacy_surface_route_match
from t2c_data.features.governance.settings import (
    LEGACY_API_AUTO_CUTOFF_MODULES,
    LEGACY_API_MANAGED_MODULES,
    get_effective_legacy_api_disabled_modules,
    get_governance_settings_snapshot,
)
from t2c_data.features.platform.analytics import legacy_api_usage_stats_by_module
from t2c_data.models.audit import AccessLog, AccessLogArchive


def _normalize_dt(value):
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _latest_legacy_request(session: Session, module: str) -> dict[str, object] | None:
    live = session.execute(
        select(
            AccessLog.created_at,
            AccessLog.route,
            AccessLog.method,
            AccessLog.user_agent,
            AccessLog.ip,
        )
        .where(AccessLog.api_version == "legacy", AccessLog.module_name == module)
        .order_by(desc(AccessLog.created_at), desc(AccessLog.id))
        .limit(1)
    ).first()
    archived = session.execute(
        select(
            AccessLogArchive.created_at,
            AccessLogArchive.route,
            AccessLogArchive.method,
            AccessLogArchive.user_agent,
            AccessLogArchive.ip,
        )
        .where(AccessLogArchive.api_version == "legacy", AccessLogArchive.module_name == module)
        .order_by(desc(AccessLogArchive.created_at), desc(AccessLogArchive.id))
        .limit(1)
    ).first()
    candidates = [row for row in (live, archived) if row is not None]
    if not candidates:
        return None
    latest = max(candidates, key=lambda row: _normalize_dt(row.created_at) or datetime.min.replace(tzinfo=timezone.utc))
    route = str(latest.route or "")
    _, canonical_path = legacy_surface_route_match(route)
    return {
        "module": module,
        "route": route or None,
        "method": str(latest.method or "").upper() or None,
        "user_agent": str(latest.user_agent or "") or None,
        "origin_ip": str(latest.ip or "") or None,
        "canonical_path": canonical_path,
        "at": _normalize_dt(latest.created_at),
    }


def legacy_api_surface_summary(session: Session) -> dict[str, object]:
    governance = get_governance_settings_snapshot(session)
    since = datetime.now(timezone.utc) - timedelta(days=max(governance.legacy_api_cutoff_window_days, 1))
    usage_by_module = legacy_api_usage_stats_by_module(session, days=governance.legacy_api_cutoff_window_days)
    disabled_modules = set(get_effective_legacy_api_disabled_modules(session))
    items: list[dict[str, object]] = []

    for item in LEGACY_API_SURFACE:
        module = str(item["module"])
        stats = usage_by_module.get(module, {})
        hits_total = int(stats.get("hits_total", 0))
        hits_in_window = int(stats.get("hits_in_window", 0))
        last_hit_at = _normalize_dt(stats.get("last_hit_at"))
        latest_request = _latest_legacy_request(session, module)
        forced_enabled = module in governance.legacy_api_force_enabled_modules
        disabled = module in disabled_modules and not forced_enabled
        removed = module in REMOVED_LEGACY_API_MODULES
        if removed:
            sunset_status = "removed"
        elif disabled:
            sunset_status = "blocked"
        elif forced_enabled:
            sunset_status = "forced_enabled"
        elif hits_in_window > 0:
            sunset_status = "active_transition"
        elif module in LEGACY_API_AUTO_CUTOFF_MODULES:
            sunset_status = "eligible_for_cutoff"
        elif last_hit_at is not None and last_hit_at < since:
            sunset_status = "eligible_for_cutoff"
        else:
            sunset_status = "awaiting_observation"
        items.append(
            {
                "module": module,
                "legacy_prefixes": list(item["legacy_prefixes"]),
                "canonical_prefixes": list(item["canonical_prefixes"]),
                "hits_total": hits_total,
                "hits_in_window": hits_in_window,
                "last_hit_at": last_hit_at,
                "latest_request": latest_request,
                "managed": module in LEGACY_API_MANAGED_MODULES,
                "disabled": disabled,
                "forced_enabled": forced_enabled,
                "physically_removed": removed,
                "sunset_status": sunset_status,
                "note": str(item["note"]),
            }
        )

    return {
        "window_days": governance.legacy_api_cutoff_window_days,
        "official_surface": "/api/v1",
        "temporary_surface": "/api",
        "recommendation": "Legado encerrado. Usar rota canônica /api/v1.",
        "items": items,
    }


__all__ = ["legacy_api_surface_summary"]
