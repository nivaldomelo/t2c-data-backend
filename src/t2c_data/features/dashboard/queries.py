from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.features.dashboard.payload_builder import build_dashboard_payload
from t2c_data.features.dashboard.executive_queries import filter_profiles, normalize_filters
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.models.catalog import DataSource
from t2c_data.features.shared_cache import get_cached_value, session_cache_key, set_cached_value

_CACHE_TTL_SECONDS = 60


def _dashboard_cache_key(session: Session, filters, *, current_user=None) -> tuple[object, ...]:
    return (
        session_cache_key(session),
        getattr(current_user, "id", None),
        tuple(sorted(user_role_names(current_user))) if current_user is not None else (),
        tuple(sorted((key, str(value)) for key, value in dict(filters or {}).items())),
    )


def _scope_datasources(session: Session, filters) -> list[DataSource]:
    if filters.get("data_source_id") is not None:
        return session.scalars(select(DataSource).where(DataSource.id == filters["data_source_id"]).order_by(DataSource.name.asc())).all()
    if filters.get("source"):
        return session.scalars(select(DataSource).where(DataSource.name == filters["source"]).order_by(DataSource.name.asc())).all()
    return session.scalars(select(DataSource).order_by(DataSource.name.asc())).all()


def get_dashboard_summary(session: Session, current_user=None, filters=None) -> dict:
    now = datetime.now(timezone.utc)
    normalized_filters = filters or normalize_filters()
    cacheable = current_user is None or is_admin_role(user_role_names(current_user))
    if cacheable:
        cache_key = _dashboard_cache_key(session, normalized_filters, current_user=current_user)
        cached = get_cached_value("dashboard_summary", cache_key, now=now)
        if isinstance(cached, dict):
            return cached

    tables, _read_model_source = load_dashboard_profiles_with_fallback(session, now, current_user=current_user)
    filtered_tables = filter_profiles(tables, normalized_filters)
    scope_datasources = _scope_datasources(session, normalized_filters)
    payload = build_dashboard_payload(session, now, filtered_tables, datasources=scope_datasources)

    if cacheable:
        cache_key = _dashboard_cache_key(session, normalized_filters, current_user=current_user)
        set_cached_value("dashboard_summary", cache_key, payload, ttl_seconds=_CACHE_TTL_SECONDS, now=now)

    return payload
