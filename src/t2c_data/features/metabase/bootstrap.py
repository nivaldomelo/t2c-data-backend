from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.models.metabase import MetabaseInstance

logger = logging.getLogger(__name__)


def _configured_metabase_name() -> str:
    name = (settings.metabase_config.name or "Metabase principal").strip()
    return name or "Metabase principal"


def ensure_metabase_instance_from_settings(session: Session, *, commit: bool = True) -> MetabaseInstance | None:
    config = settings.metabase_config
    if not settings.metabase_bootstrap_enabled:
        return None

    base_url = config.normalized_base_url()
    if not base_url and settings.env.lower() in {"dev", "development", "local", "test"}:
        base_url = "http://metabase-metabase-1:3000"
        logger.info("metabase bootstrap using fallback base_url=%s for env=%s", base_url, settings.env)
    if not base_url:
        return None

    name = _configured_metabase_name()
    instance = session.scalar(select(MetabaseInstance).where(MetabaseInstance.name == name))
    if instance is None:
        instance = session.scalar(select(MetabaseInstance).where(MetabaseInstance.base_url == base_url))

    if instance is None:
        instance = MetabaseInstance(
            name=name,
            base_url=base_url,
            auth_type=(config.auth_type or "").strip() or None,
            auth_username=(config.auth_username or "").strip() or None,
            timeout_seconds=max(int(config.timeout_seconds or 15), 1),
            sync_dashboards=bool(config.sync_dashboards),
            sync_questions=bool(config.sync_questions),
            sync_collections=bool(config.sync_collections),
            enabled=True,
        )
        instance.auth_secret = config.auth_secret
        session.add(instance)
        session.flush()
        logger.info(
            "metabase bootstrap created instance name=%s base_url=%s auth_type=%s sync_dashboards=%s sync_questions=%s sync_collections=%s",
            instance.name,
            instance.base_url,
            instance.auth_type,
            instance.sync_dashboards,
            instance.sync_questions,
            instance.sync_collections,
        )
    else:
        instance.name = name
        instance.base_url = base_url
        instance.auth_type = (config.auth_type or "").strip() or None
        instance.auth_username = (config.auth_username or "").strip() or None
        instance.auth_secret = config.auth_secret
        instance.timeout_seconds = max(int(config.timeout_seconds or 15), 1)
        instance.sync_dashboards = bool(config.sync_dashboards)
        instance.sync_questions = bool(config.sync_questions)
        instance.sync_collections = bool(config.sync_collections)
        instance.enabled = True
        session.flush()
        logger.info(
            "metabase bootstrap updated instance id=%s name=%s base_url=%s auth_type=%s sync_dashboards=%s sync_questions=%s sync_collections=%s",
            instance.id,
            instance.name,
            instance.base_url,
            instance.auth_type,
            instance.sync_dashboards,
            instance.sync_questions,
            instance.sync_collections,
        )

    if commit:
        session.commit()
    return instance


def snapshot_metabase_instance(instance: MetabaseInstance | None) -> dict[str, object] | None:
    if instance is None:
        return None
    auth_type = (instance.auth_type or "").strip().lower()
    secret_configured = bool(instance.auth_secret)
    username_configured = bool((instance.auth_username or "").strip())
    if auth_type in {"", "none"}:
        credentials_state = "not_required"
    elif auth_type == "session":
        credentials_state = "ready" if (username_configured and secret_configured) else "missing"
    elif auth_type in {"bearer", "api_key", "header"}:
        credentials_state = "ready" if secret_configured else "missing"
    else:
        credentials_state = "unknown"
    last_sync_status = (instance.last_sync_status or "").strip().lower() or "never"
    if last_sync_status == "success":
        sync_state = "sync_ok"
    elif last_sync_status == "failed":
        sync_state = "sync_failed"
    elif last_sync_status == "running":
        sync_state = "sync_running"
    else:
        sync_state = "never_synced"
    return {
        "id": instance.id,
        "name": instance.name,
        "base_url": instance.base_url,
        "enabled": bool(instance.enabled),
        "configured": bool(instance.enabled and instance.base_url),
        "auth_type": instance.auth_type,
        "credentials_state": credentials_state,
        "auth_secret_configured": secret_configured,
        "auth_username_configured": username_configured,
        "last_sync_status": instance.last_sync_status,
        "last_sync_message": instance.last_sync_message,
        "sync_state": sync_state,
    }


__all__ = ["ensure_metabase_instance_from_settings", "snapshot_metabase_instance"]
