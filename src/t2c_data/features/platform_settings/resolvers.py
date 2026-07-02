from __future__ import annotations

from dataclasses import replace
from typing import Any

from sqlalchemy import URL
from sqlalchemy.orm import Session

from t2c_data.core.config import MetabaseIntegrationConfig, OperationalIngestionDatabaseConfig, settings
from t2c_data.features.platform_settings.store import read_settings_dict
from t2c_data.integrations.spark import SparkSubmitConfig, SparkSubmitRunner, get_spark_submit_config


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    trimmed = str(value).strip()
    return trimmed or None


def resolve_spark_config(session: Session | None = None) -> SparkSubmitConfig:
    """Env/default baseline (get_spark_submit_config) overlaid with any DB overrides.

    Non-empty stored fields win; missing fields fall back to the environment. With no stored
    config this returns exactly the env/default config, so behaviour is unchanged."""
    base = get_spark_submit_config()
    d = read_settings_dict(session)
    if not d:
        return base

    overrides: dict[str, object] = {}
    if _clean(d.get("spark_master_url")):
        overrides["master_url"] = str(d["spark_master_url"]).strip()
    if _clean(d.get("spark_results_dir")):
        overrides["results_dir"] = str(d["spark_results_dir"]).strip()
    if _clean(d.get("spark_jobs_dir")):
        overrides["jobs_dir"] = str(d["spark_jobs_dir"]).strip()
    if _clean(d.get("spark_local_jars_dir")):
        overrides["local_jars_dir"] = str(d["spark_local_jars_dir"]).strip()
    if _clean(d.get("spark_driver_host")):
        overrides["driver_host"] = str(d["spark_driver_host"]).strip()
    if _clean(d.get("spark_driver_memory")):
        overrides["driver_memory"] = str(d["spark_driver_memory"]).strip()
    if _clean(d.get("spark_executor_memory")):
        overrides["executor_memory"] = str(d["spark_executor_memory"]).strip()
    if d.get("spark_submit_timeout_seconds"):
        overrides["timeout_seconds"] = int(d["spark_submit_timeout_seconds"])
    if d.get("spark_packages_enabled") is not None:
        overrides["packages_enabled"] = bool(d["spark_packages_enabled"])
    if _clean(d.get("spark_packages")):
        overrides["packages"] = str(d["spark_packages"]).strip()

    return replace(base, **overrides) if overrides else base


def resolve_spark_runner(session: Session | None = None) -> SparkSubmitRunner:
    return SparkSubmitRunner(resolve_spark_config(session))


def resolve_metabase_config(session: Session | None = None) -> MetabaseIntegrationConfig:
    """Env Metabase config overlaid with DB overrides (incl. the decrypted auth secret)."""
    base = settings.metabase_config
    d = read_settings_dict(session)
    if not d:
        return base

    updates: dict[str, object] = {}
    if d.get("metabase_enabled") is not None:
        updates["enabled"] = bool(d["metabase_enabled"])
    if _clean(d.get("metabase_base_url")):
        updates["base_url"] = str(d["metabase_base_url"]).strip()
    if _clean(d.get("metabase_auth_type")):
        updates["auth_type"] = str(d["metabase_auth_type"]).strip()
    if _clean(d.get("metabase_auth_username")):
        updates["auth_username"] = str(d["metabase_auth_username"]).strip()
    if _clean(d.get("metabase_auth_secret")):
        updates["auth_secret"] = str(d["metabase_auth_secret"])
    if d.get("metabase_timeout_seconds"):
        updates["timeout_seconds"] = int(d["metabase_timeout_seconds"])
    if d.get("metabase_sync_dashboards") is not None:
        updates["sync_dashboards"] = bool(d["metabase_sync_dashboards"])
    if d.get("metabase_sync_questions") is not None:
        updates["sync_questions"] = bool(d["metabase_sync_questions"])
    if d.get("metabase_sync_collections") is not None:
        updates["sync_collections"] = bool(d["metabase_sync_collections"])

    return base.model_copy(update=updates) if updates else base


def resolve_control_db_url(session: Session | None = None) -> str | None:
    """SQLAlchemy URL for the control/operational DB (schema `controle`), DB overrides winning.

    Falls back to the env-configured OPERATIONAL_DATABASE_URL / OPERATIONAL_DB_* when the
    stored config does not fully specify a connection."""
    env_url = OperationalIngestionDatabaseConfig().as_url()
    d = read_settings_dict(session)
    if not d:
        return env_url

    host = _clean(d.get("control_db_host"))
    name = _clean(d.get("control_db_name"))
    user = _clean(d.get("control_db_user"))
    if not (host and name and user):
        return env_url

    password = _clean(d.get("control_db_password")) or ""
    port = int(d.get("control_db_port") or 5432)
    sslmode = _clean(d.get("control_db_sslmode"))
    # URL.create escapa cada componente (evita injeção de URL/param-libpq via senha/host/name).
    return URL.create(
        "postgresql+psycopg",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
        query={"sslmode": sslmode} if sslmode else {},
    ).render_as_string(hide_password=False)
