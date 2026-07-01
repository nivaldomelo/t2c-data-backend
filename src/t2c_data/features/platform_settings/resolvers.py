from __future__ import annotations

from dataclasses import replace

from sqlalchemy.orm import Session

from t2c_data.core.config import MetabaseIntegrationConfig, OperationalIngestionDatabaseConfig, settings
from t2c_data.features.platform_settings.store import (
    decrypt_control_db_password,
    decrypt_metabase_auth_secret,
    get_settings_row,
)
from t2c_data.integrations.spark import SparkSubmitConfig, SparkSubmitRunner, get_spark_submit_config


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def resolve_spark_config(session: Session | None = None) -> SparkSubmitConfig:
    """Env/default baseline (get_spark_submit_config) overlaid with any DB overrides.

    Non-null DB fields win; NULL fields fall back to the environment. With no row (or no
    session) this returns exactly the env/default config, so behaviour is unchanged."""
    base = get_spark_submit_config()
    row = get_settings_row(session)
    if row is None:
        return base

    overrides: dict[str, object] = {}
    if _clean(row.spark_master_url):
        overrides["master_url"] = row.spark_master_url.strip()
    if _clean(row.spark_results_dir):
        overrides["results_dir"] = row.spark_results_dir.strip()
    if _clean(row.spark_jobs_dir):
        overrides["jobs_dir"] = row.spark_jobs_dir.strip()
    if _clean(row.spark_local_jars_dir):
        overrides["local_jars_dir"] = row.spark_local_jars_dir.strip()
    if _clean(row.spark_driver_host):
        overrides["driver_host"] = row.spark_driver_host.strip()
    if _clean(row.spark_driver_memory):
        overrides["driver_memory"] = row.spark_driver_memory.strip()
    if _clean(row.spark_executor_memory):
        overrides["executor_memory"] = row.spark_executor_memory.strip()
    if row.spark_submit_timeout_seconds:
        overrides["timeout_seconds"] = int(row.spark_submit_timeout_seconds)
    if row.spark_packages_enabled is not None:
        overrides["packages_enabled"] = bool(row.spark_packages_enabled)
    if _clean(row.spark_packages):
        overrides["packages"] = row.spark_packages.strip()

    return replace(base, **overrides) if overrides else base


def resolve_spark_runner(session: Session | None = None) -> SparkSubmitRunner:
    return SparkSubmitRunner(resolve_spark_config(session))


def resolve_metabase_config(session: Session | None = None) -> MetabaseIntegrationConfig:
    """Env Metabase config overlaid with DB overrides (incl. decrypted auth secret)."""
    base = settings.metabase_config
    row = get_settings_row(session)
    if row is None:
        return base

    updates: dict[str, object] = {}
    if row.metabase_enabled is not None:
        updates["enabled"] = bool(row.metabase_enabled)
    if _clean(row.metabase_base_url):
        updates["base_url"] = row.metabase_base_url.strip()
    if _clean(row.metabase_auth_type):
        updates["auth_type"] = row.metabase_auth_type.strip()
    if _clean(row.metabase_auth_username):
        updates["auth_username"] = row.metabase_auth_username.strip()
    secret = decrypt_metabase_auth_secret(row)
    if secret:
        updates["auth_secret"] = secret
    if row.metabase_timeout_seconds:
        updates["timeout_seconds"] = int(row.metabase_timeout_seconds)
    if row.metabase_sync_dashboards is not None:
        updates["sync_dashboards"] = bool(row.metabase_sync_dashboards)
    if row.metabase_sync_questions is not None:
        updates["sync_questions"] = bool(row.metabase_sync_questions)
    if row.metabase_sync_collections is not None:
        updates["sync_collections"] = bool(row.metabase_sync_collections)

    return base.model_copy(update=updates) if updates else base


def resolve_control_db_url(session: Session | None = None) -> str | None:
    """SQLAlchemy URL for the control/operational DB (schema `controle`), DB overrides winning.

    Falls back to the env-configured OPERATIONAL_DATABASE_URL / OPERATIONAL_DB_* when the
    row does not fully specify a connection."""
    env_url = OperationalIngestionDatabaseConfig().as_url()
    row = get_settings_row(session)
    if row is None:
        return env_url

    host = _clean(row.control_db_host)
    name = _clean(row.control_db_name)
    user = _clean(row.control_db_user)
    if not (host and name and user):
        return env_url

    password = decrypt_control_db_password(row) or ""
    port = int(row.control_db_port or 5432)
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"
    sslmode = _clean(row.control_db_sslmode)
    if sslmode:
        url += f"?sslmode={sslmode}"
    return url
