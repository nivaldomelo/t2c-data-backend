from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class PlatformSettingsEffective(BaseModel):
    """Read-only view of the *effective* resolved config (DB → env → default).

    Mirrors every non-secret field so the admin form can open pre-filled with the values
    that are actually in force right now — even those inherited from the environment (stored
    override is NULL). Secrets are excluded (never surfaced)."""

    # Spark
    spark_master_url: str | None = None
    spark_results_dir: str | None = None
    spark_jobs_dir: str | None = None
    spark_local_jars_dir: str | None = None
    spark_driver_host: str | None = None
    spark_driver_memory: str | None = None
    spark_executor_memory: str | None = None
    spark_submit_timeout_seconds: int | None = None
    spark_packages_enabled: bool | None = None
    spark_packages: str | None = None
    # Metabase
    metabase_enabled: bool = False
    metabase_base_url: str | None = None
    metabase_auth_type: str | None = None
    metabase_auth_username: str | None = None
    metabase_timeout_seconds: int | None = None
    metabase_sync_dashboards: bool | None = None
    metabase_sync_questions: bool | None = None
    metabase_sync_collections: bool | None = None
    # Control / operational DB
    control_db_host: str | None = None
    control_db_port: int | None = None
    control_db_name: str | None = None
    control_db_user: str | None = None
    control_db_schema: str | None = None
    control_db_sslmode: str | None = None
    # Advanced
    dq_execution_engine: str | None = None


class PlatformSettingsOut(BaseModel):
    """Stored overrides (NULL = inherit env/default). Secrets are never returned in
    plaintext — only a boolean `*_set` flag indicating whether a value is stored."""

    model_config = ConfigDict(from_attributes=True)

    # Spark
    spark_master_url: str | None = None
    spark_results_dir: str | None = None
    spark_jobs_dir: str | None = None
    spark_local_jars_dir: str | None = None
    spark_driver_host: str | None = None
    spark_driver_memory: str | None = None
    spark_executor_memory: str | None = None
    spark_submit_timeout_seconds: int | None = None
    spark_packages_enabled: bool | None = None
    spark_packages: str | None = None
    # Metabase
    metabase_enabled: bool | None = None
    metabase_base_url: str | None = None
    metabase_auth_type: str | None = None
    metabase_auth_username: str | None = None
    metabase_auth_secret_set: bool = False
    metabase_timeout_seconds: int | None = None
    metabase_sync_dashboards: bool | None = None
    metabase_sync_questions: bool | None = None
    metabase_sync_collections: bool | None = None
    # Control / operational DB (schema "controle")
    control_db_host: str | None = None
    control_db_port: int | None = None
    control_db_name: str | None = None
    control_db_user: str | None = None
    control_db_password_set: bool = False
    control_db_schema: str | None = None
    control_db_sslmode: str | None = None
    # Advanced
    dq_execution_engine: str | None = None

    effective: PlatformSettingsEffective = Field(default_factory=PlatformSettingsEffective)
    updated_at: datetime | None = None
    updated_by_user_id: int | None = None


class PlatformSettingsUpdate(BaseModel):
    """Partial update. Only fields explicitly sent are applied (exclude_unset).

    For non-secret fields, sending `null` clears the override (→ inherit env/default).
    For secret fields (`metabase_auth_secret`, `control_db_password`): send a value to
    set it, send an empty string / null to clear it, or omit the field to keep it."""

    # Spark
    spark_master_url: str | None = None
    spark_results_dir: str | None = None
    spark_jobs_dir: str | None = None
    spark_local_jars_dir: str | None = None
    spark_driver_host: str | None = None
    spark_driver_memory: str | None = None
    spark_executor_memory: str | None = None
    spark_submit_timeout_seconds: int | None = None
    spark_packages_enabled: bool | None = None
    spark_packages: str | None = None
    # Metabase
    metabase_enabled: bool | None = None
    metabase_base_url: str | None = None
    metabase_auth_type: str | None = None
    metabase_auth_username: str | None = None
    metabase_auth_secret: str | None = None  # secret (write-only)
    metabase_timeout_seconds: int | None = None
    metabase_sync_dashboards: bool | None = None
    metabase_sync_questions: bool | None = None
    metabase_sync_collections: bool | None = None
    # Control / operational DB
    control_db_host: str | None = None
    control_db_port: int | None = None
    control_db_name: str | None = None
    control_db_user: str | None = None
    control_db_password: str | None = None  # secret (write-only)
    control_db_schema: str | None = None
    control_db_sslmode: str | None = None
    # Advanced
    dq_execution_engine: str | None = None


class PlatformConfigTestResult(BaseModel):
    ok: bool
    target: str
    detail: str
    latency_ms: int | None = None
