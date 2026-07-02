from pathlib import Path

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ROOT_ENV_FILE = str(PROJECT_ROOT / ".env")
DEV_ENVIRONMENTS = {"dev", "development", "local", "test"}
SCHEDULER_MODE_ALIASES = {
    "dedicated": "worker",
    "embedded": "embedded_dev_only",
}
ALLOWED_SCHEDULER_MODES = {"worker", "embedded_dev_only", "disabled"}
ALLOWED_METABASE_STARTUP_SYNC_MODES = {"disabled", "enqueue", "manual"}


def normalize_environment(env: str | None) -> str:
    return (env or "").strip().lower()


def is_dev_environment(env: str | None) -> bool:
    return normalize_environment(env) in DEV_ENVIRONMENTS


def normalize_scheduler_mode(mode: str | None) -> str:
    normalized = (mode or "worker").strip().lower()
    return SCHEDULER_MODE_ALIASES.get(normalized, normalized)


def embedded_scheduler_allowed(mode: str | None, env: str | None) -> bool:
    return normalize_scheduler_mode(mode) == "embedded_dev_only" and is_dev_environment(env)


def normalize_metabase_startup_sync_mode(mode: str | None) -> str | None:
    normalized = (mode or "").strip().lower()
    if not normalized:
        return None
    if normalized not in ALLOWED_METABASE_STARTUP_SYNC_MODES:
        raise ValueError("METABASE_STARTUP_SYNC_MODE must be one of: disabled, enqueue, manual")
    return normalized


class OperationalIngestionDatabaseConfig(BaseSettings):
    database_url: str | None = Field(default=None, validation_alias="OPERATIONAL_DATABASE_URL")
    host: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_HOST")
    port: int | None = Field(default=None, validation_alias="OPERATIONAL_DB_PORT")
    database: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_NAME")
    user: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_USER")
    password: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_PASSWORD")
    schema_name: str = Field(default="controle", validation_alias="OPERATIONAL_DB_SCHEMA")

    # Prefer the repo-root .env so the operational configuration stays centralized.
    model_config = SettingsConfigDict(env_file=ROOT_ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    def as_url(self) -> str | None:
        url = (self.database_url or "").strip()
        if url:
            return url
        host = (self.host or "").strip()
        database = (self.database or "").strip()
        user = (self.user or "").strip()
        password = self.password or ""
        port = int(self.port or 5432)
        if not host or not database or not user:
            return None
        # URL.create escapa cada componente (evita injeção de URL/param via senha/host).
        return URL.create(
            "postgresql+psycopg", username=user, password=password, host=host, port=port, database=database
        ).render_as_string(hide_password=False)


class MetabaseIntegrationConfig(BaseSettings):
    enabled: bool = Field(default=False, validation_alias="METABASE_ENABLED")
    name: str = Field(default="Metabase principal", validation_alias="METABASE_INSTANCE_NAME")
    base_url: str | None = Field(default=None, validation_alias="METABASE_BASE_URL")
    auth_type: str | None = Field(default=None, validation_alias="METABASE_AUTH_TYPE")
    auth_username: str | None = Field(default=None, validation_alias="METABASE_AUTH_USERNAME")
    auth_secret: str | None = Field(default=None, validation_alias="METABASE_AUTH_SECRET")
    timeout_seconds: int = Field(default=15, validation_alias="METABASE_TIMEOUT_SECONDS")
    sync_dashboards: bool = Field(default=True, validation_alias="METABASE_SYNC_DASHBOARDS")
    sync_questions: bool = Field(default=True, validation_alias="METABASE_SYNC_QUESTIONS")
    sync_collections: bool = Field(default=True, validation_alias="METABASE_SYNC_COLLECTIONS")
    auto_sync_on_startup: bool = Field(default=True, validation_alias="METABASE_AUTO_SYNC_ON_STARTUP")
    startup_sync_mode: str | None = Field(default=None, validation_alias="METABASE_STARTUP_SYNC_MODE")

    # Prefer the repo-root .env so the operational configuration stays centralized.
    model_config = SettingsConfigDict(env_file=ROOT_ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    def normalized_base_url(self) -> str | None:
        url = (self.base_url or "").strip().rstrip("/")
        return url or None


class Settings(BaseSettings):
    app_name: str = "Andromeda Catalog"
    # Secure by default: an unset/unknown ENV is treated as production, so the strict
    # security validations below apply unless dev/test is explicitly opted into.
    env: str = "production"
    database_url: str
    frontend_base_url: str | None = Field(default=None, validation_alias="FRONTEND_BASE_URL")
    ingestion_operational_database_url: str | None = Field(default=None, validation_alias="OPERATIONAL_DATABASE_URL")
    legacy_ingestion_operational_database_url: str | None = Field(default=None, validation_alias="INGESTION_OPERATIONAL_DATABASE_URL")
    ingestion_operational_host: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_HOST")
    ingestion_operational_port: int | None = Field(default=None, validation_alias="OPERATIONAL_DB_PORT")
    ingestion_operational_database: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_NAME")
    ingestion_operational_username: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_USER")
    ingestion_operational_password: str | None = Field(default=None, validation_alias="OPERATIONAL_DB_PASSWORD")
    operational_db_schema: str | None = Field(default="controle", validation_alias="OPERATIONAL_DB_SCHEMA")
    airflow_source_schema: str = Field(default="airflow_meta", validation_alias="AIRFLOW_SOURCE_SCHEMA")
    airflow_metadata_contract_version: str = Field(default="v1", validation_alias="AIRFLOW_METADATA_CONTRACT_VERSION")
    jwt_secret_key: str = "change-me"
    datasource_secret_key: str | None = None
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 120
    # Number of logins a user may perform WITHOUT enrolling MFA before being locked.
    # Kept at 1 (was 3) to minimize the window where a stolen password bypasses MFA:
    # one grace login is enough to reach MFA enrollment. Override via MFA_GRACE_LOGINS.
    mfa_grace_logins: int = 1
    # Passwords must be rotated within this many days; warn within the threshold.
    password_max_age_days: int = 90
    password_expiry_warning_days: int = 10
    db_schema: str = "t2c_data"
    enable_db_seed: bool = False
    initial_admin_name: str | None = Field(default=None, validation_alias="INITIAL_ADMIN_NAME")
    initial_admin_email: str | None = Field(default=None, validation_alias="INITIAL_ADMIN_EMAIL")
    initial_admin_password: str | None = Field(default=None, validation_alias="INITIAL_ADMIN_PASSWORD")
    admin_email: str = "admin@andromeda.com"
    admin_password: str = "admin123"
    viewer_email: str = "viewer@t2c.local"
    viewer_password: str = "viewer123"
    cors_allow_origins: str = ""
    auth_rate_limit_attempts: int = 5
    auth_rate_limit_window_seconds: int = 300
    dq_execution_engine: str = "spark"
    dq_execution_mode: str = "spark_only"
    dq_sql_statement_timeout_ms: int = 30000
    dq_sql_lock_timeout_ms: int = 5000
    dq_sql_idle_transaction_timeout_ms: int = 30000
    dq_sql_preview_limit: int = 20
    dq_scheduler_enabled: bool = True
    dq_scheduler_mode: str = "worker"
    dq_scheduler_poll_interval_minutes: int = 1
    dq_profiling_scheduler_enabled: bool = True
    dq_profiling_scheduler_mode: str = "worker"
    dq_profiling_scheduler_poll_interval_minutes: int = 2
    datasource_scan_scheduler_enabled: bool = True
    datasource_scan_scheduler_mode: str = "worker"
    datasource_scan_scheduler_poll_interval_minutes: int = 5
    # Metabase automatic sync schedule (defaults: every 2h, 08:00–18:00, Mon–Fri).
    metabase_sync_scheduler_enabled: bool = True
    # "worker" => the dedicated metabase-sync-worker runs the schedule (recommended,
    # works in dev and prod). "embedded_dev_only" => also run inside the backend in dev.
    metabase_sync_scheduler_mode: str = "worker"
    metabase_sync_scheduler_poll_interval_minutes: int = 5
    metabase_sync_interval_hours: int = 2
    metabase_sync_window_start_hour: int = 8
    metabase_sync_window_end_hour: int = 18
    metabase_sync_weekdays_only: bool = True
    metabase_sync_timezone: str = "America/Sao_Paulo"
    data_lake_scan_scheduler_mode: str = "worker"
    datasource_scan_connect_timeout_seconds: int = 10
    datasource_scan_statement_timeout_ms: int = 120000
    datasource_scan_retry_attempts: int = 2
    datasource_scan_retry_backoff_ms: int = 250
    dq_rule_default_schedule_minutes: int = 60
    datalake_allow_default_env_credentials: bool = False
    spark_cluster_monitor_enabled: bool = True
    spark_cluster_monitor_interval_minutes: int = 60
    spark_cluster_monitor_connect_timeout_seconds: int = 5
    spark_cluster_monitor_alert_after_failures: int = 1
    platform_read_model_auto_refresh_enabled: bool = True
    platform_read_model_refresh_interval_minutes: int = 30
    platform_scheduler_mode: str = "worker"
    platform_scheduler_heartbeat_grace_minutes: int = 10
    platform_worker_heartbeat_grace_seconds: int = 90
    platform_usage_event_retention_days: int = 180
    user_session_retention_days: int = 365
    user_access_event_retention_days: int = 365
    audit_event_retention_days: int = 730
    export_file_ttl_hours: int = 1
    dq_sample_retention_days: int = 90
    profiling_sample_retention_days: int = 90
    incident_evidence_retention_days: int = 365
    temp_file_ttl_hours: int = 24
    row_count_snapshot_retention_days: int = 180
    certification_history_retention_days: int = 365
    privacy_review_event_retention_days: int = 365
    system_log_retention_days: int = 365
    platform_backup_enabled: bool = False
    platform_backup_command: str | None = None
    # The automation rules engine duplicates native subsystem triggers (DQ schedules,
    # incident signals, Airflow, stewardship). Disabled by default so the scheduler does
    # not evaluate it every cycle; set PLATFORM_AUTOMATION_RULES_ENABLED=true to re-enable.
    platform_automation_rules_enabled: bool = False
    platform_backup_retention_days: int = 14
    platform_backup_min_interval_hours: int = 24
    export_sync_max_rows: int = 1000
    export_async_required_rows: int = 2000
    export_download_ttl_minutes: int = 60
    dq_observability_retention_days: int = 180
    dq_evidence_sample_retention_days: int = 90
    operational_ingestion_connect_timeout_seconds: int = 3
    external_api_rate_limit_enabled: bool = True
    external_api_rate_limit_window_seconds: int = 60
    external_api_rate_limit_default_per_window: int = 600
    external_api_rate_limit_overrides_json: str | None = None
    api_legacy_deprecation_enabled: bool = True
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_use_tls: bool = True
    notification_from_email: str | None = None
    allow_plaintext_secrets: bool = Field(
        default=False,
        validation_alias=AliasChoices("ALLOW_PLAINTEXT_SECRETS", "allow_plaintext_secrets"),
    )

    # Prefer the repo-root .env so the operational configuration stays centralized.
    model_config = SettingsConfigDict(env_file=ROOT_ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    @property
    def operational_ingestion_config(self) -> OperationalIngestionDatabaseConfig:
        return OperationalIngestionDatabaseConfig()

    @property
    def operational_ingestion_database_url(self) -> str | None:
        config = self.operational_ingestion_config
        return config.as_url() or self.ingestion_operational_database_url or self.legacy_ingestion_operational_database_url

    @property
    def cors_origins_list(self) -> list[str]:
        return [item.strip() for item in self.cors_allow_origins.split(",") if item.strip()]

    @property
    def normalized_frontend_base_url(self) -> str | None:
        url = (self.frontend_base_url or "").strip().rstrip("/")
        if url:
            return url
        if self.env.lower() in {"dev", "development", "local", "test"}:
            return "http://localhost:3000"
        return None

    @property
    def operational_ingestion_configured(self) -> bool:
        return bool(self.operational_ingestion_database_url or self.operational_ingestion_config.as_url())

    @property
    def metabase_config(self) -> MetabaseIntegrationConfig:
        return MetabaseIntegrationConfig()

    @property
    def metabase_bootstrap_enabled(self) -> bool:
        config = self.metabase_config
        if not config.enabled:
            return False
        if config.normalized_base_url():
            return True
        return self.env.lower() in {"dev", "development", "local", "test"}

    @property
    def metabase_startup_sync_mode(self) -> str:
        explicit_mode = normalize_metabase_startup_sync_mode(self.metabase_config.startup_sync_mode)
        if explicit_mode is not None:
            return explicit_mode
        if not self.metabase_bootstrap_enabled or not self.metabase_config.auto_sync_on_startup:
            return "disabled"
        return "manual" if is_dev_environment(self.env) else "enqueue"

    @property
    def metabase_auto_sync_enabled(self) -> bool:
        return self.metabase_startup_sync_mode == "enqueue"

    @property
    def airflow_contract_version(self) -> str:
        return (self.airflow_metadata_contract_version or "v1").strip() or "v1"

    @property
    def bootstrap_admin_name(self) -> str:
        return (self.initial_admin_name or "Andromeda Admin").strip()

    @property
    def bootstrap_admin_email(self) -> str:
        return (self.initial_admin_email or self.admin_email).strip()

    @property
    def bootstrap_admin_password(self) -> str:
        return self.initial_admin_password or self.admin_password

    @model_validator(mode="after")
    def validate_security_defaults(self) -> "Settings":
        env_value = normalize_environment(self.env)
        bootstrap_admin_password = self.initial_admin_password or self.admin_password

        # Secret-at-rest protection should be decoupled from the JWT signing key. Outside
        # dev/test this is mandatory (enforced below). In dev/test the secret_store no longer
        # falls back to the public "change-me" default, so a missing key uses a safe
        # in-process/JWT-derived cipher instead of a guessable one.
        datasource_key = (self.datasource_secret_key or "").strip()
        scheduler_modes = {
            "DQ_SCHEDULER_MODE": self.dq_scheduler_mode,
            "DQ_PROFILING_SCHEDULER_MODE": self.dq_profiling_scheduler_mode,
            "DATASOURCE_SCAN_SCHEDULER_MODE": self.datasource_scan_scheduler_mode,
            "DATA_LAKE_SCAN_SCHEDULER_MODE": self.data_lake_scan_scheduler_mode,
            "PLATFORM_SCHEDULER_MODE": self.platform_scheduler_mode,
        }
        for name, raw_mode in scheduler_modes.items():
            normalized_mode = normalize_scheduler_mode(raw_mode)
            if normalized_mode not in ALLOWED_SCHEDULER_MODES:
                raise ValueError(f"{name} must be one of: worker, embedded_dev_only, disabled")
            setattr(self, name.lower(), normalized_mode)
            if normalized_mode == "embedded_dev_only" and not is_dev_environment(env_value):
                raise ValueError("Embedded schedulers are not allowed outside dev/test. Use worker mode.")
        if self.allow_plaintext_secrets and not is_dev_environment(env_value):
            raise ValueError("ALLOW_PLAINTEXT_SECRETS is not allowed outside dev/test")
        startup_sync_mode = normalize_metabase_startup_sync_mode(self.metabase_config.startup_sync_mode)
        if self.metabase_config.startup_sync_mode is not None and startup_sync_mode is None:
            raise ValueError("METABASE_STARTUP_SYNC_MODE must be one of: disabled, enqueue, manual")
        if self.export_sync_max_rows < 1:
            raise ValueError("EXPORT_SYNC_MAX_ROWS must be greater than zero")
        if self.export_async_required_rows < self.export_sync_max_rows:
            raise ValueError("EXPORT_ASYNC_REQUIRED_ROWS must be greater than or equal to EXPORT_SYNC_MAX_ROWS")
        if self.export_download_ttl_minutes < 1:
            raise ValueError("EXPORT_DOWNLOAD_TTL_MINUTES must be greater than zero")
        retention_values = {
            "USER_SESSION_RETENTION_DAYS": self.user_session_retention_days,
            "USER_ACCESS_EVENT_RETENTION_DAYS": self.user_access_event_retention_days,
            "AUDIT_EVENT_RETENTION_DAYS": self.audit_event_retention_days,
            "EXPORT_FILE_TTL_HOURS": self.export_file_ttl_hours,
            "DQ_SAMPLE_RETENTION_DAYS": self.dq_sample_retention_days,
            "PROFILING_SAMPLE_RETENTION_DAYS": self.profiling_sample_retention_days,
            "INCIDENT_EVIDENCE_RETENTION_DAYS": self.incident_evidence_retention_days,
            "TEMP_FILE_TTL_HOURS": self.temp_file_ttl_hours,
            "ROW_COUNT_SNAPSHOT_RETENTION_DAYS": self.row_count_snapshot_retention_days,
            "CERTIFICATION_HISTORY_RETENTION_DAYS": self.certification_history_retention_days,
            "PRIVACY_REVIEW_EVENT_RETENTION_DAYS": self.privacy_review_event_retention_days,
            "SYSTEM_LOG_RETENTION_DAYS": self.system_log_retention_days,
        }
        for name, value in retention_values.items():
            if int(value) < 1:
                raise ValueError(f"{name} must be greater than zero")
        if env_value not in {"dev", "development", "local", "test"}:
            weak_jwt = {"change-me", "dev-only-change-me", "change-me-dev-jwt-secret"}
            if not self.jwt_secret_key or self.jwt_secret_key in weak_jwt:
                raise ValueError("JWT_SECRET_KEY must be set to a strong non-default value outside dev/test")
            if not datasource_key:
                raise ValueError("DATASOURCE_SECRET_KEY must be set outside dev/test")
            if datasource_key == (self.jwt_secret_key or "").strip():
                raise ValueError("DATASOURCE_SECRET_KEY must be different from JWT_SECRET_KEY outside dev/test")
            if "change-me" in datasource_key.lower():
                raise ValueError("DATASOURCE_SECRET_KEY must be a strong non-default value outside dev/test")
            # Chave curta é força-bruta trivial contra blobs Fernet vazados (KDF SHA256).
            if len(datasource_key) < 32:
                raise ValueError("DATASOURCE_SECRET_KEY must be at least 32 characters outside dev/test")
            if len(self.jwt_secret_key.strip()) < 32:
                raise ValueError("JWT_SECRET_KEY must be at least 32 characters outside dev/test")
            if self.enable_db_seed:
                raise ValueError("ENABLE_DB_SEED must be disabled outside dev/test")
            if bootstrap_admin_password == "admin123" or "change-me" in (bootstrap_admin_password or "").lower():
                raise ValueError("Default/placeholder ADMIN_PASSWORD cannot be used outside dev/test")
            if self.viewer_password == "viewer123" or "change-me" in (self.viewer_password or "").lower():
                raise ValueError("Default/placeholder VIEWER_PASSWORD cannot be used outside dev/test")
        return self


settings = Settings()
