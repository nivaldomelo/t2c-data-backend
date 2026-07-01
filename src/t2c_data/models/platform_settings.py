from __future__ import annotations

from sqlalchemy import Boolean, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class PlatformSettings(TimestampMixin, Base):
    """Runtime, admin-editable platform configuration (single row, id=1).

    Every field is nullable: a NULL/empty value means "fall back to the environment
    variable / hardcoded default". So an empty row reproduces today's behaviour exactly,
    and each stored value *overrides* the corresponding env default at resolution time
    (see app/features/platform_settings/resolvers.py). Secrets are stored encrypted at
    rest (Fernet, `enc::` prefix) via t2c_data.core.secret_store — never in plaintext.
    """

    __tablename__ = "platform_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)

    # --- Spark (submit/driver) -------------------------------------------------
    spark_master_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    spark_results_dir: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    spark_jobs_dir: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    spark_local_jars_dir: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    spark_driver_host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    spark_driver_memory: Mapped[str | None] = mapped_column(String(20), nullable=True)
    spark_executor_memory: Mapped[str | None] = mapped_column(String(20), nullable=True)
    spark_submit_timeout_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    spark_packages_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    spark_packages: Mapped[str | None] = mapped_column(String(1000), nullable=True)

    # --- Metabase --------------------------------------------------------------
    metabase_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    metabase_base_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    metabase_auth_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    metabase_auth_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Encrypted mapping {"auth_secret": "..."} (enc:: prefix). Never returned in plaintext.
    metabase_auth_secret_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    metabase_timeout_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metabase_sync_dashboards: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    metabase_sync_questions: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    metabase_sync_collections: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # --- Control / operational DB (schema "controle", read-model) --------------
    # NOTE: only this SECONDARY database is runtime-configurable. The primary catalog
    # DATABASE_URL stays env-only (bootstrap paradox: cannot store where the DB is in the DB).
    control_db_host: Mapped[str | None] = mapped_column(String(500), nullable=True)
    control_db_port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    control_db_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    control_db_user: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Encrypted mapping {"password": "..."} (enc:: prefix). Never returned in plaintext.
    control_db_password_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    control_db_schema: Mapped[str | None] = mapped_column(String(255), nullable=True)
    control_db_sslmode: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # --- Advanced --------------------------------------------------------------
    dq_execution_engine: Mapped[str | None] = mapped_column(String(40), nullable=True)

    updated_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
