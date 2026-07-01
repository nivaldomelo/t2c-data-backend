"""platform_settings: store the whole config as one encrypted blob

Replaces the individual (mostly plaintext) config columns with a single Fernet-encrypted
`settings_encrypted` JSON document, so nothing is readable at rest. The table only ever
held a NULL seed row, so no data migration is needed.

Revision ID: w1a2b3c4d605
Revises: v1a2b3c4d604
Create Date: 2026-07-01
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "w1a2b3c4d605"
down_revision = "v1a2b3c4d604"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"

_OLD_COLUMNS = [
    "spark_master_url", "spark_results_dir", "spark_jobs_dir", "spark_local_jars_dir",
    "spark_driver_host", "spark_driver_memory", "spark_executor_memory",
    "spark_submit_timeout_seconds", "spark_packages_enabled", "spark_packages",
    "metabase_enabled", "metabase_base_url", "metabase_auth_type", "metabase_auth_username",
    "metabase_auth_secret_encrypted", "metabase_timeout_seconds", "metabase_sync_dashboards",
    "metabase_sync_questions", "metabase_sync_collections",
    "control_db_host", "control_db_port", "control_db_name", "control_db_user",
    "control_db_password_encrypted", "control_db_schema", "control_db_sslmode",
    "dq_execution_engine",
]


def _columns(table: str) -> set[str]:
    try:
        return {col["name"] for col in sa.inspect(op.get_bind()).get_columns(table, schema=SCHEMA)}
    except Exception:
        return set()


def upgrade() -> None:
    cols = _columns("platform_settings")
    if not cols:
        return  # table not present yet (v1a2b3c4d604 skipped) — nothing to alter
    if "settings_encrypted" not in cols:
        op.add_column("platform_settings", sa.Column("settings_encrypted", sa.Text(), nullable=True), schema=SCHEMA)
    for column in _OLD_COLUMNS:
        if column in cols:
            op.drop_column("platform_settings", column, schema=SCHEMA)


def downgrade() -> None:
    # Non-reversible in detail (old plaintext columns are intentionally dropped). Recreate the
    # blob column removal as a no-op-safe drop; the previous migration can recreate the table.
    cols = _columns("platform_settings")
    if "settings_encrypted" in cols:
        op.drop_column("platform_settings", "settings_encrypted", schema=SCHEMA)
