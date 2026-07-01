"""add platform_settings (runtime admin-editable platform config)

Revision ID: v1a2b3c4d604
Revises: u1a2b3c4d603
Create Date: 2026-07-01
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "v1a2b3c4d604"
down_revision = "u1a2b3c4d603"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _has_table(table: str) -> bool:
    try:
        return sa.inspect(op.get_bind()).has_table(table, schema=SCHEMA)
    except Exception:
        return False


def upgrade() -> None:
    if _has_table("platform_settings"):
        return
    op.create_table(
        "platform_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        # Spark
        sa.Column("spark_master_url", sa.String(length=500), nullable=True),
        sa.Column("spark_results_dir", sa.String(length=1000), nullable=True),
        sa.Column("spark_jobs_dir", sa.String(length=1000), nullable=True),
        sa.Column("spark_local_jars_dir", sa.String(length=1000), nullable=True),
        sa.Column("spark_driver_host", sa.String(length=255), nullable=True),
        sa.Column("spark_driver_memory", sa.String(length=20), nullable=True),
        sa.Column("spark_executor_memory", sa.String(length=20), nullable=True),
        sa.Column("spark_submit_timeout_seconds", sa.Integer(), nullable=True),
        sa.Column("spark_packages_enabled", sa.Boolean(), nullable=True),
        sa.Column("spark_packages", sa.String(length=1000), nullable=True),
        # Metabase
        sa.Column("metabase_enabled", sa.Boolean(), nullable=True),
        sa.Column("metabase_base_url", sa.String(length=500), nullable=True),
        sa.Column("metabase_auth_type", sa.String(length=40), nullable=True),
        sa.Column("metabase_auth_username", sa.String(length=255), nullable=True),
        sa.Column("metabase_auth_secret_encrypted", sa.Text(), nullable=True),
        sa.Column("metabase_timeout_seconds", sa.Integer(), nullable=True),
        sa.Column("metabase_sync_dashboards", sa.Boolean(), nullable=True),
        sa.Column("metabase_sync_questions", sa.Boolean(), nullable=True),
        sa.Column("metabase_sync_collections", sa.Boolean(), nullable=True),
        # Control / operational DB
        sa.Column("control_db_host", sa.String(length=500), nullable=True),
        sa.Column("control_db_port", sa.Integer(), nullable=True),
        sa.Column("control_db_name", sa.String(length=255), nullable=True),
        sa.Column("control_db_user", sa.String(length=255), nullable=True),
        sa.Column("control_db_password_encrypted", sa.Text(), nullable=True),
        sa.Column("control_db_schema", sa.String(length=255), nullable=True),
        sa.Column("control_db_sslmode", sa.String(length=40), nullable=True),
        # Advanced
        sa.Column("dq_execution_engine", sa.String(length=40), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        schema=SCHEMA,
    )
    # Seed the single settings row (all NULL => everything falls back to env/defaults).
    op.execute(f'INSERT INTO {SCHEMA}.platform_settings (id) VALUES (1)')


def downgrade() -> None:
    if _has_table("platform_settings"):
        op.drop_table("platform_settings", schema=SCHEMA)
