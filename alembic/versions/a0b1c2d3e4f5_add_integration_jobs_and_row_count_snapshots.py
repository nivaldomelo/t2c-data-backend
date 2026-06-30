"""add integration sync jobs and asset row count snapshots

Revision ID: a0b1c2d3e4f5
Revises: f7c8d9e0a1b2
Create Date: 2026-04-30 10:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "a0b1c2d3e4f5"
down_revision = "f7c8d9e0a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "integration_sync_jobs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_key", sa.String(length=255), nullable=False),
        sa.Column("source", sa.String(length=40), nullable=False),
        sa.Column("job_type", sa.String(length=120), nullable=False),
        sa.Column("target_type", sa.String(length=80), nullable=True),
        sa.Column("target_id", sa.Integer(), nullable=True),
        sa.Column("target_name", sa.String(length=255), nullable=True),
        sa.Column("trigger_mode", sa.String(length=20), nullable=False, server_default="manual"),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_expected_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("records_processed", sa.BigInteger(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("context_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index("ix_integration_sync_jobs_job_key", "integration_sync_jobs", ["job_key"], unique=False, schema="t2c_data")
    op.create_index("ix_integration_sync_jobs_source", "integration_sync_jobs", ["source"], unique=False, schema="t2c_data")
    op.create_index("ix_integration_sync_jobs_job_type", "integration_sync_jobs", ["job_type"], unique=False, schema="t2c_data")
    op.create_index("ix_integration_sync_jobs_target_type", "integration_sync_jobs", ["target_type"], unique=False, schema="t2c_data")
    op.create_index("ix_integration_sync_jobs_target_id", "integration_sync_jobs", ["target_id"], unique=False, schema="t2c_data")
    op.create_index("ix_integration_sync_jobs_status", "integration_sync_jobs", ["status"], unique=False, schema="t2c_data")
    op.create_index("ix_integration_sync_jobs_next_expected_run_at", "integration_sync_jobs", ["next_expected_run_at"], unique=False, schema="t2c_data")
    op.create_index(
        "ix_integration_sync_jobs_job_key_started_at",
        "integration_sync_jobs",
        ["job_key", "started_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_integration_sync_jobs_source_started_at",
        "integration_sync_jobs",
        ["source", "started_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_integration_sync_jobs_status_started_at",
        "integration_sync_jobs",
        ["status", "started_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_integration_sync_jobs_target",
        "integration_sync_jobs",
        ["target_type", "target_id"],
        unique=False,
        schema="t2c_data",
    )

    op.create_table(
        "asset_row_count_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("asset_type", sa.String(length=40), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("asset_name", sa.String(length=255), nullable=True),
        sa.Column("asset_fqn", sa.String(length=1000), nullable=True),
        sa.Column("source", sa.String(length=40), nullable=False, server_default="s3"),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("row_count_method", sa.String(length=40), nullable=True),
        sa.Column("row_count_confidence", sa.String(length=40), nullable=True),
        sa.Column("integration_sync_job_id", sa.Integer(), nullable=True),
        sa.Column("context_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["integration_sync_job_id"], ["t2c_data.integration_sync_jobs.id"], ondelete="SET NULL"),
        schema="t2c_data",
    )
    op.create_index("ix_asset_row_count_snapshots_asset_type", "asset_row_count_snapshots", ["asset_type"], unique=False, schema="t2c_data")
    op.create_index("ix_asset_row_count_snapshots_asset_id", "asset_row_count_snapshots", ["asset_id"], unique=False, schema="t2c_data")
    op.create_index("ix_asset_row_count_snapshots_source", "asset_row_count_snapshots", ["source"], unique=False, schema="t2c_data")
    op.create_index("ix_asset_row_count_snapshots_integration_job_id", "asset_row_count_snapshots", ["integration_sync_job_id"], unique=False, schema="t2c_data")
    op.create_index(
        "ix_asset_row_count_snapshots_asset_observed",
        "asset_row_count_snapshots",
        ["asset_type", "asset_id", "observed_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_asset_row_count_snapshots_source_observed",
        "asset_row_count_snapshots",
        ["source", "observed_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_asset_row_count_snapshots_integration_job",
        "asset_row_count_snapshots",
        ["integration_sync_job_id"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_asset_row_count_snapshots_integration_job", table_name="asset_row_count_snapshots", schema="t2c_data")
    op.drop_index("ix_asset_row_count_snapshots_source_observed", table_name="asset_row_count_snapshots", schema="t2c_data")
    op.drop_index("ix_asset_row_count_snapshots_asset_observed", table_name="asset_row_count_snapshots", schema="t2c_data")
    op.drop_index("ix_asset_row_count_snapshots_integration_job_id", table_name="asset_row_count_snapshots", schema="t2c_data")
    op.drop_index("ix_asset_row_count_snapshots_source", table_name="asset_row_count_snapshots", schema="t2c_data")
    op.drop_index("ix_asset_row_count_snapshots_asset_id", table_name="asset_row_count_snapshots", schema="t2c_data")
    op.drop_index("ix_asset_row_count_snapshots_asset_type", table_name="asset_row_count_snapshots", schema="t2c_data")
    op.drop_table("asset_row_count_snapshots", schema="t2c_data")

    op.drop_index("ix_integration_sync_jobs_target", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_status_started_at", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_source_started_at", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_job_key_started_at", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_next_expected_run_at", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_status", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_target_id", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_target_type", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_job_type", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_source", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_job_key", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_table("integration_sync_jobs", schema="t2c_data")
