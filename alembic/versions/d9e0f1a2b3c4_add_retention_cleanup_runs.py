"""add retention cleanup runs

Revision ID: d9e0f1a2b3c4
Revises: c0a1b2c3d4e6
Create Date: 2026-05-28 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "d9e0f1a2b3c4"
down_revision = "c0a1b2c3d4e6"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "retention_cleanup_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_name", sa.String(length=80), server_default=sa.text("'retention_cleanup_job'"), nullable=False),
        sa.Column("trigger_source", sa.String(length=40), server_default=sa.text("'scheduler'"), nullable=False),
        sa.Column("status", sa.String(length=20), server_default=sa.text("'running'"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retention_policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )
    op.create_index("ix_retention_cleanup_runs_status_created_at", "retention_cleanup_runs", ["status", "created_at"], schema=SCHEMA)
    op.create_index(
        "ix_retention_cleanup_runs_trigger_source_created_at",
        "retention_cleanup_runs",
        ["trigger_source", "created_at"],
        schema=SCHEMA,
    )
    op.create_index("ix_retention_cleanup_runs_started_at", "retention_cleanup_runs", ["started_at"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_retention_cleanup_runs_started_at", table_name="retention_cleanup_runs", schema=SCHEMA)
    op.drop_index(
        "ix_retention_cleanup_runs_trigger_source_created_at",
        table_name="retention_cleanup_runs",
        schema=SCHEMA,
    )
    op.drop_index("ix_retention_cleanup_runs_status_created_at", table_name="retention_cleanup_runs", schema=SCHEMA)
    op.drop_table("retention_cleanup_runs", schema=SCHEMA)
