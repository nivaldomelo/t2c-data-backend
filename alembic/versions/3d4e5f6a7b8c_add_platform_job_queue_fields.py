"""add platform job queue fields

Revision ID: 3d4e5f6a7b8c
Revises: 2c3d4e5f6a7b
Create Date: 2026-05-25 19:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "3d4e5f6a7b8c"
down_revision: Union[str, Sequence[str], None] = "2c3d4e5f6a7b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("integration_sync_jobs", sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True), schema="t2c_data")
    op.add_column("integration_sync_jobs", sa.Column("progress_pct", sa.Float(), nullable=True), schema="t2c_data")
    op.add_column("integration_sync_jobs", sa.Column("correlation_id", sa.String(length=120), nullable=True), schema="t2c_data")
    op.add_column("integration_sync_jobs", sa.Column("requested_by_user_id", sa.Integer(), nullable=True), schema="t2c_data")
    op.add_column("integration_sync_jobs", sa.Column("payload_json", sa.JSON(), nullable=True), schema="t2c_data")
    op.add_column("integration_sync_jobs", sa.Column("result_summary_json", sa.JSON(), nullable=True), schema="t2c_data")

    op.create_foreign_key(
        "fk_integration_sync_jobs_requested_by_user_id",
        "integration_sync_jobs",
        "users",
        ["requested_by_user_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )
    op.create_index("ix_integration_sync_jobs_queued_at", "integration_sync_jobs", ["queued_at"], unique=False, schema="t2c_data")
    op.create_index("ix_integration_sync_jobs_correlation_id", "integration_sync_jobs", ["correlation_id"], unique=False, schema="t2c_data")
    op.create_index(
        "ix_integration_sync_jobs_requested_by_user_id",
        "integration_sync_jobs",
        ["requested_by_user_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_integration_sync_jobs_status_queued_at",
        "integration_sync_jobs",
        ["status", "queued_at"],
        unique=False,
        schema="t2c_data",
    )

    op.execute("UPDATE t2c_data.integration_sync_jobs SET queued_at = started_at WHERE queued_at IS NULL")


def downgrade() -> None:
    op.drop_index("ix_integration_sync_jobs_status_queued_at", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_requested_by_user_id", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_correlation_id", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_index("ix_integration_sync_jobs_queued_at", table_name="integration_sync_jobs", schema="t2c_data")
    op.drop_constraint("fk_integration_sync_jobs_requested_by_user_id", "integration_sync_jobs", schema="t2c_data", type_="foreignkey")
    op.drop_column("integration_sync_jobs", "result_summary_json", schema="t2c_data")
    op.drop_column("integration_sync_jobs", "payload_json", schema="t2c_data")
    op.drop_column("integration_sync_jobs", "requested_by_user_id", schema="t2c_data")
    op.drop_column("integration_sync_jobs", "correlation_id", schema="t2c_data")
    op.drop_column("integration_sync_jobs", "progress_pct", schema="t2c_data")
    op.drop_column("integration_sync_jobs", "queued_at", schema="t2c_data")
