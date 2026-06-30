"""add platform worker heartbeats

Revision ID: 7c8d9e0f1a2b
Revises: 6b7c8d9e0f1a
Create Date: 2026-05-26 18:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "7c8d9e0f1a2b"
down_revision: Union[str, Sequence[str], None] = "6b7c8d9e0f1a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "platform_worker_heartbeats",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=160), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="idle"),
        sa.Column("supported_job_types_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("active_job_source", sa.String(length=40), nullable=True),
        sa.Column("active_job_type", sa.String(length=120), nullable=True),
        sa.Column("active_job_id", sa.Integer(), nullable=True),
        sa.Column("last_job_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_job_status", sa.String(length=20), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("worker_id"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_platform_worker_heartbeats_worker_id",
        "platform_worker_heartbeats",
        ["worker_id"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_platform_worker_heartbeats_hostname",
        "platform_worker_heartbeats",
        ["hostname"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_platform_worker_heartbeats_last_seen_at",
        "platform_worker_heartbeats",
        ["last_seen_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_platform_worker_heartbeats_status",
        "platform_worker_heartbeats",
        ["status"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_platform_worker_heartbeats_status_last_seen",
        "platform_worker_heartbeats",
        ["status", "last_seen_at"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_platform_worker_heartbeats_status_last_seen", table_name="platform_worker_heartbeats", schema=SCHEMA)
    op.drop_index("ix_platform_worker_heartbeats_status", table_name="platform_worker_heartbeats", schema=SCHEMA)
    op.drop_index("ix_platform_worker_heartbeats_last_seen_at", table_name="platform_worker_heartbeats", schema=SCHEMA)
    op.drop_index("ix_platform_worker_heartbeats_hostname", table_name="platform_worker_heartbeats", schema=SCHEMA)
    op.drop_index("ix_platform_worker_heartbeats_worker_id", table_name="platform_worker_heartbeats", schema=SCHEMA)
    op.drop_table("platform_worker_heartbeats", schema=SCHEMA)
