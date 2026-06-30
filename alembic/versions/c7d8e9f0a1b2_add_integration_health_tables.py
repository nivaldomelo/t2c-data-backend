"""add integration health tables

Revision ID: c7d8e9f0a1b2
Revises: b6c7d8e9f0a1
Create Date: 2026-04-14 23:45:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "c7d8e9f0a1b2"
down_revision = "b6c7d8e9f0a1"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "integration_health",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("integration_name", sa.String(length=80), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="unavailable"),
        sa.Column("status_message", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=40), nullable=True),
        sa.Column("base_url", sa.String(length=500), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("error_type", sa.String(length=160), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("breaker_state", sa.String(length=20), nullable=False, server_default="closed"),
        sa.Column("breaker_open_until_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("integration_name", name="uq_integration_health_name"),
        schema=SCHEMA,
    )
    op.create_index("ix_integration_health_status", "integration_health", ["status"], schema=SCHEMA)
    op.create_index("ix_integration_health_category", "integration_health", ["category"], schema=SCHEMA)
    op.create_index("ix_integration_health_checked_at", "integration_health", ["checked_at"], schema=SCHEMA)

    op.create_table(
        "integration_health_history",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "integration_health_id",
            sa.Integer(),
            sa.ForeignKey(f"{SCHEMA}.integration_health.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("integration_name", sa.String(length=80), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("status_message", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=40), nullable=True),
        sa.Column("base_url", sa.String(length=500), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("error_type", sa.String(length=160), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("breaker_state", sa.String(length=20), nullable=False, server_default="closed"),
        sa.Column("breaker_open_until_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_integration_health_history_integration_checked_at",
        "integration_health_history",
        ["integration_name", "checked_at"],
        schema=SCHEMA,
    )
    op.create_index("ix_integration_health_history_status", "integration_health_history", ["status"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_integration_health_history_status", table_name="integration_health_history", schema=SCHEMA)
    op.drop_index(
        "ix_integration_health_history_integration_checked_at",
        table_name="integration_health_history",
        schema=SCHEMA,
    )
    op.drop_table("integration_health_history", schema=SCHEMA)

    op.drop_index("ix_integration_health_checked_at", table_name="integration_health", schema=SCHEMA)
    op.drop_index("ix_integration_health_category", table_name="integration_health", schema=SCHEMA)
    op.drop_index("ix_integration_health_status", table_name="integration_health", schema=SCHEMA)
    op.drop_table("integration_health", schema=SCHEMA)
