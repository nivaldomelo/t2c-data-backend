"""add api rate limit buckets

Revision ID: c5d6e7f8a9b0
Revises: b3c4d5e6f7a8
Create Date: 2026-04-13 09:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "c5d6e7f8a9b0"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api_rate_limit_buckets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "api_key_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.platform_api_keys.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("route_group", sa.String(length=120), nullable=False),
        sa.Column("window_seconds", sa.Integer(), nullable=False, server_default="60"),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("counter", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint(
            "api_key_id",
            "route_group",
            "window_seconds",
            "bucket_start",
            name="uq_api_rate_limit_bucket",
        ),
        schema="t2c_data",
    )
    op.create_index(
        "ix_api_rate_limit_bucket_route",
        "api_rate_limit_buckets",
        ["route_group", "bucket_start"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_api_rate_limit_bucket_key",
        "api_rate_limit_buckets",
        ["api_key_id", "bucket_start"],
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_api_rate_limit_bucket_key", table_name="api_rate_limit_buckets", schema="t2c_data")
    op.drop_index("ix_api_rate_limit_bucket_route", table_name="api_rate_limit_buckets", schema="t2c_data")
    op.drop_table("api_rate_limit_buckets", schema="t2c_data")
