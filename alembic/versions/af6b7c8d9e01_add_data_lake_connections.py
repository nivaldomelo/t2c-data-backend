"""add data lake connections

Revision ID: af6b7c8d9e01
Revises: ae5f60718293
Create Date: 2026-04-19 01:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "af6b7c8d9e01"
down_revision = "ae5f60718293"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_lake_connections",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("bucket", sa.String(length=255), nullable=False),
        sa.Column("region", sa.String(length=80), nullable=False),
        sa.Column("prefix", sa.String(length=500), nullable=True),
        sa.Column("auth_type", sa.String(length=80), nullable=False, server_default="default_environment"),
        sa.Column("access_key_id", sa.String(length=255), nullable=True),
        sa.Column("role_arn", sa.String(length=500), nullable=True),
        sa.Column("credentials_payload", sa.Text(), nullable=False, server_default=""),
        sa.Column("last_test_status", sa.String(length=40), nullable=True),
        sa.Column("last_test_message", sa.Text(), nullable=True),
        sa.Column("last_test_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("name", name="uq_data_lake_connections_name"),
    )
    op.create_index("ix_data_lake_connections_bucket", "data_lake_connections", ["bucket"])
    op.create_index("ix_data_lake_connections_region", "data_lake_connections", ["region"])


def downgrade() -> None:
    op.drop_index("ix_data_lake_connections_region", table_name="data_lake_connections")
    op.drop_index("ix_data_lake_connections_bucket", table_name="data_lake_connections")
    op.drop_table("data_lake_connections")
