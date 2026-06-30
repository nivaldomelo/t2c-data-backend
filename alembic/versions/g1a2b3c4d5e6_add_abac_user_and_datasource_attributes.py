"""Add ABAC user and datasource attributes.

Revision ID: g1a2b3c4d5e6
Revises: e1a2b3c4d5f6
Create Date: 2026-05-28 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "g1a2b3c4d5e6"
down_revision = "e1a2b3c4d5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("allowed_domains", sa.JSON(), nullable=True), schema="t2c_data")
    op.add_column("users", sa.Column("allowed_environments", sa.JSON(), nullable=True), schema="t2c_data")
    op.add_column(
        "data_sources",
        sa.Column("environment", sa.String(length=40), nullable=True, server_default="shared"),
        schema="t2c_data",
    )
    op.create_index("ix_data_sources_environment", "data_sources", ["environment"], schema="t2c_data")


def downgrade() -> None:
    op.drop_index("ix_data_sources_environment", table_name="data_sources", schema="t2c_data")
    op.drop_column("data_sources", "environment", schema="t2c_data")
    op.drop_column("users", "allowed_environments", schema="t2c_data")
    op.drop_column("users", "allowed_domains", schema="t2c_data")
