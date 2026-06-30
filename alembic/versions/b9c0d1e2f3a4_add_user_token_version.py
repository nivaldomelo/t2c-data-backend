"""add user token version

Revision ID: b9c0d1e2f3a4
Revises: ac1b2c3d4e5f
Create Date: 2026-05-15 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "b9c0d1e2f3a4"
down_revision = "ac1b2c3d4e5f"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("token_version", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("users", "token_version", schema=SCHEMA)
