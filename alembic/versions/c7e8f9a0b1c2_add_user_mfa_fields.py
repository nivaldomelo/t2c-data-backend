"""add user mfa fields

Revision ID: c7e8f9a0b1c2
Revises: f5e6d7c8b9a0
Create Date: 2026-05-15 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "c7e8f9a0b1c2"
down_revision = "f5e6d7c8b9a0"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("mfa_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema=SCHEMA,
    )
    op.add_column(
        "users",
        sa.Column("mfa_secret_encrypted", sa.Text(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("users", "mfa_secret_encrypted", schema=SCHEMA)
    op.drop_column("users", "mfa_enabled", schema=SCHEMA)
