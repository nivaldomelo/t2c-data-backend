"""add users.ui_theme (per-user UI theme preference)

Revision ID: u1a2b3c4d603
Revises: t1a2b3c4d602
Create Date: 2026-06-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "u1a2b3c4d603"
down_revision = "t1a2b3c4d602"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _has_column(table: str, column: str) -> bool:
    try:
        return any(col["name"] == column for col in sa.inspect(op.get_bind()).get_columns(table, schema=SCHEMA))
    except Exception:
        return False


def upgrade() -> None:
    if not _has_column("users", "ui_theme"):
        op.add_column(
            "users",
            sa.Column("ui_theme", sa.String(length=30), nullable=False, server_default="atual"),
            schema=SCHEMA,
        )


def downgrade() -> None:
    if _has_column("users", "ui_theme"):
        op.drop_column("users", "ui_theme", schema=SCHEMA)
