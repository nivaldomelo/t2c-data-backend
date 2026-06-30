"""add users.password_changed_at for 90-day password rotation

Revision ID: t1a2b3c4d602
Revises: s1a2b3c4d601
Create Date: 2026-06-26

Existing rows default to now() so everyone gets a fresh rotation window from the
deploy instead of being locked out immediately.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "t1a2b3c4d602"
down_revision = "s1a2b3c4d601"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _has_column(table: str, column: str) -> bool:
    try:
        return any(col["name"] == column for col in sa.inspect(op.get_bind()).get_columns(table, schema=SCHEMA))
    except Exception:
        return False


def upgrade() -> None:
    if not _has_column("users", "password_changed_at"):
        op.add_column(
            "users",
            sa.Column("password_changed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            schema=SCHEMA,
        )


def downgrade() -> None:
    if _has_column("users", "password_changed_at"):
        op.drop_column("users", "password_changed_at", schema=SCHEMA)
