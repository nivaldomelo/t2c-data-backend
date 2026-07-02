"""add users.mfa_last_counter (TOTP anti-replay)

Revision ID: x1a2b3c4d606
Revises: w1a2b3c4d605
Create Date: 2026-07-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "x1a2b3c4d606"
down_revision = "w1a2b3c4d605"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _has_column(table: str, column: str) -> bool:
    try:
        return any(col["name"] == column for col in sa.inspect(op.get_bind()).get_columns(table, schema=SCHEMA))
    except Exception:
        return False


def upgrade() -> None:
    if not _has_column("users", "mfa_last_counter"):
        op.add_column("users", sa.Column("mfa_last_counter", sa.Integer(), nullable=True), schema=SCHEMA)


def downgrade() -> None:
    if _has_column("users", "mfa_last_counter"):
        op.drop_column("users", "mfa_last_counter", schema=SCHEMA)
