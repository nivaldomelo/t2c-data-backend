"""add MFA grace-login counter and lock fields to users

Revision ID: s1a2b3c4d601
Revises: r1a2b3c4d600
Create Date: 2026-06-26

Supports enforcing Google Authenticator MFA with a grace window: a user may log
in a few times without MFA enrolled; once the grace is exhausted they are locked
until an admin unlocks them (or they enroll within the window).
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "s1a2b3c4d601"
down_revision = "r1a2b3c4d600"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _has_column(table: str, column: str) -> bool:
    try:
        return any(col["name"] == column for col in sa.inspect(op.get_bind()).get_columns(table, schema=SCHEMA))
    except Exception:
        return False


def upgrade() -> None:
    if not _has_column("users", "mfa_grace_logins_used"):
        op.add_column(
            "users",
            sa.Column("mfa_grace_logins_used", sa.Integer(), nullable=False, server_default="0"),
            schema=SCHEMA,
        )
    if not _has_column("users", "mfa_locked"):
        op.add_column(
            "users",
            sa.Column("mfa_locked", sa.Boolean(), nullable=False, server_default="false"),
            schema=SCHEMA,
        )
    if not _has_column("users", "mfa_locked_at"):
        op.add_column(
            "users",
            sa.Column("mfa_locked_at", sa.DateTime(timezone=True), nullable=True),
            schema=SCHEMA,
        )


def downgrade() -> None:
    for column in ("mfa_locked_at", "mfa_locked", "mfa_grace_logins_used"):
        if _has_column("users", column):
            op.drop_column("users", column, schema=SCHEMA)
