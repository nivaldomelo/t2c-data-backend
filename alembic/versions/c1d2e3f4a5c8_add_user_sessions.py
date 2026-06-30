"""add user sessions

Revision ID: c1d2e3f4a5c8
Revises: b9c0d1e2f3a4
Create Date: 2026-05-15 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "c1d2e3f4a5c8"
down_revision = "b9c0d1e2f3a4"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "user_sessions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("jti", sa.String(length=64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("jti", name="uq_user_sessions_jti"),
        schema=SCHEMA,
    )
    op.create_index("ix_user_sessions_user_id", "user_sessions", ["user_id"], schema=SCHEMA)
    op.create_index("ix_user_sessions_jti", "user_sessions", ["jti"], schema=SCHEMA)
    op.create_index("ix_user_sessions_expires_at", "user_sessions", ["expires_at"], schema=SCHEMA)
    op.create_index("ix_user_sessions_revoked_at", "user_sessions", ["revoked_at"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_user_sessions_revoked_at", table_name="user_sessions", schema=SCHEMA)
    op.drop_index("ix_user_sessions_expires_at", table_name="user_sessions", schema=SCHEMA)
    op.drop_index("ix_user_sessions_jti", table_name="user_sessions", schema=SCHEMA)
    op.drop_index("ix_user_sessions_user_id", table_name="user_sessions", schema=SCHEMA)
    op.drop_table("user_sessions", schema=SCHEMA)
