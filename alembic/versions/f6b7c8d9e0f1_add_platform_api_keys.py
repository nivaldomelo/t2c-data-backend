"""add platform api keys

Revision ID: f6b7c8d9e0f1
Revises: f4a5b6c7d8e9
Create Date: 2026-04-11 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from t2c_data.core.config import settings

revision: str = "f6b7c8d9e0f1"
down_revision: Union[str, Sequence[str], None] = "f4a5b6c7d8e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    schema = settings.db_schema
    op.create_table(
        "platform_api_keys",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("public_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=24), server_default="active", nullable=False),
        sa.Column("scopes_json", postgresql.JSONB(astext_type=sa.Text()), server_default="[]", nullable=False),
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column("token_prefix", sa.String(length=16), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_ip", sa.String(length=80), nullable=True),
        sa.Column("last_used_user_agent", sa.String(length=320), nullable=True),
        sa.Column("usage_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_platform_api_keys_public_id"),
        schema=schema,
    )
    op.create_index("ix_platform_api_keys_status", "platform_api_keys", ["status"], unique=False, schema=schema)
    op.create_index("ix_platform_api_keys_expires_at", "platform_api_keys", ["expires_at"], unique=False, schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_platform_api_keys_expires_at", table_name="platform_api_keys", schema=schema)
    op.drop_index("ix_platform_api_keys_status", table_name="platform_api_keys", schema=schema)
    op.drop_table("platform_api_keys", schema=schema)
