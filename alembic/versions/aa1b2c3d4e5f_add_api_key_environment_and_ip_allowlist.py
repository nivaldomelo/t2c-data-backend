"""add api key environment and ip allowlist

Revision ID: aa1b2c3d4e5f
Revises: f6b7c8d9e0f1
Create Date: 2026-04-16 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from t2c_data.core.config import settings

revision: str = "aa1b2c3d4e5f"
down_revision: Union[str, Sequence[str], None] = "f6b7c8d9e0f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    schema = settings.db_schema
    op.add_column(
        "platform_api_keys",
        sa.Column("environment", sa.String(length=32), server_default="shared", nullable=False),
        schema=schema,
    )
    op.add_column(
        "platform_api_keys",
        sa.Column("allowed_ips_json", postgresql.JSONB(astext_type=sa.Text()), server_default="[]", nullable=False),
        schema=schema,
    )
    op.create_index("ix_platform_api_keys_environment", "platform_api_keys", ["environment"], unique=False, schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_platform_api_keys_environment", table_name="platform_api_keys", schema=schema)
    op.drop_column("platform_api_keys", "allowed_ips_json", schema=schema)
    op.drop_column("platform_api_keys", "environment", schema=schema)
