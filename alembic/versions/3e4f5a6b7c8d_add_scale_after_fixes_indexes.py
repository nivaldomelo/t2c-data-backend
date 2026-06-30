"""add scale after fixes indexes

Revision ID: 3e4f5a6b7c8d
Revises: 7c8d9e0f1a2b
Create Date: 2026-05-26 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "3e4f5a6b7c8d"
down_revision: Union[str, Sequence[str], None] = "7c8d9e0f1a2b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_index(
        "ix_tables_data_owner_updated_at",
        "tables",
        ["data_owner_id", "updated_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_tables_updated_at",
        "tables",
        ["updated_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dq_rules_active_severity_updated_at",
        "dq_rules",
        ["is_active", "severity", "updated_at"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_dq_rules_active_severity_updated_at", table_name="dq_rules", schema=SCHEMA)
    op.drop_index("ix_tables_updated_at", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_data_owner_updated_at", table_name="tables", schema=SCHEMA)
