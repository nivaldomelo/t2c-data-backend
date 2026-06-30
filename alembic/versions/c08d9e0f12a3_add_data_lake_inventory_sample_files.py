"""add data lake inventory sample files

Revision ID: c08d9e0f12a3
Revises: b07c8d9e0f12
Create Date: 2026-04-19 02:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "c08d9e0f12a3"
down_revision: Union[str, Sequence[str], None] = "b07c8d9e0f12"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "data_lake_inventory_tables",
        sa.Column("sample_parquet_files_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("data_lake_inventory_tables", "sample_parquet_files_json")
