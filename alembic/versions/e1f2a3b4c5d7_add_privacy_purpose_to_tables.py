"""add privacy purpose to tables

Revision ID: e1f2a3b4c5d7
Revises: d5f6a7b8c9d0
Create Date: 2026-05-13 23:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "e1f2a3b4c5d7"
down_revision = "d5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    op.add_column("tables", sa.Column("privacy_purpose", sa.Text(), nullable=True), schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_column("tables", "privacy_purpose", schema=schema)
