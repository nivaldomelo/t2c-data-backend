"""add connection config to datasources

Revision ID: e6d4a1b9c2f3
Revises: c4e7a2b1d9f0
Create Date: 2026-03-21 19:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "e6d4a1b9c2f3"
down_revision = "c4e7a2b1d9f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("data_sources", sa.Column("connection_config", sa.JSON(), nullable=True), schema="t2c_data")


def downgrade() -> None:
    op.drop_column("data_sources", "connection_config", schema="t2c_data")
