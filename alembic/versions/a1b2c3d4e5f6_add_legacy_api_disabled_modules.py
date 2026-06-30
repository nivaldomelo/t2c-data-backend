"""add legacy api disabled modules

Revision ID: a1b2c3d4e5f6
Revises: 9c3d4e5f6a7b
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "a1b2c3d4e5f6"
down_revision = "9c3d4e5f6a7b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("legacy_api_disabled_modules", sa.Text(), nullable=True),
        schema=settings.db_schema,
    )


def downgrade() -> None:
    op.drop_column("governance_settings", "legacy_api_disabled_modules", schema=settings.db_schema)
