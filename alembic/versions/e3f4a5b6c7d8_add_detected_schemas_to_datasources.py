"""add detected schemas to datasources

Revision ID: e3f4a5b6c7d8
Revises: f2a3b4c5d6e7
Create Date: 2026-04-20 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e3f4a5b6c7d8"
down_revision = "f2a3b4c5d6e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("data_sources", sa.Column("detected_schemas", sa.JSON(), nullable=True))
    op.execute(
        sa.text(
            """
            UPDATE data_sources
            SET detected_schemas = include_schemas
            WHERE detected_schemas IS NULL AND include_schemas IS NOT NULL
            """
        )
    )


def downgrade() -> None:
    op.drop_column("data_sources", "detected_schemas")
