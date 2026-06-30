"""add tag assignment evidence json

Revision ID: aa12bb34cc56
Revises: f6b7c8d9e0f1
Create Date: 2026-04-12 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


# revision identifiers, used by Alembic.
revision = "aa12bb34cc56"
down_revision = "f6b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tag_assignments",
        sa.Column("evidence_json", sa.JSON(), nullable=True),
        schema=settings.db_schema,
    )


def downgrade() -> None:
    op.drop_column("tag_assignments", "evidence_json", schema=settings.db_schema)
