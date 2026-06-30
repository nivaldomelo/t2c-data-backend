"""add is_primary_key to columns

Revision ID: 0005_col_pk_flag
Revises: 0004_nullable_include_schemas
Create Date: 2026-02-20 00:00:03

"""

from alembic import op
import sqlalchemy as sa


revision = "0005_col_pk_flag"
down_revision = "0004_nullable_include_schemas"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "columns",
        sa.Column("is_primary_key", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("columns", "is_primary_key")
