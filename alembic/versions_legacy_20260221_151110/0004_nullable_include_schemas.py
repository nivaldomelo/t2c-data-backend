"""make include_schemas nullable on data_sources

Revision ID: 0004_nullable_include_schemas
Revises: 0003_fix_db_type_rename
Create Date: 2026-02-20 00:00:02

"""

from alembic import op
import sqlalchemy as sa


revision = "0004_nullable_include_schemas"
down_revision = "0003_fix_db_type_rename"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("data_sources", "include_schemas", existing_type=sa.JSON(), nullable=True)


def downgrade() -> None:
    op.execute("UPDATE data_sources SET include_schemas = '[]' WHERE include_schemas IS NULL")
    op.alter_column("data_sources", "include_schemas", existing_type=sa.JSON(), nullable=False)
