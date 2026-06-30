"""add owner_email to tables

Revision ID: 0006_table_owner_email
Revises: 0005_col_pk_flag
Create Date: 2026-02-20 11:40:00

"""

from alembic import op
import sqlalchemy as sa


revision = "0006_table_owner_email"
down_revision = "0005_col_pk_flag"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tables", sa.Column("owner_email", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("tables", "owner_email")
