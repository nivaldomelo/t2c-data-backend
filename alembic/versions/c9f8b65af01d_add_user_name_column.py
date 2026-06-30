"""add user name column

Revision ID: c9f8b65af01d
Revises: 7c5bf86829b1
Create Date: 2026-02-21 17:35:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "c9f8b65af01d"
down_revision = "7c5bf86829b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("name", sa.String(length=255), nullable=True), schema="t2c_data")
    op.execute("UPDATE t2c_data.users SET name = full_name WHERE name IS NULL AND full_name IS NOT NULL")


def downgrade() -> None:
    op.drop_column("users", "name", schema="t2c_data")
