"""add description to glossary_terms

Revision ID: 0007_glossary_description
Revises: 0006_table_owner_email
Create Date: 2026-02-20 12:15:00

"""

from alembic import op
import sqlalchemy as sa


revision = "0007_glossary_description"
down_revision = "0006_table_owner_email"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("glossary_terms", sa.Column("description", sa.Text(), nullable=True))
    op.execute("UPDATE glossary_terms SET description = definition WHERE description IS NULL")


def downgrade() -> None:
    op.drop_column("glossary_terms", "description")
