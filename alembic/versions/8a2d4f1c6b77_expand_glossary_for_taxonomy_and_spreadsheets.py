"""expand glossary for taxonomy and spreadsheets

Revision ID: 8a2d4f1c6b77
Revises: 6f4d2b8c1a90
Create Date: 2026-03-19 02:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "8a2d4f1c6b77"
down_revision = "6f4d2b8c1a90"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column("glossary_terms", sa.Column("external_id", sa.String(length=40), nullable=True), schema=SCHEMA)
    op.add_column("glossary_terms", sa.Column("slug", sa.String(length=160), nullable=True), schema=SCHEMA)
    op.add_column("glossary_terms", sa.Column("category", sa.String(length=120), nullable=True), schema=SCHEMA)
    op.add_column("glossary_terms", sa.Column("subcategory", sa.String(length=120), nullable=True), schema=SCHEMA)
    op.add_column("glossary_terms", sa.Column("example_of_use", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("glossary_terms", sa.Column("synonyms", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("glossary_terms", sa.Column("suggested_priority", sa.String(length=40), nullable=True), schema=SCHEMA)
    op.add_column(
        "glossary_terms",
        sa.Column("status", sa.String(length=30), nullable=False, server_default="active"),
        schema=SCHEMA,
    )
    op.add_column("glossary_terms", sa.Column("tag_labels", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("glossary_terms", sa.Column("notes", sa.Text(), nullable=True), schema=SCHEMA)

    op.execute(
        """
        UPDATE t2c_data.glossary_terms
        SET slug = lower(
            trim(
                regexp_replace(
                    regexp_replace(name, '[^a-zA-Z0-9]+', '-', 'g'),
                    '-{2,}',
                    '-',
                    'g'
                )
            )
        )
        """
    )
    op.execute("UPDATE t2c_data.glossary_terms SET slug = trim(both '-' from slug)")
    op.execute(
        """
        UPDATE t2c_data.glossary_terms
        SET slug = concat('term-', id)
        WHERE slug IS NULL OR slug = ''
        """
    )
    op.alter_column("glossary_terms", "slug", nullable=False, schema=SCHEMA)
    op.create_unique_constraint("uq_glossary_terms_slug", "glossary_terms", ["slug"], schema=SCHEMA)
    op.create_unique_constraint("uq_glossary_terms_external_id", "glossary_terms", ["external_id"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_constraint("uq_glossary_terms_external_id", "glossary_terms", schema=SCHEMA, type_="unique")
    op.drop_constraint("uq_glossary_terms_slug", "glossary_terms", schema=SCHEMA, type_="unique")
    op.drop_column("glossary_terms", "notes", schema=SCHEMA)
    op.drop_column("glossary_terms", "tag_labels", schema=SCHEMA)
    op.drop_column("glossary_terms", "status", schema=SCHEMA)
    op.drop_column("glossary_terms", "suggested_priority", schema=SCHEMA)
    op.drop_column("glossary_terms", "synonyms", schema=SCHEMA)
    op.drop_column("glossary_terms", "example_of_use", schema=SCHEMA)
    op.drop_column("glossary_terms", "subcategory", schema=SCHEMA)
    op.drop_column("glossary_terms", "category", schema=SCHEMA)
    op.drop_column("glossary_terms", "slug", schema=SCHEMA)
    op.drop_column("glossary_terms", "external_id", schema=SCHEMA)
