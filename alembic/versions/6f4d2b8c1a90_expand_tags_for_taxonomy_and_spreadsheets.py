"""expand tags for taxonomy and spreadsheets

Revision ID: 6f4d2b8c1a90
Revises: 3c1f0b7d2e4a
Create Date: 2026-03-19 01:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6f4d2b8c1a90"
down_revision = "3c1f0b7d2e4a"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column("tags", sa.Column("external_id", sa.String(length=40), nullable=True), schema=SCHEMA)
    op.add_column("tags", sa.Column("slug", sa.String(length=160), nullable=True), schema=SCHEMA)
    op.add_column("tags", sa.Column("group_name", sa.String(length=120), nullable=True), schema=SCHEMA)
    op.add_column("tags", sa.Column("subgroup_name", sa.String(length=120), nullable=True), schema=SCHEMA)
    op.add_column("tags", sa.Column("example_of_use", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("tags", sa.Column("tag_type", sa.String(length=120), nullable=True), schema=SCHEMA)
    op.add_column("tags", sa.Column("suggested_scope", sa.String(length=160), nullable=True), schema=SCHEMA)
    op.add_column(
        "tags",
        sa.Column("status", sa.String(length=30), nullable=False, server_default="active"),
        schema=SCHEMA,
    )
    op.add_column("tags", sa.Column("synonyms", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("tags", sa.Column("notes", sa.Text(), nullable=True), schema=SCHEMA)
    op.alter_column("tags", "description", type_=sa.Text(), schema=SCHEMA)

    op.execute(
        """
        UPDATE t2c_data.tags
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
    op.execute("UPDATE t2c_data.tags SET slug = trim(both '-' from slug)")
    op.execute(
        """
        UPDATE t2c_data.tags
        SET slug = concat('tag-', id)
        WHERE slug IS NULL OR slug = ''
        """
    )
    op.alter_column("tags", "slug", nullable=False, schema=SCHEMA)
    op.create_unique_constraint("uq_tags_slug", "tags", ["slug"], schema=SCHEMA)
    op.create_unique_constraint("uq_tags_external_id", "tags", ["external_id"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_constraint("uq_tags_external_id", "tags", schema=SCHEMA, type_="unique")
    op.drop_constraint("uq_tags_slug", "tags", schema=SCHEMA, type_="unique")
    op.alter_column("tags", "description", type_=sa.String(length=255), schema=SCHEMA)
    op.drop_column("tags", "notes", schema=SCHEMA)
    op.drop_column("tags", "synonyms", schema=SCHEMA)
    op.drop_column("tags", "status", schema=SCHEMA)
    op.drop_column("tags", "suggested_scope", schema=SCHEMA)
    op.drop_column("tags", "tag_type", schema=SCHEMA)
    op.drop_column("tags", "example_of_use", schema=SCHEMA)
    op.drop_column("tags", "subgroup_name", schema=SCHEMA)
    op.drop_column("tags", "group_name", schema=SCHEMA)
    op.drop_column("tags", "slug", schema=SCHEMA)
    op.drop_column("tags", "external_id", schema=SCHEMA)
