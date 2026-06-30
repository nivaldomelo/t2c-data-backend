"""add search favorite assets

Revision ID: f7c8d9e0a1b2
Revises: e3f4a5b6c7d8
Create Date: 2026-04-29 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f7c8d9e0a1b2"
down_revision = "e3f4a5b6c7d8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "search_favorite_assets",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=False),
        sa.Column("target_url", sa.String(length=500), nullable=True),
        sa.Column("category", sa.String(length=80), nullable=True),
        sa.Column("subtitle", sa.String(length=255), nullable=True),
        sa.Column("context_path", sa.String(length=500), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "entity_type", "entity_id", name="uq_search_favorite_assets_user_entity"),
    )
    op.create_index("ix_search_favorite_assets_entity", "search_favorite_assets", ["entity_type", "entity_id"], unique=False)
    op.create_index("ix_search_favorite_assets_user_created", "search_favorite_assets", ["user_id", "created_at"], unique=False)
    op.create_index(op.f("ix_search_favorite_assets_user_id"), "search_favorite_assets", ["user_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_search_favorite_assets_user_id"), table_name="search_favorite_assets")
    op.drop_index("ix_search_favorite_assets_user_created", table_name="search_favorite_assets")
    op.drop_index("ix_search_favorite_assets_entity", table_name="search_favorite_assets")
    op.drop_table("search_favorite_assets")
