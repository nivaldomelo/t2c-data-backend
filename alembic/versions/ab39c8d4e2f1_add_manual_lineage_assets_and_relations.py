"""add manual lineage assets and relations

Revision ID: ab39c8d4e2f1
Revises: f7c1d2e3a4b5
Create Date: 2026-03-22 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "ab39c8d4e2f1"
down_revision = "f7c1d2e3a4b5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "lineage_assets",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("catalog_table_id", sa.Integer(), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("asset_key", sa.String(length=255), nullable=False),
        sa.Column("asset_name", sa.String(length=255), nullable=False),
        sa.Column("asset_type", sa.String(length=30), nullable=False),
        sa.Column("layer", sa.String(length=30), nullable=False),
        sa.Column("schema_name", sa.String(length=100), nullable=True),
        sa.Column("object_name", sa.String(length=200), nullable=True),
        sa.Column("system_name", sa.String(length=120), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["catalog_table_id"], ["t2c_data.tables.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["datasource_id"], ["t2c_data.data_sources.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_key", name="uq_lineage_assets_asset_key"),
        sa.UniqueConstraint("catalog_table_id", name="uq_lineage_assets_catalog_table_id"),
        schema="t2c_data",
    )
    op.create_index("ix_lineage_assets_asset_key", "lineage_assets", ["asset_key"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_catalog_table_id", "lineage_assets", ["catalog_table_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_datasource_id", "lineage_assets", ["datasource_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_asset_type", "lineage_assets", ["asset_type"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_layer", "lineage_assets", ["layer"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_is_active", "lineage_assets", ["is_active"], unique=False, schema="t2c_data")

    op.create_table(
        "lineage_relations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_asset_id", sa.Integer(), nullable=False),
        sa.Column("target_asset_id", sa.Integer(), nullable=False),
        sa.Column("relation_type", sa.String(length=30), nullable=False),
        sa.Column("process_name", sa.String(length=255), nullable=True),
        sa.Column("process_type", sa.String(length=50), nullable=True),
        sa.Column("dashboard_name", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("discovery_method", sa.String(length=30), nullable=False, server_default="manual"),
        sa.Column("confidence_score", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["source_asset_id"], ["t2c_data.lineage_assets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_asset_id"], ["t2c_data.lineage_assets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index("ix_lineage_relations_source_asset_id", "lineage_relations", ["source_asset_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_relations_target_asset_id", "lineage_relations", ["target_asset_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_relations_relation_type", "lineage_relations", ["relation_type"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_relations_is_active", "lineage_relations", ["is_active"], unique=False, schema="t2c_data")

    op.alter_column("lineage_assets", "is_active", server_default=None, schema="t2c_data")
    op.alter_column("lineage_relations", "discovery_method", server_default=None, schema="t2c_data")
    op.alter_column("lineage_relations", "confidence_score", server_default=None, schema="t2c_data")
    op.alter_column("lineage_relations", "is_active", server_default=None, schema="t2c_data")


def downgrade() -> None:
    op.drop_index("ix_lineage_relations_is_active", table_name="lineage_relations", schema="t2c_data")
    op.drop_index("ix_lineage_relations_relation_type", table_name="lineage_relations", schema="t2c_data")
    op.drop_index("ix_lineage_relations_target_asset_id", table_name="lineage_relations", schema="t2c_data")
    op.drop_index("ix_lineage_relations_source_asset_id", table_name="lineage_relations", schema="t2c_data")
    op.drop_table("lineage_relations", schema="t2c_data")

    op.drop_index("ix_lineage_assets_is_active", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_layer", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_asset_type", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_datasource_id", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_catalog_table_id", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_asset_key", table_name="lineage_assets", schema="t2c_data")
    op.drop_table("lineage_assets", schema="t2c_data")
