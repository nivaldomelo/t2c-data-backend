"""add metabase impact control tables

Revision ID: b7c8d9e0f2a3
Revises: a6b7c8d9e0f2
Create Date: 2026-04-16 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "b7c8d9e0f2a3"
down_revision = "a6b7c8d9e0f2"
branch_labels = None
depends_on = None


SCHEMA = "controle"
CATALOG_SCHEMA = "t2c_data"


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "controle"')

    op.create_table(
        "metabase_assets",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("instance_id", sa.BigInteger(), nullable=False),
        sa.Column("metabase_object_id", sa.BigInteger(), nullable=False),
        sa.Column("metabase_id", sa.String(length=80), nullable=False),
        sa.Column("asset_type", sa.String(length=30), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("collection_name", sa.String(length=255), nullable=True),
        sa.Column("collection_external_id", sa.String(length=80), nullable=True),
        sa.Column("url", sa.String(length=1000), nullable=True),
        sa.Column("archived", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["instance_id"], [f"{CATALOG_SCHEMA}.metabase_instances.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["metabase_object_id"], [f"{CATALOG_SCHEMA}.metabase_objects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("instance_id", "metabase_object_id", name="uq_metabase_assets_instance_object"),
        schema=SCHEMA,
    )
    op.create_index("ix_metabase_assets_instance_id", "metabase_assets", ["instance_id"], schema=SCHEMA)
    op.create_index("ix_metabase_assets_metabase_object_id", "metabase_assets", ["metabase_object_id"], schema=SCHEMA)
    op.create_index("ix_metabase_assets_metabase_id", "metabase_assets", ["metabase_id"], schema=SCHEMA)
    op.create_index("ix_metabase_assets_asset_type", "metabase_assets", ["asset_type"], schema=SCHEMA)
    op.create_index("ix_metabase_assets_archived", "metabase_assets", ["archived"], schema=SCHEMA)

    op.create_table(
        "metabase_table_dependencies",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("instance_id", sa.BigInteger(), nullable=False),
        sa.Column("table_id", sa.BigInteger(), nullable=False),
        sa.Column("metabase_asset_id", sa.BigInteger(), nullable=False),
        sa.Column("dependency_type", sa.String(length=40), nullable=False),
        sa.Column("confidence_level", sa.String(length=20), nullable=False, server_default=sa.text("'medium'")),
        sa.Column("break_risk_on_drop", sa.String(length=20), nullable=False, server_default=sa.text("'medium'")),
        sa.Column("break_risk_on_change", sa.String(length=20), nullable=False, server_default=sa.text("'medium'")),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["instance_id"], [f"{CATALOG_SCHEMA}.metabase_instances.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], [f"{CATALOG_SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["metabase_asset_id"], [f"{SCHEMA}.metabase_assets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("instance_id", "table_id", "metabase_asset_id", name="uq_metabase_table_dependencies_unique"),
        schema=SCHEMA,
    )
    op.create_index("ix_metabase_table_dependencies_instance_id", "metabase_table_dependencies", ["instance_id"], schema=SCHEMA)
    op.create_index("ix_metabase_table_dependencies_table_id", "metabase_table_dependencies", ["table_id"], schema=SCHEMA)
    op.create_index("ix_metabase_table_dependencies_metabase_asset_id", "metabase_table_dependencies", ["metabase_asset_id"], schema=SCHEMA)
    op.create_index("ix_metabase_table_dependencies_dependency_type", "metabase_table_dependencies", ["dependency_type"], schema=SCHEMA)
    op.create_index("ix_metabase_table_dependencies_confidence_level", "metabase_table_dependencies", ["confidence_level"], schema=SCHEMA)
    op.create_index("ix_metabase_table_dependencies_break_risk_on_drop", "metabase_table_dependencies", ["break_risk_on_drop"], schema=SCHEMA)
    op.create_index("ix_metabase_table_dependencies_break_risk_on_change", "metabase_table_dependencies", ["break_risk_on_change"], schema=SCHEMA)
    op.create_index("ix_metabase_table_dependencies_is_active", "metabase_table_dependencies", ["is_active"], schema=SCHEMA)

    op.create_table(
        "metabase_field_dependencies",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("instance_id", sa.BigInteger(), nullable=False),
        sa.Column("table_id", sa.BigInteger(), nullable=False),
        sa.Column("column_id", sa.BigInteger(), nullable=True),
        sa.Column("field_name", sa.String(length=255), nullable=False),
        sa.Column("metabase_asset_id", sa.BigInteger(), nullable=False),
        sa.Column("dependency_type", sa.String(length=40), nullable=False),
        sa.Column("confidence_level", sa.String(length=20), nullable=False, server_default=sa.text("'medium'")),
        sa.Column("break_risk_on_drop", sa.String(length=20), nullable=False, server_default=sa.text("'medium'")),
        sa.Column("break_risk_on_change", sa.String(length=20), nullable=False, server_default=sa.text("'medium'")),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["instance_id"], [f"{CATALOG_SCHEMA}.metabase_instances.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], [f"{CATALOG_SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["column_id"], [f"{CATALOG_SCHEMA}.columns.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["metabase_asset_id"], [f"{SCHEMA}.metabase_assets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "instance_id",
            "table_id",
            "column_id",
            "field_name",
            "metabase_asset_id",
            name="uq_metabase_field_dependencies_unique",
        ),
        schema=SCHEMA,
    )
    op.create_index("ix_metabase_field_dependencies_instance_id", "metabase_field_dependencies", ["instance_id"], schema=SCHEMA)
    op.create_index("ix_metabase_field_dependencies_table_id", "metabase_field_dependencies", ["table_id"], schema=SCHEMA)
    op.create_index("ix_metabase_field_dependencies_column_id", "metabase_field_dependencies", ["column_id"], schema=SCHEMA)
    op.create_index("ix_metabase_field_dependencies_metabase_asset_id", "metabase_field_dependencies", ["metabase_asset_id"], schema=SCHEMA)
    op.create_index("ix_metabase_field_dependencies_field_name", "metabase_field_dependencies", ["field_name"], schema=SCHEMA)
    op.create_index("ix_metabase_field_dependencies_dependency_type", "metabase_field_dependencies", ["dependency_type"], schema=SCHEMA)
    op.create_index("ix_metabase_field_dependencies_is_active", "metabase_field_dependencies", ["is_active"], schema=SCHEMA)

    op.create_table(
        "metabase_impact_snapshots",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("instance_id", sa.BigInteger(), nullable=False),
        sa.Column("table_id", sa.BigInteger(), nullable=False),
        sa.Column("dashboard_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("question_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("model_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("asset_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("break_risk_on_drop", sa.String(length=20), nullable=False, server_default=sa.text("'none'")),
        sa.Column("break_risk_on_change", sa.String(length=20), nullable=False, server_default=sa.text("'none'")),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("summary_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["instance_id"], [f"{CATALOG_SCHEMA}.metabase_instances.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], [f"{CATALOG_SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )
    op.create_index("ix_metabase_impact_snapshots_instance_id", "metabase_impact_snapshots", ["instance_id"], schema=SCHEMA)
    op.create_index("ix_metabase_impact_snapshots_table_id", "metabase_impact_snapshots", ["table_id"], schema=SCHEMA)
    op.create_index("ix_metabase_impact_snapshots_last_verified_at", "metabase_impact_snapshots", ["last_verified_at"], schema=SCHEMA)
    op.create_index(
        "ix_metabase_impact_snapshots_instance_table_created",
        "metabase_impact_snapshots",
        ["instance_id", "table_id", "created_at"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_metabase_impact_snapshots_instance_table_created", table_name="metabase_impact_snapshots", schema=SCHEMA)
    op.drop_index("ix_metabase_impact_snapshots_last_verified_at", table_name="metabase_impact_snapshots", schema=SCHEMA)
    op.drop_index("ix_metabase_impact_snapshots_table_id", table_name="metabase_impact_snapshots", schema=SCHEMA)
    op.drop_index("ix_metabase_impact_snapshots_instance_id", table_name="metabase_impact_snapshots", schema=SCHEMA)
    op.drop_table("metabase_impact_snapshots", schema=SCHEMA)

    op.drop_index("ix_metabase_field_dependencies_is_active", table_name="metabase_field_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_field_dependencies_dependency_type", table_name="metabase_field_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_field_dependencies_field_name", table_name="metabase_field_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_field_dependencies_metabase_asset_id", table_name="metabase_field_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_field_dependencies_column_id", table_name="metabase_field_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_field_dependencies_table_id", table_name="metabase_field_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_field_dependencies_instance_id", table_name="metabase_field_dependencies", schema=SCHEMA)
    op.drop_table("metabase_field_dependencies", schema=SCHEMA)

    op.drop_index("ix_metabase_table_dependencies_is_active", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_table_dependencies_break_risk_on_change", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_table_dependencies_break_risk_on_drop", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_table_dependencies_confidence_level", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_table_dependencies_dependency_type", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_table_dependencies_metabase_asset_id", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_table_dependencies_table_id", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_index("ix_metabase_table_dependencies_instance_id", table_name="metabase_table_dependencies", schema=SCHEMA)
    op.drop_table("metabase_table_dependencies", schema=SCHEMA)

    op.drop_index("ix_metabase_assets_archived", table_name="metabase_assets", schema=SCHEMA)
    op.drop_index("ix_metabase_assets_asset_type", table_name="metabase_assets", schema=SCHEMA)
    op.drop_index("ix_metabase_assets_metabase_id", table_name="metabase_assets", schema=SCHEMA)
    op.drop_index("ix_metabase_assets_metabase_object_id", table_name="metabase_assets", schema=SCHEMA)
    op.drop_index("ix_metabase_assets_instance_id", table_name="metabase_assets", schema=SCHEMA)
    op.drop_table("metabase_assets", schema=SCHEMA)
