"""add internal openlineage lineage tables

Revision ID: 0b1c2d3e4f5a
Revises: a5b6c7d8e9f0
Create Date: 2026-04-08 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0b1c2d3e4f5a"
down_revision = "a5b6c7d8e9f0"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.alter_column(
        "lineage_source_configs",
        "source_type",
        schema=SCHEMA,
        existing_type=sa.String(length=30),
        server_default=sa.text("'openlineage'"),
    )

    op.create_table(
        "lineage_column_edges",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lineage_source_id", sa.Integer(), nullable=True),
        sa.Column("lineage_job_id", sa.Integer(), nullable=True),
        sa.Column("source_asset_id", sa.Integer(), nullable=False),
        sa.Column("target_asset_id", sa.Integer(), nullable=False),
        sa.Column("source_column_name", sa.String(length=255), nullable=False),
        sa.Column("target_column_name", sa.String(length=255), nullable=False),
        sa.Column("relation_type", sa.String(length=30), nullable=False, server_default="transformation"),
        sa.Column("discovery_method", sa.String(length=30), nullable=False, server_default="automatic"),
        sa.Column("confidence_score", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("external_edge_key", sa.String(length=500), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["lineage_source_id"], [f"{SCHEMA}.lineage_source_configs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["lineage_job_id"], [f"{SCHEMA}.lineage_jobs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_asset_id"], [f"{SCHEMA}.lineage_assets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_asset_id"], [f"{SCHEMA}.lineage_assets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_asset_id",
            "target_asset_id",
            "source_column_name",
            "target_column_name",
            "relation_type",
            name="uq_lineage_column_edges_unique",
        ),
        schema=SCHEMA,
    )
    op.create_index("ix_lineage_column_edges_lineage_source_id", "lineage_column_edges", ["lineage_source_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_column_edges_lineage_job_id", "lineage_column_edges", ["lineage_job_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_column_edges_source_asset_id", "lineage_column_edges", ["source_asset_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_column_edges_target_asset_id", "lineage_column_edges", ["target_asset_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_column_edges_relation_type", "lineage_column_edges", ["relation_type"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_column_edges_is_active", "lineage_column_edges", ["is_active"], unique=False, schema=SCHEMA)

    op.create_table(
        "lineage_event_raw",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lineage_source_id", sa.Integer(), nullable=True),
        sa.Column("event_key", sa.String(length=500), nullable=False),
        sa.Column("event_type", sa.String(length=80), nullable=True),
        sa.Column("producer", sa.String(length=500), nullable=True),
        sa.Column("namespace", sa.String(length=255), nullable=True),
        sa.Column("job_name", sa.String(length=500), nullable=True),
        sa.Column("run_id", sa.String(length=255), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("schema_name", sa.String(length=100), nullable=True),
        sa.Column("object_name", sa.String(length=200), nullable=True),
        sa.Column("object_type", sa.String(length=30), nullable=True),
        sa.Column("event_time", sa.String(length=40), nullable=True),
        sa.Column("status", sa.String(length=80), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("is_processed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["lineage_source_id"], [f"{SCHEMA}.lineage_source_configs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["datasource_id"], [f"{SCHEMA}.data_sources.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lineage_source_id", "event_key", name="uq_lineage_event_raw_source_event_key"),
        schema=SCHEMA,
    )
    op.create_index("ix_lineage_event_raw_lineage_source_id", "lineage_event_raw", ["lineage_source_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_event_key", "lineage_event_raw", ["event_key"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_event_type", "lineage_event_raw", ["event_type"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_namespace", "lineage_event_raw", ["namespace"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_job_name", "lineage_event_raw", ["job_name"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_run_id", "lineage_event_raw", ["run_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_datasource_id", "lineage_event_raw", ["datasource_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_schema_name", "lineage_event_raw", ["schema_name"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_object_name", "lineage_event_raw", ["object_name"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_object_type", "lineage_event_raw", ["object_type"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_event_time", "lineage_event_raw", ["event_time"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_status", "lineage_event_raw", ["status"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_event_raw_is_processed", "lineage_event_raw", ["is_processed"], unique=False, schema=SCHEMA)

    op.create_table(
        "lineage_sync_checkpoints",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lineage_source_id", sa.Integer(), nullable=False),
        sa.Column("checkpoint_type", sa.String(length=40), nullable=False, server_default="openlineage"),
        sa.Column("last_event_raw_id", sa.Integer(), nullable=True),
        sa.Column("last_processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_status", sa.String(length=40), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("cursor_value", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["lineage_source_id"], [f"{SCHEMA}.lineage_source_configs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["last_event_raw_id"], [f"{SCHEMA}.lineage_event_raw.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lineage_source_id", "checkpoint_type", name="uq_lineage_sync_checkpoints_source_type"),
        schema=SCHEMA,
    )
    op.create_index("ix_lineage_sync_checkpoints_lineage_source_id", "lineage_sync_checkpoints", ["lineage_source_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_sync_checkpoints_checkpoint_type", "lineage_sync_checkpoints", ["checkpoint_type"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_sync_checkpoints_last_event_raw_id", "lineage_sync_checkpoints", ["last_event_raw_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_lineage_sync_checkpoints_last_status", "lineage_sync_checkpoints", ["last_status"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_lineage_sync_checkpoints_last_status", table_name="lineage_sync_checkpoints", schema=SCHEMA)
    op.drop_index("ix_lineage_sync_checkpoints_last_event_raw_id", table_name="lineage_sync_checkpoints", schema=SCHEMA)
    op.drop_index("ix_lineage_sync_checkpoints_checkpoint_type", table_name="lineage_sync_checkpoints", schema=SCHEMA)
    op.drop_index("ix_lineage_sync_checkpoints_lineage_source_id", table_name="lineage_sync_checkpoints", schema=SCHEMA)
    op.drop_table("lineage_sync_checkpoints", schema=SCHEMA)

    op.drop_index("ix_lineage_event_raw_is_processed", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_status", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_event_time", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_object_type", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_object_name", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_schema_name", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_datasource_id", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_run_id", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_job_name", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_namespace", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_event_type", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_event_key", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_index("ix_lineage_event_raw_lineage_source_id", table_name="lineage_event_raw", schema=SCHEMA)
    op.drop_table("lineage_event_raw", schema=SCHEMA)

    op.drop_index("ix_lineage_column_edges_is_active", table_name="lineage_column_edges", schema=SCHEMA)
    op.drop_index("ix_lineage_column_edges_relation_type", table_name="lineage_column_edges", schema=SCHEMA)
    op.drop_index("ix_lineage_column_edges_target_asset_id", table_name="lineage_column_edges", schema=SCHEMA)
    op.drop_index("ix_lineage_column_edges_source_asset_id", table_name="lineage_column_edges", schema=SCHEMA)
    op.drop_index("ix_lineage_column_edges_lineage_job_id", table_name="lineage_column_edges", schema=SCHEMA)
    op.drop_index("ix_lineage_column_edges_lineage_source_id", table_name="lineage_column_edges", schema=SCHEMA)
    op.drop_table("lineage_column_edges", schema=SCHEMA)

    op.alter_column(
        "lineage_source_configs",
        "source_type",
        schema=SCHEMA,
        existing_type=sa.String(length=30),
        server_default=None,
    )
