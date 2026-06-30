"""add data lake inventory tables

Revision ID: b07c8d9e0f12
Revises: af6b7c8d9e01
Create Date: 2026-04-19 02:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "b07c8d9e0f12"
down_revision = "af6b7c8d9e01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_lake_inventory_scan_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("connection_id", sa.Integer(), sa.ForeignKey("data_lake_connections.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="running"),
        sa.Column("scanned_layers_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("discovered_tables_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("discovered_parquet_files_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scanned_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index(
        "ix_data_lake_inventory_scan_runs_connection_created",
        "data_lake_inventory_scan_runs",
        ["connection_id", "created_at"],
    )
    op.create_index("ix_data_lake_inventory_scan_runs_status", "data_lake_inventory_scan_runs", ["status"])

    op.create_table(
        "data_lake_inventory_tables",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("connection_id", sa.Integer(), sa.ForeignKey("data_lake_connections.id", ondelete="CASCADE"), nullable=False),
        sa.Column("layer", sa.String(length=20), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("path_base", sa.String(length=1000), nullable=False),
        sa.Column("files_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("parquet_files_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("non_parquet_files_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("size_total_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("last_modified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("has_partitions", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("partition_pattern_detected", sa.String(length=80), nullable=True),
        sa.Column("status_scan", sa.String(length=20), nullable=False, server_default="unknown"),
        sa.Column("data_last_scan_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scan_run_id", sa.Integer(), sa.ForeignKey("data_lake_inventory_scan_runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("connection_id", "layer", "table_name", "path_base", name="uq_data_lake_inventory_tables_identity"),
    )
    op.create_index(
        "ix_data_lake_inventory_tables_connection_layer",
        "data_lake_inventory_tables",
        ["connection_id", "layer"],
    )
    op.create_index("ix_data_lake_inventory_tables_status_scan", "data_lake_inventory_tables", ["status_scan"])
    op.create_index("ix_data_lake_inventory_tables_last_scan", "data_lake_inventory_tables", ["data_last_scan_at"])


def downgrade() -> None:
    op.drop_index("ix_data_lake_inventory_tables_last_scan", table_name="data_lake_inventory_tables")
    op.drop_index("ix_data_lake_inventory_tables_status_scan", table_name="data_lake_inventory_tables")
    op.drop_index("ix_data_lake_inventory_tables_connection_layer", table_name="data_lake_inventory_tables")
    op.drop_table("data_lake_inventory_tables")
    op.drop_index("ix_data_lake_inventory_scan_runs_status", table_name="data_lake_inventory_scan_runs")
    op.drop_index("ix_data_lake_inventory_scan_runs_connection_created", table_name="data_lake_inventory_scan_runs")
    op.drop_table("data_lake_inventory_scan_runs")
