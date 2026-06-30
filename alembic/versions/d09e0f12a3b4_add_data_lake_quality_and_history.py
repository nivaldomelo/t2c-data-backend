"""add data lake quality and history

Revision ID: d09e0f12a3b4
Revises: c08d9e0f12a3
Create Date: 2026-04-19 03:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "d09e0f12a3b4"
down_revision: Union[str, Sequence[str], None] = "c08d9e0f12a3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("data_lake_connections", sa.Column("freshness_sla_hours_default", sa.Integer(), nullable=True))
    op.add_column("data_lake_connections", sa.Column("freshness_sla_hours_bronze", sa.Integer(), nullable=True))
    op.add_column("data_lake_connections", sa.Column("freshness_sla_hours_silver", sa.Integer(), nullable=True))
    op.add_column("data_lake_connections", sa.Column("freshness_sla_hours_gold", sa.Integer(), nullable=True))

    op.add_column("data_lake_inventory_tables", sa.Column("freshness_sla_hours_override", sa.Integer(), nullable=True))
    op.add_column("data_lake_inventory_tables", sa.Column("last_quality_score", sa.Float(), nullable=True))
    op.add_column(
        "data_lake_inventory_tables",
        sa.Column("last_quality_evaluated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "data_lake_table_observations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("connection_id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("source_kind", sa.String(length=20), nullable=False, server_default="detail"),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("freshness_status", sa.String(length=20), nullable=False, server_default="unknown"),
        sa.Column("freshness_age_seconds", sa.Integer(), nullable=True),
        sa.Column("freshness_sla_hours", sa.Integer(), nullable=True),
        sa.Column("quality_score", sa.Float(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("row_count_method", sa.String(length=40), nullable=True),
        sa.Column("row_count_confidence", sa.String(length=40), nullable=True),
        sa.Column("size_total_bytes", sa.BigInteger(), nullable=True),
        sa.Column("schema_variants_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("null_columns_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("missing_columns_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unreadable_files_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("drift_detected", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("signals_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["connection_id"], ["data_lake_connections.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], ["data_lake_inventory_tables.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_data_lake_table_observations_table_created",
        "data_lake_table_observations",
        ["table_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_data_lake_table_observations_connection_created",
        "data_lake_table_observations",
        ["connection_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_data_lake_table_observations_source_created",
        "data_lake_table_observations",
        ["source_kind", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_data_lake_table_observations_source_created", table_name="data_lake_table_observations")
    op.drop_index("ix_data_lake_table_observations_connection_created", table_name="data_lake_table_observations")
    op.drop_index("ix_data_lake_table_observations_table_created", table_name="data_lake_table_observations")
    op.drop_table("data_lake_table_observations")

    op.drop_column("data_lake_inventory_tables", "last_quality_evaluated_at")
    op.drop_column("data_lake_inventory_tables", "last_quality_score")
    op.drop_column("data_lake_inventory_tables", "freshness_sla_hours_override")

    op.drop_column("data_lake_connections", "freshness_sla_hours_gold")
    op.drop_column("data_lake_connections", "freshness_sla_hours_silver")
    op.drop_column("data_lake_connections", "freshness_sla_hours_bronze")
    op.drop_column("data_lake_connections", "freshness_sla_hours_default")
