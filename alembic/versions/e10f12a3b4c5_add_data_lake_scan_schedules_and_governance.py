"""add data lake scan schedules and governance

Revision ID: e10f12a3b4c5
Revises: ae5f60718293
Create Date: 2026-04-19 04:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "e10f12a3b4c5"
down_revision: Union[str, Sequence[str], None] = "d09e0f12a3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("data_lake_inventory_tables", sa.Column("data_owner_id", sa.Integer(), nullable=True))
    op.add_column("data_lake_inventory_tables", sa.Column("domain_name", sa.String(length=255), nullable=True))
    op.add_column("data_lake_inventory_tables", sa.Column("description", sa.Text(), nullable=True))
    op.add_column("data_lake_inventory_tables", sa.Column("classification", sa.String(length=120), nullable=True))
    op.add_column("data_lake_inventory_tables", sa.Column("criticality", sa.String(length=40), nullable=True))
    op.add_column(
        "data_lake_inventory_tables",
        sa.Column("is_monitored", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "data_lake_inventory_tables",
        sa.Column("governance_last_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_data_lake_inventory_tables_domain_name", "data_lake_inventory_tables", ["domain_name"], unique=False)
    op.create_index("ix_data_lake_inventory_tables_classification", "data_lake_inventory_tables", ["classification"], unique=False)
    op.create_index("ix_data_lake_inventory_tables_criticality", "data_lake_inventory_tables", ["criticality"], unique=False)
    op.create_index("ix_data_lake_inventory_tables_data_owner_id", "data_lake_inventory_tables", ["data_owner_id"], unique=False)

    op.add_column("data_lake_inventory_scan_runs", sa.Column("trigger_mode", sa.String(length=20), nullable=False, server_default="manual"))
    op.add_column("data_lake_inventory_scan_runs", sa.Column("schedule_id", sa.Integer(), nullable=True))
    op.create_index("ix_data_lake_inventory_scan_runs_schedule_id", "data_lake_inventory_scan_runs", ["schedule_id"], unique=False)

    op.create_table(
        "data_lake_scan_scheduler_status",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("scheduler_name", sa.String(length=80), nullable=False, server_default="data_lake_scan"),
        sa.Column("mode", sa.String(length=20), nullable=False, server_default="embedded"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("last_started_at", sa.String(length=64), nullable=True),
        sa.Column("last_heartbeat_at", sa.String(length=64), nullable=True),
        sa.Column("last_success_at", sa.String(length=64), nullable=True),
        sa.Column("last_failure_at", sa.String(length=64), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_run_summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "data_lake_scan_schedules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("connection_id", sa.Integer(), nullable=False),
        sa.Column("schedule_mode", sa.String(length=20), nullable=False, server_default="manual"),
        sa.Column("schedule_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("schedule_every_minutes", sa.Integer(), nullable=True),
        sa.Column("schedule_time", sa.String(length=5), nullable=True),
        sa.Column("schedule_day_of_week", sa.Integer(), nullable=True),
        sa.Column("schedule_day_of_month", sa.Integer(), nullable=True),
        sa.Column("schedule_anchor_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("schedule_last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("schedule_last_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("schedule_last_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("schedule_last_status", sa.String(length=20), nullable=True),
        sa.Column("schedule_last_error", sa.Text(), nullable=True),
        sa.Column("schedule_next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("schedule_summary", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["connection_id"], ["data_lake_connections.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("connection_id", name="uq_data_lake_scan_schedules_connection"),
    )
    op.create_index("ix_data_lake_scan_schedules_connection_id", "data_lake_scan_schedules", ["connection_id"], unique=False)
    op.create_index("ix_data_lake_scan_schedules_schedule_mode", "data_lake_scan_schedules", ["schedule_mode"], unique=False)
    op.create_index("ix_data_lake_scan_schedules_schedule_enabled", "data_lake_scan_schedules", ["schedule_enabled"], unique=False)
    op.create_index("ix_data_lake_scan_schedules_schedule_every_minutes", "data_lake_scan_schedules", ["schedule_every_minutes"], unique=False)
    op.create_index("ix_data_lake_scan_schedules_schedule_next_run_at", "data_lake_scan_schedules", ["schedule_next_run_at"], unique=False)
    op.create_index("ix_data_lake_scan_schedules_schedule_last_status", "data_lake_scan_schedules", ["schedule_last_status"], unique=False)
    op.create_index("ix_data_lake_scan_schedules_created_by_user_id", "data_lake_scan_schedules", ["created_by_user_id"], unique=False)

    op.create_foreign_key(
        "fk_data_lake_inventory_tables_data_owner_id",
        "data_lake_inventory_tables",
        "data_owners",
        ["data_owner_id"],
        ["id"],
        source_schema=None,
        referent_schema=None,
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_data_lake_inventory_scan_runs_schedule_id",
        "data_lake_inventory_scan_runs",
        "data_lake_scan_schedules",
        ["schedule_id"],
        ["id"],
        source_schema=None,
        referent_schema=None,
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_data_lake_inventory_scan_runs_schedule_id", "data_lake_inventory_scan_runs", type_="foreignkey")
    op.drop_constraint("fk_data_lake_inventory_tables_data_owner_id", "data_lake_inventory_tables", type_="foreignkey")

    op.drop_index("ix_data_lake_scan_schedules_created_by_user_id", table_name="data_lake_scan_schedules")
    op.drop_index("ix_data_lake_scan_schedules_schedule_last_status", table_name="data_lake_scan_schedules")
    op.drop_index("ix_data_lake_scan_schedules_schedule_next_run_at", table_name="data_lake_scan_schedules")
    op.drop_index("ix_data_lake_scan_schedules_schedule_every_minutes", table_name="data_lake_scan_schedules")
    op.drop_index("ix_data_lake_scan_schedules_schedule_enabled", table_name="data_lake_scan_schedules")
    op.drop_index("ix_data_lake_scan_schedules_schedule_mode", table_name="data_lake_scan_schedules")
    op.drop_index("ix_data_lake_scan_schedules_connection_id", table_name="data_lake_scan_schedules")
    op.drop_table("data_lake_scan_schedules")

    op.drop_table("data_lake_scan_scheduler_status")

    op.drop_index("ix_data_lake_inventory_scan_runs_schedule_id", table_name="data_lake_inventory_scan_runs")
    op.drop_column("data_lake_inventory_scan_runs", "schedule_id")
    op.drop_column("data_lake_inventory_scan_runs", "trigger_mode")

    op.drop_index("ix_data_lake_inventory_tables_data_owner_id", table_name="data_lake_inventory_tables")
    op.drop_index("ix_data_lake_inventory_tables_criticality", table_name="data_lake_inventory_tables")
    op.drop_index("ix_data_lake_inventory_tables_classification", table_name="data_lake_inventory_tables")
    op.drop_index("ix_data_lake_inventory_tables_domain_name", table_name="data_lake_inventory_tables")
    op.drop_column("data_lake_inventory_tables", "governance_last_updated_at")
    op.drop_column("data_lake_inventory_tables", "is_monitored")
    op.drop_column("data_lake_inventory_tables", "criticality")
    op.drop_column("data_lake_inventory_tables", "classification")
    op.drop_column("data_lake_inventory_tables", "description")
    op.drop_column("data_lake_inventory_tables", "domain_name")
    op.drop_column("data_lake_inventory_tables", "data_owner_id")
