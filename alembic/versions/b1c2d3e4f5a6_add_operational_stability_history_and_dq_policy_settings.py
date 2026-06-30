"""add operational stability history and dq policy settings

Revision ID: b1c2d3e4f5a6
Revises: a9b8c7d6e5f4
Create Date: 2026-04-01 13:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b1c2d3e4f5a6"
down_revision = "a9b8c7d6e5f4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("dq_operational_failure_penalty_points", sa.Integer(), nullable=False, server_default="15"),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("dq_operational_stale_penalty_points", sa.Integer(), nullable=False, server_default="8"),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("dq_operational_recurrent_penalty_points", sa.Integer(), nullable=False, server_default="5"),
        schema="t2c_data",
    )

    op.create_table(
        "operational_stability_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("datasource_id", sa.Integer(), sa.ForeignKey("t2c_data.data_sources.id", ondelete="SET NULL"), nullable=True),
        sa.Column("schema_name", sa.String(length=100), nullable=False),
        sa.Column("table_name", sa.String(length=200), nullable=False),
        sa.Column("pipeline_name", sa.String(length=255), nullable=True),
        sa.Column("dag_id", sa.String(length=255), nullable=True),
        sa.Column("task_name", sa.String(length=255), nullable=True),
        sa.Column("latest_status_label", sa.String(length=60), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_execution_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rows_processed", sa.Integer(), nullable=True),
        sa.Column("window_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("success_rate_pct", sa.Float(), nullable=False, server_default="0"),
        sa.Column("failed_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("recurrent_degradation", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("currently_stale", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("bucket_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("table_id", "bucket_start_at", name="uq_operational_stability_table_bucket"),
        schema="t2c_data",
    )
    op.create_index(
        "ix_operational_stability_bucket",
        "operational_stability_snapshots",
        ["bucket_start_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_operational_stability_table_bucket",
        "operational_stability_snapshots",
        ["table_id", "bucket_start_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_operational_stability_dag",
        "operational_stability_snapshots",
        ["dag_id"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_operational_stability_dag", table_name="operational_stability_snapshots", schema="t2c_data")
    op.drop_index("ix_operational_stability_table_bucket", table_name="operational_stability_snapshots", schema="t2c_data")
    op.drop_index("ix_operational_stability_bucket", table_name="operational_stability_snapshots", schema="t2c_data")
    op.drop_table("operational_stability_snapshots", schema="t2c_data")
    op.drop_column("governance_settings", "dq_operational_recurrent_penalty_points", schema="t2c_data")
    op.drop_column("governance_settings", "dq_operational_stale_penalty_points", schema="t2c_data")
    op.drop_column("governance_settings", "dq_operational_failure_penalty_points", schema="t2c_data")
