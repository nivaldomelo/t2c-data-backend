"""add dq observability tables

Revision ID: e0a4d2c3b901
Revises: c9f8b65af01d
Create Date: 2026-02-22 00:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "e0a4d2c3b901"
down_revision = "c9f8b65af01d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dq_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("datasource_id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["datasource_id"], ["t2c_data.data_sources.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index(op.f("ix_t2c_data_dq_runs_datasource_id"), "dq_runs", ["datasource_id"], unique=False, schema="t2c_data")
    op.create_index(op.f("ix_t2c_data_dq_runs_table_id"), "dq_runs", ["table_id"], unique=False, schema="t2c_data")

    op.create_table(
        "dq_table_metrics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("completeness_pct_avg", sa.Float(), nullable=False),
        sa.Column("dq_score", sa.Float(), nullable=False),
        sa.Column("duplicates_count", sa.BigInteger(), nullable=False),
        sa.Column("failed_rules", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["t2c_data.dq_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "table_id", name="uq_dq_table_metrics_run_table"),
        schema="t2c_data",
    )
    op.create_index(
        op.f("ix_t2c_data_dq_table_metrics_run_id"),
        "dq_table_metrics",
        ["run_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        op.f("ix_t2c_data_dq_table_metrics_table_id"),
        "dq_table_metrics",
        ["table_id"],
        unique=False,
        schema="t2c_data",
    )

    op.create_table(
        "dq_column_metrics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("table_metric_id", sa.Integer(), nullable=False),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=False),
        sa.Column("data_type", sa.String(length=255), nullable=False),
        sa.Column("null_count", sa.BigInteger(), nullable=False),
        sa.Column("distinct_count", sa.BigInteger(), nullable=False),
        sa.Column("null_pct", sa.Float(), nullable=False),
        sa.Column("min_value", sa.Text(), nullable=True),
        sa.Column("max_value", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["column_id"], ["t2c_data.columns.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["run_id"], ["t2c_data.dq_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_metric_id"], ["t2c_data.dq_table_metrics.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "table_metric_id", "column_name", name="uq_dq_column_metrics_unique"),
        schema="t2c_data",
    )
    op.create_index(
        op.f("ix_t2c_data_dq_column_metrics_column_id"),
        "dq_column_metrics",
        ["column_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        op.f("ix_t2c_data_dq_column_metrics_run_id"),
        "dq_column_metrics",
        ["run_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        op.f("ix_t2c_data_dq_column_metrics_table_metric_id"),
        "dq_column_metrics",
        ["table_metric_id"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_t2c_data_dq_column_metrics_table_metric_id"), table_name="dq_column_metrics", schema="t2c_data")
    op.drop_index(op.f("ix_t2c_data_dq_column_metrics_run_id"), table_name="dq_column_metrics", schema="t2c_data")
    op.drop_index(op.f("ix_t2c_data_dq_column_metrics_column_id"), table_name="dq_column_metrics", schema="t2c_data")
    op.drop_table("dq_column_metrics", schema="t2c_data")

    op.drop_index(op.f("ix_t2c_data_dq_table_metrics_table_id"), table_name="dq_table_metrics", schema="t2c_data")
    op.drop_index(op.f("ix_t2c_data_dq_table_metrics_run_id"), table_name="dq_table_metrics", schema="t2c_data")
    op.drop_table("dq_table_metrics", schema="t2c_data")

    op.drop_index(op.f("ix_t2c_data_dq_runs_table_id"), table_name="dq_runs", schema="t2c_data")
    op.drop_index(op.f("ix_t2c_data_dq_runs_datasource_id"), table_name="dq_runs", schema="t2c_data")
    op.drop_table("dq_runs", schema="t2c_data")
