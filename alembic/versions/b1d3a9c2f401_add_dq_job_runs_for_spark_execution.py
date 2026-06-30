"""add dq job runs for spark execution

Revision ID: b1d3a9c2f401
Revises: a7b5fe120b3f
Create Date: 2026-02-24 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "b1d3a9c2f401"
down_revision = "a7b5fe120b3f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "t2c_data"')
    op.create_table(
        "dq_job_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_type", sa.String(length=30), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="queued"),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("table_fqn", sa.String(length=500), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("requested_by_user_id", sa.Integer(), nullable=True),
        sa.Column("spark_app_id", sa.String(length=120), nullable=True),
        sa.Column("command", sa.Text(), nullable=True),
        sa.Column("stdout_log", sa.Text(), nullable=True),
        sa.Column("stderr_log", sa.Text(), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["datasource_id"], ["t2c_data.data_sources.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["requested_by_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index("ix_dq_job_runs_job_type", "dq_job_runs", ["job_type"], unique=False, schema="t2c_data")
    op.create_index("ix_dq_job_runs_status", "dq_job_runs", ["status"], unique=False, schema="t2c_data")
    op.create_index("ix_dq_job_runs_table_id", "dq_job_runs", ["table_id"], unique=False, schema="t2c_data")
    op.create_index("ix_dq_job_runs_table_fqn", "dq_job_runs", ["table_fqn"], unique=False, schema="t2c_data")
    op.create_index("ix_dq_job_runs_datasource_id", "dq_job_runs", ["datasource_id"], unique=False, schema="t2c_data")
    op.create_index(
        "ix_dq_job_runs_requested_by_user_id",
        "dq_job_runs",
        ["requested_by_user_id"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_dq_job_runs_requested_by_user_id", table_name="dq_job_runs", schema="t2c_data")
    op.drop_index("ix_dq_job_runs_datasource_id", table_name="dq_job_runs", schema="t2c_data")
    op.drop_index("ix_dq_job_runs_table_fqn", table_name="dq_job_runs", schema="t2c_data")
    op.drop_index("ix_dq_job_runs_table_id", table_name="dq_job_runs", schema="t2c_data")
    op.drop_index("ix_dq_job_runs_status", table_name="dq_job_runs", schema="t2c_data")
    op.drop_index("ix_dq_job_runs_job_type", table_name="dq_job_runs", schema="t2c_data")
    op.drop_table("dq_job_runs", schema="t2c_data")

