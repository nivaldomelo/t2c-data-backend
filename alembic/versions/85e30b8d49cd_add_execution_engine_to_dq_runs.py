"""add_execution_engine_to_dq_runs

Revision ID: 85e30b8d49cd
Revises: b1d3a9c2f401
Create Date: 2026-02-23 23:53:37.754681

"""
from alembic import op
import sqlalchemy as sa



# revision identifiers, used by Alembic.
revision = '85e30b8d49cd'
down_revision = 'b1d3a9c2f401'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dq_runs",
        sa.Column("execution_engine", sa.String(length=20), nullable=False, server_default="python"),
        schema="t2c_data",
    )
    op.add_column(
        "dq_rule_runs",
        sa.Column("execution_engine", sa.String(length=20), nullable=False, server_default="python"),
        schema="t2c_data",
    )
    op.add_column(
        "dq_job_runs",
        sa.Column("execution_engine", sa.String(length=20), nullable=False, server_default="spark"),
        schema="t2c_data",
    )
    op.add_column(
        "dq_job_runs",
        sa.Column("spark_master_url", sa.String(length=255), nullable=True),
        schema="t2c_data",
    )
    op.add_column(
        "dq_job_runs",
        sa.Column("logs_path", sa.String(length=1000), nullable=True),
        schema="t2c_data",
    )
    op.create_index(
        "ix_t2c_data_dq_job_runs_execution_engine",
        "dq_job_runs",
        ["execution_engine"],
        unique=False,
        schema="t2c_data",
    )

    op.alter_column("dq_runs", "execution_engine", server_default=None, schema="t2c_data")
    op.alter_column("dq_rule_runs", "execution_engine", server_default=None, schema="t2c_data")
    op.alter_column("dq_job_runs", "execution_engine", server_default=None, schema="t2c_data")


def downgrade() -> None:
    op.drop_index("ix_t2c_data_dq_job_runs_execution_engine", table_name="dq_job_runs", schema="t2c_data")
    op.drop_column("dq_job_runs", "logs_path", schema="t2c_data")
    op.drop_column("dq_job_runs", "spark_master_url", schema="t2c_data")
    op.drop_column("dq_job_runs", "execution_engine", schema="t2c_data")
    op.drop_column("dq_rule_runs", "execution_engine", schema="t2c_data")
    op.drop_column("dq_runs", "execution_engine", schema="t2c_data")
