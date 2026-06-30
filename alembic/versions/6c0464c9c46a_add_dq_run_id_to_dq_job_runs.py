"""add_dq_run_id_to_dq_job_runs

Revision ID: 6c0464c9c46a
Revises: 1d6c80ac7e58
Create Date: 2026-02-24 00:44:49.615906

"""
from alembic import op
import sqlalchemy as sa



# revision identifiers, used by Alembic.
revision = '6c0464c9c46a'
down_revision = '1d6c80ac7e58'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dq_job_runs", sa.Column("dq_run_id", sa.Integer(), nullable=True), schema="t2c_data")
    op.create_index("ix_t2c_data_dq_job_runs_dq_run_id", "dq_job_runs", ["dq_run_id"], unique=False, schema="t2c_data")
    op.create_foreign_key(
        "fk_t2c_data_dq_job_runs_dq_run_id_dq_runs",
        "dq_job_runs",
        "dq_runs",
        ["dq_run_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_t2c_data_dq_job_runs_dq_run_id_dq_runs", "dq_job_runs", schema="t2c_data", type_="foreignkey")
    op.drop_index("ix_t2c_data_dq_job_runs_dq_run_id", table_name="dq_job_runs", schema="t2c_data")
    op.drop_column("dq_job_runs", "dq_run_id", schema="t2c_data")
