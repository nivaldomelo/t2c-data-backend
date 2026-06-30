"""add_spark_fields_to_dq_runs

Revision ID: 1d6c80ac7e58
Revises: 85e30b8d49cd
Create Date: 2026-02-24 00:40:40.968264

"""
from alembic import op
import sqlalchemy as sa



# revision identifiers, used by Alembic.
revision = '1d6c80ac7e58'
down_revision = '85e30b8d49cd'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dq_runs", sa.Column("spark_app_id", sa.Text(), nullable=True), schema="t2c_data")
    op.add_column(
        "dq_runs",
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.add_column("dq_runs", sa.Column("started_at", sa.DateTime(timezone=True), nullable=True), schema="t2c_data")
    op.add_column("dq_runs", sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True), schema="t2c_data")
    op.add_column("dq_runs", sa.Column("duration_ms", sa.BigInteger(), nullable=True), schema="t2c_data")
    op.add_column("dq_runs", sa.Column("log_tail", sa.Text(), nullable=True), schema="t2c_data")
    op.alter_column("dq_runs", "queued_at", server_default=None, schema="t2c_data")


def downgrade() -> None:
    op.drop_column("dq_runs", "log_tail", schema="t2c_data")
    op.drop_column("dq_runs", "duration_ms", schema="t2c_data")
    op.drop_column("dq_runs", "finished_at", schema="t2c_data")
    op.drop_column("dq_runs", "started_at", schema="t2c_data")
    op.drop_column("dq_runs", "queued_at", schema="t2c_data")
    op.drop_column("dq_runs", "spark_app_id", schema="t2c_data")
