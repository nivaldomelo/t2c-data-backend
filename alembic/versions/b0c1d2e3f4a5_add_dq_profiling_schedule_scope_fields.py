"""add dq profiling schedule scope fields

Revision ID: b0c1d2e3f4a5
Revises: 8a9b0c1d2e3f
Create Date: 2026-05-27 17:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b0c1d2e3f4a5"
down_revision = "8a9b0c1d2e3f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dq_profiling_schedules", sa.Column("name", sa.String(length=255), nullable=True), schema="t2c_data")
    op.add_column("dq_profiling_schedules", sa.Column("table_ids_json", sa.JSON(), nullable=True), schema="t2c_data")
    op.add_column("dq_profiling_schedules", sa.Column("schedule_timezone", sa.String(length=64), nullable=True), schema="t2c_data")


def downgrade() -> None:
    op.drop_column("dq_profiling_schedules", "schedule_timezone", schema="t2c_data")
    op.drop_column("dq_profiling_schedules", "table_ids_json", schema="t2c_data")
    op.drop_column("dq_profiling_schedules", "name", schema="t2c_data")
