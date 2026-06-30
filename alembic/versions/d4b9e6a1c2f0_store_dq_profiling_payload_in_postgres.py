"""store dq profiling payload in postgres

Revision ID: d4b9e6a1c2f0
Revises: c1d2e3f4a5b6
Create Date: 2026-03-25 18:40:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "d4b9e6a1c2f0"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dq_runs", sa.Column("profile_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema="t2c_data")
    op.add_column("dq_table_metrics", sa.Column("column_count", sa.Integer(), nullable=True), schema="t2c_data")
    op.add_column("dq_table_metrics", sa.Column("metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema="t2c_data")

    op.execute(
        """
        UPDATE t2c_data.dq_table_metrics tm
        SET column_count = sub.cnt
        FROM (
            SELECT table_metric_id, COUNT(*)::integer AS cnt
            FROM t2c_data.dq_column_metrics
            GROUP BY table_metric_id
        ) sub
        WHERE sub.table_metric_id = tm.id
        """
    )
    op.execute("UPDATE t2c_data.dq_table_metrics SET column_count = 0 WHERE column_count IS NULL")
    op.alter_column("dq_table_metrics", "column_count", nullable=False, schema="t2c_data")


def downgrade() -> None:
    op.drop_column("dq_table_metrics", "metrics_json", schema="t2c_data")
    op.drop_column("dq_table_metrics", "column_count", schema="t2c_data")
    op.drop_column("dq_runs", "profile_payload_json", schema="t2c_data")
