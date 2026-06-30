"""add operational governance policy settings

Revision ID: a9b8c7d6e5f4
Revises: f8a9b0c1d2e3
Create Date: 2026-04-01 10:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a9b8c7d6e5f4"
down_revision = "f8a9b0c1d2e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column(
            "pipeline_failure_owner_sla_hours",
            sa.Integer(),
            nullable=False,
            server_default="24",
        ),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column(
            "operational_high_volume_threshold_rows",
            sa.Integer(),
            nullable=False,
            server_default="100000",
        ),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("airflow_ui_base_url", sa.Text(), nullable=True),
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_column("governance_settings", "airflow_ui_base_url", schema="t2c_data")
    op.drop_column("governance_settings", "operational_high_volume_threshold_rows", schema="t2c_data")
    op.drop_column("governance_settings", "pipeline_failure_owner_sla_hours", schema="t2c_data")
