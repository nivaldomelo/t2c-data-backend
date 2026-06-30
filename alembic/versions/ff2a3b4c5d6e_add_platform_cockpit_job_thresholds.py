"""add platform cockpit job thresholds

Revision ID: ff2a3b4c5d6e
Revises: ff1a2b3c4d5f
Create Date: 2026-05-13 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "ff2a3b4c5d6e"
down_revision: Union[str, Sequence[str], None] = "ff1a2b3c4d5f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("platform_job_running_attention_minutes", sa.Integer(), nullable=False, server_default="120"),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("platform_job_running_critical_hours", sa.Integer(), nullable=False, server_default="24"),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("platform_job_next_expected_delay_minutes", sa.Integer(), nullable=False, server_default="60"),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("platform_recent_success_window_hours", sa.Integer(), nullable=False, server_default="72"),
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_column("governance_settings", "platform_recent_success_window_hours", schema="t2c_data")
    op.drop_column("governance_settings", "platform_job_next_expected_delay_minutes", schema="t2c_data")
    op.drop_column("governance_settings", "platform_job_running_critical_hours", schema="t2c_data")
    op.drop_column("governance_settings", "platform_job_running_attention_minutes", schema="t2c_data")
