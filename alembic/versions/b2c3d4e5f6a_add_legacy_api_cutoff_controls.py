"""add legacy api cutoff controls

Revision ID: b2c3d4e5f6a
Revises: a1b2c3d4e5f6
Create Date: 2026-03-28 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("legacy_api_cutoff_window_days", sa.Integer(), nullable=False, server_default="30"),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("legacy_api_force_enabled_modules", sa.Text(), nullable=True),
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_column("governance_settings", "legacy_api_force_enabled_modules", schema="t2c_data")
    op.drop_column("governance_settings", "legacy_api_cutoff_window_days", schema="t2c_data")
