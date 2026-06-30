"""add governance high usage click threshold

Revision ID: fb1c2d3e4f50
Revises: fa0b1c2d3e4f
Create Date: 2026-04-10 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "fb1c2d3e4f50"
down_revision: Union[str, Sequence[str], None] = "fa0b1c2d3e4f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("governance_high_usage_click_threshold", sa.Integer(), nullable=False, server_default="20"),
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_column("governance_settings", "governance_high_usage_click_threshold", schema="t2c_data")
