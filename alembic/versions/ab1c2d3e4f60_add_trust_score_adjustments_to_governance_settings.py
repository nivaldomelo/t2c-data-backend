"""add trust score adjustments to governance settings

Revision ID: ab1c2d3e4f60
Revises: ff1a2b3c4d5f
Create Date: 2026-04-10 12:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ab1c2d3e4f60"
down_revision = "ff1a2b3c4d5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("trust_score_domain_adjustments", sa.Text(), nullable=True),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("trust_score_criticality_adjustments", sa.Text(), nullable=True),
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_column("governance_settings", "trust_score_criticality_adjustments", schema="t2c_data")
    op.drop_column("governance_settings", "trust_score_domain_adjustments", schema="t2c_data")
