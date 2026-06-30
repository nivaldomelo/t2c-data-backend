"""add digest state and stewardship assignment rules

Revision ID: e7f8a9b0c1d2
Revises: c2d3e4f5a6b7
Create Date: 2026-04-01 18:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "e7f8a9b0c1d2"
down_revision = "c2d3e4f5a6b7"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("stewardship_assignment_rules", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "user_notification_preferences",
        sa.Column("last_daily_digest_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "user_notification_preferences",
        sa.Column("next_daily_digest_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "user_notification_preferences",
        sa.Column("last_daily_digest_status", sa.String(length=20), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("user_notification_preferences", "last_daily_digest_status", schema=SCHEMA)
    op.drop_column("user_notification_preferences", "next_daily_digest_at", schema=SCHEMA)
    op.drop_column("user_notification_preferences", "last_daily_digest_at", schema=SCHEMA)
    op.drop_column("governance_settings", "stewardship_assignment_rules", schema=SCHEMA)
