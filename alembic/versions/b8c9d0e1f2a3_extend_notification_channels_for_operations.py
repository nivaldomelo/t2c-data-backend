"""extend notification channels for operational notifications

Revision ID: b8c9d0e1f2a3
Revises: e4f5a6b7c8d9
Create Date: 2026-05-01 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b8c9d0e1f2a3"
down_revision = "e4f5a6b7c8d9"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "notification_channels",
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey(f"{SCHEMA}.users.id", ondelete="SET NULL"), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "notification_channels",
        sa.Column("updated_by_user_id", sa.Integer(), sa.ForeignKey(f"{SCHEMA}.users.id", ondelete="SET NULL"), nullable=True),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_channels_created_by_user_id",
        "notification_channels",
        ["created_by_user_id"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_channels_updated_by_user_id",
        "notification_channels",
        ["updated_by_user_id"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_deliveries_notification_category",
        "notification_deliveries",
        ["notification_category"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_deliveries_notification_severity",
        "notification_deliveries",
        ["notification_severity"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_notification_deliveries_notification_severity", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_index("ix_notification_deliveries_notification_category", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_index("ix_notification_channels_updated_by_user_id", table_name="notification_channels", schema=SCHEMA)
    op.drop_index("ix_notification_channels_created_by_user_id", table_name="notification_channels", schema=SCHEMA)
    op.drop_column("notification_channels", "updated_by_user_id", schema=SCHEMA)
    op.drop_column("notification_channels", "created_by_user_id", schema=SCHEMA)
