"""remove webhooks and notification channels

Revision ID: ab1b2c3d4e5f
Revises: c8e9f0a1b2c4
Create Date: 2026-05-14 00:00:00.000000
"""

from alembic import op


revision = "ab1b2c3d4e5f"
down_revision = "c8e9f0a1b2c4"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.drop_table("platform_webhook_delivery_attempts", schema=SCHEMA)
    op.drop_table("platform_webhook_deliveries", schema=SCHEMA)
    op.drop_table("platform_webhook_subscriptions", schema=SCHEMA)
    op.drop_table("notification_delivery_attempts", schema=SCHEMA)
    op.drop_table("notification_deliveries", schema=SCHEMA)
    op.drop_table("notification_channels", schema=SCHEMA)
    op.drop_column("user_notification_preferences", "slack_enabled", schema=SCHEMA)
    op.drop_column("user_notification_preferences", "teams_enabled", schema=SCHEMA)
    op.drop_column("user_notification_preferences", "slack_webhook_url", schema=SCHEMA)
    op.drop_column("user_notification_preferences", "teams_webhook_url", schema=SCHEMA)


def downgrade() -> None:
    pass

