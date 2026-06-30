"""add notification channels and deliveries

Revision ID: e4f5a6b7c8d9
Revises: c9d8e7f6a5b4
Create Date: 2026-04-30 13:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "e4f5a6b7c8d9"
down_revision = "c9d8e7f6a5b4"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "notification_channels",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("secret_payload", sa.Text(), nullable=False, server_default=""),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("severity_filter_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default="[]"),
        sa.Column("category_filter_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default="[]"),
        sa.Column("only_critical_notifications", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("notify_owner_missing", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("notify_recurrent_failures", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("notify_freshness_delayed", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("notify_open_incidents", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_notification_channels_name"),
        schema=SCHEMA,
    )
    op.create_index("ix_notification_channels_provider", "notification_channels", ["provider"], unique=False, schema=SCHEMA)
    op.create_index("ix_notification_channels_enabled", "notification_channels", ["enabled"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_notification_channels_last_sent_at",
        "notification_channels",
        ["last_sent_at"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "notification_deliveries",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("channel_id", sa.Integer(), nullable=True),
        sa.Column("notification_id", sa.Integer(), nullable=True),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("channel_name", sa.String(length=160), nullable=True),
        sa.Column("notification_title", sa.String(length=255), nullable=True),
        sa.Column("notification_category", sa.String(length=40), nullable=True),
        sa.Column("notification_severity", sa.String(length=20), nullable=True),
        sa.Column("notification_href", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("payload_preview_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["channel_id"], [f"{SCHEMA}.notification_channels.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["notification_id"], [f"{SCHEMA}.user_inbox_notifications.id"], ondelete="SET NULL"),
        schema=SCHEMA,
    )
    op.create_index("ix_notification_deliveries_channel_id", "notification_deliveries", ["channel_id"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_notification_deliveries_notification_id",
        "notification_deliveries",
        ["notification_id"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index("ix_notification_deliveries_provider", "notification_deliveries", ["provider"], unique=False, schema=SCHEMA)
    op.create_index("ix_notification_deliveries_status", "notification_deliveries", ["status"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_notification_deliveries_status_sent_at",
        "notification_deliveries",
        ["status", "sent_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_deliveries_channel_sent_at",
        "notification_deliveries",
        ["channel_id", "sent_at"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_notification_deliveries_channel_sent_at", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_index("ix_notification_deliveries_status_sent_at", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_index("ix_notification_deliveries_status", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_index("ix_notification_deliveries_provider", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_index("ix_notification_deliveries_notification_id", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_index("ix_notification_deliveries_channel_id", table_name="notification_deliveries", schema=SCHEMA)
    op.drop_table("notification_deliveries", schema=SCHEMA)

    op.drop_index("ix_notification_channels_last_sent_at", table_name="notification_channels", schema=SCHEMA)
    op.drop_index("ix_notification_channels_enabled", table_name="notification_channels", schema=SCHEMA)
    op.drop_index("ix_notification_channels_provider", table_name="notification_channels", schema=SCHEMA)
    op.drop_table("notification_channels", schema=SCHEMA)
