"""add platform domain events and webhook foundations

Revision ID: c2a3b4d5e6f7
Revises: ab1c2d3e4f60
Create Date: 2026-04-10 18:15:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "c2a3b4d5e6f7"
down_revision = "ab1c2d3e4f60"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "platform_domain_events",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("event_key", sa.String(length=160), nullable=False),
        sa.Column("category", sa.String(length=40), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False, server_default="medium"),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("source_module", sa.String(length=80), nullable=True),
        sa.Column("source_action", sa.String(length=160), nullable=True),
        sa.Column("entity_type", sa.String(length=40), nullable=True),
        sa.Column("entity_id", sa.Integer(), nullable=True),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("actor_name", sa.String(length=255), nullable=True),
        sa.Column("actor_email", sa.String(length=255), nullable=True),
        sa.Column("manual_mode", sa.String(length=20), nullable=False, server_default="unknown"),
        sa.Column("correlation_key", sa.String(length=255), nullable=True),
        sa.Column("payload_json", JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["column_id"], ["t2c_data.columns.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["actor_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        schema="t2c_data",
    )
    op.create_index("ix_platform_domain_events_event_key", "platform_domain_events", ["event_key"], schema="t2c_data")
    op.create_index(
        "ix_platform_domain_events_category_created_at",
        "platform_domain_events",
        ["category", "created_at"],
        schema="t2c_data",
    )
    op.create_index("ix_platform_domain_events_entity", "platform_domain_events", ["entity_type", "entity_id"], schema="t2c_data")
    op.create_index("ix_platform_domain_events_table_created_at", "platform_domain_events", ["table_id", "created_at"], schema="t2c_data")
    op.create_index("ix_platform_domain_events_column_created_at", "platform_domain_events", ["column_id", "created_at"], schema="t2c_data")
    op.create_index("ix_platform_domain_events_severity", "platform_domain_events", ["severity"], schema="t2c_data")

    op.create_table(
        "platform_webhook_subscriptions",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("target_url", sa.Text(), nullable=False),
        sa.Column("secret_token", sa.Text(), nullable=True),
        sa.Column("event_keys_json", JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("event_categories_json", JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("entity_types_json", JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("headers_json", JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("last_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_delivery_status", sa.String(length=20), nullable=True),
        sa.Column("last_delivery_error", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["column_id"], ["t2c_data.columns.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        schema="t2c_data",
    )
    op.create_index("ix_platform_webhook_subscriptions_active", "platform_webhook_subscriptions", ["is_active"], schema="t2c_data")
    op.create_index(
        "ix_platform_webhook_subscriptions_category",
        "platform_webhook_subscriptions",
        ["event_categories_json"],
        schema="t2c_data",
        postgresql_using="gin",
    )

    op.create_table(
        "platform_webhook_deliveries",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("subscription_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_status_code", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("request_headers_json", JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("request_payload_json", JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("response_body", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("subscription_id", "event_id", name="uq_platform_webhook_delivery_subscription_event"),
        schema="t2c_data",
    )
    op.create_index("ix_platform_webhook_deliveries_status_next", "platform_webhook_deliveries", ["status", "next_attempt_at"], schema="t2c_data")
    op.create_index("ix_platform_webhook_deliveries_event_id", "platform_webhook_deliveries", ["event_id"], schema="t2c_data")
    op.create_index("ix_platform_webhook_deliveries_subscription_id", "platform_webhook_deliveries", ["subscription_id"], schema="t2c_data")


def downgrade() -> None:
    op.drop_index("ix_platform_webhook_deliveries_subscription_id", table_name="platform_webhook_deliveries", schema="t2c_data")
    op.drop_index("ix_platform_webhook_deliveries_event_id", table_name="platform_webhook_deliveries", schema="t2c_data")
    op.drop_index("ix_platform_webhook_deliveries_status_next", table_name="platform_webhook_deliveries", schema="t2c_data")
    op.drop_table("platform_webhook_deliveries", schema="t2c_data")

    op.drop_index("ix_platform_webhook_subscriptions_category", table_name="platform_webhook_subscriptions", schema="t2c_data")
    op.drop_index("ix_platform_webhook_subscriptions_active", table_name="platform_webhook_subscriptions", schema="t2c_data")
    op.drop_table("platform_webhook_subscriptions", schema="t2c_data")

    op.drop_index("ix_platform_domain_events_severity", table_name="platform_domain_events", schema="t2c_data")
    op.drop_index("ix_platform_domain_events_column_created_at", table_name="platform_domain_events", schema="t2c_data")
    op.drop_index("ix_platform_domain_events_table_created_at", table_name="platform_domain_events", schema="t2c_data")
    op.drop_index("ix_platform_domain_events_entity", table_name="platform_domain_events", schema="t2c_data")
    op.drop_index("ix_platform_domain_events_category_created_at", table_name="platform_domain_events", schema="t2c_data")
    op.drop_index("ix_platform_domain_events_event_key", table_name="platform_domain_events", schema="t2c_data")
    op.drop_table("platform_domain_events", schema="t2c_data")
