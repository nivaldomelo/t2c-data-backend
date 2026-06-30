"""add platform webhook delivery attempts

Revision ID: e2f3a4b5c6d7
Revises: d6e7f8a9b0c2
Create Date: 2026-04-10 23:55:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "e2f3a4b5c6d7"
down_revision = "d6e7f8a9b0c2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "platform_webhook_delivery_attempts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("delivery_id", sa.Integer(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("http_status_code", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("request_headers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("request_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("response_excerpt", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("correlation_key", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["delivery_id"], ["t2c_data.platform_webhook_deliveries.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("delivery_id", "attempt_number", name="uq_platform_webhook_delivery_attempt_number"),
        schema="t2c_data",
    )
    op.create_index(
        "ix_platform_webhook_delivery_attempts_delivery",
        "platform_webhook_delivery_attempts",
        ["delivery_id", "attempt_number"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_platform_webhook_delivery_attempts_status",
        "platform_webhook_delivery_attempts",
        ["status"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_platform_webhook_delivery_attempts_started",
        "platform_webhook_delivery_attempts",
        ["started_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_platform_webhook_delivery_attempts_correlation_key",
        "platform_webhook_delivery_attempts",
        ["correlation_key"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_platform_webhook_delivery_attempts_correlation_key",
        table_name="platform_webhook_delivery_attempts",
        schema="t2c_data",
    )
    op.drop_index(
        "ix_platform_webhook_delivery_attempts_started",
        table_name="platform_webhook_delivery_attempts",
        schema="t2c_data",
    )
    op.drop_index(
        "ix_platform_webhook_delivery_attempts_status",
        table_name="platform_webhook_delivery_attempts",
        schema="t2c_data",
    )
    op.drop_index(
        "ix_platform_webhook_delivery_attempts_delivery",
        table_name="platform_webhook_delivery_attempts",
        schema="t2c_data",
    )
    op.drop_table("platform_webhook_delivery_attempts", schema="t2c_data")
