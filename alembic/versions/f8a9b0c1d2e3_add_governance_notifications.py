"""add governance notifications

Revision ID: f8a9b0c1d2e3
Revises: d5e6f7a8b9c0, f3a4b5c6d7e8
Create Date: 2026-03-29 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f8a9b0c1d2e3"
down_revision: Union[str, Sequence[str], None] = ("d5e6f7a8b9c0", "f3a4b5c6d7e8")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("governance_notifications_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("governance_notification_repeat_days", sa.Integer(), nullable=False, server_default="7"),
        schema="t2c_data",
    )
    op.add_column(
        "governance_settings",
        sa.Column("governance_notification_critical_repeat_hours", sa.Integer(), nullable=False, server_default="24"),
        schema="t2c_data",
    )

    op.create_table(
        "governance_notifications",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dedupe_key", sa.String(length=255), nullable=False),
        sa.Column("rule_key", sa.String(length=80), nullable=False),
        sa.Column("channel", sa.String(length=20), nullable=False, server_default="in_app"),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        sa.Column("severity", sa.String(length=20), nullable=False, server_default="medium"),
        sa.Column("origin", sa.String(length=40), nullable=False, server_default="governance"),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False, server_default="table"),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("data_owner_id", sa.Integer(), nullable=True),
        sa.Column("target_href", sa.Text(), nullable=True),
        sa.Column("context_json", sa.JSON(), nullable=True),
        sa.Column("first_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_send_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_reason", sa.Text(), nullable=True),
        sa.Column("send_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_delivery_status", sa.String(length=20), nullable=True),
        sa.Column("last_delivery_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["data_owner_id"], ["t2c_data.data_owners.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_notifications_status",
        "governance_notifications",
        ["status"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_notifications_severity",
        "governance_notifications",
        ["severity"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_notifications_rule_key",
        "governance_notifications",
        ["rule_key"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_notifications_table_id",
        "governance_notifications",
        ["table_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_notifications_next_send_at",
        "governance_notifications",
        ["next_send_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_notifications_active_status",
        "governance_notifications",
        ["status", "next_send_at"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_notifications_dedupe_key",
        "governance_notifications",
        ["dedupe_key"],
        unique=True,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_governance_notifications_dedupe_key", table_name="governance_notifications", schema="t2c_data")
    op.drop_index("ix_governance_notifications_active_status", table_name="governance_notifications", schema="t2c_data")
    op.drop_index("ix_governance_notifications_next_send_at", table_name="governance_notifications", schema="t2c_data")
    op.drop_index("ix_governance_notifications_table_id", table_name="governance_notifications", schema="t2c_data")
    op.drop_index("ix_governance_notifications_rule_key", table_name="governance_notifications", schema="t2c_data")
    op.drop_index("ix_governance_notifications_severity", table_name="governance_notifications", schema="t2c_data")
    op.drop_index("ix_governance_notifications_status", table_name="governance_notifications", schema="t2c_data")
    op.drop_table("governance_notifications", schema="t2c_data")

    op.drop_column("governance_settings", "governance_notification_critical_repeat_hours", schema="t2c_data")
    op.drop_column("governance_settings", "governance_notification_repeat_days", schema="t2c_data")
    op.drop_column("governance_settings", "governance_notifications_enabled", schema="t2c_data")
