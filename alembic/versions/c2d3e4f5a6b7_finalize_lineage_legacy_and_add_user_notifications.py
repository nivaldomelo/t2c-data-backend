"""finalize lineage legacy and add user notifications

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
Create Date: 2026-04-01 16:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c2d3e4f5a6b7"
down_revision = "b1c2d3e4f5a6"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "user_notification_preferences",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("in_app_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("email_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("slack_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("teams_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("governance_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("stewardship_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("operational_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("only_assigned_items", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("daily_digest_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("slack_webhook_url", sa.Text(), nullable=True),
        sa.Column("teams_webhook_url", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("user_id", name="uq_user_notification_preferences_user_id"),
        schema=SCHEMA,
    )

    op.create_table(
        "user_inbox_notifications",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("dedupe_key", sa.String(length=255), nullable=False),
        sa.Column("category", sa.String(length=40), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False, server_default="medium"),
        sa.Column("source_module", sa.String(length=40), nullable=False),
        sa.Column("source_entity_type", sa.String(length=40), nullable=False),
        sa.Column("source_entity_id", sa.String(length=255), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("href", sa.Text(), nullable=True),
        sa.Column("state", sa.String(length=20), nullable=False, server_default="unread"),
        sa.Column("delivery_state", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("context_json", sa.JSON(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_notified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_delivery_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_channels_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("user_id", "dedupe_key", name="uq_user_inbox_notifications_user_dedupe"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_user_inbox_notifications_user_state",
        "user_inbox_notifications",
        ["user_id", "state"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_user_inbox_notifications_category",
        "user_inbox_notifications",
        ["category"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_user_inbox_notifications_due_delivery",
        "user_inbox_notifications",
        ["delivery_state", "next_delivery_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_user_inbox_notifications_created",
        "user_inbox_notifications",
        ["created_at"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "notification_delivery_attempts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("inbox_notification_id", sa.Integer(), nullable=False),
        sa.Column("channel", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("provider", sa.String(length=40), nullable=True),
        sa.Column("external_message_id", sa.String(length=255), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("response_payload_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["inbox_notification_id"], [f"{SCHEMA}.user_inbox_notifications.id"], ondelete="CASCADE"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_delivery_attempts_status",
        "notification_delivery_attempts",
        ["status"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_delivery_attempts_channel",
        "notification_delivery_attempts",
        ["channel"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_notification_delivery_attempts_due",
        "notification_delivery_attempts",
        ["status", "next_attempt_at"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "governance_score_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("owner_name", sa.String(length=255), nullable=True),
        sa.Column("domain_label", sa.String(length=255), nullable=True),
        sa.Column("score", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(length=40), nullable=False),
        sa.Column("tone", sa.String(length=20), nullable=False),
        sa.Column("dq_score", sa.Float(), nullable=True),
        sa.Column("open_incidents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("bucket_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["table_id"], [f"{SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["datasource_id"], [f"{SCHEMA}.data_sources.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("table_id", "bucket_date", name="uq_governance_score_snapshot_table_bucket"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_governance_score_snapshots_bucket_date",
        "governance_score_snapshots",
        ["bucket_date"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_governance_score_snapshots_table_bucket",
        "governance_score_snapshots",
        ["table_id", "bucket_date"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_governance_score_snapshots_score",
        "governance_score_snapshots",
        ["score"],
        unique=False,
        schema=SCHEMA,
    )

    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.lineage_graph_edges CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.lineage_nodes CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.lineage_edges CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.lineage_processes CASCADE")


def downgrade() -> None:
    op.drop_index("ix_governance_score_snapshots_score", table_name="governance_score_snapshots", schema=SCHEMA)
    op.drop_index("ix_governance_score_snapshots_table_bucket", table_name="governance_score_snapshots", schema=SCHEMA)
    op.drop_index("ix_governance_score_snapshots_bucket_date", table_name="governance_score_snapshots", schema=SCHEMA)
    op.drop_table("governance_score_snapshots", schema=SCHEMA)

    op.drop_index("ix_notification_delivery_attempts_due", table_name="notification_delivery_attempts", schema=SCHEMA)
    op.drop_index("ix_notification_delivery_attempts_channel", table_name="notification_delivery_attempts", schema=SCHEMA)
    op.drop_index("ix_notification_delivery_attempts_status", table_name="notification_delivery_attempts", schema=SCHEMA)
    op.drop_table("notification_delivery_attempts", schema=SCHEMA)

    op.drop_index("ix_user_inbox_notifications_created", table_name="user_inbox_notifications", schema=SCHEMA)
    op.drop_index("ix_user_inbox_notifications_due_delivery", table_name="user_inbox_notifications", schema=SCHEMA)
    op.drop_index("ix_user_inbox_notifications_category", table_name="user_inbox_notifications", schema=SCHEMA)
    op.drop_index("ix_user_inbox_notifications_user_state", table_name="user_inbox_notifications", schema=SCHEMA)
    op.drop_table("user_inbox_notifications", schema=SCHEMA)
    op.drop_table("user_notification_preferences", schema=SCHEMA)
