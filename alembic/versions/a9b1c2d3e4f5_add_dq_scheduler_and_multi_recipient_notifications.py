"""add dq scheduler and multi-recipient notifications

Revision ID: a9b1c2d3e4f5
Revises: c8d1e2f3a4b5
Create Date: 2026-04-07 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a9b1c2d3e4f5"
down_revision: Union[str, Sequence[str], None] = "c8d1e2f3a4b5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "dq_rules",
        sa.Column("schedule_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        schema=SCHEMA,
    )
    op.add_column(
        "dq_rules",
        sa.Column("schedule_every_minutes", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "dq_rules",
        sa.Column("schedule_last_run_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_index("ix_t2c_data_dq_rules_schedule_enabled", "dq_rules", ["schedule_enabled"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_t2c_data_dq_rules_schedule_every_minutes",
        "dq_rules",
        ["schedule_every_minutes"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_t2c_data_dq_rules_schedule_last_run_at",
        "dq_rules",
        ["schedule_last_run_at"],
        unique=False,
        schema=SCHEMA,
    )

    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.dq_rules
            SET schedule_every_minutes = 60
            WHERE schedule_every_minutes IS NULL
            """
        )
    )

    op.create_table(
        "dq_rule_notification_recipients",
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["rule_id"], [f"{SCHEMA}.dq_rules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("rule_id", "user_id"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_t2c_data_dq_rule_notification_recipients_user_id",
        "dq_rule_notification_recipients",
        ["user_id"],
        unique=False,
        schema=SCHEMA,
    )

    op.execute(
        sa.text(
            f"""
            INSERT INTO {SCHEMA}.dq_rule_notification_recipients (rule_id, user_id)
            SELECT id, notification_recipient_user_id
            FROM {SCHEMA}.dq_rules
            WHERE notification_recipient_user_id IS NOT NULL
            ON CONFLICT DO NOTHING
            """
        )
    )

    op.create_table(
        "dq_scheduler_status",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("scheduler_name", sa.String(length=80), nullable=False, server_default="dq_rules"),
        sa.Column("mode", sa.String(length=20), nullable=False, server_default="embedded"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("last_started_at", sa.String(length=64), nullable=True),
        sa.Column("last_heartbeat_at", sa.String(length=64), nullable=True),
        sa.Column("last_success_at", sa.String(length=64), nullable=True),
        sa.Column("last_failure_at", sa.String(length=64), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_run_summary_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )
    op.execute(
        sa.text(
            f"""
            INSERT INTO {SCHEMA}.dq_scheduler_status (id, scheduler_name, mode, is_enabled)
            VALUES (1, 'dq_rules', 'embedded', true)
            ON CONFLICT (id) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    op.drop_table("dq_scheduler_status", schema=SCHEMA)
    op.drop_index(
        "ix_t2c_data_dq_rule_notification_recipients_user_id",
        table_name="dq_rule_notification_recipients",
        schema=SCHEMA,
    )
    op.drop_table("dq_rule_notification_recipients", schema=SCHEMA)

    op.drop_index("ix_t2c_data_dq_rules_schedule_last_run_at", table_name="dq_rules", schema=SCHEMA)
    op.drop_index("ix_t2c_data_dq_rules_schedule_every_minutes", table_name="dq_rules", schema=SCHEMA)
    op.drop_index("ix_t2c_data_dq_rules_schedule_enabled", table_name="dq_rules", schema=SCHEMA)
    op.drop_column("dq_rules", "schedule_last_run_at", schema=SCHEMA)
    op.drop_column("dq_rules", "schedule_every_minutes", schema=SCHEMA)
    op.drop_column("dq_rules", "schedule_enabled", schema=SCHEMA)
