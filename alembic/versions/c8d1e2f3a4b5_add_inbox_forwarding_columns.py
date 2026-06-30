"""add inbox forwarding columns

Revision ID: c8d1e2f3a4b5
Revises: e9f0a1b2c3d4
Create Date: 2026-04-06 23:40:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "c8d1e2f3a4b5"
down_revision = "e9f0a1b2c3d4"
branch_labels = None
depends_on = None
SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "user_inbox_notifications",
        sa.Column("forwarded_from_notification_id", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "user_inbox_notifications",
        sa.Column("forwarded_by_user_id", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "user_inbox_notifications",
        sa.Column("forwarded_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "fk_user_inbox_notifications_forwarded_from_notification_id",
        "user_inbox_notifications",
        "user_inbox_notifications",
        ["forwarded_from_notification_id"],
        ["id"],
        ondelete="SET NULL",
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
    )
    op.create_foreign_key(
        "fk_user_inbox_notifications_forwarded_by_user_id",
        "user_inbox_notifications",
        "users",
        ["forwarded_by_user_id"],
        ["id"],
        ondelete="SET NULL",
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
    )
    op.create_index(
        "ix_user_inbox_notifications_forwarded_from",
        "user_inbox_notifications",
        ["forwarded_from_notification_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_user_inbox_notifications_forwarded_by",
        "user_inbox_notifications",
        ["forwarded_by_user_id"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_user_inbox_notifications_forwarded_by", table_name="user_inbox_notifications", schema=SCHEMA)
    op.drop_index("ix_user_inbox_notifications_forwarded_from", table_name="user_inbox_notifications", schema=SCHEMA)
    op.drop_constraint(
        "fk_user_inbox_notifications_forwarded_by_user_id",
        "user_inbox_notifications",
        type_="foreignkey",
        schema=SCHEMA,
    )
    op.drop_constraint(
        "fk_user_inbox_notifications_forwarded_from_notification_id",
        "user_inbox_notifications",
        type_="foreignkey",
        schema=SCHEMA,
    )
    op.drop_column("user_inbox_notifications", "forwarded_at", schema=SCHEMA)
    op.drop_column("user_inbox_notifications", "forwarded_by_user_id", schema=SCHEMA)
    op.drop_column("user_inbox_notifications", "forwarded_from_notification_id", schema=SCHEMA)
