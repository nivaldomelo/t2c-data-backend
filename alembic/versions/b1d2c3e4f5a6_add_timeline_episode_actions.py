"""add timeline episode actions

Revision ID: b1d2c3e4f5a6
Revises: fa0b1c2d3e4f
Create Date: 2026-04-13 12:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b1d2c3e4f5a6"
down_revision: Union[str, Sequence[str], None] = "fa0b1c2d3e4f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "timeline_episode_actions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("episode_key", sa.String(length=255), nullable=False),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=True),
        sa.Column("column_id", sa.Integer(), sa.ForeignKey("t2c_data.columns.id", ondelete="CASCADE"), nullable=True),
        sa.Column("action_type", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="open"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("silent_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("actor_user_id", sa.Integer(), sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("actor_name", sa.String(length=255), nullable=True),
        sa.Column("actor_email", sa.String(length=255), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_index(
        "ix_timeline_episode_actions_episode_created",
        "timeline_episode_actions",
        ["episode_key", "created_at"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_timeline_episode_actions_table_created",
        "timeline_episode_actions",
        ["table_id", "created_at"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_timeline_episode_actions_action_type",
        "timeline_episode_actions",
        ["action_type"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_timeline_episode_actions_status",
        "timeline_episode_actions",
        ["status"],
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_timeline_episode_actions_status", table_name="timeline_episode_actions", schema="t2c_data")
    op.drop_index("ix_timeline_episode_actions_action_type", table_name="timeline_episode_actions", schema="t2c_data")
    op.drop_index("ix_timeline_episode_actions_table_created", table_name="timeline_episode_actions", schema="t2c_data")
    op.drop_index("ix_timeline_episode_actions_episode_created", table_name="timeline_episode_actions", schema="t2c_data")
    op.drop_table("timeline_episode_actions", schema="t2c_data")
