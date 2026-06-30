"""add stewardship workflow tables

Revision ID: f3a4b5c6d7e8
Revises: e1f2a3b4c5d6
Create Date: 2026-03-29 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f3a4b5c6d7e8"
down_revision: Union[str, Sequence[str], None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "stewardship_requests",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("request_type", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("request_origin", sa.String(length=40), nullable=False, server_default="manual"),
        sa.Column("requested_by_user_id", sa.Integer(), nullable=True),
        sa.Column("approver_user_id", sa.Integer(), nullable=True),
        sa.Column("decided_by_user_id", sa.Integer(), nullable=True),
        sa.Column("requester_comment", sa.Text(), nullable=True),
        sa.Column("decision_comment", sa.Text(), nullable=True),
        sa.Column("current_value_json", sa.JSON(), nullable=True),
        sa.Column("proposed_value_json", sa.JSON(), nullable=True),
        sa.Column("context_json", sa.JSON(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["approver_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["decided_by_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["requested_by_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index("ix_stewardship_requests_status", "stewardship_requests", ["status"], unique=False, schema="t2c_data")
    op.create_index("ix_stewardship_requests_request_type", "stewardship_requests", ["request_type"], unique=False, schema="t2c_data")
    op.create_index("ix_stewardship_requests_table_id", "stewardship_requests", ["table_id"], unique=False, schema="t2c_data")

    op.create_table(
        "stewardship_request_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("stewardship_request_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=30), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["actor_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["stewardship_request_id"], ["t2c_data.stewardship_requests.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index(
        "ix_stewardship_request_events_request_id",
        "stewardship_request_events",
        ["stewardship_request_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_stewardship_request_events_event_type",
        "stewardship_request_events",
        ["event_type"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_stewardship_request_events_event_type", table_name="stewardship_request_events", schema="t2c_data")
    op.drop_index("ix_stewardship_request_events_request_id", table_name="stewardship_request_events", schema="t2c_data")
    op.drop_table("stewardship_request_events", schema="t2c_data")

    op.drop_index("ix_stewardship_requests_table_id", table_name="stewardship_requests", schema="t2c_data")
    op.drop_index("ix_stewardship_requests_request_type", table_name="stewardship_requests", schema="t2c_data")
    op.drop_index("ix_stewardship_requests_status", table_name="stewardship_requests", schema="t2c_data")
    op.drop_table("stewardship_requests", schema="t2c_data")
