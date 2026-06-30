"""add user audit sessions and events

Revision ID: c0a1b2c3d4e6
Revises: b0c1d2e3f4a5
Create Date: 2026-05-27 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "c0a1b2c3d4e6"
down_revision = "b0c1d2e3f4a5"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column("user_sessions", sa.Column("started_at", sa.DateTime(timezone=True), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("duration_seconds", sa.Integer(), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("end_reason", sa.String(length=30), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("device_type", sa.String(length=40), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("browser", sa.String(length=80), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("os", sa.String(length=80), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("country", sa.String(length=80), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("city", sa.String(length=80), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("auth_method", sa.String(length=20), nullable=True), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("mfa_used", sa.Boolean(), server_default=sa.text("false"), nullable=False), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("success", sa.Boolean(), server_default=sa.text("true"), nullable=False), schema=SCHEMA)
    op.add_column("user_sessions", sa.Column("failure_reason", sa.Text(), nullable=True), schema=SCHEMA)
    op.create_index("ix_user_sessions_user_started_at", "user_sessions", ["user_id", "started_at"], schema=SCHEMA)
    op.create_index("ix_user_sessions_last_seen_at", "user_sessions", ["last_seen_at"], schema=SCHEMA)
    op.create_index("ix_user_sessions_ended_at", "user_sessions", ["ended_at"], schema=SCHEMA)
    op.create_index("ix_user_sessions_auth_method", "user_sessions", ["auth_method"], schema=SCHEMA)

    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.user_sessions
            SET started_at = COALESCE(started_at, created_at),
                last_seen_at = COALESCE(last_seen_at, created_at),
                mfa_used = COALESCE(mfa_used, false),
                success = COALESCE(success, true)
            """
        )
    )
    op.alter_column("user_sessions", "started_at", nullable=False, schema=SCHEMA)
    op.alter_column("user_sessions", "last_seen_at", nullable=False, schema=SCHEMA)

    op.create_table(
        "user_access_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("session_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(length=40), nullable=False),
        sa.Column("page_key", sa.String(length=80), nullable=True),
        sa.Column("route_path", sa.Text(), nullable=True),
        sa.Column("http_method", sa.String(length=10), nullable=True),
        sa.Column("resource_type", sa.String(length=40), nullable=True),
        sa.Column("resource_id", sa.String(length=80), nullable=True),
        sa.Column("resource_fqn", sa.String(length=1000), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("action", sa.String(length=40), nullable=True),
        sa.Column("sensitivity_level", sa.String(length=40), nullable=True),
        sa.Column("has_personal_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("has_sensitive_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("privacy_classification", sa.String(length=120), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ip_address", postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["session_id"], ["user_sessions.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )
    op.create_index("ix_user_access_events_created_at", "user_access_events", ["created_at"], schema=SCHEMA)
    op.create_index("ix_user_access_events_user_created_at", "user_access_events", ["user_id", "created_at"], schema=SCHEMA)
    op.create_index("ix_user_access_events_session_created_at", "user_access_events", ["session_id", "created_at"], schema=SCHEMA)
    op.create_index("ix_user_access_events_event_type_created_at", "user_access_events", ["event_type", "created_at"], schema=SCHEMA)
    op.create_index("ix_user_access_events_page_key_created_at", "user_access_events", ["page_key", "created_at"], schema=SCHEMA)
    op.create_index("ix_user_access_events_resource_type_resource_id", "user_access_events", ["resource_type", "resource_id"], schema=SCHEMA)
    op.create_index("ix_user_access_events_datasource_schema_table", "user_access_events", ["datasource_id", "schema_name", "table_id"], schema=SCHEMA)
    op.create_index("ix_user_access_events_sensitivity_level", "user_access_events", ["sensitivity_level"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_user_access_events_sensitivity_level", table_name="user_access_events", schema=SCHEMA)
    op.drop_index("ix_user_access_events_datasource_schema_table", table_name="user_access_events", schema=SCHEMA)
    op.drop_index("ix_user_access_events_resource_type_resource_id", table_name="user_access_events", schema=SCHEMA)
    op.drop_index("ix_user_access_events_page_key_created_at", table_name="user_access_events", schema=SCHEMA)
    op.drop_index("ix_user_access_events_event_type_created_at", table_name="user_access_events", schema=SCHEMA)
    op.drop_index("ix_user_access_events_session_created_at", table_name="user_access_events", schema=SCHEMA)
    op.drop_index("ix_user_access_events_user_created_at", table_name="user_access_events", schema=SCHEMA)
    op.drop_index("ix_user_access_events_created_at", table_name="user_access_events", schema=SCHEMA)
    op.drop_table("user_access_events", schema=SCHEMA)

    op.drop_index("ix_user_sessions_auth_method", table_name="user_sessions", schema=SCHEMA)
    op.drop_index("ix_user_sessions_ended_at", table_name="user_sessions", schema=SCHEMA)
    op.drop_index("ix_user_sessions_last_seen_at", table_name="user_sessions", schema=SCHEMA)
    op.drop_index("ix_user_sessions_user_started_at", table_name="user_sessions", schema=SCHEMA)
    op.drop_column("user_sessions", "failure_reason", schema=SCHEMA)
    op.drop_column("user_sessions", "success", schema=SCHEMA)
    op.drop_column("user_sessions", "mfa_used", schema=SCHEMA)
    op.drop_column("user_sessions", "auth_method", schema=SCHEMA)
    op.drop_column("user_sessions", "city", schema=SCHEMA)
    op.drop_column("user_sessions", "country", schema=SCHEMA)
    op.drop_column("user_sessions", "os", schema=SCHEMA)
    op.drop_column("user_sessions", "browser", schema=SCHEMA)
    op.drop_column("user_sessions", "device_type", schema=SCHEMA)
    op.drop_column("user_sessions", "end_reason", schema=SCHEMA)
    op.drop_column("user_sessions", "duration_seconds", schema=SCHEMA)
    op.drop_column("user_sessions", "ended_at", schema=SCHEMA)
    op.drop_column("user_sessions", "last_seen_at", schema=SCHEMA)
    op.drop_column("user_sessions", "started_at", schema=SCHEMA)
