"""add collaboration workflow tables

Revision ID: ae5f60718293
Revises: ad4e5f607182
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "ae5f60718293"
down_revision = "ad4e5f607182"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    op.create_table(
        "collaboration_tasks",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("entity_label", sa.String(length=255), nullable=False),
        sa.Column("title", sa.String(length=220), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("task_type", sa.String(length=60), nullable=False),
        sa.Column("status", sa.String(length=24), server_default="open", nullable=False),
        sa.Column("priority", sa.String(length=20), server_default="medium", nullable=False),
        sa.Column("responsibility_role", sa.String(length=80), nullable=True),
        sa.Column("assigned_to_user_id", sa.Integer(), nullable=True),
        sa.Column("assigned_by_user_id", sa.Integer(), nullable=True),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("linked_request_type", sa.String(length=40), nullable=True),
        sa.Column("context_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["assigned_by_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["assigned_to_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["completed_by_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_collaboration_tasks_entity", "collaboration_tasks", ["entity_type", "entity_id"], unique=False, schema=schema)
    op.create_index("ix_collaboration_tasks_status", "collaboration_tasks", ["status"], unique=False, schema=schema)
    op.create_index("ix_collaboration_tasks_assigned_to", "collaboration_tasks", ["assigned_to_user_id"], unique=False, schema=schema)
    op.create_index("ix_collaboration_tasks_task_type", "collaboration_tasks", ["task_type"], unique=False, schema=schema)
    op.create_index("ix_collaboration_tasks_due_at", "collaboration_tasks", ["due_at"], unique=False, schema=schema)

    op.create_table(
        "collaboration_comments",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("entity_label", sa.String(length=255), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=True),
        sa.Column("parent_comment_id", sa.Integer(), nullable=True),
        sa.Column("comment_kind", sa.String(length=40), server_default="comment", nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("visibility_scope", sa.String(length=24), server_default="collaboration", nullable=False),
        sa.Column("is_resolved", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("author_user_id", sa.Integer(), nullable=True),
        sa.Column("author_name", sa.String(length=255), nullable=True),
        sa.Column("author_email", sa.String(length=255), nullable=True),
        sa.Column("resolved_by_user_id", sa.Integer(), nullable=True),
        sa.Column("context_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["author_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["parent_comment_id"], [f"{schema}.collaboration_comments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["resolved_by_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["task_id"], [f"{schema}.collaboration_tasks.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_collaboration_comments_entity", "collaboration_comments", ["entity_type", "entity_id"], unique=False, schema=schema)
    op.create_index("ix_collaboration_comments_task_id", "collaboration_comments", ["task_id"], unique=False, schema=schema)
    op.create_index("ix_collaboration_comments_author_user_id", "collaboration_comments", ["author_user_id"], unique=False, schema=schema)

    op.create_table(
        "collaboration_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=60), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("status_from", sa.String(length=24), nullable=True),
        sa.Column("status_to", sa.String(length=24), nullable=True),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("actor_name", sa.String(length=255), nullable=True),
        sa.Column("actor_email", sa.String(length=255), nullable=True),
        sa.Column("comment_id", sa.Integer(), nullable=True),
        sa.Column("task_id", sa.Integer(), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["actor_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["comment_id"], [f"{schema}.collaboration_comments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["task_id"], [f"{schema}.collaboration_tasks.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_collaboration_events_entity", "collaboration_events", ["entity_type", "entity_id"], unique=False, schema=schema)
    op.create_index("ix_collaboration_events_event_type", "collaboration_events", ["event_type"], unique=False, schema=schema)
    op.create_index("ix_collaboration_events_task_id", "collaboration_events", ["task_id"], unique=False, schema=schema)
    op.create_index("ix_collaboration_events_comment_id", "collaboration_events", ["comment_id"], unique=False, schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_collaboration_events_comment_id", table_name="collaboration_events", schema=schema)
    op.drop_index("ix_collaboration_events_task_id", table_name="collaboration_events", schema=schema)
    op.drop_index("ix_collaboration_events_event_type", table_name="collaboration_events", schema=schema)
    op.drop_index("ix_collaboration_events_entity", table_name="collaboration_events", schema=schema)
    op.drop_table("collaboration_events", schema=schema)

    op.drop_index("ix_collaboration_comments_author_user_id", table_name="collaboration_comments", schema=schema)
    op.drop_index("ix_collaboration_comments_task_id", table_name="collaboration_comments", schema=schema)
    op.drop_index("ix_collaboration_comments_entity", table_name="collaboration_comments", schema=schema)
    op.drop_table("collaboration_comments", schema=schema)

    op.drop_index("ix_collaboration_tasks_due_at", table_name="collaboration_tasks", schema=schema)
    op.drop_index("ix_collaboration_tasks_task_type", table_name="collaboration_tasks", schema=schema)
    op.drop_index("ix_collaboration_tasks_assigned_to", table_name="collaboration_tasks", schema=schema)
    op.drop_index("ix_collaboration_tasks_status", table_name="collaboration_tasks", schema=schema)
    op.drop_index("ix_collaboration_tasks_entity", table_name="collaboration_tasks", schema=schema)
    op.drop_table("collaboration_tasks", schema=schema)
