"""add certification decision events

Revision ID: d5f6a7b8c9d0
Revises: c4f5e6a7b8c9
Create Date: 2026-05-13 18:15:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "d5f6a7b8c9d0"
down_revision = "c4f5e6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    op.create_table(
        "certification_decision_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("asset_name", sa.String(length=255), nullable=False),
        sa.Column("database_name", sa.String(length=255), nullable=False),
        sa.Column("schema_name", sa.String(length=255), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("previous_status", sa.String(length=40), nullable=True),
        sa.Column("new_status", sa.String(length=40), nullable=False),
        sa.Column("previous_readiness", sa.Integer(), nullable=True),
        sa.Column("new_readiness", sa.Integer(), nullable=True),
        sa.Column("decision_type", sa.String(length=40), nullable=False),
        sa.Column("decision_source", sa.String(length=20), server_default="manual", nullable=False),
        sa.Column("reviewer_user_id", sa.Integer(), nullable=True),
        sa.Column("reviewer", sa.String(length=255), nullable=True),
        sa.Column("reviewer_email", sa.String(length=255), nullable=True),
        sa.Column("observation", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revalidation_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("goal_id", sa.Integer(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["asset_id"], [f"{schema}.tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["goal_id"], [f"{schema}.certification_goals.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["reviewer_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_certification_decision_events_asset_id", "certification_decision_events", ["asset_id"], schema=schema)
    op.create_index("ix_certification_decision_events_decision_type", "certification_decision_events", ["decision_type"], schema=schema)
    op.create_index("ix_certification_decision_events_decision_source", "certification_decision_events", ["decision_source"], schema=schema)
    op.create_index("ix_certification_decision_events_created_at", "certification_decision_events", ["created_at"], schema=schema)
    op.create_index("ix_certification_decision_events_goal_id", "certification_decision_events", ["goal_id"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_certification_decision_events_goal_id", table_name="certification_decision_events", schema=schema)
    op.drop_index("ix_certification_decision_events_created_at", table_name="certification_decision_events", schema=schema)
    op.drop_index("ix_certification_decision_events_decision_source", table_name="certification_decision_events", schema=schema)
    op.drop_index("ix_certification_decision_events_decision_type", table_name="certification_decision_events", schema=schema)
    op.drop_index("ix_certification_decision_events_asset_id", table_name="certification_decision_events", schema=schema)
    op.drop_table("certification_decision_events", schema=schema)
