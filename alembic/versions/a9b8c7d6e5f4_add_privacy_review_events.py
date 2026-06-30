"""add privacy review events

Revision ID: a8c7e6d5f4b3
Revises: e1f2a3b4c5d7
Create Date: 2026-05-14 00:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "a8c7e6d5f4b3"
down_revision = "e1f2a3b4c5d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    op.create_table(
        "privacy_review_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey(f'{schema}.tables.id', ondelete="CASCADE"), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("database_name", sa.String(length=255), nullable=False),
        sa.Column("schema_name", sa.String(length=255), nullable=False),
        sa.Column("previous_sensitivity_level", sa.String(length=40), nullable=True),
        sa.Column("new_sensitivity_level", sa.String(length=40), nullable=True),
        sa.Column("previous_has_personal_data", sa.Boolean(), nullable=True),
        sa.Column("new_has_personal_data", sa.Boolean(), nullable=True),
        sa.Column("previous_has_sensitive_personal_data", sa.Boolean(), nullable=True),
        sa.Column("new_has_sensitive_personal_data", sa.Boolean(), nullable=True),
        sa.Column("previous_legal_basis", sa.String(length=50), nullable=True),
        sa.Column("new_legal_basis", sa.String(length=50), nullable=True),
        sa.Column("previous_privacy_purpose", sa.Text(), nullable=True),
        sa.Column("new_privacy_purpose", sa.Text(), nullable=True),
        sa.Column("previous_retention_policy", sa.String(length=255), nullable=True),
        sa.Column("new_retention_policy", sa.String(length=255), nullable=True),
        sa.Column("previous_access_scope", sa.String(length=40), nullable=True),
        sa.Column("new_access_scope", sa.String(length=40), nullable=True),
        sa.Column("previous_access_roles", sa.JSON(), nullable=True),
        sa.Column("new_access_roles", sa.JSON(), nullable=True),
        sa.Column("previous_is_masked", sa.Boolean(), nullable=True),
        sa.Column("new_is_masked", sa.Boolean(), nullable=True),
        sa.Column("previous_external_sharing", sa.Boolean(), nullable=True),
        sa.Column("new_external_sharing", sa.Boolean(), nullable=True),
        sa.Column("previous_privacy_notes", sa.Text(), nullable=True),
        sa.Column("new_privacy_notes", sa.Text(), nullable=True),
        sa.Column("review_type", sa.String(length=40), nullable=False),
        sa.Column("review_source", sa.String(length=20), nullable=False, server_default="manual"),
        sa.Column("reviewer_user_id", sa.Integer(), sa.ForeignKey(f'{schema}.users.id', ondelete="SET NULL"), nullable=True),
        sa.Column("reviewer_name", sa.String(length=255), nullable=True),
        sa.Column("reviewer_email", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("risk_before", sa.String(length=20), nullable=True),
        sa.Column("risk_after", sa.String(length=20), nullable=True),
        sa.Column("next_review_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema=schema,
    )
    op.create_index("ix_privacy_review_events_table_id", "privacy_review_events", ["table_id"], schema=schema)
    op.create_index("ix_privacy_review_events_created_at", "privacy_review_events", ["created_at"], schema=schema)
    op.create_index("ix_privacy_review_events_review_type", "privacy_review_events", ["review_type"], schema=schema)
    op.create_index("ix_privacy_review_events_reviewer_user_id", "privacy_review_events", ["reviewer_user_id"], schema=schema)
    op.create_index("ix_privacy_review_events_risk_after", "privacy_review_events", ["risk_after"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_privacy_review_events_risk_after", table_name="privacy_review_events", schema=schema)
    op.drop_index("ix_privacy_review_events_reviewer_user_id", table_name="privacy_review_events", schema=schema)
    op.drop_index("ix_privacy_review_events_review_type", table_name="privacy_review_events", schema=schema)
    op.drop_index("ix_privacy_review_events_created_at", table_name="privacy_review_events", schema=schema)
    op.drop_index("ix_privacy_review_events_table_id", table_name="privacy_review_events", schema=schema)
    op.drop_table("privacy_review_events", schema=schema)
