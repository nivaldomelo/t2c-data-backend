"""add certification goals

Revision ID: c4f5e6a7b8c9
Revises: b8c9d0e1f2a3
Create Date: 2026-05-13 15:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "c4f5e6a7b8c9"
down_revision = "b8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    op.create_table(
        "certification_goals",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("target_certified_assets", sa.Integer(), server_default="0", nullable=False),
        sa.Column("target_eligible_assets", sa.Integer(), server_default="0", nullable=False),
        sa.Column("target_reviewed_assets", sa.Integer(), server_default="0", nullable=False),
        sa.Column("target_revalidated_assets", sa.Integer(), server_default="0", nullable=False),
        sa.Column("scope_type", sa.String(length=40), server_default="global", nullable=False),
        sa.Column("scope_value", sa.String(length=255), nullable=True),
        sa.Column("owner", sa.String(length=160), nullable=True),
        sa.Column("status", sa.String(length=20), server_default="active", nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_certification_goals_status", "certification_goals", ["status"], schema=schema)
    op.create_index("ix_certification_goals_period", "certification_goals", ["period_start", "period_end"], schema=schema)
    op.create_index("ix_certification_goals_scope", "certification_goals", ["scope_type", "scope_value"], schema=schema)
    op.create_index("ix_certification_goals_owner", "certification_goals", ["owner"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_certification_goals_owner", table_name="certification_goals", schema=schema)
    op.drop_index("ix_certification_goals_scope", table_name="certification_goals", schema=schema)
    op.drop_index("ix_certification_goals_period", table_name="certification_goals", schema=schema)
    op.drop_index("ix_certification_goals_status", table_name="certification_goals", schema=schema)
    op.drop_table("certification_goals", schema=schema)
