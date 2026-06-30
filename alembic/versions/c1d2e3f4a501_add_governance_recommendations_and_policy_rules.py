"""add governance recommendations and policy rules

Revision ID: c1d2e3f4a501
Revises: ab1c2d3e4f60
Create Date: 2026-04-10 15:20:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c1d2e3f4a501"
down_revision: Union[str, Sequence[str], None] = "ab1c2d3e4f60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("governance_policy_rules", sa.Text(), nullable=True),
        schema="t2c_data",
    )
    op.create_table(
        "governance_recommendations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("dedupe_key", sa.String(length=255), nullable=False),
        sa.Column("recommendation_key", sa.String(length=120), nullable=False),
        sa.Column("policy_rule_key", sa.String(length=120), nullable=True),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("domain_name", sa.String(length=255), nullable=True),
        sa.Column("source_kind", sa.String(length=40), nullable=False, server_default="governance"),
        sa.Column("source_label", sa.String(length=120), nullable=False, server_default="Governança"),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("impact", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="open"),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("confidence_score", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("trust_score", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("risk_score", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("action_key", sa.String(length=120), nullable=False),
        sa.Column("action_label", sa.String(length=160), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("context_value", sa.String(length=255), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("context_json", sa.JSON(), nullable=True),
        sa.Column("explanation_json", sa.JSON(), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by_user_id", sa.Integer(), nullable=True),
        sa.Column("resolution_action", sa.String(length=40), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["column_id"], ["t2c_data.columns.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["datasource_id"], ["t2c_data.data_sources.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["resolved_by_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("dedupe_key", name="uq_governance_recommendations_dedupe_key"),
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_recommendations_status",
        "governance_recommendations",
        ["status"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_recommendations_severity",
        "governance_recommendations",
        ["severity"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_recommendations_priority",
        "governance_recommendations",
        ["priority"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_recommendations_table",
        "governance_recommendations",
        ["table_id"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_recommendations_column",
        "governance_recommendations",
        ["column_id"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_recommendations_domain",
        "governance_recommendations",
        ["domain_name"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_recommendations_due_at",
        "governance_recommendations",
        ["due_at"],
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_governance_recommendations_due_at", table_name="governance_recommendations", schema="t2c_data")
    op.drop_index("ix_governance_recommendations_domain", table_name="governance_recommendations", schema="t2c_data")
    op.drop_index("ix_governance_recommendations_column", table_name="governance_recommendations", schema="t2c_data")
    op.drop_index("ix_governance_recommendations_table", table_name="governance_recommendations", schema="t2c_data")
    op.drop_index("ix_governance_recommendations_priority", table_name="governance_recommendations", schema="t2c_data")
    op.drop_index("ix_governance_recommendations_severity", table_name="governance_recommendations", schema="t2c_data")
    op.drop_index("ix_governance_recommendations_status", table_name="governance_recommendations", schema="t2c_data")
    op.drop_table("governance_recommendations", schema="t2c_data")
    op.drop_column("governance_settings", "governance_policy_rules", schema="t2c_data")
