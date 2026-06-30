"""add visual rule builder to dq rules

Revision ID: 0a9b8c7d6e5f
Revises: ff3a4b5c6d7e
Create Date: 2026-05-25 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0a9b8c7d6e5f"
down_revision = "ff3a4b5c6d7e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dq_rules", sa.Column("rule_builder_version", sa.Integer(), nullable=True), schema="t2c_data")
    op.add_column("dq_rules", sa.Column("rule_definition_json", sa.JSON(), nullable=True), schema="t2c_data")
    op.add_column("dq_rules", sa.Column("legacy_rule_type", sa.String(length=50), nullable=True), schema="t2c_data")
    op.add_column(
        "dq_rules",
        sa.Column("archived", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema="t2c_data",
    )
    op.add_column("dq_rules", sa.Column("archived_reason", sa.String(length=255), nullable=True), schema="t2c_data")
    op.add_column("dq_rules", sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True), schema="t2c_data")
    op.create_index(
        op.f("ix_t2c_data_dq_rules_archived"),
        "dq_rules",
        ["archived"],
        unique=False,
        schema="t2c_data",
    )

    op.execute(
        """
        UPDATE t2c_data.dq_rules
        SET legacy_rule_type = rule_type,
            archived = TRUE,
            archived_reason = 'legacy_sql_rule_removed',
            archived_at = CURRENT_TIMESTAMP,
            is_active = FALSE
        WHERE COALESCE(NULLIF(TRIM(sql_text), ''), '') <> ''
        """
    )

    op.alter_column("dq_rules", "archived", server_default=None, schema="t2c_data")


def downgrade() -> None:
    op.drop_index(op.f("ix_t2c_data_dq_rules_archived"), table_name="dq_rules", schema="t2c_data")
    op.drop_column("dq_rules", "archived_at", schema="t2c_data")
    op.drop_column("dq_rules", "archived_reason", schema="t2c_data")
    op.drop_column("dq_rules", "archived", schema="t2c_data")
    op.drop_column("dq_rules", "legacy_rule_type", schema="t2c_data")
    op.drop_column("dq_rules", "rule_definition_json", schema="t2c_data")
    op.drop_column("dq_rules", "rule_builder_version", schema="t2c_data")
