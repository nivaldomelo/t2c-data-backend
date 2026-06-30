"""add dq rules tables

Revision ID: 9f2a6d1d77aa
Revises: 8b7a6cc9f2d1
Create Date: 2026-02-22 18:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "9f2a6d1d77aa"
down_revision = "8b7a6cc9f2d1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dq_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("table_fqn", sa.String(length=500), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("rule_type", sa.String(length=50), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("sql_text", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["table_id"], ["t2c_data.tables.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index(op.f("ix_t2c_data_dq_rules_table_id"), "dq_rules", ["table_id"], unique=False, schema="t2c_data")
    op.create_index(op.f("ix_t2c_data_dq_rules_table_fqn"), "dq_rules", ["table_fqn"], unique=False, schema="t2c_data")
    op.create_index(op.f("ix_t2c_data_dq_rules_is_active"), "dq_rules", ["is_active"], unique=False, schema="t2c_data")

    op.create_table(
        "dq_rule_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("violations_count", sa.BigInteger(), nullable=False),
        sa.Column("sample_rows_json", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["rule_id"], ["t2c_data.dq_rules.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_data",
    )
    op.create_index(op.f("ix_t2c_data_dq_rule_runs_rule_id"), "dq_rule_runs", ["rule_id"], unique=False, schema="t2c_data")


def downgrade() -> None:
    op.drop_index(op.f("ix_t2c_data_dq_rule_runs_rule_id"), table_name="dq_rule_runs", schema="t2c_data")
    op.drop_table("dq_rule_runs", schema="t2c_data")

    op.drop_index(op.f("ix_t2c_data_dq_rules_is_active"), table_name="dq_rules", schema="t2c_data")
    op.drop_index(op.f("ix_t2c_data_dq_rules_table_fqn"), table_name="dq_rules", schema="t2c_data")
    op.drop_index(op.f("ix_t2c_data_dq_rules_table_id"), table_name="dq_rules", schema="t2c_data")
    op.drop_table("dq_rules", schema="t2c_data")
