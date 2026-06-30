"""add platform automation rules

Revision ID: ad4e5f607182
Revises: ac3d4e5f6071
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "ad4e5f607182"
down_revision = "ac3d4e5f6071"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    op.create_table(
        "platform_automation_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=20), server_default="active", nullable=False),
        sa.Column("scope_kind", sa.String(length=40), server_default="asset", nullable=False),
        sa.Column("scope_value", sa.String(length=255), nullable=True),
        sa.Column("condition_kind", sa.String(length=60), nullable=False),
        sa.Column("condition_operator", sa.String(length=20), server_default="gte", nullable=False),
        sa.Column("threshold_value", sa.Integer(), nullable=True),
        sa.Column("window_days", sa.Integer(), server_default="7", nullable=False),
        sa.Column("action_key", sa.String(length=120), nullable=False),
        sa.Column("action_target_json", sa.JSON(), nullable=True),
        sa.Column("execution_mode", sa.String(length=20), server_default="automatic", nullable=False),
        sa.Column("notify_owner", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("open_incident", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("schedule_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_triggered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_triggered_status", sa.String(length=20), nullable=True),
        sa.Column("last_triggered_summary_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_platform_automation_rules_status", "platform_automation_rules", ["status"], schema=schema)
    op.create_index("ix_platform_automation_rules_action_key", "platform_automation_rules", ["action_key"], schema=schema)
    op.create_index("ix_platform_automation_rules_scope", "platform_automation_rules", ["scope_kind", "scope_value"], schema=schema)
    op.create_index("ix_platform_automation_rules_condition", "platform_automation_rules", ["condition_kind", "condition_operator"], schema=schema)

    op.create_table(
        "platform_automation_executions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=True),
        sa.Column("action_key", sa.String(length=120), nullable=False),
        sa.Column("action_label", sa.String(length=200), nullable=False),
        sa.Column("execution_mode", sa.String(length=20), server_default="manual", nullable=False),
        sa.Column("status", sa.String(length=20), server_default="suggested", nullable=False),
        sa.Column("trigger_source", sa.String(length=40), server_default="manual", nullable=False),
        sa.Column("scope_kind", sa.String(length=40), server_default="asset", nullable=False),
        sa.Column("scope_value", sa.String(length=255), nullable=True),
        sa.Column("entity_type", sa.String(length=40), nullable=True),
        sa.Column("entity_id", sa.Integer(), nullable=True),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("domain_name", sa.String(length=200), nullable=True),
        sa.Column("product_name", sa.String(length=200), nullable=True),
        sa.Column("target_json", sa.JSON(), nullable=True),
        sa.Column("input_json", sa.JSON(), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=True),
        sa.Column("impact_json", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("executed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["executed_by_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["created_by_user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["rule_id"], [f"{schema}.platform_automation_rules.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["table_id"], [f"{schema}.tables.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_platform_automation_executions_created_at", "platform_automation_executions", ["created_at"], schema=schema)
    op.create_index("ix_platform_automation_executions_status", "platform_automation_executions", ["status"], schema=schema)
    op.create_index("ix_platform_automation_executions_action_key", "platform_automation_executions", ["action_key"], schema=schema)
    op.create_index("ix_platform_automation_executions_rule_id", "platform_automation_executions", ["rule_id"], schema=schema)
    op.create_index("ix_platform_automation_executions_scope", "platform_automation_executions", ["scope_kind", "scope_value"], schema=schema)
    op.create_index("ix_platform_automation_executions_entity", "platform_automation_executions", ["entity_type", "entity_id"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_platform_automation_executions_entity", table_name="platform_automation_executions", schema=schema)
    op.drop_index("ix_platform_automation_executions_scope", table_name="platform_automation_executions", schema=schema)
    op.drop_index("ix_platform_automation_executions_rule_id", table_name="platform_automation_executions", schema=schema)
    op.drop_index("ix_platform_automation_executions_action_key", table_name="platform_automation_executions", schema=schema)
    op.drop_index("ix_platform_automation_executions_status", table_name="platform_automation_executions", schema=schema)
    op.drop_index("ix_platform_automation_executions_created_at", table_name="platform_automation_executions", schema=schema)
    op.drop_table("platform_automation_executions", schema=schema)

    op.drop_index("ix_platform_automation_rules_condition", table_name="platform_automation_rules", schema=schema)
    op.drop_index("ix_platform_automation_rules_scope", table_name="platform_automation_rules", schema=schema)
    op.drop_index("ix_platform_automation_rules_action_key", table_name="platform_automation_rules", schema=schema)
    op.drop_index("ix_platform_automation_rules_status", table_name="platform_automation_rules", schema=schema)
    op.drop_table("platform_automation_rules", schema=schema)
