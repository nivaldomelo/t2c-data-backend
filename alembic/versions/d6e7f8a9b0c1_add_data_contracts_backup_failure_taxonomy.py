"""add data contracts, backup tracking, and operational failure taxonomy

Revision ID: d6e7f8a9b0c1
Revises: c5d6e7f8a9b0
Create Date: 2026-04-13 18:30:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "d6e7f8a9b0c1"
down_revision = "c5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_contracts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="draft"),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("owner_user_id", sa.Integer(), sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL")),
        sa.Column("steward_user_id", sa.Integer(), sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL")),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("freshness_hours", sa.Integer()),
        sa.Column("min_row_count", sa.Integer()),
        sa.Column("max_row_count", sa.Integer()),
        sa.Column("compatibility_rules_json", sa.JSON(), nullable=True),
        sa.Column("last_validation_status", sa.String(length=30)),
        sa.Column("last_validation_at", sa.DateTime(timezone=True)),
        sa.Column("last_validation_issues", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("table_id", "version", name="uq_data_contract_table_version"),
        schema="t2c_data",
    )
    op.create_index("ix_data_contracts_table", "data_contracts", ["table_id"], schema="t2c_data")

    op.create_table(
        "data_contract_columns",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("contract_id", sa.Integer(), sa.ForeignKey("t2c_data.data_contracts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("column_name", sa.String(length=160), nullable=False),
        sa.Column("data_type", sa.String(length=120)),
        sa.Column("is_nullable", sa.Boolean()),
        sa.Column("is_primary_key", sa.Boolean()),
        sa.Column("is_required", sa.Boolean()),
        sa.Column("ordinal_position", sa.Integer()),
        sa.Column("notes", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("contract_id", "column_name", name="uq_data_contract_column"),
        schema="t2c_data",
    )
    op.create_index("ix_data_contract_columns_contract", "data_contract_columns", ["contract_id"], schema="t2c_data")

    op.create_table(
        "data_contract_validations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("contract_id", sa.Integer(), sa.ForeignKey("t2c_data.data_contracts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("issues_json", sa.JSON(), nullable=True),
        sa.Column("summary_json", sa.JSON(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_index("ix_data_contract_validations_contract", "data_contract_validations", ["contract_id"], schema="t2c_data")
    op.create_index("ix_data_contract_validations_table", "data_contract_validations", ["table_id"], schema="t2c_data")

    op.create_table(
        "operational_failure_taxonomy",
        sa.Column("code", sa.String(length=80), primary_key=True),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column("default_severity", sa.String(length=30), nullable=False, server_default="medium"),
        sa.Column("retryable", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("source_group", sa.String(length=120)),
        schema="t2c_data",
    )

    op.create_table(
        "operational_failure_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("category_code", sa.String(length=80), sa.ForeignKey("t2c_data.operational_failure_taxonomy.code", ondelete="RESTRICT"), nullable=False),
        sa.Column("severity", sa.String(length=30), nullable=False, server_default="medium"),
        sa.Column("source", sa.String(length=120), nullable=False),
        sa.Column("error_type", sa.String(length=160)),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("context_json", sa.JSON()),
        sa.Column("retryable", sa.Boolean()),
        sa.Column("datasource_id", sa.Integer(), sa.ForeignKey("t2c_data.data_sources.id", ondelete="SET NULL")),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="SET NULL")),
        sa.Column("scheduler_name", sa.String(length=120)),
        sa.Column("job_name", sa.String(length=160)),
        sa.Column("route", sa.String(length=240)),
        sa.Column("external_reference", sa.String(length=200)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("source", "external_reference", name="uq_operational_failure_source_reference"),
        schema="t2c_data",
    )
    op.create_index("ix_operational_failure_category", "operational_failure_events", ["category_code"], schema="t2c_data")
    op.create_index("ix_operational_failure_source", "operational_failure_events", ["source"], schema="t2c_data")

    op.create_table(
        "backup_executions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scope", sa.String(length=80), nullable=False, server_default="platform"),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("retention_days", sa.Integer()),
        sa.Column("storage_uri", sa.Text()),
        sa.Column("size_bytes", sa.Integer()),
        sa.Column("error_message", sa.Text()),
        sa.Column("trigger_source", sa.String(length=40), nullable=False, server_default="manual"),
        sa.Column("triggered_by_user_id", sa.Integer(), sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL")),
        sa.Column("metadata_json", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_index("ix_backup_executions_scope", "backup_executions", ["scope", "started_at"], schema="t2c_data")

    taxonomy = [
        {"code": "AUTHENTICATION_ERROR", "name": "Authentication error", "description": "Falha de autenticação.", "default_severity": "high", "retryable": False},
        {"code": "AUTHORIZATION_ERROR", "name": "Authorization error", "description": "Falha de autorização.", "default_severity": "high", "retryable": False},
        {"code": "CONNECTIVITY_ERROR", "name": "Connectivity error", "description": "Erro de conectividade.", "default_severity": "high", "retryable": True},
        {"code": "SCHEMA_DRIFT", "name": "Schema drift", "description": "Mudança inesperada de schema.", "default_severity": "medium", "retryable": False},
        {"code": "SQL_PARSE_ERROR", "name": "SQL parse error", "description": "Erro de sintaxe ou parse em SQL.", "default_severity": "medium", "retryable": False},
        {"code": "PERMISSION_ERROR", "name": "Permission error", "description": "Erro de permissão em origem.", "default_severity": "high", "retryable": False},
        {"code": "TIMEOUT_ERROR", "name": "Timeout error", "description": "Tempo limite excedido.", "default_severity": "high", "retryable": True},
        {"code": "RESOURCE_VOLUME_ERROR", "name": "Resource/volume error", "description": "Volume ou recurso insuficiente.", "default_severity": "high", "retryable": True},
        {"code": "RULE_EXECUTION_ERROR", "name": "Rule execution error", "description": "Falha na execução de regra.", "default_severity": "medium", "retryable": True},
        {"code": "VALIDATION_ERROR", "name": "Validation error", "description": "Erro de validação funcional.", "default_severity": "medium", "retryable": False},
        {"code": "EXTERNAL_DEPENDENCY_ERROR", "name": "External dependency error", "description": "Falha de dependência externa.", "default_severity": "high", "retryable": True},
        {"code": "UNKNOWN_OPERATIONAL_ERROR", "name": "Unknown operational error", "description": "Falha operacional sem classificação.", "default_severity": "medium", "retryable": True},
    ]
    op.bulk_insert(
        sa.Table(
            "operational_failure_taxonomy",
            sa.MetaData(),
            sa.Column("code", sa.String(length=80)),
            sa.Column("name", sa.String(length=160)),
            sa.Column("description", sa.Text()),
            sa.Column("default_severity", sa.String(length=30)),
            sa.Column("retryable", sa.Boolean()),
            schema="t2c_data",
        ),
        taxonomy,
    )


def downgrade() -> None:
    op.drop_index("ix_backup_executions_scope", table_name="backup_executions", schema="t2c_data")
    op.drop_table("backup_executions", schema="t2c_data")
    op.drop_index("ix_operational_failure_source", table_name="operational_failure_events", schema="t2c_data")
    op.drop_index("ix_operational_failure_category", table_name="operational_failure_events", schema="t2c_data")
    op.drop_table("operational_failure_events", schema="t2c_data")
    op.drop_table("operational_failure_taxonomy", schema="t2c_data")
    op.drop_index("ix_data_contract_validations_table", table_name="data_contract_validations", schema="t2c_data")
    op.drop_index("ix_data_contract_validations_contract", table_name="data_contract_validations", schema="t2c_data")
    op.drop_table("data_contract_validations", schema="t2c_data")
    op.drop_index("ix_data_contract_columns_contract", table_name="data_contract_columns", schema="t2c_data")
    op.drop_table("data_contract_columns", schema="t2c_data")
    op.drop_index("ix_data_contracts_table", table_name="data_contracts", schema="t2c_data")
    op.drop_table("data_contracts", schema="t2c_data")
