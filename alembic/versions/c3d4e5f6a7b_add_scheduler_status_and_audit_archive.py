"""add scheduler status and audit archive

Revision ID: c3d4e5f6a7b
Revises: b2c3d4e5f6a
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "c3d4e5f6a7b"
down_revision = "b2c3d4e5f6a"
branch_labels = None
depends_on = None


def _sync_pk_sequence(schema: str, table_name: str, column_name: str = "id") -> None:
    op.execute(
        sa.text(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{schema}.{table_name}', '{column_name}'),
                GREATEST((SELECT COALESCE(MAX({column_name}), 0) FROM {schema}.{table_name}), 1),
                (SELECT COALESCE(MAX({column_name}), 0) > 0 FROM {schema}.{table_name})
            )
            """
        )
    )


def upgrade() -> None:
    schema = settings.db_schema
    op.create_table(
        "audit_log_archive",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("actor_name", sa.Text(), nullable=True),
        sa.Column("user_email", sa.Text(), nullable=True),
        sa.Column("ip", sa.dialects.postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=True),
        sa.Column("entity_id", sa.Text(), nullable=True),
        sa.Column("parent_entity_type", sa.Text(), nullable=True),
        sa.Column("parent_entity_id", sa.Text(), nullable=True),
        sa.Column("change_set_id", sa.Text(), nullable=True),
        sa.Column("change_type", sa.Text(), nullable=True),
        sa.Column("field_name", sa.Text(), nullable=True),
        sa.Column("source_module", sa.Text(), nullable=True),
        sa.Column("is_sensitive_change", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("sensitive_category", sa.Text(), nullable=True),
        sa.Column("route", sa.Text(), nullable=True),
        sa.Column("method", sa.Text(), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.Column("before_json", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.Column("after_json", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.Column("metadata_json", sa.dialects.postgresql.JSONB(), nullable=True),
        schema=schema,
    )
    op.create_index("ix_audit_log_archive_created_at", "audit_log_archive", ["created_at"], schema=schema)
    op.create_index("ix_audit_log_archive_action_created_at", "audit_log_archive", ["action", "created_at"], schema=schema)
    op.create_index("ix_audit_log_archive_entity_created_at", "audit_log_archive", ["entity_type", "entity_id", "created_at"], schema=schema)
    op.create_index("ix_audit_log_archive_source_created_at", "audit_log_archive", ["source_module", "created_at"], schema=schema)

    op.create_table(
        "platform_scheduler_status",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scheduler_name", sa.String(length=80), nullable=False, server_default="platform_maintenance"),
        sa.Column("mode", sa.String(length=20), nullable=False, server_default="embedded"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("last_started_at", sa.String(length=64), nullable=True),
        sa.Column("last_heartbeat_at", sa.String(length=64), nullable=True),
        sa.Column("last_success_at", sa.String(length=64), nullable=True),
        sa.Column("last_failure_at", sa.String(length=64), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_run_summary_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema=schema,
    )

    op.add_column(
        "governance_settings",
        sa.Column("audit_log_archive_retention_days", sa.Integer(), nullable=False, server_default="2555"),
        schema=schema,
    )

    op.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.platform_scheduler_status (id, scheduler_name, mode, is_enabled)
            VALUES (1, 'platform_maintenance', 'embedded', true)
            ON CONFLICT (id) DO NOTHING
            """
        )
    )
    _sync_pk_sequence(schema, "platform_scheduler_status")


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_column("governance_settings", "audit_log_archive_retention_days", schema=schema)
    op.drop_table("platform_scheduler_status", schema=schema)
    op.drop_index("ix_audit_log_archive_source_created_at", table_name="audit_log_archive", schema=schema)
    op.drop_index("ix_audit_log_archive_entity_created_at", table_name="audit_log_archive", schema=schema)
    op.drop_index("ix_audit_log_archive_action_created_at", table_name="audit_log_archive", schema=schema)
    op.drop_index("ix_audit_log_archive_created_at", table_name="audit_log_archive", schema=schema)
    op.drop_table("audit_log_archive", schema=schema)
