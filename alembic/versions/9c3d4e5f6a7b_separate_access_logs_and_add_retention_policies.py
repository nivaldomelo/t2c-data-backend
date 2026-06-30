"""separate access logs and add retention policies

Revision ID: 9c3d4e5f6a7b
Revises: 8b2c3d4e5f6a
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "9c3d4e5f6a7b"
down_revision = "8b2c3d4e5f6a"
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
        "access_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey(f"{schema}.users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("actor_name", sa.Text(), nullable=True),
        sa.Column("user_email", sa.Text(), nullable=True),
        sa.Column("ip", sa.dialects.postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("route", sa.Text(), nullable=False),
        sa.Column("method", sa.Text(), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.Column("api_version", sa.Text(), nullable=False, server_default="v1"),
        sa.Column("module_name", sa.Text(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("metadata_json", sa.dialects.postgresql.JSONB(), nullable=True),
        schema=schema,
    )
    op.create_table(
        "access_log_archive",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("actor_name", sa.Text(), nullable=True),
        sa.Column("user_email", sa.Text(), nullable=True),
        sa.Column("ip", sa.dialects.postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("route", sa.Text(), nullable=False),
        sa.Column("method", sa.Text(), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.Column("api_version", sa.Text(), nullable=False, server_default="v1"),
        sa.Column("module_name", sa.Text(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("metadata_json", sa.dialects.postgresql.JSONB(), nullable=True),
        schema=schema,
    )
    op.create_index("ix_access_log_created_at", "access_log", ["created_at"], schema=schema)
    op.create_index("ix_access_log_api_version_created_at", "access_log", ["api_version", "created_at"], schema=schema)
    op.create_index("ix_access_log_module_created_at", "access_log", ["module_name", "created_at"], schema=schema)
    op.create_index("ix_access_log_route_created_at", "access_log", ["route", "created_at"], schema=schema)
    op.create_index("ix_access_log_archive_created_at", "access_log_archive", ["created_at"], schema=schema)
    op.create_index("ix_access_log_archive_api_version_created_at", "access_log_archive", ["api_version", "created_at"], schema=schema)
    op.create_index("ix_access_log_archive_module_created_at", "access_log_archive", ["module_name", "created_at"], schema=schema)

    op.add_column("governance_settings", sa.Column("audit_log_retention_days", sa.Integer(), nullable=False, server_default="730"), schema=schema)
    op.add_column("governance_settings", sa.Column("access_log_retention_days", sa.Integer(), nullable=False, server_default="30"), schema=schema)
    op.add_column("governance_settings", sa.Column("access_log_archive_retention_days", sa.Integer(), nullable=False, server_default="365"), schema=schema)
    op.add_column("governance_settings", sa.Column("platform_usage_event_retention_days", sa.Integer(), nullable=False, server_default="180"), schema=schema)
    op.add_column("governance_settings", sa.Column("search_result_click_retention_days", sa.Integer(), nullable=False, server_default="180"), schema=schema)

    op.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.access_log (
                id, created_at, user_id, actor_name, user_email, ip, user_agent, route, method,
                status_code, request_id, api_version, module_name, duration_ms, metadata_json
            )
            SELECT
                id,
                created_at,
                user_id,
                actor_name,
                user_email,
                ip,
                user_agent,
                route,
                method,
                status_code,
                request_id,
                CASE WHEN route LIKE '/api/v1/%' OR route = '/api/v1' THEN 'v1' ELSE 'legacy' END,
                CASE
                    WHEN route LIKE '/api/v1/%' THEN split_part(substr(route, 9), '/', 1)
                    WHEN route LIKE '/api/%' THEN split_part(substr(route, 6), '/', 1)
                    ELSE 'api'
                END,
                CAST(COALESCE((metadata_json->>'duration_ms')::numeric, 0) AS integer),
                metadata_json
            FROM {schema}.audit_log
            WHERE action = 'http_request'
            """
        )
    )
    op.execute(sa.text(f"DELETE FROM {schema}.audit_log WHERE action = 'http_request'"))
    _sync_pk_sequence(schema, "access_log")


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_column("governance_settings", "search_result_click_retention_days", schema=schema)
    op.drop_column("governance_settings", "platform_usage_event_retention_days", schema=schema)
    op.drop_column("governance_settings", "access_log_archive_retention_days", schema=schema)
    op.drop_column("governance_settings", "access_log_retention_days", schema=schema)
    op.drop_column("governance_settings", "audit_log_retention_days", schema=schema)
    op.drop_index("ix_access_log_archive_module_created_at", table_name="access_log_archive", schema=schema)
    op.drop_index("ix_access_log_archive_api_version_created_at", table_name="access_log_archive", schema=schema)
    op.drop_index("ix_access_log_archive_created_at", table_name="access_log_archive", schema=schema)
    op.drop_table("access_log_archive", schema=schema)
    op.drop_index("ix_access_log_route_created_at", table_name="access_log", schema=schema)
    op.drop_index("ix_access_log_module_created_at", table_name="access_log", schema=schema)
    op.drop_index("ix_access_log_api_version_created_at", table_name="access_log", schema=schema)
    op.drop_index("ix_access_log_created_at", table_name="access_log", schema=schema)
    op.drop_table("access_log", schema=schema)
