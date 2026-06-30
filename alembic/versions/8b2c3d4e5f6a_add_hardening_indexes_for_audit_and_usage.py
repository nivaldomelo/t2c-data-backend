"""add hardening indexes for audit and usage

Revision ID: 8b2c3d4e5f6a
Revises: 7a1b2c3d4e5f
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op

from t2c_data.core.config import settings


revision = "8b2c3d4e5f6a"
down_revision = "7a1b2c3d4e5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.create_index("ix_audit_log_created_at", "audit_log", ["created_at"], schema=schema)
    op.create_index("ix_audit_log_action_created_at", "audit_log", ["action", "created_at"], schema=schema)
    op.create_index("ix_audit_log_entity_created_at", "audit_log", ["entity_type", "entity_id", "created_at"], schema=schema)
    op.create_index("ix_audit_log_source_created_at", "audit_log", ["source_module", "created_at"], schema=schema)
    op.create_index("ix_platform_usage_events_created_at", "platform_usage_events", ["created_at"], schema=schema)
    op.create_index("ix_platform_usage_events_module_created", "platform_usage_events", ["module_name", "created_at"], schema=schema)
    op.create_index("ix_platform_usage_events_event_created", "platform_usage_events", ["event_name", "created_at"], schema=schema)
    op.create_index("ix_search_result_clicks_created_at", "search_result_clicks", ["created_at"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_search_result_clicks_created_at", table_name="search_result_clicks", schema=schema)
    op.drop_index("ix_platform_usage_events_event_created", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_platform_usage_events_module_created", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_platform_usage_events_created_at", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_audit_log_source_created_at", table_name="audit_log", schema=schema)
    op.drop_index("ix_audit_log_entity_created_at", table_name="audit_log", schema=schema)
    op.drop_index("ix_audit_log_action_created_at", table_name="audit_log", schema=schema)
    op.drop_index("ix_audit_log_created_at", table_name="audit_log", schema=schema)
