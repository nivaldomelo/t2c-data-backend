"""add platform read models visibility and usage

Revision ID: 6f0a1b2c3d4e
Revises: 5e8f9a1b2c3d
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "6f0a1b2c3d4e"
down_revision = "5e8f9a1b2c3d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.create_table(
        "search_read_model",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("parent_table_id", sa.Integer(), nullable=True),
        sa.Column("category", sa.String(length=80), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("subtitle", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("context_path", sa.Text(), nullable=True),
        sa.Column("target_url", sa.String(length=1000), nullable=False),
        sa.Column("searchable_name", sa.JSON(), nullable=False),
        sa.Column("searchable_aliases", sa.JSON(), nullable=False),
        sa.Column("searchable_synonyms", sa.JSON(), nullable=False),
        sa.Column("searchable_descriptions", sa.JSON(), nullable=False),
        sa.Column("searchable_context", sa.JSON(), nullable=False),
        sa.Column("source_name", sa.String(length=255), nullable=True),
        sa.Column("database_name", sa.String(length=255), nullable=True),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("owner_name", sa.String(length=255), nullable=True),
        sa.Column("domain_name", sa.String(length=255), nullable=True),
        sa.Column("classification", sa.String(length=120), nullable=True),
        sa.Column("certified", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("open_incidents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("popularity_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["parent_table_id"], [f"{schema}.tables.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("entity_type", "entity_id", name="uq_search_read_model_entity"),
        schema=schema,
    )
    op.create_index("ix_search_read_model_entity_type", "search_read_model", ["entity_type"], schema=schema)
    op.create_index("ix_search_read_model_entity_id", "search_read_model", ["entity_id"], schema=schema)
    op.create_index("ix_search_read_model_parent_table_id", "search_read_model", ["parent_table_id"], schema=schema)

    op.create_table(
        "dashboard_asset_read_model",
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("datasource_id", sa.Integer(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=True),
        sa.Column("schema_id", sa.Integer(), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("table_type", sa.String(length=40), nullable=False),
        sa.Column("schema_name", sa.String(length=255), nullable=False),
        sa.Column("database_name", sa.String(length=255), nullable=False),
        sa.Column("datasource_name", sa.String(length=255), nullable=False),
        sa.Column("engine", sa.String(length=40), nullable=False),
        sa.Column("owner_defined", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("description_complete", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("dictionary_complete", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("classification_defined", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("tags_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("terms_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("certification_status", sa.String(length=40), nullable=False),
        sa.Column("certification_criticality", sa.String(length=40), nullable=True),
        sa.Column("certification_badges", sa.JSON(), nullable=False),
        sa.Column("certification_decided_at", sa.String(length=64), nullable=True),
        sa.Column("certification_review_at", sa.String(length=64), nullable=True),
        sa.Column("certification_expires_at", sa.String(length=64), nullable=True),
        sa.Column("review_recent", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("dq_score", sa.Float(), nullable=True),
        sa.Column("completeness_pct_avg", sa.Float(), nullable=True),
        sa.Column("freshness_seconds", sa.Integer(), nullable=True),
        sa.Column("open_incidents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("critical_open_incidents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("owner_name", sa.String(length=255), nullable=True),
        sa.Column("data_owner_id", sa.Integer(), nullable=True),
        sa.Column("domain_name", sa.String(length=255), nullable=True),
        sa.Column("sensitivity_level", sa.String(length=40), nullable=True),
        sa.Column("has_personal_data", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("has_sensitive_personal_data", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("owner_reviewed_at", sa.String(length=64), nullable=True),
        sa.Column("privacy_reviewed_at", sa.String(length=64), nullable=True),
        sa.Column("last_review_at", sa.String(length=64), nullable=True),
        sa.Column("last_sync_at", sa.String(length=64), nullable=True),
        sa.Column("last_updated_at", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["table_id"], [f"{schema}.tables.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("table_id"),
        schema=schema,
    )
    op.create_index("ix_dashboard_asset_read_model_datasource_id", "dashboard_asset_read_model", ["datasource_id"], schema=schema)
    op.create_index("ix_dashboard_asset_read_model_database_id", "dashboard_asset_read_model", ["database_id"], schema=schema)
    op.create_index("ix_dashboard_asset_read_model_schema_id", "dashboard_asset_read_model", ["schema_id"], schema=schema)
    op.create_index("ix_dashboard_asset_read_model_data_owner_id", "dashboard_asset_read_model", ["data_owner_id"], schema=schema)

    op.create_table(
        "asset_visibility_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("allowed_role", sa.String(length=80), nullable=True),
        sa.Column("allowed_user_id", sa.Integer(), nullable=True),
        sa.Column("visibility_scope", sa.String(length=20), nullable=False, server_default="full"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["allowed_user_id"], [f"{schema}.users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_asset_visibility_rules_entity_type", "asset_visibility_rules", ["entity_type"], schema=schema)
    op.create_index("ix_asset_visibility_rules_entity_id", "asset_visibility_rules", ["entity_id"], schema=schema)
    op.create_index("ix_asset_visibility_rules_allowed_role", "asset_visibility_rules", ["allowed_role"], schema=schema)
    op.create_index("ix_asset_visibility_rules_allowed_user_id", "asset_visibility_rules", ["allowed_user_id"], schema=schema)

    op.create_table(
        "platform_usage_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("event_name", sa.String(length=80), nullable=False),
        sa.Column("module_name", sa.String(length=80), nullable=False),
        sa.Column("page_path", sa.String(length=255), nullable=True),
        sa.Column("entity_type", sa.String(length=40), nullable=True),
        sa.Column("entity_id", sa.Integer(), nullable=True),
        sa.Column("target_url", sa.String(length=1000), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], [f"{schema}.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_platform_usage_events_user_id", "platform_usage_events", ["user_id"], schema=schema)
    op.create_index("ix_platform_usage_events_event_name", "platform_usage_events", ["event_name"], schema=schema)
    op.create_index("ix_platform_usage_events_module_name", "platform_usage_events", ["module_name"], schema=schema)
    op.create_index("ix_platform_usage_events_page_path", "platform_usage_events", ["page_path"], schema=schema)
    op.create_index("ix_platform_usage_events_entity_type", "platform_usage_events", ["entity_type"], schema=schema)
    op.create_index("ix_platform_usage_events_entity_id", "platform_usage_events", ["entity_id"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_platform_usage_events_entity_id", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_platform_usage_events_entity_type", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_platform_usage_events_page_path", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_platform_usage_events_module_name", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_platform_usage_events_event_name", table_name="platform_usage_events", schema=schema)
    op.drop_index("ix_platform_usage_events_user_id", table_name="platform_usage_events", schema=schema)
    op.drop_table("platform_usage_events", schema=schema)

    op.drop_index("ix_asset_visibility_rules_allowed_user_id", table_name="asset_visibility_rules", schema=schema)
    op.drop_index("ix_asset_visibility_rules_allowed_role", table_name="asset_visibility_rules", schema=schema)
    op.drop_index("ix_asset_visibility_rules_entity_id", table_name="asset_visibility_rules", schema=schema)
    op.drop_index("ix_asset_visibility_rules_entity_type", table_name="asset_visibility_rules", schema=schema)
    op.drop_table("asset_visibility_rules", schema=schema)

    op.drop_index("ix_dashboard_asset_read_model_data_owner_id", table_name="dashboard_asset_read_model", schema=schema)
    op.drop_index("ix_dashboard_asset_read_model_schema_id", table_name="dashboard_asset_read_model", schema=schema)
    op.drop_index("ix_dashboard_asset_read_model_database_id", table_name="dashboard_asset_read_model", schema=schema)
    op.drop_index("ix_dashboard_asset_read_model_datasource_id", table_name="dashboard_asset_read_model", schema=schema)
    op.drop_table("dashboard_asset_read_model", schema=schema)

    op.drop_index("ix_search_read_model_parent_table_id", table_name="search_read_model", schema=schema)
    op.drop_index("ix_search_read_model_entity_id", table_name="search_read_model", schema=schema)
    op.drop_index("ix_search_read_model_entity_type", table_name="search_read_model", schema=schema)
    op.drop_table("search_read_model", schema=schema)
