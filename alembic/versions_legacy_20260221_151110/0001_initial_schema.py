"""initial schema

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-02-19 00:00:00

"""
from alembic import op
import sqlalchemy as sa


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "roles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", name="uq_roles_name"),
    )

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=True),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )

    op.create_table(
        "user_role",
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("role_id", sa.Integer(), sa.ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
    )

    op.create_table(
        "data_sources",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("type", sa.String(length=30), nullable=False),
        sa.Column("connection_uri", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", name="uq_data_sources_name"),
    )

    op.create_table(
        "databases",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("datasource_id", sa.Integer(), sa.ForeignKey("data_sources.id", ondelete="CASCADE")),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description_source", sa.Text(), nullable=True),
        sa.Column("description_manual", sa.Text(), nullable=True),
        sa.Column("owner", sa.String(length=120), nullable=True),
        sa.Column("lifecycle_status", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("datasource_id", "name", name="uq_databases_datasource_name"),
    )

    op.create_table(
        "schemas",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("database_id", sa.Integer(), sa.ForeignKey("databases.id", ondelete="CASCADE")),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description_source", sa.Text(), nullable=True),
        sa.Column("description_manual", sa.Text(), nullable=True),
        sa.Column("owner", sa.String(length=120), nullable=True),
        sa.Column("lifecycle_status", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("database_id", "name", name="uq_schemas_database_name"),
    )

    op.create_table(
        "tables",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("schema_id", sa.Integer(), sa.ForeignKey("schemas.id", ondelete="CASCADE")),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("table_type", sa.String(length=20), nullable=False),
        sa.Column("description_source", sa.Text(), nullable=True),
        sa.Column("description_manual", sa.Text(), nullable=True),
        sa.Column("owner", sa.String(length=120), nullable=True),
        sa.Column("lifecycle_status", sa.String(length=50), nullable=True),
        sa.Column("schema_hash", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("schema_id", "name", name="uq_tables_schema_name"),
    )
    op.create_index("ix_tables_schema_hash", "tables", ["schema_hash"])

    op.create_table(
        "columns",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE")),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("data_type", sa.String(length=200), nullable=False),
        sa.Column("is_nullable", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("ordinal_position", sa.Integer(), nullable=False),
        sa.Column("description_source", sa.Text(), nullable=True),
        sa.Column("description_manual", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("table_id", "name", name="uq_columns_table_name"),
    )

    op.create_table(
        "scan_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("datasource_id", sa.Integer(), sa.ForeignKey("data_sources.id", ondelete="CASCADE")),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("started_by", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL")),
        sa.Column("summary", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "scan_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scan_run_id", sa.Integer(), sa.ForeignKey("scan_runs.id", ondelete="CASCADE")),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_key", sa.String(length=400), nullable=False),
        sa.Column("entity_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_scan_snapshots_entity_key", "scan_snapshots", ["entity_key"])
    op.create_index("ix_scan_snapshots_entity_hash", "scan_snapshots", ["entity_hash"])

    op.create_table(
        "scan_diffs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scan_run_id", sa.Integer(), sa.ForeignKey("scan_runs.id", ondelete="CASCADE")),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_key", sa.String(length=400), nullable=False),
        sa.Column("diff_type", sa.String(length=20), nullable=False),
        sa.Column("old_hash", sa.String(length=64), nullable=True),
        sa.Column("new_hash", sa.String(length=64), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_scan_diffs_entity_key", "scan_diffs", ["entity_key"])

    op.create_table(
        "tags",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("color", sa.String(length=20), nullable=True),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", name="uq_tags_name"),
    )

    op.create_table(
        "tag_assignments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tag_id", sa.Integer(), sa.ForeignKey("tags.id", ondelete="CASCADE"), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("tag_id", "entity_type", "entity_id", name="uq_tag_assignment_entity"),
    )

    op.create_table(
        "glossary_terms",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("definition", sa.Text(), nullable=False),
        sa.Column("steward", sa.String(length=120), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", name="uq_glossary_terms_name"),
    )

    op.create_table(
        "glossary_assignments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("term_id", sa.Integer(), sa.ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("term_id", "entity_type", "entity_id", name="uq_glossary_assignment_entity"),
    )

    op.create_table(
        "lineage_processes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "lineage_edges",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("process_id", sa.Integer(), sa.ForeignKey("lineage_processes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("from_entity_type", sa.String(length=40), nullable=False),
        sa.Column("from_entity_id", sa.Integer(), nullable=False),
        sa.Column("to_entity_type", sa.String(length=40), nullable=False),
        sa.Column("to_entity_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("actor_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("action", sa.String(length=60), nullable=False),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("changes", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("audit_logs")
    op.drop_table("lineage_edges")
    op.drop_table("lineage_processes")
    op.drop_table("glossary_assignments")
    op.drop_table("glossary_terms")
    op.drop_table("tag_assignments")
    op.drop_table("tags")
    op.drop_index("ix_scan_diffs_entity_key", table_name="scan_diffs")
    op.drop_table("scan_diffs")
    op.drop_index("ix_scan_snapshots_entity_hash", table_name="scan_snapshots")
    op.drop_index("ix_scan_snapshots_entity_key", table_name="scan_snapshots")
    op.drop_table("scan_snapshots")
    op.drop_table("scan_runs")
    op.drop_table("columns")
    op.drop_index("ix_tables_schema_hash", table_name="tables")
    op.drop_table("tables")
    op.drop_table("schemas")
    op.drop_table("databases")
    op.drop_table("data_sources")
    op.drop_table("user_role")
    op.drop_table("users")
    op.drop_table("roles")
