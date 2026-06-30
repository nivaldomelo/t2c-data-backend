"""add metabase integration models

Revision ID: b5c6d7e8f9a1
Revises: 7e6d5c4b3a29
Create Date: 2026-04-14 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "b5c6d7e8f9a1"
down_revision = "7e6d5c4b3a29"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(
        sa.text(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.metabase_instances (
                id SERIAL NOT NULL,
                name VARCHAR(120) NOT NULL,
                base_url VARCHAR(500) NOT NULL,
                auth_type VARCHAR(30),
                auth_username VARCHAR(255),
                auth_secret TEXT NOT NULL DEFAULT '',
                timeout_seconds INTEGER NOT NULL DEFAULT 10,
                sync_dashboards BOOLEAN NOT NULL DEFAULT true,
                sync_questions BOOLEAN NOT NULL DEFAULT true,
                sync_collections BOOLEAN NOT NULL DEFAULT true,
                enabled BOOLEAN NOT NULL DEFAULT true,
                last_sync_at TIMESTAMP WITH TIME ZONE,
                last_sync_status VARCHAR(40),
                last_sync_message TEXT,
                last_sync_dashboards INTEGER NOT NULL DEFAULT 0,
                last_sync_questions INTEGER NOT NULL DEFAULT 0,
                last_sync_collections INTEGER NOT NULL DEFAULT 0,
                last_sync_links INTEGER NOT NULL DEFAULT 0,
                last_sync_unresolved INTEGER NOT NULL DEFAULT 0,
                last_sync_warnings INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                PRIMARY KEY (id),
                CONSTRAINT uq_metabase_instances_name UNIQUE (name)
            )
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.metabase_objects (
                id SERIAL NOT NULL,
                instance_id INTEGER NOT NULL REFERENCES {schema}.metabase_instances (id) ON DELETE CASCADE,
                external_id VARCHAR(80) NOT NULL,
                object_type VARCHAR(30) NOT NULL,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                collection_external_id VARCHAR(80),
                collection_name VARCHAR(255),
                url VARCHAR(1000),
                database_id INTEGER,
                archived BOOLEAN NOT NULL DEFAULT false,
                last_seen_at TIMESTAMP WITH TIME ZONE,
                remote_updated_at TIMESTAMP WITH TIME ZONE,
                metadata_json JSONB,
                dataset_query_json JSONB,
                raw_json JSONB,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                PRIMARY KEY (id),
                CONSTRAINT uq_metabase_objects_instance_type_external UNIQUE (instance_id, object_type, external_id)
            )
            """
        )
    )
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_objects_instance_id ON {schema}.metabase_objects (instance_id)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_objects_object_type ON {schema}.metabase_objects (object_type)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_objects_collection_external_id ON {schema}.metabase_objects (collection_external_id)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_objects_database_id ON {schema}.metabase_objects (database_id)"))
    op.execute(
        sa.text(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.metabase_object_links (
                id SERIAL NOT NULL,
                instance_id INTEGER NOT NULL REFERENCES {schema}.metabase_instances (id) ON DELETE CASCADE,
                metabase_object_id INTEGER NOT NULL REFERENCES {schema}.metabase_objects (id) ON DELETE CASCADE,
                table_id INTEGER NOT NULL REFERENCES {schema}.tables (id) ON DELETE CASCADE,
                column_id INTEGER REFERENCES {schema}.columns (id) ON DELETE SET NULL,
                match_method VARCHAR(40) NOT NULL,
                confidence_level VARCHAR(20) NOT NULL DEFAULT 'partial',
                confidence_reason TEXT,
                source_table_name VARCHAR(255),
                source_schema_name VARCHAR(255),
                source_database_name VARCHAR(255),
                source_column_name VARCHAR(255),
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                PRIMARY KEY (id)
            )
            """
        )
    )
    op.execute(
        sa.text(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uq_metabase_object_links_object_table_column_method "
            f"ON {schema}.metabase_object_links (metabase_object_id, table_id, COALESCE(column_id, -1), match_method)"
        )
    )
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_object_links_instance_id ON {schema}.metabase_object_links (instance_id)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_object_links_metabase_object_id ON {schema}.metabase_object_links (metabase_object_id)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_object_links_table_id ON {schema}.metabase_object_links (table_id)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_object_links_column_id ON {schema}.metabase_object_links (column_id)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_object_links_is_active ON {schema}.metabase_object_links (is_active)"))
    op.execute(
        sa.text(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.metabase_sync_runs (
                id SERIAL NOT NULL,
                instance_id INTEGER NOT NULL REFERENCES {schema}.metabase_instances (id) ON DELETE CASCADE,
                status VARCHAR(20) NOT NULL DEFAULT 'running',
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                finished_at TIMESTAMP WITH TIME ZONE,
                dashboards_count INTEGER NOT NULL DEFAULT 0,
                questions_count INTEGER NOT NULL DEFAULT 0,
                collections_count INTEGER NOT NULL DEFAULT 0,
                links_count INTEGER NOT NULL DEFAULT 0,
                unresolved_count INTEGER NOT NULL DEFAULT 0,
                warnings_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                summary_json JSONB,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                PRIMARY KEY (id)
            )
            """
        )
    )
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_sync_runs_instance_id ON {schema}.metabase_sync_runs (instance_id)"))
    op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_metabase_sync_runs_status ON {schema}.metabase_sync_runs (status)"))


def downgrade() -> None:
    schema = settings.db_schema
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_sync_runs_status"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_sync_runs_instance_id"))
    op.execute(sa.text(f"DROP TABLE IF EXISTS {schema}.metabase_sync_runs"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_object_links_is_active"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_object_links_column_id"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_object_links_table_id"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_object_links_metabase_object_id"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_object_links_instance_id"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.uq_metabase_object_links_object_table_column_method"))
    op.execute(sa.text(f"DROP TABLE IF EXISTS {schema}.metabase_object_links"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_objects_database_id"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_objects_collection_external_id"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_objects_object_type"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {schema}.ix_metabase_objects_instance_id"))
    op.execute(sa.text(f"DROP TABLE IF EXISTS {schema}.metabase_objects"))
    op.execute(sa.text(f"DROP TABLE IF EXISTS {schema}.metabase_instances"))
