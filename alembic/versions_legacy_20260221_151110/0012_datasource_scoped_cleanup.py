"""add datasource scoping to polymorphic assignment and lineage tables

Revision ID: 0012_datasource_scoped_cleanup
Revises: 0011_datasource_fk_cascades
Create Date: 2026-02-22 00:15:00.000000
"""

from alembic import op


revision = "0012_datasource_scoped_cleanup"
down_revision = "0011_datasource_fk_cascades"
branch_labels = None
depends_on = None


def _add_column_if_missing(schema_name: str, table_name: str, column_name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
          ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = '{table_name}'
              AND column_name = '{column_name}'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.{table_name} ADD COLUMN {column_name} INTEGER';
          END IF;
        END $$;
        """
    )


def _backfill_assignments(schema_name: str, table_name: str) -> None:
    op.execute(
        f"""
        UPDATE {schema_name}.{table_name} a
        SET datasource_id = d.id
        FROM {schema_name}.databases d
        WHERE a.datasource_id IS NULL
          AND a.entity_type = 'database'
          AND a.entity_id = d.id;

        UPDATE {schema_name}.{table_name} a
        SET datasource_id = d.id
        FROM {schema_name}.schemas s
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE a.datasource_id IS NULL
          AND a.entity_type = 'schema'
          AND a.entity_id = s.id;

        UPDATE {schema_name}.{table_name} a
        SET datasource_id = d.id
        FROM {schema_name}.tables t
        JOIN {schema_name}.schemas s ON s.id = t.schema_id
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE a.datasource_id IS NULL
          AND a.entity_type = 'table'
          AND a.entity_id = t.id;

        UPDATE {schema_name}.{table_name} a
        SET datasource_id = d.id
        FROM {schema_name}.columns c
        JOIN {schema_name}.tables t ON t.id = c.table_id
        JOIN {schema_name}.schemas s ON s.id = t.schema_id
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE a.datasource_id IS NULL
          AND a.entity_type = 'column'
          AND a.entity_id = c.id;
        """
    )


def _backfill_lineage_edges(schema_name: str) -> None:
    op.execute(
        f"""
        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.tables t
        JOIN {schema_name}.schemas s ON s.id = t.schema_id
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE e.datasource_id IS NULL
          AND e.from_entity_type = 'table'
          AND e.from_entity_id = t.id;

        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.tables t
        JOIN {schema_name}.schemas s ON s.id = t.schema_id
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE e.datasource_id IS NULL
          AND e.to_entity_type = 'table'
          AND e.to_entity_id = t.id;

        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.schemas s
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE e.datasource_id IS NULL
          AND e.from_entity_type = 'schema'
          AND e.from_entity_id = s.id;

        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.schemas s
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE e.datasource_id IS NULL
          AND e.to_entity_type = 'schema'
          AND e.to_entity_id = s.id;

        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.databases d
        WHERE e.datasource_id IS NULL
          AND e.from_entity_type = 'database'
          AND e.from_entity_id = d.id;

        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.databases d
        WHERE e.datasource_id IS NULL
          AND e.to_entity_type = 'database'
          AND e.to_entity_id = d.id;

        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.columns c
        JOIN {schema_name}.tables t ON t.id = c.table_id
        JOIN {schema_name}.schemas s ON s.id = t.schema_id
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE e.datasource_id IS NULL
          AND e.from_entity_type = 'column'
          AND e.from_entity_id = c.id;

        UPDATE {schema_name}.lineage_edges e
        SET datasource_id = d.id
        FROM {schema_name}.columns c
        JOIN {schema_name}.tables t ON t.id = c.table_id
        JOIN {schema_name}.schemas s ON s.id = t.schema_id
        JOIN {schema_name}.databases d ON d.id = s.database_id
        WHERE e.datasource_id IS NULL
          AND e.to_entity_type = 'column'
          AND e.to_entity_id = c.id;
        """
    )


def _add_fk_if_missing(schema_name: str, table_name: str, constraint_name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = '{table_name}'
              AND column_name = 'datasource_id'
          ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.table_constraints
            WHERE table_schema = '{schema_name}'
              AND table_name = '{table_name}'
              AND constraint_name = '{constraint_name}'
          ) THEN
            EXECUTE '
              ALTER TABLE {schema_name}.{table_name}
              ADD CONSTRAINT {constraint_name}
              FOREIGN KEY (datasource_id)
              REFERENCES {schema_name}.data_sources(id)
              ON DELETE CASCADE
            ';
          END IF;
        END $$;
        """
    )


def _drop_fk_if_exists(schema_name: str, table_name: str, constraint_name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.{table_name} DROP CONSTRAINT IF EXISTS {constraint_name}';
          END IF;
        END $$;
        """
    )


def _drop_column_if_exists(schema_name: str, table_name: str, column_name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = '{table_name}'
              AND column_name = '{column_name}'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.{table_name} DROP COLUMN {column_name}';
          END IF;
        END $$;
        """
    )


def upgrade() -> None:
    for schema_name in ("t2c_data", "public"):
        _add_column_if_missing(schema_name, "tag_assignments", "datasource_id")
        _add_column_if_missing(schema_name, "glossary_assignments", "datasource_id")
        _add_column_if_missing(schema_name, "lineage_edges", "datasource_id")

        _backfill_assignments(schema_name, "tag_assignments")
        _backfill_assignments(schema_name, "glossary_assignments")
        _backfill_lineage_edges(schema_name)

        _add_fk_if_missing(schema_name, "tag_assignments", "tag_assignments_datasource_id_fkey")
        _add_fk_if_missing(schema_name, "glossary_assignments", "glossary_assignments_datasource_id_fkey")
        _add_fk_if_missing(schema_name, "lineage_edges", "lineage_edges_datasource_id_fkey")

    op.execute("CREATE INDEX IF NOT EXISTS ix_tag_assignments_datasource_id ON t2c_data.tag_assignments(datasource_id)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_glossary_assignments_datasource_id ON t2c_data.glossary_assignments(datasource_id)"
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_lineage_edges_datasource_id ON t2c_data.lineage_edges(datasource_id)")


def downgrade() -> None:
    for schema_name in ("t2c_data", "public"):
        _drop_fk_if_exists(schema_name, "lineage_edges", "lineage_edges_datasource_id_fkey")
        _drop_fk_if_exists(schema_name, "glossary_assignments", "glossary_assignments_datasource_id_fkey")
        _drop_fk_if_exists(schema_name, "tag_assignments", "tag_assignments_datasource_id_fkey")

        _drop_column_if_exists(schema_name, "lineage_edges", "datasource_id")
        _drop_column_if_exists(schema_name, "glossary_assignments", "datasource_id")
        _drop_column_if_exists(schema_name, "tag_assignments", "datasource_id")

    op.execute("DROP INDEX IF EXISTS t2c_data.ix_lineage_edges_datasource_id")
    op.execute("DROP INDEX IF EXISTS t2c_data.ix_glossary_assignments_datasource_id")
    op.execute("DROP INDEX IF EXISTS t2c_data.ix_tag_assignments_datasource_id")
