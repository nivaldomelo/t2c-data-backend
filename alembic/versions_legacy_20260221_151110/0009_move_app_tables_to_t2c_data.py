"""move app tables and sequences to t2c_data schema

Revision ID: 0009_move_app_tables_to_t2c_data
Revises: 0008_lineage_nodes_edges
Create Date: 2026-02-21 12:00:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0009_move_app_tables_to_t2c_data"
down_revision = "0008_lineage_nodes_edges"
branch_labels = None
depends_on = None


TABLES = [
    "users",
    "roles",
    "user_role",
    "data_sources",
    "databases",
    "schemas",
    "tables",
    "columns",
    "scan_runs",
    "scan_snapshots",
    "scan_diffs",
    "tags",
    "tag_assignments",
    "glossary_terms",
    "glossary_assignments",
    "lineage_processes",
    "lineage_edges",
    "lineage_nodes",
    "lineage_graph_edges",
    "audit_logs",
]


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS t2c_data")

    for table in TABLES:
        op.execute(
            f"""
            DO $$
            BEGIN
              IF EXISTS (
                SELECT 1
                FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{table}'
              ) THEN
                EXECUTE 'ALTER TABLE public.{table} SET SCHEMA t2c_data';
              END IF;
            END $$;
            """
        )

    # Move owned sequences alongside their tables.
    op.execute(
        """
        DO $$
        DECLARE
          seq record;
        BEGIN
          FOR seq IN
            SELECT n.nspname AS sequence_schema, c.relname AS sequence_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
            JOIN pg_class t ON t.oid = d.refobjid
            JOIN pg_namespace tn ON tn.oid = t.relnamespace
            WHERE c.relkind = 'S'
              AND tn.nspname = 't2c_data'
              AND n.nspname = 'public'
          LOOP
            EXECUTE format(
              'ALTER SEQUENCE %I.%I SET SCHEMA t2c_data',
              seq.sequence_schema,
              seq.sequence_name
            );
          END LOOP;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS public")

    for table in TABLES:
        op.execute(
            f"""
            DO $$
            BEGIN
              IF EXISTS (
                SELECT 1
                FROM pg_tables
                WHERE schemaname = 't2c_data' AND tablename = '{table}'
              ) THEN
                EXECUTE 'ALTER TABLE t2c_data.{table} SET SCHEMA public';
              END IF;
            END $$;
            """
        )

    op.execute(
        """
        DO $$
        DECLARE
          seq record;
        BEGIN
          FOR seq IN
            SELECT n.nspname AS sequence_schema, c.relname AS sequence_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
            JOIN pg_class t ON t.oid = d.refobjid
            JOIN pg_namespace tn ON tn.oid = t.relnamespace
            WHERE c.relkind = 'S'
              AND tn.nspname = 'public'
              AND n.nspname = 't2c_data'
          LOOP
            EXECUTE format(
              'ALTER SEQUENCE %I.%I SET SCHEMA public',
              seq.sequence_schema,
              seq.sequence_name
            );
          END LOOP;
        END $$;
        """
    )
