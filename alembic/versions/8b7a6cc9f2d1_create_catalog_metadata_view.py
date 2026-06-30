"""create catalog metadata view

Revision ID: 8b7a6cc9f2d1
Revises: 5e0cb6f8c1a2
Create Date: 2026-02-22 10:00:00.000000
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "8b7a6cc9f2d1"
down_revision = "5e0cb6f8c1a2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE VIEW t2c_data.vw_catalog_metadata AS
        WITH table_comments AS (
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                obj_description(c.oid, 'pg_class') AS table_description
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
        ),
        column_comments AS (
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                a.attname AS column_name,
                col_description(a.attrelid, a.attnum) AS column_description
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE a.attnum > 0
              AND NOT a.attisdropped
              AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
        ),
        pk_columns AS (
            SELECT
                kcu.table_schema,
                kcu.table_name,
                kcu.column_name,
                TRUE AS is_primary_key
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        ),
        fk_columns AS (
            SELECT
                kcu.table_schema,
                kcu.table_name,
                kcu.column_name,
                concat(ccu.table_schema, '.', ccu.table_name, '.', ccu.column_name) AS foreign_key_ref
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.constraint_schema = tc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
        )
        SELECT
            c.table_schema,
            c.table_name,
            concat(c.table_schema, '.', c.table_name) AS table_fqn,
            tc.table_description,
            c.column_name,
            c.data_type,
            c.is_nullable,
            cc.column_description,
            COALESCE(pk.is_primary_key, FALSE) AS is_primary_key,
            fk.foreign_key_ref
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        LEFT JOIN table_comments tc
          ON tc.table_schema = c.table_schema
         AND tc.table_name = c.table_name
        LEFT JOIN column_comments cc
          ON cc.table_schema = c.table_schema
         AND cc.table_name = c.table_name
         AND cc.column_name = c.column_name
        LEFT JOIN pk_columns pk
          ON pk.table_schema = c.table_schema
         AND pk.table_name = c.table_name
         AND pk.column_name = c.column_name
        LEFT JOIN fk_columns fk
          ON fk.table_schema = c.table_schema
         AND fk.table_name = c.table_name
         AND fk.column_name = c.column_name
        WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
          AND c.table_schema NOT LIKE 'pg_toast%'
          AND t.table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN TABLE')
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_catalog_metadata")
