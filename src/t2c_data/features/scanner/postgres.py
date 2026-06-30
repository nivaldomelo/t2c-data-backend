from __future__ import annotations

import psycopg
from t2c_data.features.scanner.types import ScanPayload, ScannedColumn, ScannedTable


def _map_relkind(relkind: str) -> str:
    if relkind == "r":
        return "table"
    if relkind == "v":
        return "view"
    if relkind == "m":
        return "materialized_view"
    return "unknown"


def _is_technical_schema(schema_name: str) -> bool:
    return (
        schema_name in {"pg_catalog", "information_schema"}
        or schema_name.startswith("pg_toast")
        or schema_name.startswith("pg_temp_")
    )


def _is_allowed_schema(schema_name: str, include_schemas: list[str], exclude_schemas: list[str]) -> bool:
    if _is_technical_schema(schema_name):
        return False
    if include_schemas and schema_name not in include_schemas:
        return False
    if exclude_schemas and schema_name in exclude_schemas:
        return False
    return True


def scan_postgres(
    connection_uri: str,
    include_schemas: list[str] | None = None,
    exclude_schemas: list[str] | None = None,
    connect_timeout_seconds: int = 10,
    statement_timeout_ms: int = 120000,
) -> ScanPayload:
    include_schemas = include_schemas or []
    exclude_schemas = exclude_schemas or []

    with psycopg.connect(connection_uri, connect_timeout=max(int(connect_timeout_seconds or 10), 1)) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = %s", (max(int(statement_timeout_ms or 120000), 1),))
            cur.execute("SELECT current_database()")
            database_name = cur.fetchone()[0]

            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS table_name,
                    c.relkind AS relkind,
                    obj_description(c.oid, 'pg_class') AS table_comment
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'v', 'm')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%'
                  AND n.nspname NOT LIKE 'pg_temp_%'
                ORDER BY n.nspname, c.relname
                """
            )
            table_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    format_type(a.atttypid, a.atttypmod) AS data_type,
                    EXISTS (
                        SELECT 1
                        FROM pg_index i
                        WHERE i.indrelid = c.oid
                          AND i.indisprimary
                          AND a.attnum = ANY(i.indkey)
                    ) AS is_primary_key,
                    NOT a.attnotnull AS is_nullable,
                    a.attnum AS ordinal_position,
                    col_description(c.oid, a.attnum) AS column_comment
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'v', 'm')
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%'
                  AND n.nspname NOT LIKE 'pg_temp_%'
                ORDER BY n.nspname, c.relname, a.attnum
                """
            )
            column_rows = cur.fetchall()

    table_rows = [row for row in table_rows if _is_allowed_schema(row[0], include_schemas, exclude_schemas)]
    column_rows = [row for row in column_rows if _is_allowed_schema(row[0], include_schemas, exclude_schemas)]

    columns_by_table: dict[tuple[str, str], list[ScannedColumn]] = {}
    for row in column_rows:
        key = (row[0], row[1])
        columns_by_table.setdefault(key, []).append(
            ScannedColumn(
                name=row[2],
                data_type=row[3],
                is_primary_key=row[4],
                is_nullable=row[5],
                ordinal_position=row[6],
                comment=row[7],
            )
        )

    tables: list[ScannedTable] = []
    for row in table_rows:
        key = (row[0], row[1])
        tables.append(
            ScannedTable(
                schema_name=row[0],
                table_name=row[1],
                table_type=_map_relkind(row[2]),
                comment=row[3],
                columns=columns_by_table.get(key, []),
            )
        )

    return ScanPayload(database_name=database_name, tables=tables)
