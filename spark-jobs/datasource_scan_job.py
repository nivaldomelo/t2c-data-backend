from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Any

from dq_common import build_spark, load_connection_config, write_json_output


def _parse_json_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


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


def _read_query(spark, *, jdbc_url: str, jdbc_user: str, jdbc_password: str, query: str):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("query", query)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def _map_relkind(relkind: str) -> str:
    if relkind == "r":
        return "table"
    if relkind == "v":
        return "view"
    if relkind == "m":
        return "materialized_view"
    return "unknown"


def _row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row)


def _emit_stage(stage: str, *, scan_run_id: str | None = None) -> None:
    suffix = f" scan_run_id={scan_run_id}" if scan_run_id else ""
    print(f"[datasource-scan] stage={stage}{suffix}", flush=True)


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    connection = load_connection_config(args)
    include_schemas = _parse_json_list(getattr(args, "include_schemas_json", None))
    exclude_schemas = _parse_json_list(getattr(args, "exclude_schemas_json", None))
    scan_run_id = str(getattr(args, "scan_run_id", "") or "").strip() or None

    spark = build_spark("datasource-scan")
    try:
        _emit_stage("startup", scan_run_id=scan_run_id)
        database_name = _read_query(
            spark,
            jdbc_url=connection["jdbc_url"],
            jdbc_user=connection["jdbc_user"],
            jdbc_password=connection["jdbc_password"],
            query="SELECT current_database() AS database_name",
        ).collect()[0]["database_name"]

        _emit_stage("connection_test", scan_run_id=scan_run_id)
        _emit_stage("schema_discovery", scan_run_id=scan_run_id)
        table_rows = [
            _row_to_dict(row)
            for row in _read_query(
                spark,
                jdbc_url=connection["jdbc_url"],
                jdbc_user=connection["jdbc_user"],
                jdbc_password=connection["jdbc_password"],
                query="""
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
                """,
            ).collect()
        ]
        _emit_stage("table_discovery", scan_run_id=scan_run_id)
        _emit_stage("column_discovery", scan_run_id=scan_run_id)
        column_rows = [
            _row_to_dict(row)
            for row in _read_query(
                spark,
                jdbc_url=connection["jdbc_url"],
                jdbc_user=connection["jdbc_user"],
                jdbc_password=connection["jdbc_password"],
                query="""
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
                """,
            ).collect()
        ]
    finally:
        spark.stop()

    table_rows = [row for row in table_rows if _is_allowed_schema(str(row.get("schema_name") or ""), include_schemas, exclude_schemas)]
    column_rows = [row for row in column_rows if _is_allowed_schema(str(row.get("schema_name") or ""), include_schemas, exclude_schemas)]

    columns_by_table: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in column_rows:
        key = (str(row.get("schema_name") or ""), str(row.get("table_name") or ""))
        columns_by_table[key].append(
            {
                "name": str(row.get("column_name") or ""),
                "data_type": str(row.get("data_type") or "text"),
                "is_primary_key": bool(row.get("is_primary_key")),
                "is_nullable": bool(row.get("is_nullable")),
                "ordinal_position": int(row.get("ordinal_position") or 0),
                "comment": row.get("column_comment"),
            }
        )

    tables: list[dict[str, Any]] = []
    for row in table_rows:
        schema_name = str(row.get("schema_name") or "")
        table_name = str(row.get("table_name") or "")
        tables.append(
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "table_type": _map_relkind(str(row.get("relkind") or "")),
                "comment": row.get("table_comment"),
                "columns": columns_by_table.get((schema_name, table_name), []),
            }
        )

    _emit_stage("completed", scan_run_id=scan_run_id)
    return {
        "database_name": str(database_name or "").strip(),
        "tables": tables,
        "execution_engine": "spark",
        "include_schemas": include_schemas,
        "exclude_schemas": exclude_schemas,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasource-id", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--scan-run-id", required=False)
    parser.add_argument("--jdbc-url", required=False)
    parser.add_argument("--jdbc-user", required=False)
    parser.add_argument("--jdbc-password", required=False)
    parser.add_argument("--include-schemas-json", required=False)
    parser.add_argument("--exclude-schemas-json", required=False)
    args = parser.parse_args()
    payload = build_payload(args)
    write_json_output(args.output_json, payload)


if __name__ == "__main__":
    main()
