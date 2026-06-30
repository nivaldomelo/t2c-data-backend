from __future__ import annotations

import os
import unittest
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.connectors.implementations.sql_family import PostgresConnector
from t2c_data.features.scanner.postgres import scan_postgres


class _FakeCursor:
    def __init__(self, schema_rows=None, table_rows=None, column_rows=None, database_name="analytics") -> None:
        self.schema_rows = schema_rows or []
        self.table_rows = table_rows or []
        self.column_rows = column_rows or []
        self.database_name = database_name
        self._last_query = ""

    def execute(self, query, params=None):  # noqa: ANN001
        self._last_query = str(query)
        return None

    def fetchone(self):
        if "current_database()" in self._last_query:
            return (self.database_name,)
        if "SELECT version()" in self._last_query:
            return ("PostgreSQL 16",)
        return None

    def fetchall(self):
        if "FROM pg_class" in self._last_query:
            return self.table_rows
        if "FROM pg_attribute" in self._last_query:
            return self.column_rows
        if "FROM pg_namespace" in self._last_query:
            return [
                row
                for row in self.schema_rows
                if row[0] not in {"pg_catalog", "information_schema"}
                and not str(row[0]).startswith("pg_toast")
                and not str(row[0]).startswith("pg_temp_")
            ]
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


class PostgresSchemaSelectionTests(unittest.TestCase):
    def test_connector_list_schemas_ignores_system_schemas(self) -> None:
        cursor = _FakeCursor(schema_rows=[("bronze",), ("pg_catalog",), ("information_schema",), ("pg_temp_7",), ("silver",)])
        connection = _FakeConnection(cursor)

        with patch("t2c_data.connectors.implementations.sql_family.psycopg.connect", return_value=connection):
            schemas = PostgresConnector(
                connection={"host": "db.local", "database": "analytics", "username": "catalog"},
                secrets={"password": "secret"},
            ).list_schemas()

        self.assertEqual(schemas, ["bronze", "silver"])

    def test_scan_postgres_respects_include_exclude_and_system_schemas(self) -> None:
        table_rows = [
            ("bronze", "orders", "r", "Pedidos bronze"),
            ("silver", "orders_clean", "r", "Pedidos silver"),
            ("pg_catalog", "pg_tables", "r", "system"),
        ]
        column_rows = [
            ("bronze", "orders", "id", "integer", True, False, 1, "PK"),
            ("silver", "orders_clean", "id", "integer", True, False, 1, "PK"),
            ("pg_catalog", "pg_tables", "id", "integer", False, False, 1, "system"),
        ]
        cursor = _FakeCursor(table_rows=table_rows, column_rows=column_rows)
        connection = _FakeConnection(cursor)

        with patch("t2c_data.features.scanner.postgres.psycopg.connect", return_value=connection):
            payload = scan_postgres(
                connection_uri="postgresql://db.local/analytics",
                include_schemas=["bronze", "silver"],
                exclude_schemas=["silver"],
            )

        self.assertEqual(payload.database_name, "analytics")
        self.assertEqual([(table.schema_name, table.table_name) for table in payload.tables], [("bronze", "orders")])
        self.assertEqual(payload.tables[0].columns[0].name, "id")


if __name__ == "__main__":
    unittest.main()
