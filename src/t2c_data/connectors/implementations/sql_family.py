from __future__ import annotations

from typing import Any

import psycopg

from t2c_data.connectors.base import BaseConnector, ConnectorCapabilities, MissingDriverError
from t2c_data.connectors.implementations.helpers import optional_int, require_secret, require_value


def _is_technical_schema(schema_name: str) -> bool:
    return (
        schema_name in {"pg_catalog", "information_schema"}
        or schema_name.startswith("pg_toast")
        or schema_name.startswith("pg_temp_")
    )


class PostgresConnector(BaseConnector):
    engine = "postgres"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return {
            "host": require_value(connection, "host"),
            "port": optional_int(connection, "port", 5432),
            "database": require_value(connection, "database"),
            "username": require_value(connection, "username"),
        }

    def _connect(self):
        return psycopg.connect(
            host=require_value(self.connection, "host"),
            port=optional_int(self.connection, "port", 5432),
            dbname=require_value(self.connection, "database"),
            user=require_value(self.connection, "username"),
            password=require_secret(self.secrets, "password"),
            connect_timeout=5,
            sslmode=(self.connection.get("ssl_mode") or "prefer"),
        )

    def test_connection(self) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
        return {"server_version": version}

    def get_database_info(self) -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user")
                database, user = cur.fetchone()
        return {"database": database, "user": user}

    def list_schemas(self) -> list[str]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT nspname
                    FROM pg_namespace
                    WHERE nspname NOT IN ('pg_catalog', 'information_schema')
                      AND nspname NOT LIKE 'pg_toast%%'
                      AND nspname NOT LIKE 'pg_temp_%%'
                    ORDER BY nspname
                    """
                )
                schemas = [row[0] for row in cur.fetchall()]
                if schemas:
                    return schemas

                default_schema = str(self.connection.get("default_schema") or "public").strip()
                if not default_schema or _is_technical_schema(default_schema):
                    return []

                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.schemata
                        WHERE schema_name = %s
                    )
                    """,
                    (default_schema,),
                )
                exists = bool(cur.fetchone()[0])
                return [default_schema] if exists else []

    def list_tables(self, schema: str | None = None) -> list[str]:
        target_schema = schema or self.connection.get("default_schema") or "public"
        if _is_technical_schema(str(target_schema)):
            return []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                    """,
                    (target_schema,),
                )
                return [row[0] for row in cur.fetchall()]


class MySQLConnector(BaseConnector):
    engine = "mysql"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return {
            "host": require_value(connection, "host"),
            "port": optional_int(connection, "port", 3306),
            "database": require_value(connection, "database"),
            "username": require_value(connection, "username"),
        }

    def _connect(self):
        try:
            import pymysql
        except ImportError as exc:
            raise MissingDriverError(self.engine, "pymysql") from exc
        return pymysql.connect(
            host=require_value(self.connection, "host"),
            port=optional_int(self.connection, "port", 3306),
            user=require_value(self.connection, "username"),
            password=require_secret(self.secrets, "password"),
            database=require_value(self.connection, "database"),
            connect_timeout=5,
            ssl_disabled=(self.connection.get("ssl_mode") == "disable"),
        )

    def test_connection(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                version = cur.fetchone()[0]
            return {"server_version": version}
        finally:
            conn.close()

    def get_database_info(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT DATABASE(), CURRENT_USER()")
                database, user = cur.fetchone()
            return {"database": database, "user": user}
        finally:
            conn.close()

    def list_schemas(self) -> list[str]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW DATABASES")
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def list_tables(self, schema: str | None = None) -> list[str]:
        conn = self._connect()
        try:
            database_name = schema or require_value(self.connection, "database")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                    """,
                    (database_name,),
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()


class MariaDbConnector(MySQLConnector):
    engine = "mariadb"


class RedshiftConnector(PostgresConnector):
    engine = "redshift"

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return {
            "host": require_value(connection, "host"),
            "port": optional_int(connection, "port", 5439),
            "database": require_value(connection, "database"),
            "username": require_value(connection, "username"),
        }

    def _connect(self):
        try:
            import redshift_connector
        except ImportError as exc:
            raise MissingDriverError(self.engine, "redshift-connector") from exc
        return redshift_connector.connect(
            host=require_value(self.connection, "host"),
            port=optional_int(self.connection, "port", 5439),
            database=require_value(self.connection, "database"),
            user=require_value(self.connection, "username"),
            password=require_secret(self.secrets, "password"),
            timeout=5,
        )


class SqliteConnector(BaseConnector):
    engine = "sqlite"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        file_path = require_value(connection, "file_path")
        return {
            "host": file_path,
            "port": 0,
            "database": file_path,
            "username": "local",
        }

    def _connect(self):
        import sqlite3

        return sqlite3.connect(require_value(self.connection, "file_path"), timeout=5)

    def test_connection(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT sqlite_version()")
            return {"server_version": cur.fetchone()[0]}
        finally:
            conn.close()

    def get_database_info(self) -> dict[str, Any]:
        return {"file_path": require_value(self.connection, "file_path")}

    def list_schemas(self) -> list[str]:
        return ["main"]

    def list_tables(self, schema: str | None = None) -> list[str]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
