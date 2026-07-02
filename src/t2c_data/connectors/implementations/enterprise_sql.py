from __future__ import annotations

from typing import Any

from t2c_data.connectors.base import BaseConnector, ConnectorCapabilities, ConnectorError, MissingDriverError
from t2c_data.connectors.implementations.helpers import optional_int, require_secret, require_value


class SqlServerConnector(BaseConnector):
    engine = "sqlserver"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return {
            "host": require_value(connection, "host"),
            "port": optional_int(connection, "port", 1433),
            "database": require_value(connection, "database"),
            "username": require_value(connection, "username"),
        }

    def _connect(self):
        try:
            import pyodbc
        except ImportError as exc:
            raise MissingDriverError(self.engine, "pyodbc") from exc
        driver = self.connection.get("driver") or "ODBC Driver 18 for SQL Server"
        host = require_value(self.connection, "host")
        port = optional_int(self.connection, "port", 1433)
        database = require_value(self.connection, "database")
        username = require_value(self.connection, "username")
        password = require_secret(self.secrets, "password")
        encrypt = str(self.connection.get("encrypt") or "yes")
        trust_cert = str(self.connection.get("trust_server_certificate") or "yes")
        # Anti connection-string injection: campos estruturais não podem conter ; { } (delimitadores ODBC).
        for label, value in (
            ("driver", driver), ("host", host), ("database", database),
            ("encrypt", encrypt), ("trust_server_certificate", trust_cert),
        ):
            if any(ch in str(value) for ch in ";{}"):
                raise ConnectorError(f"Valor inválido em '{label}': ';', '{{' e '}}' não são permitidos.", code="invalid_config")

        def _brace(value: str) -> str:
            # Valores livres (user/senha) entre chaves, com '}' escapado como '}}' (sintaxe ODBC).
            return "{" + str(value).replace("}", "}}") + "}"

        conn_str = (
            f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={_brace(username)};PWD={_brace(password)};"
            f"Encrypt={encrypt};TrustServerCertificate={trust_cert};Connection Timeout=5;"
        )
        return pyodbc.connect(conn_str, timeout=5)

    def test_connection(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT @@VERSION")
            version = cur.fetchone()[0]
            return {"server_version": version}
        finally:
            conn.close()

    def get_database_info(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT DB_NAME(), SUSER_SNAME()")
            database, user = cur.fetchone()
            return {"database": database, "user": user}
        finally:
            conn.close()

    def list_schemas(self) -> list[str]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sys.schemas ORDER BY name")
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def list_tables(self, schema: str | None = None) -> list[str]:
        target_schema = schema or self.connection.get("default_schema") or "dbo"
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ?
                ORDER BY table_name
                """,
                (target_schema,),
            )
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()


class OracleConnector(BaseConnector):
    engine = "oracle"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        service_name = connection.get("service_name") or connection.get("database") or "oracle"
        return {
            "host": require_value(connection, "host"),
            "port": optional_int(connection, "port", 1521),
            "database": str(service_name),
            "username": require_value(connection, "username"),
        }

    def _connect(self):
        try:
            import oracledb
        except ImportError as exc:
            raise MissingDriverError(self.engine, "oracledb") from exc
        dsn = oracledb.makedsn(
            require_value(self.connection, "host"),
            optional_int(self.connection, "port", 1521),
            service_name=require_value(self.connection, "service_name", "service_name"),
        )
        return oracledb.connect(
            user=require_value(self.connection, "username"),
            password=require_secret(self.secrets, "password"),
            dsn=dsn,
        )

    def test_connection(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT banner FROM v$version")
            row = cur.fetchone()
            return {"server_version": row[0] if row else "Oracle"}
        finally:
            conn.close()

    def get_database_info(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT SYS_CONTEXT('USERENV', 'DB_NAME'), USER FROM dual")
            database, user = cur.fetchone()
            return {"database": database, "user": user}
        finally:
            conn.close()

    def list_schemas(self) -> list[str]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT username FROM all_users ORDER BY username")
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def list_tables(self, schema: str | None = None) -> list[str]:
        target_schema = (schema or self.connection.get("default_schema") or require_value(self.connection, "username")).upper()
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT table_name FROM all_tables WHERE owner = :owner ORDER BY table_name",
                owner=target_schema,
            )
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
