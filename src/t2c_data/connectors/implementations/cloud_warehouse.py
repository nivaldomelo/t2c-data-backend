from __future__ import annotations

import json
from typing import Any

from t2c_data.connectors.base import BaseConnector, ConnectorCapabilities, ConnectorError, MissingDriverError
from t2c_data.connectors.implementations.helpers import require_secret, require_value
from t2c_data.core.sql_utils import safe_identifier


def _safe_warehouse_identifier(value: Any, *, label: str = "identifier") -> str:
    """Validate a (possibly dotted) warehouse identifier before interpolating it into a
    SHOW statement. Each dotted part must match the strict identifier grammar, so no
    quotes/spaces/semicolons can break out of the statement (prevents SQL injection)."""
    raw = str(value or "").strip()
    parts = raw.split(".")
    if not raw or any(not part for part in parts):
        raise ConnectorError(f"Identificador inválido para {label}.", code="invalid_config")
    try:
        return ".".join(safe_identifier(part, label=label) for part in parts)
    except ValueError as exc:
        raise ConnectorError(f"Identificador inválido para {label}.", code="invalid_config") from exc


class SnowflakeConnector(BaseConnector):
    engine = "snowflake"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        account = require_value(connection, "account")
        return {
            "host": f"{account}.snowflakecomputing.com",
            "port": 443,
            "database": require_value(connection, "database"),
            "username": require_value(connection, "user"),
        }

    def _connect(self):
        try:
            import snowflake.connector
        except ImportError as exc:
            raise MissingDriverError(self.engine, "snowflake-connector-python") from exc
        return snowflake.connector.connect(
            account=require_value(self.connection, "account"),
            user=require_value(self.connection, "user"),
            password=require_secret(self.secrets, "password"),
            warehouse=require_value(self.connection, "warehouse"),
            database=require_value(self.connection, "database"),
            schema=self.connection.get("schema"),
            role=self.connection.get("role"),
            login_timeout=5,
        )

    def test_connection(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_VERSION()")
            return {"server_version": cur.fetchone()[0]}
        finally:
            conn.close()

    def get_database_info(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_WAREHOUSE()")
            account, region, warehouse = cur.fetchone()
            return {"account": account, "region": region, "warehouse": warehouse}
        finally:
            conn.close()

    def list_schemas(self) -> list[str]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SHOW SCHEMAS")
            return [row[1] for row in cur.fetchall()]
        finally:
            conn.close()

    def list_tables(self, schema: str | None = None) -> list[str]:
        conn = self._connect()
        try:
            target_schema = schema or self.connection.get("schema")
            if not target_schema:
                raise ConnectorError("Informe o schema do Snowflake para listar tabelas.", code="invalid_config")
            safe_schema = _safe_warehouse_identifier(target_schema, label="schema")
            cur = conn.cursor()
            cur.execute(f"SHOW TABLES IN SCHEMA {safe_schema}")
            return [row[1] for row in cur.fetchall()]
        finally:
            conn.close()


class BigQueryConnector(BaseConnector):
    engine = "bigquery"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return {
            "host": "bigquery.googleapis.com",
            "port": 443,
            "database": require_value(connection, "project_id"),
            "username": "adc" if str(connection.get("use_adc") or "").lower() in {"1", "true", "yes"} else "service-account",
        }

    def _client(self):
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except ImportError as exc:
            raise MissingDriverError(self.engine, "google-cloud-bigquery") from exc
        project_id = require_value(self.connection, "project_id")
        use_adc = str(self.connection.get("use_adc") or "").lower() in {"1", "true", "yes"}
        if use_adc:
            return bigquery.Client(project=project_id)
        credentials_json = require_secret(self.secrets, "service_account_json")
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        return bigquery.Client(project=project_id, credentials=credentials)

    def test_connection(self) -> dict[str, Any]:
        client = self._client()
        list(client.list_datasets(max_results=1))
        return {"project_id": require_value(self.connection, "project_id")}

    def get_database_info(self) -> dict[str, Any]:
        client = self._client()
        return {"project_id": client.project}

    def list_schemas(self) -> list[str]:
        client = self._client()
        return sorted(dataset.dataset_id for dataset in client.list_datasets())

    def list_tables(self, schema: str | None = None) -> list[str]:
        client = self._client()
        dataset_id = schema or self.connection.get("dataset")
        if not dataset_id:
            raise ConnectorError("Informe o dataset do BigQuery para listar tabelas.", code="invalid_config")
        safe_dataset = _safe_warehouse_identifier(dataset_id, label="dataset")
        return sorted(table.table_id for table in client.list_tables(f"{client.project}.{safe_dataset}"))


class DatabricksConnector(BaseConnector):
    engine = "databricks"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return {
            "host": require_value(connection, "server_hostname"),
            "port": 443,
            "database": str(connection.get("catalog") or "databricks"),
            "username": "token",
        }

    def _connect(self):
        try:
            from databricks import sql
        except ImportError as exc:
            raise MissingDriverError(self.engine, "databricks-sql-connector") from exc
        return sql.connect(
            server_hostname=require_value(self.connection, "server_hostname"),
            http_path=require_value(self.connection, "http_path"),
            access_token=require_secret(self.secrets, "access_token"),
        )

    def test_connection(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_catalog()")
                catalog = cur.fetchone()[0]
                return {"catalog": catalog}
        finally:
            conn.close()

    def get_database_info(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_catalog(), current_schema()")
                catalog, schema = cur.fetchone()
                return {"catalog": catalog, "schema": schema}
        finally:
            conn.close()

    def list_schemas(self) -> list[str]:
        conn = self._connect()
        try:
            catalog = self.connection.get("catalog")
            if catalog:
                query = f"SHOW SCHEMAS IN {_safe_warehouse_identifier(catalog, label='catalog')}"
            else:
                query = "SHOW SCHEMAS"
            with conn.cursor() as cur:
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def list_tables(self, schema: str | None = None) -> list[str]:
        conn = self._connect()
        try:
            target_schema = schema or self.connection.get("schema")
            if not target_schema:
                raise ConnectorError("Informe o schema do Databricks para listar tabelas.", code="invalid_config")
            catalog = self.connection.get("catalog")
            safe_schema = _safe_warehouse_identifier(target_schema, label="schema")
            if catalog:
                identifier = f"{_safe_warehouse_identifier(catalog, label='catalog')}.{safe_schema}"
            else:
                identifier = safe_schema
            with conn.cursor() as cur:
                cur.execute(f"SHOW TABLES IN {identifier}")
                rows = cur.fetchall()
                return [row[1] if len(row) > 1 else row[0] for row in rows]
        finally:
            conn.close()
