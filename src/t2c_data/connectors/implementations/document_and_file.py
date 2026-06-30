from __future__ import annotations

from typing import Any

from t2c_data.connectors.base import BaseConnector, ConnectorCapabilities, ConnectorError, MissingDriverError, UnsupportedConnectorError
from t2c_data.connectors.implementations.helpers import parse_mongodb_uri, require_secret, require_value


class MongoConnector(BaseConnector):
    engine = "mongodb"
    capabilities = ConnectorCapabilities(test_connection=True, list_schemas=True, list_tables=True, get_database_info=True)

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        summary = parse_mongodb_uri(require_secret(secrets, "uri"))
        return {
            "host": summary["host"],
            "port": summary["port"],
            "database": str(connection.get("database") or summary["database"]),
            "username": summary["username"],
        }

    def _client(self):
        try:
            from pymongo import MongoClient
        except ImportError as exc:
            raise MissingDriverError(self.engine, "pymongo") from exc
        return MongoClient(require_secret(self.secrets, "uri"), serverSelectionTimeoutMS=5000)

    def test_connection(self) -> dict[str, Any]:
        client = self._client()
        try:
            response = client.admin.command("ping")
            return {"ping": response.get("ok")}
        finally:
            client.close()

    def get_database_info(self) -> dict[str, Any]:
        client = self._client()
        try:
            return client.server_info()
        finally:
            client.close()

    def list_schemas(self) -> list[str]:
        client = self._client()
        try:
            return sorted(client.list_database_names())
        finally:
            client.close()

    def list_tables(self, schema: str | None = None) -> list[str]:
        client = self._client()
        try:
            database_name = schema or self.connection.get("database")
            if not database_name:
                raise ConnectorError("Informe o database do MongoDB para listar collections.", code="invalid_config")
            return sorted(client[str(database_name)].list_collection_names())
        finally:
            client.close()


class OtherConnector(BaseConnector):
    engine = "other"
    capabilities = ConnectorCapabilities(test_connection=False, list_schemas=False, list_tables=False, get_database_info=False)

    def test_connection(self) -> dict[str, Any]:
        raise UnsupportedConnectorError(self.engine)
