from __future__ import annotations

from typing import Any, Protocol

from t2c_data.connectors.factory import create_connector


class DatasourceConnectorGateway(Protocol):
    def summarize_connection(self, *, engine: str, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]: ...

    def test_connection(self, *, engine: str, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]: ...

    def list_schemas(self, *, engine: str, connection: dict[str, Any], secrets: dict[str, str]) -> list[str]: ...

    def list_tables(
        self,
        *,
        engine: str,
        connection: dict[str, Any],
        secrets: dict[str, str],
        schema: str | None = None,
    ) -> list[str]: ...


class DefaultDatasourceConnectorGateway:
    def summarize_connection(self, *, engine: str, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return create_connector(engine, connection=connection, secrets=secrets).summarize_connection(connection, secrets)

    def test_connection(self, *, engine: str, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return create_connector(engine, connection=connection, secrets=secrets).test_connection()

    def list_schemas(self, *, engine: str, connection: dict[str, Any], secrets: dict[str, str]) -> list[str]:
        return create_connector(engine, connection=connection, secrets=secrets).list_schemas()

    def list_tables(
        self,
        *,
        engine: str,
        connection: dict[str, Any],
        secrets: dict[str, str],
        schema: str | None = None,
    ) -> list[str]:
        return create_connector(engine, connection=connection, secrets=secrets).list_tables(schema=schema)
