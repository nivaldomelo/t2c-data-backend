from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.datasource.application import (
    list_datasource_schemas_via_connector,
    list_datasource_tables_via_connector,
    retest_saved_datasource_connection,
    test_datasource_connection,
)


class _FakeConnectorGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def summarize_connection(self, *, engine: str, connection: dict, secrets: dict) -> dict:
        self.calls.append(("summarize", {"engine": engine, "connection": connection, "secrets": secrets}))
        return {
            "host": connection.get("host", "localhost"),
            "port": connection.get("port", 5432),
            "database": connection.get("database", "analytics"),
            "username": connection.get("username", "tester"),
        }

    def test_connection(self, *, engine: str, connection: dict, secrets: dict) -> dict:
        self.calls.append(("test", {"engine": engine, "connection": connection, "secrets": secrets}))
        return {"server_version": "16"}

    def list_schemas(self, *, engine: str, connection: dict, secrets: dict) -> list[str]:
        self.calls.append(("schemas", {"engine": engine, "connection": connection, "secrets": secrets}))
        return ["public", "silver"]

    def list_tables(self, *, engine: str, connection: dict, secrets: dict, schema: str | None = None) -> list[str]:
        self.calls.append(
            ("tables", {"engine": engine, "connection": connection, "secrets": secrets, "schema": schema})
        )
        return ["orders", "customers"]


class _FakeDB:
    def __init__(self, datasource) -> None:
        self.datasource = datasource

    def get(self, model, key):
        return self.datasource if self.datasource and getattr(self.datasource, "id", None) == key else None


class DatasourceConnectorGatewayTests(unittest.TestCase):
    def test_test_datasource_connection_uses_injected_gateway(self) -> None:
        gateway = _FakeConnectorGateway()
        payload = SimpleNamespace(
            db_type="postgres",
            connection={"host": "db.local", "port": 5432, "database": "analytics", "username": "tester"},
            secrets={"password": "secret"},
        )

        result = test_datasource_connection(
            payload=payload,
            normalize_connection=lambda value: value,
            normalize_secrets=lambda value: value,
            run_connection_test=lambda engine, connection, secrets, connector_gateway: {
                "engine": engine,
                "details": connector_gateway.test_connection(engine=engine, connection=connection, secrets=secrets),
            },
            connector_gateway=gateway,
        )

        self.assertEqual(result["engine"], "postgres")
        self.assertEqual(result["details"], {"server_version": "16"})
        self.assertEqual(gateway.calls[0][0], "test")

    def test_retest_saved_datasource_connection_uses_injected_gateway(self) -> None:
        gateway = _FakeConnectorGateway()
        datasource = SimpleNamespace(
            id=7,
            db_type="postgres",
            secret_values={"password": "secret"},
        )
        db = _FakeDB(datasource)

        result = retest_saved_datasource_connection(
            db=db,
            datasource_id=7,
            resolved_connection=lambda entity: {"host": "db.local", "database": "analytics"},
            run_connection_test=lambda engine, connection, secrets, connector_gateway: connector_gateway.test_connection(
                engine=engine,
                connection=connection,
                secrets=secrets,
            ),
            connector_gateway=gateway,
        )

        self.assertEqual(result, {"server_version": "16"})
        self.assertEqual(gateway.calls[0][0], "test")

    def test_list_datasource_schemas_via_connector_uses_injected_gateway(self) -> None:
        gateway = _FakeConnectorGateway()
        datasource = SimpleNamespace(id=9, db_type="postgres", secret_values={"password": "secret"})
        db = _FakeDB(datasource)

        result = list_datasource_schemas_via_connector(
            db=db,
            datasource_id=9,
            resolved_connection=lambda entity: {"host": "db.local", "database": "analytics"},
            capabilities_out=lambda engine: {"engine": engine},
            sanitize_error_message=lambda exc: ("error", str(exc), "failed"),
            connector_gateway=gateway,
        )

        self.assertEqual(result["schemas"], ["public", "silver"])
        self.assertEqual(result["engine"], "postgres")
        self.assertEqual(gateway.calls[0][0], "schemas")

    def test_list_datasource_tables_via_connector_uses_injected_gateway(self) -> None:
        gateway = _FakeConnectorGateway()
        datasource = SimpleNamespace(id=11, db_type="postgres", secret_values={"password": "secret"})
        db = _FakeDB(datasource)

        result = list_datasource_tables_via_connector(
            db=db,
            datasource_id=11,
            schema="silver",
            resolved_connection=lambda entity: {"host": "db.local", "database": "analytics"},
            capabilities_out=lambda engine: {"engine": engine},
            sanitize_error_message=lambda exc: ("error", str(exc), "failed"),
            connector_gateway=gateway,
        )

        self.assertEqual(result["tables"], ["orders", "customers"])
        self.assertEqual(result["schema"], "silver")
        self.assertEqual(gateway.calls[0][0], "tables")


if __name__ == "__main__":
    unittest.main()
