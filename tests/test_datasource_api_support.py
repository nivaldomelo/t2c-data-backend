from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from t2c_data.features.datasource.application import retest_saved_datasource_connection
from t2c_data.features.datasource.api_support import datasource_detail, run_connection_test
from t2c_data.connectors.implementations.sql_family import PostgresConnector
from t2c_data.models.catalog import DataSource


class _FakeConnectorGateway:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def summarize_connection(self, *, engine: str, connection: dict, secrets: dict) -> dict:
        self.calls.append("summarize_connection")
        return {
            "host": connection.get("host", "localhost"),
            "database": connection.get("database", "analytics"),
        }

    def test_connection(self, *, engine: str, connection: dict, secrets: dict) -> dict:
        self.calls.append("test_connection")
        return {"server_version": "16"}

    def list_schemas(self, *, engine: str, connection: dict, secrets: dict) -> list[str]:
        self.calls.append("list_schemas")
        return ["public", "silver"]


class _EmptySchemasConnectorGateway(_FakeConnectorGateway):
    def list_schemas(self, *, engine: str, connection: dict, secrets: dict) -> list[str]:
        self.calls.append("list_schemas")
        return []


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={"t2c_data": None}
    )
    DataSource.__table__.create(bind=engine)
    return sessionmaker(bind=engine, future=True)


class RunConnectionTestTests(unittest.TestCase):
    def test_datasource_detail_never_returns_secret_values(self) -> None:
        SessionLocal = _session_factory()
        with SessionLocal() as session:
            datasource = DataSource(
                name="warehouse",
                db_type="postgres",
                host="db.local",
                port=5432,
                database="analytics",
                username="tester",
                is_active=True,
            )
            datasource.set_secret_values({"password": "secret", "token": "abc"})
            session.add(datasource)
            session.commit()
            session.refresh(datasource)

            detail = datasource_detail(datasource)

            self.assertEqual(detail.configured_secrets, ["password", "token"])
            self.assertNotIn("password", detail.connection)
            self.assertFalse(hasattr(detail, "secret_values"))

    def test_run_connection_test_lists_schemas_for_supported_connector(self) -> None:
        gateway = _FakeConnectorGateway()

        result = run_connection_test(
            "postgres",
            {"host": "db.local", "database": "analytics"},
            {"password": "secret"},
            connector_gateway=gateway,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.schemas, ["public", "silver"])
        self.assertEqual(
            gateway.calls,
            ["summarize_connection", "test_connection", "list_schemas"],
        )

    def test_run_connection_test_warns_when_schema_list_is_empty(self) -> None:
        gateway = _EmptySchemasConnectorGateway()

        result = run_connection_test(
            "postgres",
            {"host": "db.local", "database": "analytics", "default_schema": "public"},
            {"password": "secret"},
            connector_gateway=gateway,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.schemas, [])
        self.assertIsNotNone(result.warning)
        self.assertIn("schema padrão", result.warning or "")
        self.assertEqual(result.default_schema, "public")

    def test_postgres_connector_list_schemas_falls_back_to_default_schema(self) -> None:
        class _Cursor:
            def __init__(self) -> None:
                self.calls: list[tuple[str, tuple | None]] = []
                self.step = 0

            def execute(self, query: str, params: tuple | None = None) -> None:
                self.calls.append((query, params))
                self.step += 1

            def fetchall(self):
                if self.step == 1:
                    return []
                return [(True,)]

            def fetchone(self):
                return (True,)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class _Connection:
            def __init__(self) -> None:
                self.cursor_obj = _Cursor()

            def cursor(self):
                return self.cursor_obj

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        fake_connection = _Connection()

        with patch("t2c_data.connectors.implementations.sql_family.psycopg.connect", return_value=fake_connection):
            connector = PostgresConnector(
                connection={"host": "db.local", "database": "analytics", "username": "tester", "default_schema": "public"},
                secrets={"password": "secret"},
            )
            schemas = connector.list_schemas()

        self.assertEqual(schemas, ["public"])

    def test_retest_saved_datasource_persists_detected_schemas(self) -> None:
        SessionLocal = _session_factory()
        gateway = _FakeConnectorGateway()

        def _run_connection_test(engine, connection, secrets, connector_gateway):
            connector_gateway.test_connection(engine=engine, connection=connection, secrets=secrets)
            return SimpleNamespace(success=True, schemas=["public", "silver"], message="ok")

        with SessionLocal() as session:
            datasource = DataSource(
                name="warehouse",
                db_type="postgres",
                host="db.local",
                port=5432,
                database="analytics",
                username="tester",
            )
            datasource.set_secret_values({"password": "secret"})
            session.add(datasource)
            session.commit()
            session.refresh(datasource)

            result = retest_saved_datasource_connection(
                db=session,
                datasource_id=datasource.id,
                resolved_connection=lambda entity: {"host": "db.local", "database": "analytics"},
                run_connection_test=_run_connection_test,
                connector_gateway=gateway,
            )

            refreshed = session.get(DataSource, datasource.id)
            self.assertTrue(result.success)
            self.assertEqual(result.schemas, ["public", "silver"])
            self.assertIsNotNone(refreshed)
            self.assertEqual(refreshed.detected_schemas, ["public", "silver"])


if __name__ == "__main__":
    unittest.main()
