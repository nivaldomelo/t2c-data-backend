from __future__ import annotations

import os
import unittest
from unittest import mock
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import sessionmaker

from t2c_data.connectors.base import ConnectorError
from t2c_data.core.config import settings
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.platform import PlatformDomainEvent
from t2c_data.models.scan import ScanDiff, ScanRun, ScanSnapshot
from t2c_data.models.tag import Tag, TagAssignment, TagAssignmentOverride, TagAutomationRule, TagIntelligenceEvent
from t2c_data.schemas.catalog import TableVolumeSnapshotOut
from t2c_data.services.postgres_scanner import ScanPayload, ScannedColumn, ScannedTable
from t2c_data.services.scanner import run_scan


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


class _FakeScanGateway:
    def __init__(self, payload: ScanPayload | None = None, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls: list[int] = []

    def scan(self, datasource: DataSource) -> ScanPayload:
        self.calls.append(datasource.id)
        if self.error:
            raise self.error
        assert self.payload is not None
        return self.payload


class _RetryingScanGateway:
    def __init__(self, payload: ScanPayload, *, fail_times: int = 1) -> None:
        self.payload = payload
        self.fail_times = fail_times
        self.calls: list[int] = []

    def scan(self, datasource: DataSource) -> ScanPayload:
        self.calls.append(datasource.id)
        if len(self.calls) <= self.fail_times:
            raise ConnectorError("Timeout temporário", detail="temporary timeout", code="timeout")
        return self.payload


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        ColumnEntity.__table__.create(bind=conn)
        ScanRun.__table__.create(bind=conn)
        ScanSnapshot.__table__.create(bind=conn)
        ScanDiff.__table__.create(bind=conn)
        Tag.__table__.create(bind=conn)
        TagAssignment.__table__.create(bind=conn)
        TagAssignmentOverride.__table__.create(bind=conn)
        TagAutomationRule.__table__.create(bind=conn)
        TagIntelligenceEvent.__table__.create(bind=conn)
        AuditLog.__table__.create(bind=conn)
        PlatformDomainEvent.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed_datasource(session) -> DataSource:
    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="analytics",
        username="tester",
    )
    datasource.set_secret_values({"password": "secret"})
    session.add(datasource)
    session.commit()
    session.refresh(datasource)
    return datasource


class ScannerGatewayTests(unittest.TestCase):
    def test_run_scan_uses_injected_gateway_and_persists_scan(self) -> None:
        SessionLocal = _session_factory()
        payload = ScanPayload(
            database_name="analytics",
            tables=[
                ScannedTable(
                    schema_name="silver",
                    table_name="orders",
                    table_type="table",
                    comment="Pedidos tratados",
                    columns=[
                        ScannedColumn(
                            name="id",
                            data_type="integer",
                            is_primary_key=True,
                            is_nullable=False,
                            ordinal_position=1,
                            comment="PK",
                        )
                    ],
                )
            ],
        )
        gateway = _FakeScanGateway(payload=payload)

        with SessionLocal() as session:
            datasource = _seed_datasource(session)
            with mock.patch("t2c_data.features.scanner.persistence.measure_table_volume", return_value=None):
                scan_run = run_scan(session, datasource, started_by=None, scan_gateway=gateway)

            self.assertEqual(gateway.calls, [datasource.id])
            self.assertEqual(scan_run.status, "partial_success")
            self.assertEqual(scan_run.summary["database"], "analytics")
            self.assertEqual(scan_run.summary["tables"], 1)
            self.assertEqual(session.scalar(select(Database.name)), "analytics")
            self.assertEqual(session.scalar(select(Schema.name)), "silver")
            self.assertEqual(session.scalar(select(TableEntity.name)), "orders")
            self.assertEqual(session.scalar(select(ColumnEntity.name)), "id")
            self.assertEqual(len(session.scalars(select(ScanSnapshot)).all()), 2)

    def test_run_scan_measures_row_count_snapshots_during_persistence(self) -> None:
        SessionLocal = _session_factory()
        payload = ScanPayload(
            database_name="analytics",
            tables=[
                ScannedTable(
                    schema_name="silver",
                    table_name="orders",
                    table_type="table",
                    comment="Pedidos tratados",
                    columns=[
                        ScannedColumn(
                            name="id",
                            data_type="integer",
                            is_primary_key=True,
                            is_nullable=False,
                            ordinal_position=1,
                            comment="PK",
                        )
                    ],
                )
            ],
        )
        gateway = _FakeScanGateway(payload=payload)

        def _measure(*, db, table_id: int, measurement_context: str = "manual"):
            assert measurement_context == "datasource_run"
            return TableVolumeSnapshotOut(
                table_id=table_id,
                datasource_id=1,
                schema_id=1,
                connection_name="warehouse",
                database_name="analytics",
                schema_name="silver",
                table_name="orders",
                fqn="warehouse.analytics.silver.orders",
                row_count=42,
                measurement_type="exact",
                measurement_source="postgres_count",
                status="success",
                measured_at=datetime.now(timezone.utc),
                duration_ms=11,
                error_message=None,
            )

        with SessionLocal() as session:
            datasource = _seed_datasource(session)
            with mock.patch("t2c_data.features.scanner.persistence.measure_table_volume", side_effect=_measure) as mocked:
                scan_run = run_scan(session, datasource, started_by=None, scan_gateway=gateway)

            assert mocked.call_count == 1
            assert scan_run.summary["row_counts"]["success"] == 1
            assert scan_run.summary["row_counts"]["failed"] == 0
            assert scan_run.summary["row_counts"]["skipped"] == 0

    def test_run_scan_continues_when_row_count_measurement_raises(self) -> None:
        SessionLocal = _session_factory()
        payload = ScanPayload(
            database_name="analytics",
            tables=[
                ScannedTable(
                    schema_name="silver",
                    table_name="orders",
                    table_type="table",
                    comment="Pedidos tratados",
                    columns=[],
                )
            ],
        )
        gateway = _FakeScanGateway(payload=payload)

        def _measure(*, db, table_id: int, measurement_context: str = "manual"):
            assert measurement_context == "datasource_run"
            raise RuntimeError("boom")

        with SessionLocal() as session:
            datasource = _seed_datasource(session)
            with mock.patch("t2c_data.features.scanner.persistence.measure_table_volume", side_effect=_measure):
                scan_run = run_scan(session, datasource, started_by=None, scan_gateway=gateway)

            assert scan_run.status == "partial_success"
            assert scan_run.summary["row_counts"]["success"] == 0
            assert scan_run.summary["row_counts"]["failed"] == 0
            assert scan_run.summary["row_counts"]["skipped"] == 1
            assert session.scalar(select(TableEntity.name)) == "orders"

    def test_run_scan_counts_success_and_error_row_count_snapshots_per_table(self) -> None:
        SessionLocal = _session_factory()
        payload = ScanPayload(
            database_name="analytics",
            tables=[
                ScannedTable(
                    schema_name="silver",
                    table_name="orders",
                    table_type="table",
                    comment="Pedidos tratados",
                    columns=[],
                ),
                ScannedTable(
                    schema_name="silver",
                    table_name="customers",
                    table_type="table",
                    comment="Clientes tratados",
                    columns=[],
                ),
            ],
        )
        gateway = _FakeScanGateway(payload=payload)
        calls: list[int] = []

        def _measure(*, db, table_id: int, measurement_context: str = "manual"):
            assert measurement_context == "datasource_run"
            calls.append(table_id)
            if len(calls) == 1:
                return TableVolumeSnapshotOut(
                    table_id=table_id,
                    datasource_id=1,
                    schema_id=1,
                    connection_name="warehouse",
                    database_name="analytics",
                    schema_name="silver",
                    table_name="orders",
                    fqn="warehouse.analytics.silver.orders",
                    row_count=42,
                    measurement_type="exact",
                    measurement_source="postgres_count",
                    status="success",
                    measured_at=datetime.now(timezone.utc),
                    duration_ms=11,
                    error_message=None,
                )

            return TableVolumeSnapshotOut(
                table_id=table_id,
                datasource_id=1,
                schema_id=1,
                connection_name="warehouse",
                database_name="analytics",
                schema_name="silver",
                table_name="customers",
                fqn="warehouse.analytics.silver.customers",
                row_count=None,
                measurement_type="unavailable",
                measurement_source="postgres_count",
                status="error",
                measured_at=datetime.now(timezone.utc),
                duration_ms=21,
                error_message="timeout",
            )

        with SessionLocal() as session:
            datasource = _seed_datasource(session)
            with mock.patch("t2c_data.features.scanner.persistence.measure_table_volume", side_effect=_measure):
                scan_run = run_scan(session, datasource, started_by=None, scan_gateway=gateway)

            assert scan_run.status == "partial_success"
            assert scan_run.summary["row_counts"]["success"] == 1
            assert scan_run.summary["row_counts"]["failed"] == 1
            assert scan_run.summary["row_counts"]["skipped"] == 0
            assert len(session.scalars(select(TableEntity)).all()) == 2

    def test_run_scan_records_failed_scan_when_gateway_raises(self) -> None:
        SessionLocal = _session_factory()
        gateway = _FakeScanGateway(error=ConnectorError("Credenciais inválidas", detail="bad auth", code="invalid_credentials"))

        with SessionLocal() as session:
            datasource = _seed_datasource(session)
            failed_run = run_scan(session, datasource, started_by=None, scan_gateway=gateway)

            self.assertEqual(gateway.calls, [datasource.id])
            self.assertEqual(failed_run.status, "failed")
            self.assertEqual(failed_run.summary["engine"], "postgres")
            self.assertEqual(failed_run.summary["error_code"], "invalid_credentials")
            self.assertEqual(len(session.scalars(select(ScanRun)).all()), 1)

    def test_run_scan_retries_transient_connector_errors_before_persisting(self) -> None:
        SessionLocal = _session_factory()
        payload = ScanPayload(
            database_name="analytics",
            tables=[
                ScannedTable(
                    schema_name="silver",
                    table_name="orders",
                    table_type="table",
                    comment="Pedidos tratados",
                    columns=[],
                )
            ],
        )
        gateway = _RetryingScanGateway(payload=payload, fail_times=1)

        with SessionLocal() as session:
            datasource = _seed_datasource(session)
            datasource.connection_config = {"scan_retry_attempts": 2, "scan_retry_backoff_ms": 0}
            session.add(datasource)
            session.commit()
            with mock.patch("t2c_data.features.scanner.persistence.measure_table_volume", return_value=None):
                scan_run = run_scan(session, datasource, started_by=None, scan_gateway=gateway)

            self.assertEqual(gateway.calls, [datasource.id, datasource.id])
            self.assertEqual(scan_run.status, "partial_success")
            self.assertEqual(scan_run.summary["tables"], 1)

    def test_run_scan_continues_when_row_count_measurement_returns_error_snapshot(self) -> None:
        SessionLocal = _session_factory()
        payload = ScanPayload(
            database_name="analytics",
            tables=[
                ScannedTable(
                    schema_name="silver",
                    table_name="orders",
                    table_type="table",
                    comment="Pedidos tratados",
                    columns=[],
                )
            ],
        )
        gateway = _FakeScanGateway(payload=payload)

        def _measure(*, db, table_id: int, measurement_context: str = "manual"):
            assert measurement_context == "datasource_run"
            return TableVolumeSnapshotOut(
                table_id=table_id,
                datasource_id=1,
                schema_id=1,
                connection_name="warehouse",
                database_name="analytics",
                schema_name="silver",
                table_name="orders",
                fqn="warehouse.analytics.silver.orders",
                row_count=None,
                measurement_type="unavailable",
                measurement_source="postgres_count",
                status="error",
                measured_at=datetime.now(timezone.utc),
                duration_ms=21,
                error_message="timeout",
            )

        with SessionLocal() as session:
            datasource = _seed_datasource(session)
            with mock.patch("t2c_data.features.scanner.persistence.measure_table_volume", side_effect=_measure):
                scan_run = run_scan(session, datasource, started_by=None, scan_gateway=gateway)

            assert scan_run.status == "partial_success"
            assert scan_run.summary["row_counts"]["success"] == 0
            assert scan_run.summary["row_counts"]["failed"] == 1
            assert scan_run.summary["row_counts"]["skipped"] == 0


if __name__ == "__main__":
    unittest.main()
