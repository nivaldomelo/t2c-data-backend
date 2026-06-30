from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.row_count_metrics import build_row_count_metrics
from t2c_data.features.catalog.table_volume import get_latest_table_volume, measure_table_volume
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.execute("ATTACH DATABASE ':memory:' AS controle")
        cursor.close()

    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE controle.table_row_count_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER NOT NULL,
                datasource_id INTEGER,
                schema_id INTEGER,
                connection_name TEXT,
                database_name TEXT,
                schema_name TEXT,
                table_name TEXT,
                fqn TEXT,
                row_count BIGINT,
                measurement_type TEXT,
                measurement_source TEXT,
                status TEXT NOT NULL,
                measured_at TIMESTAMP,
                duration_ms INTEGER,
                error_message TEXT,
                collection_method TEXT,
                collection_status TEXT,
                snapshot_at TIMESTAMP,
                snapshot_date DATE,
                created_at TIMESTAMP
            )
            """
        )

    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def _seed_table(session: Session, *, db_type: str = "postgresql") -> TableEntity:
    datasource = DataSource(
        name="operational-source",
        db_type=db_type,
        host="localhost",
        port=5432,
        database="catalog",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="catalog", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="audit_logs", table_type="table", schema=schema)
    session.add_all([datasource, database, schema, table])
    session.commit()
    return table


class _FakeResult:
    def __init__(self, value: int | None) -> None:
        self._value = value

    def scalar_one(self) -> int | None:
        return self._value


class _FakeDialect:
    name = "postgresql"

    class _IdentifierPreparer:
        @staticmethod
        def quote(identifier: str) -> str:
            return f'"{identifier}"'

    identifier_preparer = _IdentifierPreparer()


class _FakeConnection:
    dialect = _FakeDialect()

    def __init__(self, row_count: int, *, should_fail: bool = False) -> None:
        self.row_count = row_count
        self.should_fail = should_fail
        self.executed_sql: list[str] = []

    def exec_driver_sql(self, sql: str):  # type: ignore[no-untyped-def]
        self.executed_sql.append(sql)
        if sql.startswith("SET "):
            return _FakeResult(None)
        if self.should_fail:
            raise RuntimeError("boom")
        return _FakeResult(self.row_count)


@contextmanager
def _fake_datasource_connection(fake_connection: _FakeConnection):
    yield fake_connection


def _insert_snapshot_for_history(session: Session, table_id: int, row_count: int | None, measured_at: datetime) -> None:
    session.execute(
        text(
            """
            INSERT INTO controle.table_row_count_snapshots (
                table_id,
                datasource_id,
                schema_id,
                connection_name,
                database_name,
                schema_name,
                table_name,
                fqn,
                row_count,
                measurement_type,
                measurement_source,
                status,
                measured_at,
                duration_ms,
                error_message,
                collection_method,
                collection_status,
                snapshot_at,
                snapshot_date,
                created_at
            ) VALUES (
                :table_id,
                1,
                1,
                'operational-source',
                'catalog',
                'bronze',
                'audit_logs',
                'local-andromeda.andromeda.bronze.audit_logs',
                :row_count,
                'exact',
                'postgres_count',
                'success',
                :measured_at,
                10,
                NULL,
                'exact',
                'success',
                :measured_at,
                :snapshot_date,
                :created_at
            )
            """
        ),
        {
            "table_id": table_id,
            "row_count": row_count,
            "measured_at": measured_at,
            "snapshot_date": measured_at.date(),
            "created_at": measured_at,
        },
    )
    session.commit()


def test_measure_table_volume_persists_real_count_for_postgresql_alias(monkeypatch) -> None:
    session_factory = _build_session_factory()
    fake_connection = _FakeConnection(row_count=0)

    with session_factory() as session:
        table = _seed_table(session, db_type="postgresql")
        monkeypatch.setattr(
            "t2c_data.features.catalog.table_volume._datasource_connection",
            lambda datasource: _fake_datasource_connection(fake_connection),
        )

        snapshot = measure_table_volume(db=session, table_id=table.id)
        latest = get_latest_table_volume(db=session, table_id=table.id)
        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert snapshot is not None
    assert snapshot.status == "success"
    assert snapshot.row_count == 0
    assert snapshot.measurement_source == "postgres_count"
    assert latest is not None
    assert latest.row_count == 0
    assert metrics is not None
    assert metrics.current_row_count == 0
    assert metrics.status == "success"
    assert metrics.measurement_source == "postgres_count"


def test_measure_table_volume_persists_error_snapshot_without_zero(monkeypatch) -> None:
    session_factory = _build_session_factory()
    fake_connection = _FakeConnection(row_count=0, should_fail=True)

    with session_factory() as session:
        table = _seed_table(session)
        monkeypatch.setattr(
            "t2c_data.features.catalog.table_volume._datasource_connection",
            lambda datasource: _fake_datasource_connection(fake_connection),
        )

        snapshot = measure_table_volume(db=session, table_id=table.id)
        latest = get_latest_table_volume(db=session, table_id=table.id)
        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert snapshot is not None
    assert snapshot.status == "error"
    assert snapshot.row_count is None
    assert snapshot.measurement_source == "postgres_count"
    assert latest is not None
    assert latest.status == "error"
    assert latest.row_count is None
    assert metrics is not None
    assert metrics.status == "error"
    assert metrics.current_row_count is None


def test_measure_table_volume_uses_non_unknown_source_for_unsupported_databases() -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        table = _seed_table(session, db_type="oracle")
        snapshot = measure_table_volume(db=session, table_id=table.id)

    assert snapshot is not None
    assert snapshot.status == "skipped"
    assert snapshot.measurement_source == "oracle_count"
    assert snapshot.measurement_source != "unknown"


def test_measure_table_volume_falls_back_to_estimate_for_large_tables(monkeypatch) -> None:
    session_factory = _build_session_factory()
    fake_connection = _FakeConnection(row_count=0)
    calls: list[str] = []

    with session_factory() as session:
        table = _seed_table(session, db_type="postgresql")
        datasource = table.schema.database.datasource
        datasource.connection_config = {
            "row_count_strategy": "exact",
            "row_count_exact_max_rows_before_estimate": 1000,
        }
        session.add(datasource)
        session.commit()

        monkeypatch.setattr(
            "t2c_data.features.catalog.table_volume._datasource_connection",
            lambda datasource: _fake_datasource_connection(fake_connection),
        )

        def _measure_with_large_estimate(*, connection, datasource, schema_name, table_name, strategy):
            calls.append(strategy)
            if strategy == "estimated":
                return 5000
            raise AssertionError("exact count should not run when estimate exceeds threshold")

        monkeypatch.setattr("t2c_data.features.catalog.table_volume._measure_relation_row_count", _measure_with_large_estimate)

        snapshot = measure_table_volume(db=session, table_id=table.id)

    assert snapshot is not None
    assert snapshot.status == "success"
    assert snapshot.measurement_type == "estimated"
    assert snapshot.row_count == 5000
    assert calls == ["estimated"]
