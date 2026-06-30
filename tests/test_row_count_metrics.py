from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.row_count_metrics import build_row_count_metrics
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


def _seed_table(session: Session) -> TableEntity:
    datasource = DataSource(
        name="operational-source",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="catalog",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="catalog", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="orders", table_type="table", schema=schema)
    session.add_all([datasource, database, schema, table])
    session.commit()
    return table


def _insert_snapshot(
    session: Session,
    *,
    table_id: int,
    row_count: int | None,
    snapshot_at: datetime,
    method: str = "exact",
    source: str = "postgres_count",
    status: str = "success",
    error_message: str | None = None,
) -> None:
    session.execute(
        text(
            """
        INSERT INTO controle.table_row_count_snapshots (
            table_id,
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
        )
        VALUES (
            :table_id,
            :row_count,
            :measurement_type,
            :measurement_source,
            :status,
            :measured_at,
            :duration_ms,
            :error_message,
            :collection_method,
            :collection_status,
            :snapshot_at,
            :snapshot_date,
            :created_at
        )
        """
        ),
        {
            "table_id": table_id,
            "row_count": row_count,
            "measurement_type": method,
            "measurement_source": source,
            "status": status,
            "measured_at": snapshot_at,
            "duration_ms": 12,
            "error_message": error_message,
            "collection_method": method,
            "collection_status": status,
            "snapshot_at": snapshot_at,
            "snapshot_date": snapshot_at.date(),
            "created_at": snapshot_at,
        },
    )
    session.commit()


def test_build_row_count_metrics_uses_two_latest_success_snapshots() -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        table = _seed_table(session)
        _insert_snapshot(
            session,
            table_id=table.id,
            row_count=18328108,
            snapshot_at=datetime(2026, 4, 15, 3, 0, tzinfo=timezone.utc),
            source="postgres_count",
            method="exact",
        )
        _insert_snapshot(
            session,
            table_id=table.id,
            row_count=18452991,
            snapshot_at=datetime(2026, 4, 16, 3, 0, tzinfo=timezone.utc),
            source="postgres_count",
            method="exact",
        )

        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert metrics is not None
    assert metrics.current_row_count == 18452991
    assert metrics.previous_row_count == 18328108
    assert metrics.snapshot_at == datetime(2026, 4, 16, 3, 0, tzinfo=timezone.utc)
    assert metrics.previous_snapshot_at == datetime(2026, 4, 15, 3, 0, tzinfo=timezone.utc)
    assert metrics.collection_method == "exact"
    assert metrics.collection_status == "success"
    assert metrics.measurement_source == "postgres_count"
    assert metrics.status == "success"
    assert metrics.growth_absolute == 124883
    assert metrics.growth_percent == 0.6814
    assert metrics.has_history is True


def test_build_row_count_metrics_returns_current_snapshot_without_variation_for_single_value() -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        table = _seed_table(session)
        _insert_snapshot(
            session,
            table_id=table.id,
            row_count=42,
            snapshot_at=datetime(2026, 4, 16, 3, 0, tzinfo=timezone.utc),
            method="estimated",
            source="postgres_count",
        )

        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert metrics is not None
    assert metrics.current_row_count == 42
    assert metrics.previous_row_count is None
    assert metrics.growth_absolute is None
    assert metrics.growth_percent is None
    assert metrics.has_history is False
    assert metrics.collection_method == "estimated"
    assert metrics.measurement_source == "postgres_count"


def test_build_row_count_metrics_ignores_default_zero_without_trusted_source() -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        table = _seed_table(session)
        _insert_snapshot(
            session,
            table_id=table.id,
            row_count=0,
            snapshot_at=datetime(2026, 4, 16, 3, 0, tzinfo=timezone.utc),
            method="exact",
            source="unknown",
        )

        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert metrics is not None
    assert metrics.current_row_count is None
    assert metrics.collection_status is None
    assert metrics.status is None


def test_build_row_count_metrics_accepts_real_zero_from_trusted_source() -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        table = _seed_table(session)
        _insert_snapshot(
            session,
            table_id=table.id,
            row_count=0,
            snapshot_at=datetime(2026, 4, 16, 3, 0, tzinfo=timezone.utc),
            method="exact",
            source="postgres_count",
        )

        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert metrics is not None
    assert metrics.current_row_count == 0
    assert metrics.measurement_source == "postgres_count"
    assert metrics.status == "success"


def test_build_row_count_metrics_returns_empty_block_when_no_snapshot_exists() -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        table = _seed_table(session)
        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert metrics is not None
    assert metrics.current_row_count is None
    assert metrics.previous_row_count is None
    assert metrics.snapshot_at is None
    assert metrics.previous_snapshot_at is None
    assert metrics.collection_method is None
    assert metrics.collection_status is None
    assert metrics.measurement_source is None
    assert metrics.growth_absolute is None
    assert metrics.growth_percent is None
    assert metrics.has_history is False


def test_build_row_count_metrics_gracefully_handles_repository_failure(monkeypatch) -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        table = _seed_table(session)
        monkeypatch.setattr(
            "t2c_data.features.catalog.row_count_metrics.get_latest_row_count_snapshots",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("controle unavailable")),
        )

        metrics = build_row_count_metrics(db=session, table_id=table.id)

    assert metrics is None


if __name__ == "__main__":
    test_build_row_count_metrics_uses_two_latest_success_snapshots()
    test_build_row_count_metrics_returns_current_snapshot_without_variation_for_single_value()
    test_build_row_count_metrics_returns_empty_block_when_no_snapshot_exists()
    print("row count metrics tests: OK")
