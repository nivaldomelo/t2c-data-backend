from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.data_quality.profiling_watermarks import (
    detect_watermark_column,
    resolve_profiling_window,
)
from t2c_data.features.data_quality.profiling_watermarks import resolve_effective_watermark_column
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQProfilingTableSetting, DQProfilingWatermark


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        ColumnEntity.__table__.create(bind=conn)
        DQProfilingWatermark.__table__.create(bind=conn)
        DQProfilingTableSetting.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _make_table(session, *, columns: list[tuple[str, str]]) -> int:
    datasource = DataSource(name="wh", db_type="postgres", host="localhost", port=5432, database="db", username="u")
    datasource.password = "secret"
    session.add(datasource)
    session.flush()
    database = Database(datasource_id=datasource.id, name="db")
    session.add(database)
    session.flush()
    schema = Schema(database_id=database.id, name="gold")
    session.add(schema)
    session.flush()
    table = TableEntity(schema_id=schema.id, name="fato", table_type="table")
    session.add(table)
    session.flush()
    for index, (name, data_type) in enumerate(columns):
        session.add(
            ColumnEntity(table_id=table.id, name=name, data_type=data_type, ordinal_position=index)
        )
    session.flush()
    return table.id


def test_detect_watermark_prefers_convention_and_temporal_type():
    Session = _session_factory()
    with Session() as session:
        table_id = _make_table(
            session,
            columns=[
                ("id", "integer"),
                ("nome", "character varying"),
                ("created_at", "timestamp with time zone"),
                ("updated_at", "timestamp with time zone"),
            ],
        )
        # updated_at has higher priority than created_at and both are temporal.
        assert detect_watermark_column(session, table_id) == "updated_at"


def test_detect_watermark_ignores_non_temporal_named_columns():
    Session = _session_factory()
    with Session() as session:
        table_id = _make_table(
            session,
            columns=[
                ("id", "integer"),
                ("updated_at", "character varying"),  # named like a watermark but not temporal
                ("valor", "numeric"),
            ],
        )
        assert detect_watermark_column(session, table_id) is None


def test_resolve_window_first_full_then_delta():
    Session = _session_factory()
    with Session() as session:
        table_id = _make_table(session, columns=[("id", "integer"), ("created_at", "timestamp")])

        first = resolve_profiling_window(session, table_id=table_id, now=datetime(2026, 6, 25, 12, 0, tzinfo=timezone.utc))
        assert first.mode == "full"
        assert first.watermark_column == "created_at"
        assert first.window_start is None

        # Simulate a successful run advancing the watermark.
        session.add(
            DQProfilingWatermark(
                table_id=table_id,
                mode="full",
                watermark_column="created_at",
                window_start=None,
                window_end=first.window_end,
                status="success",
                rows_processed=10,
            )
        )
        session.flush()

        second = resolve_profiling_window(
            session, table_id=table_id, now=first.window_end + timedelta(hours=1)
        )
        assert second.mode == "delta"
        assert second.watermark_column == "created_at"
        assert second.window_start == first.window_end


def test_resolve_window_full_when_no_watermark_column():
    Session = _session_factory()
    with Session() as session:
        table_id = _make_table(session, columns=[("id", "integer"), ("valor", "numeric")])
        window = resolve_profiling_window(session, table_id=table_id, now=datetime(2026, 6, 25, tzinfo=timezone.utc))
        assert window.mode == "full"
        assert window.watermark_column is None


def test_start_date_floor_makes_first_run_bounded_delta():
    Session = _session_factory()
    with Session() as session:
        table_id = _make_table(session, columns=[("id", "integer"), ("created_at", "timestamp")])
        floor = datetime(2026, 1, 1, tzinfo=timezone.utc)
        session.add(DQProfilingTableSetting(table_id=table_id, start_date=floor))
        session.flush()

        window = resolve_profiling_window(
            session, table_id=table_id, now=datetime(2026, 6, 25, 12, 0, tzinfo=timezone.utc)
        )
        assert window.mode == "delta"  # not full, despite being the first run
        assert window.watermark_column == "created_at"
        assert window.window_start == floor


def test_override_watermark_column_is_used_when_autodetect_fails():
    Session = _session_factory()
    with Session() as session:
        # No conventional name, but the column is temporal; auto-detect would miss it.
        table_id = _make_table(session, columns=[("id", "integer"), ("competencia", "date")])
        assert resolve_effective_watermark_column(session, table_id) is None
        session.add(DQProfilingTableSetting(table_id=table_id, watermark_column="competencia"))
        session.flush()
        assert resolve_effective_watermark_column(session, table_id) == "competencia"


def test_resolve_window_ignores_failed_runs_for_advancement():
    Session = _session_factory()
    with Session() as session:
        table_id = _make_table(session, columns=[("id", "integer"), ("created_at", "timestamp")])
        now = datetime(2026, 6, 25, 12, 0, tzinfo=timezone.utc)
        # A failed run must not advance the watermark -> next run stays full.
        session.add(
            DQProfilingWatermark(
                table_id=table_id,
                mode="full",
                watermark_column="created_at",
                window_end=now,
                status="failed",
            )
        )
        session.flush()
        nxt = resolve_profiling_window(session, table_id=table_id, now=now + timedelta(hours=1))
        assert nxt.mode == "full"
