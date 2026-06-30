from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.data_quality.profiling_executions import (
    get_profiling_execution_detail,
    list_profiling_executions,
)
from sqlalchemy import select

from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRun, DQTableMetric


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        DQRun.__table__.create(bind=conn)
        DQTableMetric.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed(session):
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="analytics", username="tester")
    datasource.password = "secret"
    session.add(datasource)
    session.flush()

    database = Database(datasource_id=datasource.id, name="analytics")
    session.add(database)
    session.flush()

    schema = Schema(database_id=database.id, name="gold")
    session.add(schema)
    session.flush()

    table_a = TableEntity(schema_id=schema.id, name="fato_vendas", table_type="table")
    table_b = TableEntity(schema_id=schema.id, name="fato_clientes", table_type="table")
    session.add_all([table_a, table_b])
    session.flush()

    parent = DQRun(datasource_id=datasource.id, scope="schema", schema_name="gold", status="running", execution_engine="spark")
    session.add(parent)
    session.flush()

    child_ok = DQRun(
        datasource_id=datasource.id,
        table_id=table_a.id,
        scope="table",
        schema_name="gold",
        parent_run_id=parent.id,
        status="success",
        execution_engine="spark",
        profile_payload_json={"observation": "Execucao concluida sem alertas."},
    )
    child_fail = DQRun(
        datasource_id=datasource.id,
        table_id=table_b.id,
        scope="table",
        schema_name="gold",
        parent_run_id=parent.id,
        status="failed",
        execution_engine="spark",
        error_message="Falha no Spark.",
    )
    session.add_all([child_ok, child_fail])
    session.flush()

    session.add(
        DQTableMetric(
            run_id=child_ok.id,
            table_id=table_a.id,
            row_count=120,
            column_count=8,
            completeness_pct_avg=98.5,
            dq_score=96.0,
            duplicates_count=1,
            failed_rules=0,
            metrics_json={},
        )
    )
    session.commit()
    return datasource, parent, child_ok, child_fail


def test_list_profiling_executions_returns_top_level_schema_runs():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        datasource, parent, child_ok, child_fail = _seed(session)
        page = list_profiling_executions(session, datasource_id=datasource.id, limit=20)

    assert page.total == 1
    assert len(page.items) == 1
    assert page.items[0].id == parent.id
    assert page.items[0].scope == "schema"
    assert page.items[0].total_items == 2
    assert page.items[0].success_items == 1
    assert page.items[0].failed_items == 1


def test_get_profiling_execution_detail_includes_child_metrics_and_observation():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        _datasource, parent, child_ok, child_fail = _seed(session)
        detail = get_profiling_execution_detail(session, parent.id)

    assert detail is not None
    assert detail.id == parent.id
    assert len(detail.items) == 2
    success_item = next(item for item in detail.items if item.id == child_ok.id)
    failed_item = next(item for item in detail.items if item.id == child_fail.id)
    assert success_item.table_fqn == "gold.fato_vendas"
    assert success_item.row_count == 120
    assert success_item.dq_score == 96.0
    assert success_item.observation == "Execucao concluida sem alertas."
    assert failed_item.error_message == "Falha no Spark."


def test_list_profiling_executions_respects_limit_offset_search_and_date_range():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        datasource, _parent, _child_ok, _child_fail = _seed(session)
        existing_table = session.scalar(select(TableEntity).limit(1))
        assert existing_table is not None
        base_time = datetime(2026, 5, 22, 12, 0, tzinfo=UTC)
        for idx in range(12):
            run = DQRun(
                datasource_id=datasource.id,
                scope="table",
                schema_name="gold",
                status="success",
                execution_engine="spark",
                started_at=base_time + timedelta(minutes=idx),
                queued_at=base_time + timedelta(minutes=idx),
            )
            session.add(run)
            session.flush()
            table = TableEntity(schema_id=existing_table.schema_id, name=f"busca_{idx}", table_type="table")
            session.add(table)
            session.flush()
            run.table_id = table.id
        session.commit()

        page_one = list_profiling_executions(session, datasource_id=datasource.id, limit=10, offset=0)
        page_two = list_profiling_executions(session, datasource_id=datasource.id, limit=10, offset=10)
        search_results = list_profiling_executions(session, datasource_id=datasource.id, search="busca_11", limit=10, offset=0)
        date_results = list_profiling_executions(
            session,
            datasource_id=datasource.id,
            started_from=date(2026, 5, 22),
            started_to=date(2026, 5, 22),
            limit=20,
            offset=0,
        )

    assert page_one.total == 13
    assert len(page_one.items) == 10
    assert len(page_two.items) == 3
    page_one_fqns = [item.table_fqn for item in page_one.items]
    assert "gold.busca_11" in page_one_fqns[:2]
    assert search_results.total == 1
    assert search_results.items[0].table_fqn == "gold.busca_11"
    assert date_results.total == 12
