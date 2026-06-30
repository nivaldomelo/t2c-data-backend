from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.data_quality.run_outputs import build_dq_job_out
import t2c_data.features.data_quality.spark_runs as spark_runs_module
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQProfilingSchedule, DQRun


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        User.__table__.create(bind=conn)
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        DQProfilingSchedule.__table__.create(bind=conn)
        DQRun.__table__.create(bind=conn)
        DQJobRun.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed(session):
    user = User(email="runner@example.com", name="Runner", full_name="Runner User", password_hash="secret")
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="analytics", username="tester")
    datasource.password = "secret"
    session.add_all([user, datasource])
    session.flush()

    database = Database(datasource_id=datasource.id, name="analytics")
    session.add(database)
    session.flush()

    schema = Schema(database_id=database.id, name="gold")
    session.add(schema)
    session.flush()

    table = TableEntity(schema_id=schema.id, name="fato_vendas", table_type="table")
    session.add(table)
    session.flush()

    schedule = DQProfilingSchedule(
        scope="table",
        table_id=table.id,
        datasource_id=datasource.id,
        schema_name=schema.name,
        execution_engine="python",
        schedule_mode="manual",
        schedule_enabled=True,
    )
    session.add(schedule)
    session.flush()

    dq_run = DQRun(
        datasource_id=datasource.id,
        profiling_schedule_id=schedule.id,
        table_id=table.id,
        scope="table",
        schema_name=schema.name,
        status="running",
        execution_engine="python",
    )
    session.add(dq_run)
    session.flush()

    job_run = DQJobRun(
        job_type="profiling",
        status="running",
        execution_engine="python",
        dq_run_id=dq_run.id,
        table_id=table.id,
        table_fqn="gold.fato_vendas",
        datasource_id=datasource.id,
        requested_by_user_id=user.id,
        spark_app_id="app-1",
        command="python profiling",
    )
    session.add(job_run)
    session.flush()
    return user, table, schedule, dq_run, job_run


def test_dq_job_run_payload_includes_origin_and_owner():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        _user, _table, schedule, dq_run, job_run = _seed(session)
        payload = build_dq_job_out(job_run, session)

    assert payload.profiling_schedule_id == schedule.id
    assert payload.requested_by_user_name == "Runner"
    assert payload.requested_by_user_email == "runner@example.com"
    assert payload.trigger_source == "scheduled"
    assert payload.queued_at is not None


def test_dq_runs_list_can_filter_profiling_history(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(spark_runs_module, "SessionLocal", SessionLocal)
    with SessionLocal() as session:
        _user, table, _schedule, dq_run, job_run = _seed(session)
        job_run_id = job_run.id
        other = DQJobRun(
            job_type="rules",
            status="failed",
            execution_engine="spark",
            dq_run_id=None,
            table_id=table.id,
            table_fqn="gold.fato_vendas",
            datasource_id=None,
            requested_by_user_id=None,
            error_message="boom",
        )
        session.add(other)
        session.commit()

        filtered = spark_runs_module.get_dq_job_runs(limit=10, table_id=table.id, job_type="profiling")
        profiling_out = build_dq_job_out(filtered[0], session)

    assert len(filtered) == 1
    assert filtered[0].id == job_run_id
    assert profiling_out.job_type == "profiling"
    assert profiling_out.trigger_source == "scheduled"
