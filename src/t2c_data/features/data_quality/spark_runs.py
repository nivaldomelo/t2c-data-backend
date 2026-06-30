from __future__ import annotations

from datetime import datetime, timezone
from time import sleep
from typing import Any

from sqlalchemy import select

from t2c_data.core.db import SessionLocal
from t2c_data.features.data_quality.queries import resolve_table_context_by_fqn
from t2c_data.models.dq import DQJobRun, DQRun

TERMINAL_DQ_RUN_STATUSES = {"success", "failed", "no_data", "timeout"}
ACTIVE_DQ_RUN_STATUSES = {"queued", "running"}


def update_job_run(job_run_id: int, **updates: Any) -> None:
    with SessionLocal() as session:
        entity = session.get(DQJobRun, job_run_id)
        if not entity:
            return
        for key, value in updates.items():
            setattr(entity, key, value)
        session.add(entity)
        session.commit()


def update_dq_run_status(
    dq_run_id: int,
    *,
    status: str,
    error_message: str | None = None,
    execution_engine: str | None = None,
) -> None:
    with SessionLocal() as session:
        entity = session.get(DQRun, dq_run_id)
        if not entity:
            return
        entity.status = status
        if execution_engine is not None:
            entity.execution_engine = execution_engine
        entity.error_message = error_message
        if status == "running" and entity.started_at is None:
            entity.started_at = datetime.now(timezone.utc)
        if status in TERMINAL_DQ_RUN_STATUSES:
            entity.finished_at = datetime.now(timezone.utc)
            ref = entity.started_at or entity.queued_at
            if ref and entity.finished_at:
                entity.duration_ms = int((entity.finished_at - ref).total_seconds() * 1000)
        session.add(entity)
        session.commit()


def update_dq_run_fields(dq_run_id: int, **updates: Any) -> None:
    with SessionLocal() as session:
        entity = session.get(DQRun, dq_run_id)
        if not entity:
            return
        for key, value in updates.items():
            setattr(entity, key, value)
        session.add(entity)
        session.commit()


def create_job_run(
    *,
    job_type: str,
    dq_run_id: int | None,
    table_id: int | None,
    table_fqn: str | None,
    requested_by_user_id: int | None,
    spark_master_url: str,
    execution_engine: str = "spark",
) -> DQJobRun:
    with SessionLocal() as session:
        job = DQJobRun(
            job_type=job_type,
            status="queued",
            execution_engine=execution_engine,
            dq_run_id=dq_run_id,
            table_id=table_id,
            table_fqn=table_fqn,
            requested_by_user_id=requested_by_user_id,
            spark_master_url=spark_master_url,
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        return job


def create_spark_dq_run(
    *,
    table_id: int | None,
    table_fqn: str | None,
    profiling_schedule_id: int | None = None,
    execution_engine: str = "spark",
) -> DQRun:
    with SessionLocal() as session:
        table, schema, _database, datasource = resolve_table_context_by_fqn(session, table_fqn) if table_id is None and table_fqn else _table_context_from_id_or_fqn(session, table_id=table_id, table_fqn=table_fqn)
        dq_run = DQRun(
            datasource_id=datasource.id,
            profiling_schedule_id=profiling_schedule_id,
            table_id=table.id,
            scope="table",
            schema_name=schema.name,
            status="queued",
            execution_engine=execution_engine,
            queued_at=datetime.now(timezone.utc),
            error_message=None,
        )
        session.add(dq_run)
        session.commit()
        session.refresh(dq_run)
        return dq_run


def create_spark_schema_dq_run(
    *,
    datasource_id: int | None,
    schema_name: str,
    profiling_schedule_id: int | None = None,
    execution_engine: str = "spark",
) -> DQRun:
    with SessionLocal() as session:
        dq_run = DQRun(
            datasource_id=datasource_id,
            profiling_schedule_id=profiling_schedule_id,
            table_id=None,
            scope="schema",
            schema_name=schema_name,
            status="queued",
            execution_engine=execution_engine,
            queued_at=datetime.now(timezone.utc),
            error_message=None,
        )
        session.add(dq_run)
        session.commit()
        session.refresh(dq_run)
        return dq_run


def create_spark_batch_dq_run(
    *,
    datasource_id: int | None,
    scope: str,
    schema_name: str | None = None,
    profiling_schedule_id: int | None = None,
    execution_engine: str = "spark",
) -> DQRun:
    with SessionLocal() as session:
        dq_run = DQRun(
            datasource_id=datasource_id,
            profiling_schedule_id=profiling_schedule_id,
            table_id=None,
            scope=scope,
            schema_name=schema_name,
            status="queued",
            execution_engine=execution_engine,
            queued_at=datetime.now(timezone.utc),
            error_message=None,
        )
        session.add(dq_run)
        session.commit()
        session.refresh(dq_run)
        return dq_run


def list_dq_run_children(parent_run_id: int) -> list[DQRun]:
    with SessionLocal() as session:
        return session.scalars(select(DQRun).where(DQRun.parent_run_id == parent_run_id).order_by(DQRun.id)).all()


def get_dq_run(dq_run_id: int) -> DQRun | None:
    with SessionLocal() as session:
        return session.get(DQRun, dq_run_id)


def get_dq_job_runs(
    limit: int = 100,
    *,
    table_id: int | None = None,
    dq_run_id: int | None = None,
    job_type: str | None = None,
    status: str | None = None,
    execution_engine: str | None = None,
) -> list[DQJobRun]:
    with SessionLocal() as session:
        query = select(DQJobRun)
        if table_id is not None:
            query = query.where(DQJobRun.table_id == table_id)
        if dq_run_id is not None:
            query = query.where(DQJobRun.dq_run_id == dq_run_id)
        if job_type is not None:
            query = query.where(DQJobRun.job_type == job_type)
        if status is not None:
            query = query.where(DQJobRun.status == status)
        if execution_engine is not None:
            query = query.where(DQJobRun.execution_engine == execution_engine)
        return session.scalars(query.order_by(DQJobRun.id.desc()).limit(limit)).all()


def get_dq_job_run(run_id: int) -> DQJobRun | None:
    with SessionLocal() as session:
        return session.get(DQJobRun, run_id)


def wait_for_dq_job_run(run_id: int, *, timeout_seconds: int = 180, poll_interval_seconds: float = 1.0) -> DQJobRun | None:
    waited = 0.0
    while waited < timeout_seconds:
        entity = get_dq_job_run(run_id)
        if entity and entity.status in TERMINAL_DQ_RUN_STATUSES:
            return entity
        sleep(poll_interval_seconds)
        waited += poll_interval_seconds
    return get_dq_job_run(run_id)


def _table_context_from_id_or_fqn(session, *, table_id: int | None, table_fqn: str | None):
    if table_id is not None:
        from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity

        table = session.get(TableEntity, table_id)
        if not table:
            raise ValueError("Table not found")
        schema = session.get(Schema, table.schema_id)
        if not schema:
            raise ValueError("Schema not found")
        database = session.get(Database, schema.database_id)
        if not database:
            raise ValueError("Database not found")
        datasource = session.get(DataSource, database.datasource_id)
        if not datasource:
            raise ValueError("Datasource not found")
        return table, schema, database, datasource
    if table_fqn:
        return resolve_table_context_by_fqn(session, table_fqn)
    raise ValueError("table_id or table_fqn is required")


__all__ = [
    "create_job_run",
    "create_spark_dq_run",
    "create_spark_batch_dq_run",
    "create_spark_schema_dq_run",
    "get_dq_job_run",
    "get_dq_job_runs",
    "get_dq_run",
    "list_dq_run_children",
    "ACTIVE_DQ_RUN_STATUSES",
    "TERMINAL_DQ_RUN_STATUSES",
    "update_dq_run_fields",
    "update_dq_run_status",
    "update_job_run",
    "wait_for_dq_job_run",
]
