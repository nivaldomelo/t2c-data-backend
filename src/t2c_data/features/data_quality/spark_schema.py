from __future__ import annotations

import logging
from datetime import datetime, timezone
from time import sleep
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import SessionLocal
from t2c_data.features.data_quality.notifications import notify_dq_profiling_failure
from t2c_data.features.data_quality.profiling_schedules import update_profiling_schedule_run_state
from t2c_data.features.data_quality.spark_runs import TERMINAL_DQ_RUN_STATUSES, create_spark_dq_run, get_dq_job_run, get_dq_run, update_dq_run_fields
from t2c_data.models.catalog import TableEntity
from t2c_data.models.dq import DQProfilingSchedule, DQRun
from t2c_data.services.audit import write_audit_log_sync

logger = logging.getLogger(__name__)


def execute_schema_profiling_orchestration(
    *,
    parent_run_id: int,
    table_targets: list[dict[str, Any]],
    requested_by_user_id: int | None,
    concurrency: int,
    sample_fraction: float | None = None,
    columns: list[str] | None = None,
    enqueue_profiling_fn: Callable[..., Any],
    create_child_run_fn: Callable[..., DQRun] | None = None,
    child_execution_engine: str = "spark",
) -> None:
    columns = columns or []
    logger.info(
        "dq_schema_profiling_started",
        extra={
            **_dq_log_context(parent_run_id=parent_run_id, job_type="schema_profiling"),
            "tables_total": len(table_targets),
            "concurrency": concurrency,
        },
    )
    update_dq_run_fields(parent_run_id, status="running", started_at=datetime.now(timezone.utc), error_message=None)
    active: dict[int, int] = {}
    pending = list(table_targets)
    had_failures = False
    parent_run = get_dq_run(parent_run_id)
    parent_schedule_id = parent_run.profiling_schedule_id if parent_run else None
    child_factory = create_child_run_fn or (
        lambda *, table_id, table_fqn, profiling_schedule_id=None: create_spark_dq_run(
            table_id=table_id,
            table_fqn=table_fqn,
            profiling_schedule_id=profiling_schedule_id,
            execution_engine=child_execution_engine,
        )
    )

    try:
        while pending or active:
            while pending and len(active) < max(1, concurrency):
                item = pending.pop(0)
                child = child_factory(
                    table_id=item["table_id"],
                    table_fqn=item["table_fqn"],
                    profiling_schedule_id=parent_schedule_id,
                )
                update_dq_run_fields(
                    child.id,
                    parent_run_id=parent_run_id,
                    scope="table",
                    schema_name=item.get("schema_name"),
                )
                job = enqueue_profiling_fn(
                    table_id=item["table_id"],
                    table_fqn=item["table_fqn"],
                    columns=columns,
                    sample_fraction=sample_fraction,
                    requested_by_user_id=requested_by_user_id,
                    dq_run_id=child.id,
                )
                active[job.id] = child.id

            finished_job_ids: list[int] = []
            for job_id, child_run_id in list(active.items()):
                job = get_dq_job_run(job_id)
                if not job or job.status not in TERMINAL_DQ_RUN_STATUSES:
                    continue
                child_run = get_dq_run(child_run_id)
                if child_run and child_run.status in {"failed", "timeout"}:
                    had_failures = True
                elif job.status in {"failed", "timeout"}:
                    had_failures = True
                finished_job_ids.append(job_id)
            for job_id in finished_job_ids:
                active.pop(job_id, None)

            if pending or active:
                sleep(1.0)

        finished_at = datetime.now(timezone.utc)
        update_dq_run_fields(
            parent_run_id,
            status="failed" if had_failures else "success",
            finished_at=finished_at,
        )
        parent = get_dq_run(parent_run_id)
        if parent and parent.finished_at:
            ref = parent.started_at or parent.queued_at
            if ref:
                update_dq_run_fields(
                    parent_run_id,
                    duration_ms=int((parent.finished_at - ref).total_seconds() * 1000),
                )
        with SessionLocal() as session:
            parent = session.get(DQRun, parent_run_id)
            if parent:
                update_profiling_schedule_run_state(
                    session,
                    schedule_id=parent.profiling_schedule_id,
                    status=parent.status,
                    error_message=parent.error_message,
                    started_at=parent.started_at,
                    finished_at=parent.finished_at,
                )
                if had_failures:
                    try:
                        table = session.get(TableEntity, parent.table_id) if parent.table_id is not None else None
                        schedule = session.get(DQProfilingSchedule, parent.profiling_schedule_id) if parent.profiling_schedule_id else None
                        notify_dq_profiling_failure(
                            session,
                            schedule=schedule,
                            table=table,
                            table_fqn=f"{parent.schema_name}.*" if parent.schema_name else None,
                            dq_run=parent,
                            error_message=parent.error_message or "Schema profiling completed with one or more table failures",
                            reporter_user_id=requested_by_user_id,
                        )
                    except Exception:
                        pass
                _audit_schema_run_finish(
                    session,
                    parent=parent,
                    user_id=requested_by_user_id,
                    metadata={"result": "failed" if had_failures else "success", "tables_total": len(table_targets)},
                )
                session.commit()
        logger.info(
            "dq_schema_profiling_finished",
            extra={
                **_dq_log_context(parent_run_id=parent_run_id, job_type="schema_profiling"),
                "tables_total": len(table_targets),
                "result": "failed" if had_failures else "success",
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "dq_schema_profiling_failed",
            extra={
                **_dq_log_context(parent_run_id=parent_run_id, job_type="schema_profiling"),
                "tables_total": len(table_targets),
            },
        )
        update_dq_run_fields(
            parent_run_id,
            status="failed",
            error_message=str(exc),
            finished_at=datetime.now(timezone.utc),
        )
        with SessionLocal() as session:
            parent = session.get(DQRun, parent_run_id)
            if parent:
                update_profiling_schedule_run_state(
                    session,
                    schedule_id=parent.profiling_schedule_id,
                    status="failed",
                    error_message=str(exc),
                    started_at=parent.started_at,
                    finished_at=parent.finished_at,
                )
                try:
                    table = session.get(TableEntity, parent.table_id) if parent.table_id is not None else None
                    schedule = session.get(DQProfilingSchedule, parent.profiling_schedule_id) if parent.profiling_schedule_id else None
                    notify_dq_profiling_failure(
                        session,
                        schedule=schedule,
                        table=table,
                        table_fqn=f"{parent.schema_name}.*" if parent.schema_name else None,
                        dq_run=parent,
                        error_message=str(exc),
                        reporter_user_id=requested_by_user_id,
                    )
                except Exception:
                    pass
                _audit_schema_run_finish(
                    session,
                    parent=parent,
                    user_id=requested_by_user_id,
                    metadata={"result": "failed", "error": str(exc)},
                )
                session.commit()


def _audit_schema_run_finish(session: Session, *, parent: DQRun, user_id: int | None, metadata: dict[str, Any]) -> None:
    write_audit_log_sync(
        session,
        action="dq.profiling.schema_run.finish",
        user_id=user_id,
        entity_type="dq_run",
        entity_id=parent.id,
        after={
            "status": parent.status,
            "execution_engine": parent.execution_engine,
            "spark_app_id": parent.spark_app_id,
            "queued_at": parent.queued_at,
            "started_at": parent.started_at,
            "finished_at": parent.finished_at,
            "duration_ms": parent.duration_ms,
        },
        metadata=metadata,
    )


def _dq_log_context(
    *,
    parent_run_id: int | None = None,
    job_type: str | None = None,
) -> dict[str, Any]:
    return {
        "parent_run_id": parent_run_id,
        "job_type": job_type,
    }


__all__ = ["execute_schema_profiling_orchestration"]
