from __future__ import annotations

import logging
from datetime import datetime, timezone
from threading import Thread
from time import sleep
from typing import Any

from t2c_data.core.config import settings
from t2c_data.core.db import SessionLocal
from t2c_data.core.request_context import capture_request_context, run_with_request_context
from t2c_data.features.data_quality.spark_persistence import (
    persist_profiling_output,
    persist_profiling_output_into_existing_run,
    persist_rules_output,
    validate_profiling_payload,
)
from t2c_data.features.data_quality.latest_runs import sync_latest_snapshot_for_job
from t2c_data.features.data_quality.spark_worker_support import temporary_result_file
from t2c_data.features.data_quality.spark_runs import (
    create_job_run,
    create_spark_dq_run,
    create_spark_schema_dq_run,
    get_dq_job_run,
    get_dq_job_runs,
    get_dq_run,
    list_dq_run_children,
    update_dq_run_fields,
    update_dq_run_status,
    update_job_run,
    wait_for_dq_job_run,
)
from t2c_data.features.data_quality.spark_workers import execute_profiling_job, execute_rules_job
from t2c_data.features.platform_settings.resolvers import resolve_spark_config
from t2c_data.integrations.spark import get_spark_submit_config
from t2c_data.models.dq import DQJobRun, DQRun
from t2c_data.services.audit import write_audit_log_sync

logger = logging.getLogger(__name__)
SPARK_CONFIG = get_spark_submit_config()


def _resolved_master_url() -> str:
    """Effective Spark master URL (DB override → env → default), best-effort."""
    try:
        with SessionLocal() as session:
            return resolve_spark_config(session).master_url
    except Exception:
        return SPARK_CONFIG.master_url


def _dq_log_context(
    *,
    job_run_id: int | None = None,
    dq_run_id: int | None = None,
    table_id: int | None = None,
    table_fqn: str | None = None,
    parent_run_id: int | None = None,
    job_type: str | None = None,
) -> dict[str, Any]:
    return {
        "job_run_id": job_run_id,
        "dq_run_id": dq_run_id,
        "table_id": table_id,
        "table_fqn": table_fqn,
        "parent_run_id": parent_run_id,
        "job_type": job_type,
    }


def _audit_dq_run(
    session,
    *,
    action: str,
    dq_run: DQRun | None,
    job: DQJobRun | None,
    user_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    if not dq_run and not job:
        return
    try:
        write_audit_log_sync(
            session,
            action=action,
            user_id=user_id,
            entity_type="dq_run",
            entity_id=(dq_run.id if dq_run else job.dq_run_id if job else None),
            after=(
                None
                if dq_run is None
                else {
                    "status": dq_run.status,
                    "execution_engine": dq_run.execution_engine,
                    "spark_app_id": dq_run.spark_app_id,
                    "queued_at": dq_run.queued_at,
                    "started_at": dq_run.started_at,
                    "finished_at": dq_run.finished_at,
                    "duration_ms": dq_run.duration_ms,
                }
            ),
            metadata={
                **(metadata or {}),
                "job_run_id": getattr(job, "id", None),
                "job_type": getattr(job, "job_type", None),
                "job_status": getattr(job, "status", None),
                "spark_master_url": getattr(job, "spark_master_url", None) or SPARK_CONFIG.master_url,
            },
        )
    except Exception:
        # Audit must never break DQ execution paths.
        pass


def enqueue_profiling_job(
    *,
    table_id: int | None,
    table_fqn: str | None,
    columns: list[str],
    sample_fraction: float | None,
    requested_by_user_id: int | None,
    dq_run_id: int | None = None,
) -> DQJobRun:
    request_context = capture_request_context()
    job = create_job_run(
        job_type="profiling",
        dq_run_id=dq_run_id,
        table_id=table_id,
        table_fqn=table_fqn,
        requested_by_user_id=requested_by_user_id,
        spark_master_url=_resolved_master_url(),
    )
    Thread(
        target=run_with_request_context,
        args=(request_context, execute_profiling_job, job.id),
        kwargs={
            "table_id": table_id,
            "table_fqn": table_fqn,
            "columns": columns,
            "sample_fraction": sample_fraction,
            "user_id": requested_by_user_id,
            "dq_run_id": dq_run_id,
        },
        daemon=True,
    ).start()
    return job


def enqueue_rules_job(
    *,
    table_id: int | None,
    table_fqn: str | None,
    rule_ids: list[int],
    requested_by_user_id: int | None,
    dq_run_id: int | None = None,
) -> DQJobRun:
    request_context = capture_request_context()
    job = create_job_run(
        job_type="rules",
        dq_run_id=dq_run_id,
        table_id=table_id,
        table_fqn=table_fqn,
        requested_by_user_id=requested_by_user_id,
        spark_master_url=_resolved_master_url(),
    )
    with SessionLocal() as session:
        entity = session.get(DQJobRun, job.id)
        if entity:
            entity.result_json = {"requested_rule_ids": rule_ids}
            session.add(entity)
            sync_latest_snapshot_for_job(
                session,
                job_run=entity,
                rule_ids=rule_ids,
                table_id=table_id,
            )
            session.commit()
    Thread(
        target=run_with_request_context,
        args=(request_context, execute_rules_job, job.id),
        kwargs={
            "table_id": table_id,
            "table_fqn": table_fqn,
            "rule_ids": rule_ids,
            "user_id": requested_by_user_id,
            "dq_run_id": dq_run_id,
        },
        daemon=True,
    ).start()
    return job


def enqueue_schema_profiling_run(
    *,
    parent_run_id: int,
    table_targets: list[dict[str, Any]],
    requested_by_user_id: int | None,
    concurrency: int,
    sample_fraction: float | None = None,
    columns: list[str] | None = None,
) -> None:
    columns = columns or []
    from t2c_data.features.data_quality.spark_schema import execute_schema_profiling_orchestration

    request_context = capture_request_context()
    logger.info(
        "dq_schema_profiling_enqueued",
        extra={
            **_dq_log_context(parent_run_id=parent_run_id, job_type="schema_profiling"),
            "tables_total": len(table_targets),
            "concurrency": concurrency,
        },
    )

    Thread(
        target=run_with_request_context,
        args=(
            request_context,
            execute_schema_profiling_orchestration,
        ),
        kwargs={
            "parent_run_id": parent_run_id,
            "table_targets": table_targets,
            "requested_by_user_id": requested_by_user_id,
            "concurrency": concurrency,
            "sample_fraction": sample_fraction,
            "columns": columns,
            "enqueue_profiling_fn": enqueue_profiling_job,
        },
        daemon=True,
    ).start()


__all__ = [
    "create_job_run",
    "create_spark_dq_run",
    "create_spark_schema_dq_run",
    "enqueue_profiling_job",
    "enqueue_rules_job",
    "enqueue_schema_profiling_run",
    "get_dq_job_run",
    "get_dq_job_runs",
    "get_dq_run",
    "list_dq_run_children",
    "execute_profiling_job",
    "execute_rules_job",
    "execute_schema_profiling_orchestration",
    "persist_profiling_output",
    "persist_profiling_output_into_existing_run",
    "persist_rules_output",
    "update_dq_run_fields",
    "update_dq_run_status",
    "update_job_run",
    "validate_profiling_payload",
    "wait_for_dq_job_run",
]

_persist_profiling_output = persist_profiling_output
_validate_profiling_payload = validate_profiling_payload
_temporary_result_file = temporary_result_file
