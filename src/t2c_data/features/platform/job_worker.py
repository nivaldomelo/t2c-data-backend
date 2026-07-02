from __future__ import annotations

import logging
from collections.abc import Callable
from time import sleep
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy import select

from t2c_data.core.db import SessionLocal
from t2c_data.features.datasource.schedules import mark_scan_schedule_dispatched, update_scan_schedule_run_state
from t2c_data.features.integrations.data_lake_schedules import (
    mark_data_lake_scan_schedule_dispatched,
    update_data_lake_scan_schedule_run_state,
)
from t2c_data.features.export_jobs import process_export_job
from t2c_data.features.platform.jobs import claim_queued_integration_job, finish_integration_job_record
from t2c_data.features.platform.scheduler import process_platform_maintenance_job
from t2c_data.features.platform.worker_health import (
    WorkerHeartbeatContext,
    build_worker_heartbeat_context,
    heartbeat_worker,
)
from t2c_data.features.scanner.spark_execution import execute_spark_datasource_scan
from t2c_data.models.catalog import DataSource
from t2c_data.models.metabase import MetabaseSyncRun
from t2c_data.models.platform import DataLakeInventoryScanRun, IntegrationSyncJob
from t2c_data.models.scan import ScanRun

logger = logging.getLogger(__name__)


def _payload_dict(job: IntegrationSyncJob) -> dict[str, Any]:
    return dict(job.payload_json or {}) if isinstance(job.payload_json, dict) else {}


def _reload_job(session: Session, job_id: int) -> IntegrationSyncJob | None:
    return session.scalar(
        select(IntegrationSyncJob)
        .where(IntegrationSyncJob.id == job_id)
        .execution_options(populate_existing=True)
        .limit(1)
    )


def _reload_job_fresh(session: Session, job_id: int) -> IntegrationSyncJob | None:
    session.expunge_all()
    return _reload_job(session, job_id)


def _run_datasource_scan_job(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
    payload = _payload_dict(job)
    datasource_id = int(payload.get("datasource_id") or 0)
    scan_run_id = int(payload.get("scan_run_id") or 0) if payload.get("scan_run_id") is not None else None
    schedule_id = int(payload.get("schedule_id") or 0) if payload.get("schedule_id") is not None else None
    started_by = int(payload.get("started_by") or 0) if payload.get("started_by") is not None else None

    datasource = session.get(DataSource, datasource_id)
    scan_run = session.get(ScanRun, scan_run_id) if scan_run_id else None
    if schedule_id:
        mark_scan_schedule_dispatched(session, schedule_id, started_at=job.started_at)
        session.commit()

    if datasource is None:
        if scan_run is not None:
            scan_run.status = "failed"
            scan_run.summary = {"error": "Datasource not found", "error_code": "datasource_not_found"}
            session.add(scan_run)
            session.commit()
        if schedule_id:
            update_scan_schedule_run_state(
                session,
                schedule_id=schedule_id,
                status="failed",
                error_message="Datasource not found",
                started_at=job.started_at,
                finished_at=job.started_at,
            )
            session.commit()
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error="Datasource not found",
            context_json={"datasource_id": datasource_id, "scan_run_id": scan_run_id, "schedule_id": schedule_id},
            result_summary_json={"error": "Datasource not found"},
            progress_pct=100.0,
        ) or job

    if scan_run is None:
        scan_run = session.get(ScanRun, scan_run_id) if scan_run_id else None
    if scan_run is None:
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error="Scan run not found",
            context_json={"datasource_id": datasource_id, "scan_run_id": scan_run_id, "schedule_id": schedule_id},
            result_summary_json={"error": "Scan run not found"},
            progress_pct=100.0,
        ) or job

    outcome = execute_spark_datasource_scan(
        session,
        datasource=datasource,
        scan_run=scan_run,
        started_by=started_by,
        integration_job_id=job.id,
        worker_heartbeat_at=job.started_at,
    )
    if schedule_id:
        update_scan_schedule_run_state(
            session,
            schedule_id=schedule_id,
            status=outcome.scan_run.status,
            error_message=(outcome.scan_run.summary or {}).get("error") if isinstance(outcome.scan_run.summary, dict) else None,
            started_at=job.started_at,
            finished_at=outcome.scan_run.updated_at or outcome.scan_run.created_at,
        )
        session.commit()

    return finish_integration_job_record(
        session,
        job,
        status=outcome.job_status,
        records_processed=outcome.job_records,
        error=outcome.job_error,
        context_json=outcome.job_context,
        result_summary_json=outcome.scan_run.summary,
        progress_pct=100.0,
    ) or job


def _run_metabase_sync_job(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
    from t2c_data.features.metabase.service import run_metabase_instance_sync

    payload = _payload_dict(job)
    instance_id = int(payload.get("instance_id") or job.target_id or 0)
    sync_run_id = int(payload.get("sync_run_id") or 0) if payload.get("sync_run_id") is not None else None

    if instance_id <= 0:
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error="Metabase instance not found",
            context_json={"instance_id": instance_id or None, "sync_run_id": sync_run_id},
            result_summary_json={"error": "Metabase instance not found"},
            progress_pct=100.0,
        ) or job

    try:
        run_metabase_instance_sync(
            session,
            instance_id,
            commit=True,
            integration_job=job,
            sync_run_id=sync_run_id,
        )
    except Exception as exc:
        refresh_job = _reload_job_fresh(session, job.id)
        if refresh_job is not None:
            return refresh_job
        if sync_run_id:
            sync_run = session.get(MetabaseSyncRun, sync_run_id)
            if sync_run is not None and sync_run.status == "queued":
                sync_run.status = "failed"
                sync_run.finished_at = job.finished_at
                sync_run.error_message = str(exc)
                session.add(sync_run)
                session.commit()
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error=str(exc),
            context_json={"instance_id": instance_id, "sync_run_id": sync_run_id, "error": str(exc)},
            result_summary_json={"error": str(exc)},
            progress_pct=100.0,
        ) or job

    refreshed = _reload_job_fresh(session, job.id)
    return refreshed or job


def _run_data_lake_inventory_scan_job(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
    from t2c_data.features.integrations.data_lake_inventory import _run_data_lake_inventory_scan

    payload = _payload_dict(job)
    connection_id = int(payload.get("connection_id") or job.target_id or 0)
    scan_run_id = int(payload.get("scan_run_id") or 0) if payload.get("scan_run_id") is not None else None
    schedule_id = int(payload.get("schedule_id") or 0) if payload.get("schedule_id") is not None else None
    requested_by_user_id = int(payload.get("requested_by_user_id") or 0) if payload.get("requested_by_user_id") is not None else None

    scan_run = session.get(DataLakeInventoryScanRun, scan_run_id) if scan_run_id else None
    if schedule_id:
        mark_data_lake_scan_schedule_dispatched(session, schedule_id, started_at=job.started_at)
        session.commit()

    if connection_id <= 0:
        if scan_run is not None:
            scan_run.status = "error"
            scan_run.error_message = "Data Lake connection not found"
            scan_run.finished_at = job.started_at
            session.add(scan_run)
            session.commit()
        if schedule_id:
            update_data_lake_scan_schedule_run_state(
                session,
                schedule_id=schedule_id,
                status="failed",
                error_message="Data Lake connection not found",
                started_at=job.started_at,
                finished_at=job.started_at,
            )
            session.commit()
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error="Data Lake connection not found",
            context_json={"connection_id": connection_id or None, "scan_run_id": scan_run_id, "schedule_id": schedule_id},
            result_summary_json={"error": "Data Lake connection not found"},
            progress_pct=100.0,
        ) or job

    try:
        outcome = _run_data_lake_inventory_scan(
            session,
            connection_id,
            current_user=None,
            audit_kwargs={"user_id": requested_by_user_id, "request_id": job.correlation_id} if requested_by_user_id else {"request_id": job.correlation_id},
            trigger_mode=str(payload.get("trigger_mode") or job.trigger_mode or "manual"),
            schedule_id=schedule_id,
            scan_run=scan_run,
        )
    except Exception as exc:
        refresh_job = _reload_job_fresh(session, job.id)
        persisted_scan_run = session.get(DataLakeInventoryScanRun, scan_run_id) if scan_run_id else None
        if persisted_scan_run is not None and persisted_scan_run.status in {"queued", "running"}:
            persisted_scan_run.status = "error"
            persisted_scan_run.error_message = str(exc)
            persisted_scan_run.finished_at = job.finished_at or job.started_at
            session.add(persisted_scan_run)
            session.commit()
            persisted_scan_run = session.get(DataLakeInventoryScanRun, scan_run_id)
        finished_at = getattr(persisted_scan_run, "finished_at", None) or job.finished_at or job.started_at
        if schedule_id:
            update_data_lake_scan_schedule_run_state(
                session,
                schedule_id=schedule_id,
                status="failed",
                error_message=str(exc),
                started_at=job.started_at,
                finished_at=finished_at,
            )
            session.commit()
        if refresh_job is not None:
            return finish_integration_job_record(
                session,
                refresh_job,
                status="failed",
                error=str(exc),
                context_json={
                    "connection_id": connection_id,
                    "scan_run_id": scan_run_id,
                    "schedule_id": schedule_id,
                    "trigger_mode": payload.get("trigger_mode") or job.trigger_mode,
                },
                result_summary_json={"error": str(exc)},
                progress_pct=100.0,
            ) or refresh_job
        return finish_integration_job_record(
            session,
            job,
            status="failed",
            error=str(exc),
            context_json={
                "connection_id": connection_id,
                "scan_run_id": scan_run_id,
                "schedule_id": schedule_id,
                "trigger_mode": payload.get("trigger_mode") or job.trigger_mode,
            },
            result_summary_json={"error": str(exc)},
            progress_pct=100.0,
        ) or job

    persisted_scan_run = session.get(DataLakeInventoryScanRun, scan_run_id) if scan_run_id else None
    final_scan_status = str(getattr(persisted_scan_run, "status", None) or outcome.scan_run.status or "success").strip().lower()
    final_job_status = "partial_success" if final_scan_status == "partial_success" else "success" if final_scan_status == "success" else "failed"
    if schedule_id:
        update_data_lake_scan_schedule_run_state(
            session,
            schedule_id=schedule_id,
            status=final_scan_status,
            error_message=getattr(persisted_scan_run, "error_message", None),
            started_at=job.started_at,
            finished_at=getattr(persisted_scan_run, "finished_at", None) or job.finished_at,
        )
        session.commit()

    return finish_integration_job_record(
        session,
        job,
        status=final_job_status,
        records_processed=outcome.summary.total_tables,
        error=getattr(persisted_scan_run, "error_message", None),
        context_json={
            "connection_id": connection_id,
            "scan_run_id": getattr(persisted_scan_run, "id", None) or outcome.scan_run.id,
            "schedule_id": schedule_id,
            "trigger_mode": payload.get("trigger_mode") or job.trigger_mode,
            "status": final_scan_status,
        },
        result_summary_json={
            "scan_run_id": getattr(persisted_scan_run, "id", None) or outcome.scan_run.id,
            "status": final_scan_status,
            "tables": outcome.summary.total_tables,
            "parquet_files": outcome.summary.total_parquet_files,
            "layers": len(outcome.summary.layers_detected or []),
            "total_bytes": outcome.summary.total_bytes,
        },
        progress_pct=100.0,
    ) or job


def process_claimed_integration_job(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
    if job.source == "s3" and job.job_type in {"inventory_scan", "data_lake_inventory_scan", "data_lake_scan", "scan"}:
        return _run_data_lake_inventory_scan_job(session, job)
    if job.source == "datasource" and job.job_type in {"scan", "datasource_scan"}:
        return _run_datasource_scan_job(session, job)
    if job.source == "metabase" and job.job_type == "sync":
        return _run_metabase_sync_job(session, job)
    if job.source == "platform" and job.job_type in {"maintenance", "read_model_refresh", "read_models"}:
        return process_platform_maintenance_job(session, job)
    if job.source == "export":
        return process_export_job(session, job)

    return finish_integration_job_record(
        session,
        job,
        status="failed",
        error=f"Unsupported dedicated worker job: {job.source}:{job.job_type}",
        context_json={"source": job.source, "job_type": job.job_type},
        result_summary_json={"error": "unsupported_job"},
        progress_pct=100.0,
    ) or job


def process_next_integration_job(
    *,
    source: str | None = None,
    job_type: str | None = None,
    session_factory: Callable[[], Session] = SessionLocal,
    worker_context: WorkerHeartbeatContext | None = None,
) -> IntegrationSyncJob | None:
    context = worker_context or build_worker_heartbeat_context(source=source, job_type=job_type)
    with session_factory() as session:
        job = claim_queued_integration_job(session, source=source, job_type=job_type)
        if job is None:
            heartbeat_worker(session, context, status="idle")
            return None
        heartbeat_worker(session, context, status="running", active_job=job)
        logger.info(
            "integration_worker_processing job_id=%s source=%s job_type=%s",
            job.id,
            job.source,
            job.job_type,
        )
        try:
            processed = process_claimed_integration_job(session, job)
        except Exception:
            # A conexão pode ter caído no meio do job (RDS idle/SSL EOF). Faz rollback
            # ANTES do heartbeat para não estourar PendingRollbackError numa transação abortada.
            try:
                session.rollback()
            except Exception:  # noqa: BLE001
                pass
            try:
                heartbeat_worker(session, context, status="degraded", active_job=job)
            except Exception:  # noqa: BLE001
                logger.warning("heartbeat após falha do job não pôde ser registrado", exc_info=True)
            raise
        heartbeat_worker(
            session,
            context,
            status="idle",
            last_job_status=getattr(processed, "status", None),
        )
        refreshed = _reload_job(session, processed.id)
        if refreshed is not None:
            session.refresh(refreshed)
            session.expunge(refreshed)
            return refreshed
        return processed


def run_integration_worker_forever(
    *,
    source: str | None = None,
    job_type: str | None = None,
    poll_interval_seconds: float = 2.0,
    session_factory: Callable[[], Session] = SessionLocal,
) -> None:
    interval = max(float(poll_interval_seconds), 0.1)
    worker_context = build_worker_heartbeat_context(source=source, job_type=job_type)
    logger.info(
        "integration_worker_started source=%s job_type=%s poll_interval_seconds=%s",
        source,
        job_type,
        interval,
    )
    while True:
        try:
            job = process_next_integration_job(
                source=source,
                job_type=job_type,
                session_factory=session_factory,
                worker_context=worker_context,
            )
        except Exception:  # noqa: BLE001 - nunca deixe uma queda transitória (ex.: DB/SSL EOF) matar o worker
            logger.exception(
                "integration_worker_iteration_failed source=%s job_type=%s; retomando após pausa",
                source,
                job_type,
            )
            job = None
        if job is None:
            sleep(interval)


def run_platform_maintenance_worker_forever(
    *,
    poll_interval_seconds: float = 2.0,
    session_factory: Callable[[], Session] = SessionLocal,
) -> None:
    interval = max(float(poll_interval_seconds), 0.1)
    worker_context = build_worker_heartbeat_context(source="platform", job_type="maintenance")
    logger.info(
        "platform_maintenance_worker_started poll_interval_seconds=%s",
        interval,
    )
    while True:
        try:
            job = process_next_integration_job(
                source="platform",
                job_type="maintenance",
                session_factory=session_factory,
                worker_context=worker_context,
            )
        except Exception:  # noqa: BLE001 - resiliência a quedas transitórias de conexão
            logger.exception("platform_maintenance_worker_iteration_failed; retomando após pausa")
            job = None
        if job is None:
            sleep(interval)


__all__ = [
    "process_claimed_integration_job",
    "process_next_integration_job",
    "run_integration_worker_forever",
    "run_platform_maintenance_worker_forever",
]
