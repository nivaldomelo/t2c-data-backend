from __future__ import annotations

import logging
from dataclasses import dataclass
from time import sleep

from sqlalchemy import update
from sqlalchemy.orm import Session

from t2c_data.connectors.base import ConnectorError
from t2c_data.core.config import settings
from t2c_data.features.platform.jobs import enqueue_integration_job, finish_integration_job, maybe_start_integration_job
from t2c_data.features.scanner.contracts import DefaultMetadataScanGateway, MetadataScanGateway
from t2c_data.features.scanner.persistence import (
    create_queued_scan_run,
    create_running_scan_run,
    mark_scan_run_failed,
    persist_scan_payload,
)
from t2c_data.models.catalog import DataSource
from t2c_data.models.platform import IntegrationSyncJob
from t2c_data.models.scan import ScanRun

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ScanRuntimeConfig:
    retry_attempts: int
    retry_backoff_ms: int


@dataclass(slots=True)
class DatasourceScanExecutionOutcome:
    scan_run: ScanRun
    job_status: str
    job_records: int | None
    job_error: str | None
    job_context: dict[str, object]


def _normalize_int(value: object | None, default: int, *, minimum: int = 1) -> int:
    try:
        return max(int(value), minimum)
    except (TypeError, ValueError):
        return default


def _scan_runtime_config(datasource: DataSource) -> ScanRuntimeConfig:
    connection_config = datasource.connection_config or {}
    return ScanRuntimeConfig(
        retry_attempts=_normalize_int(
            connection_config.get("scan_retry_attempts"),
            settings.datasource_scan_retry_attempts,
        ),
        retry_backoff_ms=_normalize_int(
            connection_config.get("scan_retry_backoff_ms"),
            settings.datasource_scan_retry_backoff_ms,
            minimum=0,
        ),
    )


def _is_retryable_scan_error(exc: Exception) -> bool:
    if isinstance(exc, ConnectorError):
        return exc.code in {"timeout", "connection_error", "temporary_unavailable", "scan_failed"}
    return True


def sanitize_scan_error(exc: Exception) -> tuple[str, str, str]:
    if isinstance(exc, ConnectorError):
        return exc.message, exc.detail, exc.code

    detail = str(exc).strip() or exc.__class__.__name__
    lowered = detail.lower()

    if "authentication failed" in lowered or "access denied" in lowered:
        return "Credenciais inválidas para executar o scan.", detail[:300], "invalid_credentials"
    if "getaddrinfo" in lowered or "name or service not known" in lowered or "nodename nor servname provided" in lowered:
        return "Host da fonte de dados não encontrado.", detail[:300], "invalid_host"
    if "unknownhostexception" in lowered or "connection refused" in lowered or "could not connect to server" in lowered:
        return "Não foi possível alcançar a fonte de dados a partir do Spark.", detail[:300], "invalid_host"
    if "spark-submit" in lowered and "no such file or directory" in lowered:
        return "O executável spark-submit não está disponível neste ambiente.", detail[:300], "spark_unavailable"
    if "classnotfoundexception" in lowered and "org.postgresql.driver" in lowered:
        return "O driver JDBC do PostgreSQL não está disponível no Spark.", detail[:300], "spark_jdbc_driver_missing"
    if "no suitable driver" in lowered and "postgres" in lowered:
        return "O driver JDBC do PostgreSQL não foi carregado pelo Spark.", detail[:300], "spark_jdbc_driver_missing"
    if "timeout" in lowered or "timed out" in lowered:
        return "Tempo limite excedido ao executar o scan.", detail[:300], "timeout"
    if "not authorized" in lowered or "unauthorized" in lowered:
        return "Sem permissão para listar objetos da fonte de dados.", detail[:300], "permission_denied"
    if "invalid uri" in lowered:
        return "URI inválida para executar o scan.", detail[:300], "invalid_uri"
    return "Falha ao executar o scan da fonte de dados.", detail[:300], "scan_failed"


def execute_datasource_scan(
    session: Session,
    *,
    datasource: DataSource,
    started_by: int | None = None,
    scan_gateway: MetadataScanGateway | None = None,
    scan_run: ScanRun | None = None,
) -> DatasourceScanExecutionOutcome:
    runtime_config = _scan_runtime_config(datasource)
    logger.info(
        "datasource_scan_started datasource_id=%s datasource_name=%s engine=%s retry_attempts=%s",
        datasource.id,
        datasource.name,
        datasource.db_type,
        runtime_config.retry_attempts,
    )
    job_context: dict[str, object] = {
        "datasource_id": datasource.id,
        "datasource_name": datasource.name,
        "engine": datasource.db_type,
    }
    if scan_run is None:
        scan_run = create_running_scan_run(session, datasource_id=datasource.id, started_by=started_by)
    else:
        scan_run.status = "running"
        scan_run.summary = {}
        session.add(scan_run)
        session.commit()
        session.refresh(scan_run)
    job_context["scan_run_id"] = scan_run.id

    try:
        gateway = scan_gateway or DefaultMetadataScanGateway()
        last_error: Exception | None = None
        scanned = None
        for attempt in range(1, runtime_config.retry_attempts + 1):
            try:
                logger.info(
                    "datasource_scan_attempt datasource_id=%s datasource_name=%s engine=%s attempt=%s max_attempts=%s",
                    datasource.id,
                    datasource.name,
                    datasource.db_type,
                    attempt,
                    runtime_config.retry_attempts,
                )
                scanned = gateway.scan(datasource)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= runtime_config.retry_attempts or not _is_retryable_scan_error(exc):
                    raise
                logger.warning(
                    "datasource_scan_attempt_failed datasource_id=%s datasource_name=%s engine=%s attempt=%s max_attempts=%s error=%s",
                    datasource.id,
                    datasource.name,
                    datasource.db_type,
                    attempt,
                    runtime_config.retry_attempts,
                    str(exc)[:300],
                )
                if runtime_config.retry_backoff_ms > 0:
                    sleep(runtime_config.retry_backoff_ms / 1000)

        if scanned is None and last_error is not None:
            raise last_error

        persisted = persist_scan_payload(
            session,
            scan_run=scan_run,
            datasource=datasource,
            scanned=scanned,
        )
        job_status = str(persisted.status or "success")
        job_records = int((persisted.summary or {}).get("tables") or 0)
        job_context = {
            **job_context,
            "status": persisted.status,
            "tables": (persisted.summary or {}).get("tables"),
            "snapshots": (persisted.summary or {}).get("snapshots"),
            "diffs": (persisted.summary or {}).get("diffs"),
            "row_counts": (persisted.summary or {}).get("row_counts"),
        }
        logger.info(
            "datasource_scan_finished datasource_id=%s datasource_name=%s engine=%s status=%s tables=%s",
            datasource.id,
            datasource.name,
            datasource.db_type,
            persisted.status,
            (persisted.summary or {}).get("tables"),
        )
        return DatasourceScanExecutionOutcome(
            scan_run=persisted,
            job_status=job_status,
            job_records=job_records,
            job_error=None,
            job_context=job_context,
        )
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        message, detail, code = sanitize_scan_error(exc)
        logger.exception(
            "scan.run.failed datasource_id=%s engine=%s code=%s detail=%s",
            datasource.id,
            datasource.db_type,
            code,
            detail,
        )
        failed = mark_scan_run_failed(
            session,
            scan_run=scan_run,
            datasource=datasource,
            message=message,
            detail=detail,
            code=code,
        )
        return DatasourceScanExecutionOutcome(
            scan_run=failed,
            job_status="failed",
            job_records=None,
            job_error=str(exc),
            job_context={
                **job_context,
                "error": str(exc),
                "error_code": code,
                "error_detail": detail,
                "error_message": message,
            },
        )


def run_datasource_scan(
    session: Session,
    *,
    datasource: DataSource,
    started_by: int | None = None,
    scan_gateway: MetadataScanGateway | None = None,
) -> ScanRun:
    job_handle = maybe_start_integration_job(
        session,
        source="datasource",
        job_type="scan",
        target_type="datasource",
        target_id=datasource.id,
        target_name=datasource.name,
        trigger_mode="manual",
    )
    outcome = execute_datasource_scan(
        session,
        datasource=datasource,
        started_by=started_by,
        scan_gateway=scan_gateway,
    )
    finish_integration_job(
        session,
        job_handle,
        status=outcome.job_status,
        records_processed=outcome.job_records,
        error=outcome.job_error,
        context_json=outcome.job_context,
    )
    return outcome.scan_run


def enqueue_datasource_scan(
    session: Session,
    *,
    datasource: DataSource,
    started_by: int | None = None,
    trigger_mode: str = "manual",
    schedule_id: int | None = None,
) -> tuple[ScanRun, IntegrationSyncJob | None]:
    scan_run = create_queued_scan_run(session, datasource_id=datasource.id, started_by=started_by)
    session.commit()
    session.refresh(scan_run)
    scan_run_id = scan_run.id
    try:
        job = enqueue_integration_job(
            session,
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=datasource.id,
            target_name=datasource.name,
            trigger_mode=trigger_mode,
            requested_by_user_id=started_by,
            payload_json={
                "datasource_id": datasource.id,
                "started_by": started_by,
                "scan_run_id": scan_run.id,
                "schedule_id": schedule_id,
            },
            context_json={
                "datasource_id": datasource.id,
                "datasource_name": datasource.name,
                "engine": datasource.db_type,
                "scan_run_id": scan_run.id,
                "schedule_id": schedule_id,
            },
        )
    except Exception:
        queued = session.get(ScanRun, scan_run_id)
        if queued is not None:
            session.delete(queued)
            session.commit()
        raise
    session.execute(
        update(ScanRun)
        .where(ScanRun.id == scan_run_id)
        .values(
            summary={
                "queued": True,
                "status": "queued",
                "execution_engine": "spark",
                "integration_job_id": job.id if job is not None else None,
            }
        )
    )
    session.commit()
    refreshed_scan_run = session.get(ScanRun, scan_run_id)
    if refreshed_scan_run is None:
        raise RuntimeError(f"Queued scan run {scan_run_id} was not persisted.")
    return refreshed_scan_run, job


__all__ = [
    "DatasourceScanExecutionOutcome",
    "enqueue_datasource_scan",
    "execute_datasource_scan",
    "run_datasource_scan",
    "sanitize_scan_error",
]
