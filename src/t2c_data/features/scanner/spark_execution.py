from __future__ import annotations

import json
import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from t2c_data.features.data_quality.spark_worker_support import (
    extract_spark_app_id,
    extract_spark_driver_id,
    resolve_spark_runtime,
    sanitize_process_output,
)
from t2c_data.features.platform_settings.results_storage import write_results_text
from t2c_data.features.scanner.application import sanitize_scan_error
from t2c_data.features.scanner.execution_diagnostics import infer_scan_failure_stage
from t2c_data.features.scanner.persistence import mark_scan_run_failed, persist_scan_payload
from t2c_data.features.scanner.types import ScanPayload, ScannedColumn, ScannedTable
from t2c_data.integrations.spark import SparkSubmitRunner, get_spark_submit_config
from t2c_data.models.catalog import DataSource
from t2c_data.models.scan import ScanRun

logger = logging.getLogger(__name__)
SPARK_CONFIG = get_spark_submit_config()
SPARK_RUNNER = SparkSubmitRunner(SPARK_CONFIG)


@dataclass(slots=True)
class SparkDatasourceScanExecutionOutcome:
    scan_run: ScanRun
    job_status: str
    job_records: int | None
    job_error: str | None
    job_context: dict[str, object]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_schema_list(value: list[str] | None) -> list[str]:
    return [str(item).strip() for item in (value or []) if str(item).strip()]


def _read_result_payload(result_file: Path) -> ScanPayload:
    payload = json.loads(result_file.read_text())
    database_name = str(payload.get("database_name") or "").strip()
    tables_payload = payload.get("tables") or []
    if not database_name:
        raise ValueError("Spark scan did not return a database name.")
    if not isinstance(tables_payload, list):
        raise ValueError("Spark scan returned an invalid tables payload.")

    tables: list[ScannedTable] = []
    for table_payload in tables_payload:
        if not isinstance(table_payload, dict):
            continue
        columns_payload = table_payload.get("columns") or []
        columns: list[ScannedColumn] = []
        for column_payload in columns_payload:
            if not isinstance(column_payload, dict):
                continue
            columns.append(
                ScannedColumn(
                    name=str(column_payload.get("name") or "").strip(),
                    data_type=str(column_payload.get("data_type") or "text"),
                    is_primary_key=bool(column_payload.get("is_primary_key")),
                    is_nullable=bool(column_payload.get("is_nullable")),
                    ordinal_position=int(column_payload.get("ordinal_position") or 0),
                    comment=column_payload.get("comment"),
                )
            )
        tables.append(
            ScannedTable(
                schema_name=str(table_payload.get("schema_name") or "").strip(),
                table_name=str(table_payload.get("table_name") or "").strip(),
                table_type=str(table_payload.get("table_type") or "unknown"),
                comment=table_payload.get("comment"),
                columns=columns,
            )
        )

    return ScanPayload(database_name=database_name, tables=tables)


def _write_scan_logs(job_run_id: int, *, stdout_log: str, stderr_log: str, config=None) -> str:
    body = (
        "=== STDOUT ===\n"
        f"{sanitize_process_output(stdout_log).strip()}\n\n"
        "=== STDERR ===\n"
        f"{sanitize_process_output(stderr_log).strip()}\n"
    )
    results_dir = (config or SPARK_CONFIG).results_dir
    return write_results_text(results_dir, f"datasource-scan-run-{job_run_id}.log", body)


def _summary_dict(scan_run: ScanRun) -> dict[str, object]:
    return dict(scan_run.summary or {}) if isinstance(scan_run.summary, dict) else {}


def _failure_excerpt(*texts: str, limit: int = 2000) -> str:
    combined = "\n".join(text.strip() for text in texts if text and text.strip())
    if not combined:
        return ""
    return combined[-limit:]


def _combined_failure_message(stdout_log: str, stderr_log: str) -> tuple[str, str, str]:
    combined = _failure_excerpt(stdout_log, stderr_log, limit=4000)
    if not combined:
        return sanitize_scan_error(RuntimeError("spark-submit failed"))

    lowered = combined.lower()
    if "namespace" in lowered and "jdbc_url" in lowered and "attributeerror" in lowered:
        return (
            "Falha no job Spark: argumentos JDBC ausentes no scan da fonte.",
            combined,
            "scan_job_argument_error",
        )
    if "no such file or directory" in lowered and "spark-submit" in lowered:
        return (
            "O executável spark-submit não está disponível neste ambiente.",
            combined,
            "spark_unavailable",
        )
    if "classnotfoundexception" in lowered and "org.postgresql.driver" in lowered:
        return (
            "O driver JDBC do PostgreSQL não está disponível no Spark.",
            combined,
            "spark_jdbc_driver_missing",
        )
    if "unknownhostexception" in lowered or "getaddrinfo" in lowered or "name or service not known" in lowered:
        return (
            "Spark não conseguiu resolver o host da fonte de dados.",
            combined,
            "invalid_host",
        )
    if "connection refused" in lowered or "could not connect to server" in lowered:
        return (
            "Spark não conseguiu conectar ao PostgreSQL.",
            combined,
            "invalid_host",
        )
    if "permission denied" in lowered or "not authorized" in lowered or "access denied" in lowered:
        return (
            "Usuário sem permissão para executar o scan da fonte de dados.",
            combined,
            "permission_denied",
        )

    message, detail, code = sanitize_scan_error(RuntimeError(combined))
    return message, combined if combined else detail, code


def _update_summary(
    session: Session,
    *,
    scan_run: ScanRun,
    status: str | None = None,
    updates: dict[str, object] | None = None,
) -> ScanRun:
    summary = _summary_dict(scan_run)
    if updates:
        summary.update(updates)
    if status is not None:
        scan_run.status = status
        summary["status"] = status
    scan_run.summary = summary
    session.add(scan_run)
    session.commit()
    session.refresh(scan_run)
    return scan_run


def _discovery_summary(payload: ScanPayload) -> dict[str, int]:
    schemas = {table.schema_name for table in payload.tables if table.schema_name}
    tables = len(payload.tables)
    columns = sum(len(table.columns) for table in payload.tables)
    return {
        "schemas": len(schemas),
        "tables": tables,
        "columns": columns,
    }


def _job_context_base(
    *,
    datasource: DataSource,
    scan_run: ScanRun,
    spark_app_id: str | None,
    spark_driver_id: str | None,
    logs_path: str | None,
    started_at: datetime,
    worker_heartbeat_at: datetime | None,
) -> dict[str, object]:
    return {
        "datasource_id": datasource.id,
        "scan_run_id": scan_run.id,
        "spark_app_id": spark_app_id,
        "spark_driver_id": spark_driver_id,
        "logs_path": logs_path,
        "worker_heartbeat_at": (worker_heartbeat_at or started_at).isoformat(),
    }


def execute_spark_datasource_scan(
    session: Session,
    *,
    datasource: DataSource,
    scan_run: ScanRun,
    started_by: int | None = None,
    integration_job_id: int | None = None,
    worker_heartbeat_at: datetime | None = None,
) -> SparkDatasourceScanExecutionOutcome:
    # Resolve effective Spark config at run time (DB overrides → env → default).
    spark_config, spark_runner = resolve_spark_runtime(session)
    started_at = _utcnow()
    summary_updates = {
        "execution_engine": "spark",
        "spark_master_url": spark_config.master_url,
        "submitted_at": started_at.isoformat(),
        "worker_heartbeat_at": (worker_heartbeat_at or started_at).isoformat(),
        "integration_job_id": integration_job_id,
        "requested_by_user_id": started_by,
        "current_stage": "submit",
    }
    _update_summary(session, scan_run=scan_run, status="submitted", updates=summary_updates)

    result_handle = tempfile.NamedTemporaryFile(prefix=f"datasource-scan-{scan_run.id}-", suffix=".json", delete=False)
    result_file = Path(result_handle.name)
    result_handle.close()
    logs_path: str | None = None
    spark_app_id: str | None = None
    spark_driver_id: str | None = None
    discovery: dict[str, int] = {"schemas": 0, "tables": 0, "columns": 0}
    persist_stage = "catalog_persist"
    try:
        _update_summary(
            session,
            scan_run=scan_run,
            status="running",
            updates={
                "running_at": _utcnow().isoformat(),
                "result_file": str(result_file),
                "current_stage": "startup",
            },
        )
        args = [
            "--datasource-id",
            str(datasource.id),
            "--output-json",
            str(result_file),
            "--include-schemas-json",
            json.dumps(_normalize_schema_list(datasource.include_schemas)),
            "--exclude-schemas-json",
            json.dumps(_normalize_schema_list(datasource.exclude_schemas)),
            "--scan-run-id",
            str(scan_run.id),
        ]
        completed = spark_runner.run("datasource_scan_job.py", args)
        stdout_log = (completed.stdout or "").strip()
        stderr_log = (completed.stderr or "").strip()
        logs_path = _write_scan_logs(scan_run.id, stdout_log=stdout_log, stderr_log=stderr_log, config=spark_config)
        spark_app_id = extract_spark_app_id(stdout_log, stderr_log)
        spark_driver_id = extract_spark_driver_id(stdout_log, stderr_log)

        if completed.returncode != 0:
            message, detail, code = _combined_failure_message(stdout_log, stderr_log)
            failure_stage = infer_scan_failure_stage(stdout_log=stdout_log, stderr_log=stderr_log, fallback_stage="submit")
            finished_at = _utcnow()
            failed_run = mark_scan_run_failed(
                session,
                scan_run=scan_run,
                datasource=datasource,
                message=message,
                detail=detail,
                code=code,
            )
            _update_summary(
                session,
                scan_run=failed_run,
                status="failed",
                updates={
                    "execution_engine": "spark",
                    "spark_master_url": spark_config.master_url,
                    "spark_app_id": spark_app_id,
                    "spark_driver_id": spark_driver_id,
                    "logs_path": logs_path,
                    "logs_url": f"/api/v1/scan-runs/{failed_run.id}/logs",
                    "finished_at": finished_at.isoformat(),
                    "duration_seconds": max(int((finished_at - started_at).total_seconds()), 0),
                    "failure_stage": failure_stage,
                    "current_stage": failure_stage,
                    "error_code": code,
                    "error_detail": detail,
                    "error_stacktrace": _failure_excerpt(stdout_log, stderr_log, limit=8000),
                    "discovery": _discovery_summary(ScanPayload(database_name="", tables=[])),
                },
            )
            return SparkDatasourceScanExecutionOutcome(
                scan_run=failed_run,
                job_status="failed",
                job_records=None,
                job_error=message,
                job_context={
                    **_job_context_base(
                        datasource=datasource,
                        scan_run=failed_run,
                        spark_app_id=spark_app_id,
                        spark_driver_id=spark_driver_id,
                        logs_path=logs_path,
                        started_at=started_at,
                        worker_heartbeat_at=worker_heartbeat_at,
                    ),
                    "spark_master_url": spark_config.master_url,
                    "status": "failed",
                    "error_code": code,
                    "error_detail": detail,
                    "error_message": message,
                    "failure_stage": failure_stage,
                    "finished_at": finished_at.isoformat(),
                },
            )

        payload = _read_result_payload(result_file)
        discovery = _discovery_summary(payload)

        def _track_progress(stage: str) -> None:
            nonlocal persist_stage
            persist_stage = stage

        persisted = persist_scan_payload(
            session,
            scan_run=scan_run,
            datasource=datasource,
            scanned=payload,
            progress_callback=_track_progress,
        )
        legacy_status = str(persisted.status or "success").strip().lower()
        final_status = "succeeded"
        job_status = "partial_success" if legacy_status == "partial_success" else "success"
        finished_at = _utcnow()
        updated_run = _update_summary(
            session,
            scan_run=persisted,
            status=final_status,
            updates={
                "execution_engine": "spark",
                "spark_master_url": spark_config.master_url,
                "spark_app_id": spark_app_id,
                "spark_driver_id": spark_driver_id,
                "logs_path": logs_path,
                "logs_url": f"/api/v1/scan-runs/{persisted.id}/logs",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": max(int((finished_at - started_at).total_seconds()), 0),
                "legacy_status": legacy_status,
                "database": payload.database_name,
                "submitted_at": started_at.isoformat(),
                "running_at": started_at.isoformat(),
                "worker_heartbeat_at": (worker_heartbeat_at or started_at).isoformat(),
                "current_stage": "completed",
                "failure_stage": None,
                "discovery": discovery,
                "schemas": discovery["schemas"],
                "tables": discovery["tables"],
                "columns": discovery["columns"],
            },
        )
        return SparkDatasourceScanExecutionOutcome(
            scan_run=updated_run,
            job_status=job_status,
            job_records=discovery["tables"],
            job_error=None,
            job_context={
                **_job_context_base(
                    datasource=datasource,
                    scan_run=updated_run,
                    spark_app_id=spark_app_id,
                    spark_driver_id=spark_driver_id,
                    logs_path=logs_path,
                    started_at=started_at,
                    worker_heartbeat_at=worker_heartbeat_at,
                ),
                "spark_master_url": spark_config.master_url,
                "status": final_status,
                "legacy_status": legacy_status,
                "database": payload.database_name,
                "tables": discovery["tables"],
                "schemas": discovery["schemas"],
                "columns": discovery["columns"],
                "logs_url": f"/api/v1/scan-runs/{updated_run.id}/logs",
                "finished_at": finished_at.isoformat(),
            },
        )
    except Exception as exc:  # noqa: BLE001
        failure_text = _failure_excerpt(str(exc), limit=4000)
        message, detail, code = sanitize_scan_error(RuntimeError(failure_text or str(exc)))
        failure_stage = persist_stage
        finished_at = _utcnow()
        failed_run = mark_scan_run_failed(
            session,
            scan_run=scan_run,
            datasource=datasource,
            message=message,
            detail=detail,
            code=code,
        )
        _update_summary(
            session,
            scan_run=failed_run,
            status="failed",
            updates={
                "execution_engine": "spark",
                "spark_master_url": spark_config.master_url,
                "spark_app_id": spark_app_id,
                "spark_driver_id": spark_driver_id,
                "logs_path": logs_path,
                "logs_url": f"/api/v1/scan-runs/{failed_run.id}/logs" if logs_path else None,
                "finished_at": finished_at.isoformat(),
                "duration_seconds": max(int((finished_at - started_at).total_seconds()), 0),
                "failure_stage": failure_stage,
                "current_stage": failure_stage,
                "error_code": code,
                "error_detail": detail,
                "error_stacktrace": failure_text or str(exc),
                "discovery": discovery,
            },
        )
        logger.exception(
            "datasource_spark_scan_failed datasource_id=%s scan_run_id=%s code=%s stage=%s detail=%s",
            datasource.id,
            scan_run.id,
            code,
            failure_stage,
            detail,
        )
        return SparkDatasourceScanExecutionOutcome(
            scan_run=failed_run,
            job_status="failed",
            job_records=None,
            job_error=message,
            job_context={
                **_job_context_base(
                    datasource=datasource,
                    scan_run=failed_run,
                    spark_app_id=spark_app_id,
                    spark_driver_id=spark_driver_id,
                    logs_path=logs_path,
                    started_at=started_at,
                    worker_heartbeat_at=worker_heartbeat_at,
                ),
                "spark_master_url": spark_config.master_url,
                "status": "failed",
                "error_code": code,
                "error_detail": detail,
                "error_message": message,
                "failure_stage": failure_stage,
                "logs_url": f"/api/v1/scan-runs/{failed_run.id}/logs" if logs_path else None,
                "finished_at": finished_at.isoformat(),
                "discovery": discovery,
            },
        )
    finally:
        result_file.unlink(missing_ok=True)


__all__ = ["SPARK_CONFIG", "SPARK_RUNNER", "SparkDatasourceScanExecutionOutcome", "execute_spark_datasource_scan"]
