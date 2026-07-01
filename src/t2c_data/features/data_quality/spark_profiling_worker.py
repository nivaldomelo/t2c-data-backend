from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from t2c_data.core.db import SessionLocal
from t2c_data.features.data_quality.guardrails import sanitize_execution_error
from t2c_data.features.data_quality.incident_signals import handle_profiling_incident_signals
from t2c_data.features.data_quality.notifications import notify_dq_profiling_failure
from t2c_data.features.data_quality.profiling_schedules import update_profiling_schedule_run_state
from t2c_data.features.data_quality.profiling_watermarks import (
    finalize_watermark_record,
    open_watermark_record,
    resolve_profiling_window,
)
from t2c_data.features.data_quality.spark_persistence import (
    _profiling_status_from_payload,
    persist_profiling_output,
    persist_profiling_output_into_existing_run,
    validate_profiling_payload,
)
from t2c_data.integrations.spark import SparkSubmitError
from t2c_data.features.data_quality.spark_runs import update_dq_run_status, update_job_run
from t2c_data.features.data_quality.spark_worker_support import (
    audit_dq_run,
    build_connection_reference_args,
    dq_log_context,
    extract_spark_app_id,
    logger,
    resolve_spark_runtime,
    sanitize_process_output,
    serialize_spark_command,
    table_context_from_id_or_fqn,
    temporary_result_file,
    write_job_logs,
)
from t2c_data.models.dq import DQJobRun, DQProfilingSchedule, DQRun, DQTableMetric
from sqlalchemy import select


def execute_profiling_job(
    job_run_id: int,
    *,
    table_id: int | None,
    table_fqn: str | None,
    columns: list[str],
    sample_fraction: float | None,
    user_id: int | None,
    dq_run_id: int | None = None,
) -> None:
    result_file: Path | None = None
    dq_run: DQRun | None = None
    table = None
    schema = None
    datasource = None
    watermark_id: int | None = None
    logger.info(
        "dq_profiling_worker_started",
        extra=dq_log_context(
            job_run_id=job_run_id,
            dq_run_id=dq_run_id,
            table_id=table_id,
            table_fqn=table_fqn,
            job_type="profiling",
        ),
    )
    with SessionLocal() as session:
        job = session.get(DQJobRun, job_run_id)
        if not job:
            return
        spark_config, spark_runner = resolve_spark_runtime(session)
        try:
            table, schema, _database, datasource = table_context_from_id_or_fqn(session, table_id=table_id, table_fqn=table_fqn)
            if dq_run_id:
                dq_run = session.get(DQRun, dq_run_id)
                if dq_run:
                    dq_run.status = "running"
                    dq_run.execution_engine = "spark"
                    dq_run.started_at = datetime.now(timezone.utc)
                    session.add(dq_run)

            job.status = "running"
            job.execution_engine = "spark"
            job.spark_master_url = spark_config.master_url
            job.table_id = table.id
            job.datasource_id = datasource.id
            job.table_fqn = f"{schema.name}.{table.name}"
            session.add(job)
            audit_dq_run(
                session,
                action="dq.profiling.run.start",
                dq_run=dq_run,
                job=job,
                user_id=user_id,
                metadata={"table_id": table.id, "table_fqn": f"{schema.name}.{table.name}"},
            )
            session.commit()

            logger.info(
                "dq_profiling_started",
                extra=dq_log_context(
                    job_run_id=job_run_id,
                    dq_run_id=(dq_run.id if dq_run else dq_run_id),
                    table_id=table.id,
                    table_fqn=f"{schema.name}.{table.name}",
                    job_type="profiling",
                ),
            )
            # Incremental profiling: first run per table is FULL, subsequent runs read
            # only the delta window. The window is decided here and passed to the Spark job.
            window = resolve_profiling_window(session, table_id=table.id, now=datetime.now(timezone.utc))
            watermark_id = open_watermark_record(
                table_id=table.id,
                datasource_id=datasource.id,
                dq_run_id=dq_run.id if dq_run else dq_run_id,
                job_id=job_run_id,
                window=window,
            )
            logger.info(
                "dq_profiling_window_resolved",
                extra={
                    **dq_log_context(
                        job_run_id=job_run_id,
                        dq_run_id=(dq_run.id if dq_run else dq_run_id),
                        table_id=table.id,
                        table_fqn=f"{schema.name}.{table.name}",
                        job_type="profiling",
                    ),
                    "profiling_mode": window.mode,
                    "watermark_column": window.watermark_column,
                    "window_start": window.window_start.isoformat() if window.window_start else None,
                    "window_end": window.window_end.isoformat(),
                },
            )

            result_file = temporary_result_file(job_type="profiling", job_run_id=job_run_id, config=spark_config)
            job_args = [
                *build_connection_reference_args(datasource_id=datasource.id),
                "--table-fqn",
                f"{schema.name}.{table.name}",
                "--output-json",
                str(result_file),
            ]
            if dq_run_id:
                job_args.extend(["--run-id", str(dq_run_id)])
            if columns:
                job_args.extend(["--columns-json", json.dumps(columns)])
            if sample_fraction is not None:
                job_args.extend(["--sample-fraction", str(sample_fraction)])
            job_args.extend(["--profiling-mode", window.mode])
            if window.mode == "delta" and window.watermark_column and window.window_start is not None:
                job_args.extend(
                    [
                        "--watermark-column",
                        window.watermark_column,
                        "--window-start",
                        window.window_start.isoformat(),
                        "--window-end",
                        window.window_end.isoformat(),
                    ]
                )
            completed = spark_runner.run("dq_profiling_job.py", job_args)
            stdout_log = sanitize_process_output(completed.stdout or "")[-20000:]
            stderr_log = sanitize_process_output(completed.stderr or "")[-20000:]
            logs_path = write_job_logs(job_run_id, job_type="profiling", stdout_log=stdout_log, stderr_log=stderr_log, config=spark_config)
            spark_app_id = extract_spark_app_id(stdout_log, stderr_log)
            job.spark_app_id = spark_app_id
            job.spark_master_url = spark_config.master_url
            job.logs_path = logs_path
            job.command = serialize_spark_command(list(completed.args) if isinstance(completed.args, list) else [str(completed.args)])
            job.stdout_log = stdout_log
            job.stderr_log = stderr_log
            if completed.returncode != 0:
                if dq_run:
                    dq_run.spark_app_id = spark_app_id
                    dq_run.log_tail = ((stderr_log or "") + "\n" + (stdout_log or ""))[-4000:]
                    dq_run.error_message = f"spark-submit failed ({completed.returncode})"
                    dq_run.status = "failed"
                    dq_run.finished_at = datetime.now(timezone.utc)
                    ref = dq_run.started_at or dq_run.queued_at
                    if ref and dq_run.finished_at:
                        dq_run.duration_ms = int((dq_run.finished_at - ref).total_seconds() * 1000)
                    session.add(dq_run)
                    update_profiling_schedule_run_state(
                        session,
                        schedule_id=dq_run.profiling_schedule_id,
                        status="failed",
                        error_message=dq_run.error_message,
                        started_at=dq_run.started_at,
                        finished_at=dq_run.finished_at,
                    )
                    try:
                        notify_dq_profiling_failure(
                            session,
                            schedule=session.get(DQProfilingSchedule, dq_run.profiling_schedule_id) if dq_run.profiling_schedule_id else None,
                            table=table,
                            table_fqn=f"{schema.name}.{table.name}" if table and schema else table_fqn,
                            dq_run=dq_run,
                            error_message=dq_run.error_message,
                            reporter_user_id=user_id,
                        )
                    except Exception:
                        pass
                    audit_dq_run(
                        session,
                        action="dq.profiling.run.finish",
                        dq_run=dq_run,
                        job=job,
                        user_id=user_id,
                        metadata={"result": "failed", "return_code": completed.returncode},
                    )
                session.add(job)
                session.commit()
                raise RuntimeError(f"spark-submit failed ({completed.returncode})")
            payload = validate_profiling_payload(json.loads(result_file.read_text()))
            profiling_status = _profiling_status_from_payload(payload)
            payload["sampled"] = bool(sample_fraction is not None)
            payload["sample_ratio"] = sample_fraction
            trigger_type = "scheduled" if dq_run and dq_run.profiling_schedule_id else "manual" if user_id else "system"
            if dq_run_id and dq_run:
                dq_run = persist_profiling_output_into_existing_run(
                    session,
                    dq_run=dq_run,
                    table=table,
                    datasource=datasource,
                    schema_name=schema.name,
                    payload=payload,
                    job_id=job_run_id,
                    created_by_user_id=user_id,
                    trigger_type=trigger_type,
                )
                dq_run.spark_app_id = spark_app_id
                dq_run.log_tail = ((stderr_log or "") + "\n" + (stdout_log or ""))[-4000:]
                dq_run.finished_at = datetime.now(timezone.utc)
                ref = dq_run.started_at or dq_run.queued_at
                if ref and dq_run.finished_at:
                    dq_run.duration_ms = int((dq_run.finished_at - ref).total_seconds() * 1000)
                session.add(dq_run)
            else:
                dq_run = persist_profiling_output(
                    session,
                    table,
                    datasource,
                    schema.name,
                    payload,
                    job_id=job_run_id,
                    created_by_user_id=user_id,
                    trigger_type=trigger_type,
                )
            table_metric = session.scalar(
                select(DQTableMetric)
                .where(DQTableMetric.run_id == dq_run.id, DQTableMetric.table_id == table.id)
                .order_by(DQTableMetric.id.desc())
                .limit(1)
            )
            if table_metric is not None:
                handle_profiling_incident_signals(
                    session,
                    table=table,
                    schema_name=schema.name,
                    dq_run=dq_run,
                    table_metric=table_metric,
                    reporter_user_id=user_id,
                )
            update_profiling_schedule_run_state(
                session,
                schedule_id=dq_run.profiling_schedule_id if dq_run else None,
                status=("success" if profiling_status == "no_data" else profiling_status),
                error_message=None,
                started_at=dq_run.started_at if dq_run else None,
                finished_at=dq_run.finished_at if dq_run else datetime.now(timezone.utc),
            )
            job.status = profiling_status
            job.error_message = None
            job.result_json = {
                "dq_run_id": dq_run.id,
                "status": profiling_status,
                "observation": payload.get("observation"),
                "table_metric": payload,
            }
            session.add(job)
            audit_dq_run(
                session,
                action="dq.profiling.run.finish",
                dq_run=dq_run,
                job=job,
                user_id=user_id,
                metadata={"result": profiling_status, "row_count": payload.get("row_count")},
            )
            session.commit()
            # Advance the watermark only after a confirmed successful persist.
            finalize_watermark_record(
                record_id=watermark_id,
                status=("success" if profiling_status in {"success", "no_data"} else "failed"),
                rows_processed=payload.get("row_count"),
            )
            logger.info(
                "dq_profiling_persisted_to_postgres",
                extra={
                    **dq_log_context(
                        job_run_id=job_run_id,
                        dq_run_id=dq_run.id,
                        table_id=table.id,
                        table_fqn=f"{schema.name}.{table.name}",
                        job_type="profiling",
                    ),
                    "row_count": payload.get("row_count"),
                    "column_count": len(payload.get("columns") or []),
                },
            )
        except SparkSubmitError as exc:
            session.rollback()
            finalize_watermark_record(record_id=watermark_id, status="failed", note="Timeout no Spark — janela mantida para nova tentativa.")
            logger.exception(
                "dq_profiling_timeout",
                extra=dq_log_context(
                    job_run_id=job_run_id,
                    dq_run_id=dq_run_id,
                    table_id=table_id,
                    table_fqn=table_fqn,
                    job_type="profiling",
                ),
            )
            timeout_message = "Tempo limite excedido ao executar profiling Spark."
            if dq_run_id:
                update_dq_run_status(dq_run_id, status="timeout", error_message=timeout_message)
                if dq_run is not None:
                    update_profiling_schedule_run_state(
                        session,
                        schedule_id=dq_run.profiling_schedule_id,
                        status="failed",
                        error_message=timeout_message,
                    )
                    try:
                        notify_dq_profiling_failure(
                            session,
                            schedule=session.get(DQProfilingSchedule, dq_run.profiling_schedule_id) if dq_run.profiling_schedule_id else None,
                            table=table,
                            table_fqn=f"{schema.name}.{table.name}" if table and schema else table_fqn,
                            dq_run=dq_run,
                            error_message=timeout_message,
                            reporter_user_id=user_id,
                        )
                    except Exception:
                        pass
            update_job_run(
                job_run_id,
                status="timeout",
                error_message=timeout_message,
                stderr_log=sanitize_process_output(f"{type(exc).__name__}: {exc}")[-20000:],
            )
        except Exception as exc:  # noqa: BLE001
            session.rollback()
            finalize_watermark_record(record_id=watermark_id, status="failed", note="Falha no profiling — janela mantida para nova tentativa.")
            logger.exception(
                "dq_profiling_failed",
                extra=dq_log_context(
                    job_run_id=job_run_id,
                    dq_run_id=dq_run_id,
                    table_id=table_id,
                    table_fqn=table_fqn,
                    job_type="profiling",
                ),
            )
            safe_error = sanitize_execution_error(
                exc,
                default_message="Falha ao executar profiling DQ no cluster Spark.",
            )
            if dq_run_id:
                update_dq_run_status(dq_run_id, status="failed", error_message=safe_error)
                if dq_run is not None:
                    update_profiling_schedule_run_state(
                        session,
                        schedule_id=dq_run.profiling_schedule_id,
                        status="failed",
                        error_message=safe_error,
                    )
                    try:
                        notify_dq_profiling_failure(
                            session,
                            schedule=session.get(DQProfilingSchedule, dq_run.profiling_schedule_id) if dq_run.profiling_schedule_id else None,
                            table=table,
                            table_fqn=f"{schema.name}.{table.name}" if table and schema else table_fqn,
                            dq_run=dq_run,
                            error_message=safe_error,
                            reporter_user_id=user_id,
                        )
                    except Exception:
                        pass
            elif table is not None:
                try:
                    notify_dq_profiling_failure(
                        session,
                        schedule=None,
                        table=table,
                        table_fqn=f"{schema.name}.{table.name}" if table and schema else table_fqn,
                        dq_run=None,
                        error_message=safe_error,
                        reporter_user_id=user_id,
                    )
                except Exception:
                    pass
            update_job_run(
                job_run_id,
                status="failed",
                error_message=safe_error,
                stderr_log=(f"{type(exc).__name__}: {safe_error}")[-20000:],
            )
        finally:
            if result_file is not None:
                try:
                    result_file.unlink(missing_ok=True)
                except Exception:
                    logger.warning(
                        "dq_profiling_tempfile_cleanup_failed",
                        extra={
                            **dq_log_context(
                                job_run_id=job_run_id,
                                dq_run_id=dq_run_id,
                                table_id=table_id,
                                table_fqn=table_fqn,
                                job_type="profiling",
                            ),
                            "path": str(result_file),
                        },
                    )


__all__ = ["execute_profiling_job"]
