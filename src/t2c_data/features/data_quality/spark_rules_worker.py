from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from t2c_data.core.db import SessionLocal
from t2c_data.features.data_quality.guardrails import sanitize_execution_error
from t2c_data.features.data_quality.spark_persistence import persist_rules_output
from t2c_data.features.data_quality.spark_runs import update_dq_run_status, update_job_run
from t2c_data.features.data_quality.latest_runs import sync_latest_snapshot_for_job
from t2c_data.features.data_quality.spark_worker_support import (
    SPARK_CONFIG,
    SPARK_RUNNER,
    audit_dq_run,
    build_connection_reference_args,
    dq_log_context,
    extract_spark_app_id,
    logger,
    sanitize_process_output,
    serialize_spark_command,
    table_context_from_id_or_fqn,
    temporary_result_file,
    write_job_logs,
)
from t2c_data.models.dq import DQJobRun, DQRule, DQRun


def execute_rules_job(
    job_run_id: int,
    *,
    table_id: int | None,
    table_fqn: str | None,
    rule_ids: list[int],
    user_id: int | None,
    dq_run_id: int | None = None,
) -> None:
    result_file: Path | None = None
    logger.info(
        "dq_rules_worker_started",
        extra=dq_log_context(
            job_run_id=job_run_id,
            dq_run_id=dq_run_id,
            table_id=table_id,
            table_fqn=table_fqn,
            job_type="rules",
        ),
    )
    with SessionLocal() as session:
        job = session.get(DQJobRun, job_run_id)
        if not job:
            return
        try:
            table, schema, _database, datasource = table_context_from_id_or_fqn(session, table_id=table_id, table_fqn=table_fqn)
            rules_query = select(DQRule).where(
                DQRule.is_active.is_(True),
                DQRule.archived.is_(False),
                DQRule.rule_definition_json.is_not(None),
                DQRule.table_id == table.id,
            )
            if rule_ids:
                rules_query = rules_query.where(DQRule.id.in_(rule_ids))
            rules = session.scalars(rules_query.order_by(DQRule.id)).all()
            if not rules:
                raise ValueError("No active structured rules found for table")

            if dq_run_id:
                dq_run = session.get(DQRun, dq_run_id)
                if dq_run:
                    dq_run.status = "running"
                    dq_run.execution_engine = "spark"
                    dq_run.started_at = datetime.now(timezone.utc)
                    session.add(dq_run)
            else:
                dq_run = None

            job.status = "running"
            job.execution_engine = "spark"
            job.spark_master_url = SPARK_CONFIG.master_url
            job.table_id = table.id
            job.datasource_id = datasource.id
            job.table_fqn = f"{schema.name}.{table.name}"
            session.add(job)
            sync_latest_snapshot_for_job(
                session,
                job_run=job,
                rule_ids=[rule.id for rule in rules],
                table_id=table.id,
            )
            audit_dq_run(
                session,
                action="dq.rules.run.start",
                dq_run=dq_run,
                job=job,
                user_id=user_id,
                metadata={"table_id": table.id, "table_fqn": f"{schema.name}.{table.name}", "rules_count": len(rules)},
            )
            session.commit()

            result_file = temporary_result_file(job_type="rules", job_run_id=job_run_id)
            rules_payload = [
                {
                    "id": r.id,
                    "name": r.name,
                    "severity": r.severity,
                    "rule_type": r.rule_type,
                    "rule_definition_json": r.rule_definition_json,
                    "table_fqn": r.table_fqn,
                }
                for r in rules
            ]
            job_args = [
                *build_connection_reference_args(datasource_id=datasource.id),
                "--table-fqn",
                f"{schema.name}.{table.name}",
                "--rules-json",
                json.dumps(rules_payload),
                "--output-json",
                str(result_file),
            ]
            if dq_run_id:
                job_args.extend(["--run-id", str(dq_run_id)])
            completed = SPARK_RUNNER.run("dq_rules_job.py", job_args)
            stdout_log = sanitize_process_output(completed.stdout or "")[-20000:]
            stderr_log = sanitize_process_output(completed.stderr or "")[-20000:]
            logs_path = write_job_logs(job_run_id, job_type="rules", stdout_log=stdout_log, stderr_log=stderr_log)
            spark_app_id = extract_spark_app_id(stdout_log, stderr_log)
            job.spark_app_id = spark_app_id
            job.spark_master_url = SPARK_CONFIG.master_url
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
                    audit_dq_run(
                        session,
                        action="dq.rules.run.finish",
                        dq_run=dq_run,
                        job=job,
                        user_id=user_id,
                        metadata={"result": "failed", "return_code": completed.returncode},
                    )
                session.add(job)
                session.commit()
                raise RuntimeError(f"spark-submit failed ({completed.returncode})")
            payload = json.loads(result_file.read_text())
            created_runs = persist_rules_output(session, table=table, rules=rules, payload=payload, reporter_user_id=user_id)
            session.flush()
            if dq_run:
                dq_run.status = "success"
                dq_run.execution_engine = "spark"
                dq_run.spark_app_id = spark_app_id
                dq_run.log_tail = ((stderr_log or "") + "\n" + (stdout_log or ""))[-4000:]
                dq_run.error_message = None
                dq_run.finished_at = datetime.now(timezone.utc)
                ref = dq_run.started_at or dq_run.queued_at
                if ref and dq_run.finished_at:
                    dq_run.duration_ms = int((dq_run.finished_at - ref).total_seconds() * 1000)
                session.add(dq_run)
            job.status = "success"
            job.result_json = {
                "requested_rule_ids": [r.id for r in rules],
                "rule_run_ids": [r.id for r in created_runs],
                "summary": payload.get("summary", {}),
                "violations_count_total": sum(int(r.violations_count or 0) for r in created_runs),
            }
            session.add(job)
            sync_latest_snapshot_for_job(
                session,
                job_run=job,
                rule_ids=[r.id for r in rules],
                table_id=table.id,
            )
            audit_dq_run(
                session,
                action="dq.rules.run.finish",
                dq_run=dq_run,
                job=job,
                user_id=user_id,
                metadata={
                    "result": "success",
                    "rules_count": len(created_runs),
                    "violations_count_total": sum(int(r.violations_count or 0) for r in created_runs),
                },
            )
            session.commit()
        except Exception as exc:  # noqa: BLE001
            session.rollback()
            logger.exception(
                "dq_rules_failed",
                extra=dq_log_context(
                    job_run_id=job_run_id,
                    dq_run_id=dq_run_id,
                    table_id=table_id,
                    table_fqn=table_fqn,
                    job_type="rules",
                ),
            )
            safe_error = sanitize_execution_error(
                exc,
                default_message="Falha ao executar regras DQ no cluster Spark.",
            )
            try:
                with SessionLocal() as failure_session:
                    failed_job = failure_session.get(DQJobRun, job_run_id)
                    if failed_job is not None:
                        failed_job.status = "failed"
                        failed_job.error_message = safe_error
                        if table_id is not None:
                            failed_job.table_id = table_id
                        if table_fqn is not None:
                            failed_job.table_fqn = table_fqn
                        failure_session.add(failed_job)
                        sync_latest_snapshot_for_job(
                            failure_session,
                            job_run=failed_job,
                            rule_ids=rule_ids,
                            table_id=table_id,
                        )
                        failure_session.commit()
            except Exception:  # noqa: BLE001
                logger.exception(
                    "dq_rules_failed_snapshot_update",
                    extra=dq_log_context(
                        job_run_id=job_run_id,
                        dq_run_id=dq_run_id,
                        table_id=table_id,
                        table_fqn=table_fqn,
                        job_type="rules",
                    ),
                )
            if dq_run_id:
                update_dq_run_status(dq_run_id, status="failed", error_message=safe_error)
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
                        "dq_rules_tempfile_cleanup_failed",
                        extra={
                            **dq_log_context(
                                job_run_id=job_run_id,
                                dq_run_id=dq_run_id,
                                table_id=table_id,
                                table_fqn=table_fqn,
                                job_type="rules",
                            ),
                            "path": str(result_file),
                        },
                    )


__all__ = ["execute_rules_job"]
