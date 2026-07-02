from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from t2c_data.core.redaction import format_command_for_log, redact_sensitive_string
from t2c_data.features.data_quality.queries import resolve_table_context_by_fqn
from t2c_data.integrations.spark import SparkSubmitRunner, get_spark_submit_config
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRun
from t2c_data.services.audit import write_audit_log_sync

logger = logging.getLogger(__name__)
SPARK_CONFIG = get_spark_submit_config()
SPARK_RUNNER = SparkSubmitRunner(SPARK_CONFIG)


def dq_log_context(
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


def audit_dq_run(
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
        pass


def extract_spark_app_id(*texts: str) -> str | None:
    patterns = [
        r"\bapplication[_-]\d+[_-]\d+\b",
        r"\bdriver-[A-Za-z0-9_-]+\b",
        r"\bapp-[A-Za-z0-9_-]+\b",
    ]
    for text in texts:
        if not text:
            continue
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(0)
    return None


def extract_spark_driver_id(*texts: str) -> str | None:
    patterns = [
        r"\bdriver-[A-Za-z0-9_-]+\b",
        r"\bexecutor-[A-Za-z0-9_-]+\b",
    ]
    for text in texts:
        if not text:
            continue
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(0)
    return None


def resolve_spark_runtime(session) -> tuple["SparkSubmitConfig", SparkSubmitRunner]:
    """Resolve the effective Spark config (DB overrides → env → default) plus a runner.

    Lazily imported to avoid import cycles; falls back to the module env/default singletons."""
    from t2c_data.features.platform_settings.resolvers import resolve_spark_config

    config = resolve_spark_config(session)
    return config, SparkSubmitRunner(config)


def write_job_logs(job_run_id: int, *, job_type: str, stdout_log: str, stderr_log: str, config=None) -> str:
    from t2c_data.features.platform_settings.results_storage import write_results_text

    body = (
        "=== STDOUT ===\n"
        f"{sanitize_process_output(stdout_log or '')}\n\n"
        "=== STDERR ===\n"
        f"{sanitize_process_output(stderr_log or '')}\n"
    )
    results_dir = (config or SPARK_CONFIG).results_dir
    return write_results_text(results_dir, f"{job_type}-run-{job_run_id}.log", body)


_JDBC_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_JDBC_DBNAME_RE = re.compile(r"^[A-Za-z0-9._$-]+$")


def jdbc_config_for_datasource(datasource: DataSource) -> dict[str, str]:
    if datasource.db_type != "postgres":
        raise ValueError("Spark DQ MVP currently supports only Postgres datasources")
    # Anti-injeção de parâmetros JDBC: host/database entram cru na URL do driver → validar charset.
    host = str(datasource.host or "")
    database = str(datasource.database or "")
    if not _JDBC_HOSTNAME_RE.match(host):
        raise ValueError("Host do datasource inválido para JDBC.")
    if not _JDBC_DBNAME_RE.match(database):
        raise ValueError("Nome do banco do datasource inválido para JDBC.")
    port = int(datasource.port or 5432)
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": datasource.username,
        "password": datasource.password,
    }


def sanitize_process_output(value: str) -> str:
    return redact_sensitive_string(value or "")


def temporary_connection_file(*, job_type: str, job_run_id: int, datasource_id: int) -> Path:
    handle = tempfile.NamedTemporaryFile(prefix=f"{job_type}-conn-{job_run_id}-", suffix=".json", delete=False)
    path = Path(handle.name)
    payload = {
        "datasource_id": datasource_id,
    }
    try:
        with handle:
            handle.write(json.dumps(payload).encode("utf-8"))
        os.chmod(path, 0o600)
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def build_connection_reference_args(*, datasource_id: int) -> list[str]:
    return [
        "--datasource-id",
        str(datasource_id),
    ]


def serialize_spark_command(command: list[str] | tuple[str, ...]) -> str:
    return format_command_for_log(command)


def table_context_from_id_or_fqn(session, *, table_id: int | None, table_fqn: str | None) -> tuple[TableEntity, Schema, Database, DataSource]:
    if table_id is not None:
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


def temporary_result_file(*, job_type: str, job_run_id: int, config=None) -> Path:
    return (config or SPARK_CONFIG).temporary_result_file(job_type=job_type, job_run_id=job_run_id)


__all__ = [
    "SPARK_CONFIG",
    "SPARK_RUNNER",
    "audit_dq_run",
    "build_connection_reference_args",
    "dq_log_context",
    "extract_spark_app_id",
    "extract_spark_driver_id",
    "jdbc_config_for_datasource",
    "logger",
    "resolve_spark_runtime",
    "sanitize_process_output",
    "serialize_spark_command",
    "table_context_from_id_or_fqn",
    "temporary_connection_file",
    "temporary_result_file",
    "write_job_logs",
]
