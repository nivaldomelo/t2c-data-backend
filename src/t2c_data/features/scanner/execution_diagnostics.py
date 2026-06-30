from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from t2c_data.models.catalog import DataSource
from t2c_data.models.scan import ScanRun

SCAN_STAGE_MARKER_RE = re.compile(r"\[datasource-scan\]\s+stage=(?P<stage>[a-z_]+)", re.IGNORECASE)


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    return None


def _summary_dict(scan_run: ScanRun) -> dict[str, Any]:
    summary = scan_run.summary if isinstance(scan_run.summary, dict) else {}
    return dict(summary)


def _read_logs_text(logs_path: str | None) -> tuple[str, str]:
    if not logs_path:
        return "", ""
    try:
        content = Path(logs_path).read_text(encoding="utf-8")
    except OSError:
        return "", ""
    if "=== STDERR ===" not in content:
        return content, ""
    stdout_part, _, stderr_part = content.partition("=== STDERR ===")
    stdout = stdout_part.replace("=== STDOUT ===", "", 1).strip()
    stderr = stderr_part.strip()
    return stdout, stderr


def _meaningful_log_excerpt(*texts: str) -> str | None:
    lines: list[str] = []
    for text in texts:
      if not text:
        continue
      for raw_line in str(text).splitlines():
        line = raw_line.strip()
        if not line:
          continue
        lines.append(line)
    for pattern in ("exception", "error", "failed", "denied", "refused", "unknownhost", "jdbc", "attributeerror"):
        for line in reversed(lines):
            if pattern in line.lower():
                return line
    return lines[-1] if lines else None


def extract_scan_stage_markers(*texts: str) -> list[str]:
    stages: list[str] = []
    for text in texts:
        if not text:
            continue
        for match in SCAN_STAGE_MARKER_RE.finditer(text):
            stage = match.group("stage").strip().lower()
            if stage and stage not in stages:
                stages.append(stage)
    return stages


def infer_scan_failure_stage(*, stdout_log: str = "", stderr_log: str = "", fallback_stage: str = "submit") -> str:
    stages = extract_scan_stage_markers(stdout_log, stderr_log)
    if stages:
        return stages[-1]

    combined = f"{stdout_log}\n{stderr_log}".strip().lower()
    if not combined:
        return fallback_stage

    if "spark-submit" in combined and (
        "no such file or directory" in combined
        or "not found" in combined
        or "permission denied" in combined
    ):
        return "submit"
    if "classnotfoundexception" in combined and "org.postgresql.driver" in combined:
        return "startup"
    if "unknownhostexception" in combined or "getaddrinfo" in combined or "name or service not known" in combined:
        return "connection_test"
    if "connection refused" in combined or "could not connect to server" in combined or "network is unreachable" in combined:
        return "connection_test"
    if "permission denied" in combined or "not authorized" in combined or "access denied" in combined:
        return "connection_test"
    if "result file" in combined or "output-json" in combined:
        return "catalog_persist"
    return fallback_stage


def infer_scan_stage_from_summary(scan_run: ScanRun) -> str | None:
    summary = _summary_dict(scan_run)
    failure_stage = summary.get("failure_stage")
    if isinstance(failure_stage, str) and failure_stage.strip():
        return failure_stage.strip().lower()
    stage = summary.get("current_stage") or summary.get("stage")
    if isinstance(stage, str) and stage.strip():
        return stage.strip().lower()
    return None


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> int | None:
    if started_at is None or finished_at is None:
        return None
    return max(int((finished_at - started_at).total_seconds()), 0)


def _coerce_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return default
        try:
            return int(float(raw))
        except ValueError:
            return default
    return default


def serialize_scan_run_detail(scan_run: ScanRun, *, datasource: DataSource | None = None) -> dict[str, Any]:
    summary = _summary_dict(scan_run)
    row_counts = summary.get("row_counts") if isinstance(summary.get("row_counts"), dict) else {}
    discovery = summary.get("discovery") if isinstance(summary.get("discovery"), dict) else {}
    logs_path = summary.get("logs_path") if isinstance(summary.get("logs_path"), str) else None
    stdout_text, stderr_text = _read_logs_text(logs_path)

    submitted_at = _coerce_datetime(summary.get("submitted_at") or scan_run.created_at)
    running_at = _coerce_datetime(summary.get("running_at"))
    finished_at = _coerce_datetime(summary.get("finished_at") or scan_run.updated_at)
    started_at = running_at or submitted_at

    tables_discovered = summary.get("tables")
    if tables_discovered is None:
        tables_discovered = discovery.get("tables")
    schemas_discovered = summary.get("schemas")
    if schemas_discovered is None:
        schemas_discovered = discovery.get("schemas")
    columns_discovered = summary.get("columns")
    if columns_discovered is None:
        columns_discovered = discovery.get("columns")

    spark_app_id = summary.get("spark_app_id")
    spark_driver_id = summary.get("spark_driver_id")
    failure_stage = summary.get("failure_stage") or summary.get("current_stage")
    duration_seconds = summary.get("duration_seconds")
    if duration_seconds is None:
        duration_seconds = _duration_seconds(started_at, finished_at)

    raw_error_message = summary.get("error") or summary.get("error_message")
    raw_error_detail = summary.get("error_detail")
    raw_error_stacktrace = summary.get("error_stacktrace")
    if (not raw_error_detail or str(raw_error_message or "").lower() in {"falha ao executar o scan da fonte de dados.", "scan_failed"}) and (stdout_text or stderr_text):
        raw_error_detail = _meaningful_log_excerpt(stdout_text, stderr_text) or raw_error_detail
    if raw_error_message and str(raw_error_message).strip().lower() in {"falha ao executar o scan da fonte de dados.", "scan_failed"} and raw_error_detail:
        raw_error_message = raw_error_detail
    if not raw_error_stacktrace and (stdout_text or stderr_text):
        raw_error_stacktrace = "\n".join(part for part in (stdout_text, stderr_text) if part)
    if not raw_error_message and raw_error_detail:
        raw_error_message = raw_error_detail

    datasource_name = datasource.name if datasource is not None else None

    return {
        "id": scan_run.id,
        "datasource_id": scan_run.datasource_id,
        "datasource_name": datasource_name,
        "status": scan_run.status,
        "execution_engine": summary.get("execution_engine") or summary.get("engine") or "spark",
        "spark_master_url": summary.get("spark_master_url"),
        "spark_application_id": spark_app_id,
        "spark_driver_id": spark_driver_id,
        "spark_logs_path": logs_path,
        "spark_logs_url": f"/api/v1/scan-runs/{scan_run.id}/logs" if logs_path else None,
        "failure_stage": failure_stage,
        "error_code": summary.get("error_code"),
        "error_message": raw_error_message,
        "error_detail": raw_error_detail,
        "error_stacktrace": raw_error_stacktrace,
        "submitted_at": submitted_at,
        "running_at": running_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "discovery": {
            "schemas": _coerce_int(schemas_discovered),
            "tables": _coerce_int(tables_discovered),
            "columns": _coerce_int(columns_discovered),
        },
        "row_counts": {str(key): _coerce_int(value) for key, value in row_counts.items()},
        "snapshots": summary.get("snapshots"),
        "diffs": summary.get("diffs"),
        "legacy_status": summary.get("legacy_status"),
        "summary": summary,
    }


__all__ = [
    "extract_scan_stage_markers",
    "infer_scan_failure_stage",
    "infer_scan_stage_from_summary",
    "serialize_scan_run_detail",
]
