from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from t2c_data.core.json_utils import to_jsonable
from t2c_data.models.operations import OperationalFailureEvent, OperationalFailureTaxonomy


DEFAULT_TAXONOMY: list[dict[str, object]] = [
    {"code": "AUTHENTICATION_ERROR", "name": "Authentication error", "default_severity": "high", "retryable": False},
    {"code": "AUTHORIZATION_ERROR", "name": "Authorization error", "default_severity": "high", "retryable": False},
    {"code": "CONNECTIVITY_ERROR", "name": "Connectivity error", "default_severity": "high", "retryable": True},
    {"code": "SCHEMA_DRIFT", "name": "Schema drift", "default_severity": "medium", "retryable": False},
    {"code": "SQL_PARSE_ERROR", "name": "SQL parse or syntax error", "default_severity": "medium", "retryable": False},
    {"code": "PERMISSION_ERROR", "name": "Permission error", "default_severity": "high", "retryable": False},
    {"code": "TIMEOUT_ERROR", "name": "Timeout error", "default_severity": "high", "retryable": True},
    {"code": "RESOURCE_VOLUME_ERROR", "name": "Resource or volume error", "default_severity": "high", "retryable": True},
    {"code": "RULE_EXECUTION_ERROR", "name": "Rule execution error", "default_severity": "medium", "retryable": True},
    {"code": "VALIDATION_ERROR", "name": "Validation error", "default_severity": "medium", "retryable": False},
    {"code": "EXTERNAL_DEPENDENCY_ERROR", "name": "External dependency error", "default_severity": "high", "retryable": True},
    {"code": "UNKNOWN_OPERATIONAL_ERROR", "name": "Unknown operational error", "default_severity": "medium", "retryable": True},
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def classify_operational_error(exc: Exception | str, *, source: str) -> tuple[str, str, bool]:
    message = str(exc).lower()
    if "authentication" in message or "invalid password" in message:
        return "AUTHENTICATION_ERROR", "high", False
    if "permission" in message or "not authorized" in message or "forbidden" in message:
        return "AUTHORIZATION_ERROR", "high", False
    if "timeout" in message or "timed out" in message:
        return "TIMEOUT_ERROR", "high", True
    if (
        "could not connect" in message
        or "connection refused" in message
        or "getaddrinfo" in message
        or "network is unreachable" in message
        or "server closed the connection unexpectedly" in message
        or "connection reset" in message
        or "no route to host" in message
    ):
        return "CONNECTIVITY_ERROR", "high", True
    if "host.docker.internal" in message:
        return "CONNECTIVITY_ERROR", "high", True
    if "syntax" in message or "parse" in message:
        return "SQL_PARSE_ERROR", "medium", False
    if "schema" in message and "not found" in message:
        return "SCHEMA_DRIFT", "medium", False
    if "memory" in message or "too large" in message or "out of" in message:
        return "RESOURCE_VOLUME_ERROR", "high", True
    if "rule" in message or "constraint" in message:
        return "RULE_EXECUTION_ERROR", "medium", True
    if isinstance(exc, SQLAlchemyError):
        return "SQL_PARSE_ERROR", "medium", False
    return "UNKNOWN_OPERATIONAL_ERROR", "medium", True


def record_operational_failure(
    session: Session,
    *,
    source: str,
    message: str,
    category_code: str | None = None,
    severity: str | None = None,
    retryable: bool | None = None,
    error_type: str | None = None,
    table_id: int | None = None,
    datasource_id: int | None = None,
    scheduler_name: str | None = None,
    job_name: str | None = None,
    route: str | None = None,
    external_reference: str | None = None,
    context: dict[str, object] | None = None,
) -> OperationalFailureEvent:
    category = category_code or "UNKNOWN_OPERATIONAL_ERROR"
    taxonomy = session.get(OperationalFailureTaxonomy, category)
    if taxonomy is None:
        category = "UNKNOWN_OPERATIONAL_ERROR"
        taxonomy = session.get(OperationalFailureTaxonomy, category)

    resolved_severity = severity or (taxonomy.default_severity if taxonomy is not None else "medium")
    resolved_retryable = retryable if retryable is not None else (taxonomy.retryable if taxonomy is not None else True)

    event = OperationalFailureEvent(
        occurred_at=_now(),
        category_code=category,
        severity=resolved_severity,
        retryable=resolved_retryable,
        source=source,
        error_type=error_type,
        message=message[:2000],
        context_json=to_jsonable(context) if context else None,
        table_id=table_id,
        datasource_id=datasource_id,
        scheduler_name=scheduler_name,
        job_name=job_name,
        route=route,
        external_reference=external_reference,
    )
    session.add(event)
    return event


def failure_summary(session: Session, *, limit: int = 30) -> dict[str, object]:
    recent = session.scalars(
        select(OperationalFailureEvent).order_by(OperationalFailureEvent.occurred_at.desc()).limit(limit)
    ).all()
    counts = session.execute(
        select(OperationalFailureEvent.category_code, func.count(OperationalFailureEvent.id))
        .group_by(OperationalFailureEvent.category_code)
        .order_by(func.count(OperationalFailureEvent.id).desc())
        .limit(12)
    ).all()
    return {
        "recent": [
            {
                "id": item.id,
                "occurred_at": item.occurred_at,
                "category_code": item.category_code,
                "severity": item.severity,
                "source": item.source,
                "message": item.message,
                "table_id": item.table_id,
                "datasource_id": item.datasource_id,
            }
            for item in recent
        ],
        "by_category": [{"category": row[0], "count": int(row[1])} for row in counts],
    }


__all__ = ["DEFAULT_TAXONOMY", "classify_operational_error", "record_operational_failure", "failure_summary"]
