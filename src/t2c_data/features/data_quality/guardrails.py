from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from psycopg import Cursor

from t2c_data.core.config import settings
from t2c_data.core.redaction import redact_sensitive_string
from t2c_data.services.data_quality import local_execution_disabled_message

_SENSITIVE_ERROR_TOKENS = (
    "password",
    "passwd",
    "secret",
    "token",
    "authorization",
    "cookie",
    "postgresql://",
    "jdbc:",
    "aws_access_key",
    "aws_secret_access_key",
)


def python_execution_allowed() -> bool:
    return False


def ensure_python_execution_allowed() -> None:
    raise RuntimeError(local_execution_disabled_message())


def apply_postgres_read_only_guardrails(cursor: Cursor[Any], *, extra_statements: Iterable[str] | None = None) -> None:
    statements = [
        "SET statement_timeout = %s",
        "SET lock_timeout = %s",
        "SET idle_in_transaction_session_timeout = %s",
        "SET default_transaction_read_only = on",
    ]
    parameters = [
        max(int(settings.dq_sql_statement_timeout_ms or 30000), 1000),
        max(int(settings.dq_sql_lock_timeout_ms or 5000), 500),
        max(int(settings.dq_sql_idle_transaction_timeout_ms or 30000), 1000),
    ]

    cursor.execute(statements[0], (parameters[0],))
    cursor.execute(statements[1], (parameters[1],))
    cursor.execute(statements[2], (parameters[2],))
    cursor.execute(statements[3])

    for statement in extra_statements or ():
        cursor.execute(statement)


def sanitize_execution_error(exc: Exception, *, default_message: str) -> str:
    raw_message = str(exc or "").strip()
    if not raw_message:
        return default_message

    normalized = raw_message.lower()
    if any(token in normalized for token in _SENSITIVE_ERROR_TOKENS):
        return default_message

    compact = " ".join(raw_message.split())
    compact = redact_sensitive_string(compact)
    if len(compact) > 280:
        compact = f"{compact[:277]}..."
    return compact


__all__ = [
    "apply_postgres_read_only_guardrails",
    "ensure_python_execution_allowed",
    "python_execution_allowed",
    "sanitize_execution_error",
]
