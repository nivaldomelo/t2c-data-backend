from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, TypeVar

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.json_utils import make_json_safe
from t2c_data.features.operations.failures import classify_operational_error
from t2c_data.models.integrations import IntegrationHealth, IntegrationHealthHistory

T = TypeVar("T")

INTEGRATION_HEALTH_STATUSES = {"healthy", "degraded", "unavailable", "misconfigured", "empty"}
DEFAULT_BREAKER_THRESHOLD = 3
DEFAULT_BREAKER_OPEN_SECONDS = 300
DEFAULT_HEALTH_RETRY_ATTEMPTS = 3
DEFAULT_HEALTH_RETRY_BASE_DELAY_SECONDS = 0.5
DEFAULT_HEALTH_RETRY_MAX_DELAY_SECONDS = 4.0


@dataclass(slots=True)
class IntegrationHealthSnapshot:
    integration_name: str
    status: str
    status_message: str | None
    category: str | None
    base_url: str | None
    checked_at: datetime
    reason_code: str | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    consecutive_failures: int = 0
    failure_count: int = 0
    latency_ms: int | None = None
    error_type: str | None = None
    error_summary: str | None = None
    details_json: dict[str, Any] | list[Any] | None = None
    breaker_state: str = "closed"
    breaker_open_until_at: datetime | None = None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def retry_with_backoff(
    operation: Callable[[], T],
    *,
    attempts: int = DEFAULT_HEALTH_RETRY_ATTEMPTS,
    base_delay_seconds: float = DEFAULT_HEALTH_RETRY_BASE_DELAY_SECONDS,
    max_delay_seconds: float = DEFAULT_HEALTH_RETRY_MAX_DELAY_SECONDS,
    retryable: Callable[[Exception], bool] | None = None,
) -> T:
    last_exc: Exception | None = None
    for attempt in range(1, max(int(attempts or 1), 1) + 1):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            should_retry = retryable(exc) if retryable is not None else True
            if attempt >= attempts or not should_retry:
                raise
            delay = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
            jitter = random.uniform(0.0, max(0.05, delay * 0.2))
            time.sleep(delay + jitter)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("retry_with_backoff exhausted without exception")


def get_integration_health(session: Session, integration_name: str) -> IntegrationHealth | None:
    return session.scalar(select(IntegrationHealth).where(IntegrationHealth.integration_name == integration_name))


def get_integration_health_details(health: IntegrationHealth | None) -> dict[str, Any]:
    if health is None:
        return {}
    details = health.details_json
    if isinstance(details, dict):
        return details
    return {}


def is_breaker_open(health: IntegrationHealth | None, *, current_time: datetime | None = None) -> bool:
    if health is None:
        return False
    if (health.breaker_state or "").lower() != "open":
        return False
    if health.breaker_open_until_at is None:
        return False
    now = current_time or now_utc()
    return health.breaker_open_until_at > now


def _copy_history(session: Session, health: IntegrationHealth) -> None:
    history = IntegrationHealthHistory(
        integration_health_id=health.id,
        integration_name=health.integration_name,
        status=health.status,
        status_message=health.status_message,
        reason_code=health.reason_code,
        category=health.category,
        base_url=health.base_url,
        checked_at=health.checked_at,
        last_success_at=health.last_success_at,
        last_failure_at=health.last_failure_at,
        consecutive_failures=health.consecutive_failures,
        failure_count=health.failure_count,
        latency_ms=health.latency_ms,
        error_type=health.error_type,
        error_summary=health.error_summary,
        details_json=make_json_safe(health.details_json),
        breaker_state=health.breaker_state,
        breaker_open_until_at=health.breaker_open_until_at,
    )
    session.add(history)


def upsert_integration_health(
    session: Session,
    snapshot: IntegrationHealthSnapshot,
    *,
    record_history: bool = True,
) -> IntegrationHealth:
    health = get_integration_health(session, snapshot.integration_name)
    if health is None:
        health = IntegrationHealth(integration_name=snapshot.integration_name)
        session.add(health)

    health.status = snapshot.status
    health.status_message = snapshot.status_message
    health.reason_code = snapshot.reason_code or snapshot.error_type or snapshot.category
    health.category = snapshot.category
    health.base_url = snapshot.base_url
    health.checked_at = snapshot.checked_at
    health.last_success_at = snapshot.last_success_at
    health.last_failure_at = snapshot.last_failure_at
    health.consecutive_failures = max(int(snapshot.consecutive_failures or 0), 0)
    health.failure_count = max(int(snapshot.failure_count or 0), 0)
    health.latency_ms = snapshot.latency_ms
    health.error_type = snapshot.error_type
    health.error_summary = snapshot.error_summary
    health.details_json = make_json_safe(snapshot.details_json)
    health.breaker_state = snapshot.breaker_state
    health.breaker_open_until_at = snapshot.breaker_open_until_at
    session.flush()
    if record_history:
        _copy_history(session, health)
    return health


def build_retryable_predicate(*, source: str) -> Callable[[Exception], bool]:
    def _predicate(exc: Exception) -> bool:
        _category, _severity, retryable = classify_operational_error(exc, source=source)
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code >= 500 or status_code == 429
        return retryable

    return _predicate


def classify_integration_issue(exc: Exception | str, *, integration_name: str, phase: str) -> dict[str, Any]:
    message = str(exc)
    lowered = message.lower()
    source = f"integrations.{integration_name}.{phase}"
    category_code, severity, retryable = classify_operational_error(exc, source=source)
    status = "unavailable"
    error_type = category_code.lower()
    category = "connectivity"

    if "not configured" in lowered or "missing" in lowered or "requires" in lowered or "unsupported" in lowered:
        status = "misconfigured"
        category = "configuration"
        error_type = "configuration_error"
        retryable = False
    elif "unauthorized" in lowered or "invalid password" in lowered or "authentication failed" in lowered or "403" in lowered or "401" in lowered:
        status = "misconfigured"
        category = "configuration"
        error_type = "auth_error"
        retryable = False
    elif "does not exist" in lowered or "relation" in lowered and "not exist" in lowered:
        status = "misconfigured"
        category = "configuration"
        error_type = "schema_missing"
        retryable = False
    elif "empty" in lowered and ("sync" in lowered or "source" in lowered):
        status = "empty"
        category = "consumption"
        error_type = "empty_source"
        retryable = False
    elif "partial sync" in lowered or "unresolved" in lowered or "warnings" in lowered:
        status = "degraded"
        category = "sync"
        error_type = "partial_sync"
    elif "timeout" in lowered or "timed out" in lowered:
        status = "unavailable"
        category = "connectivity"
        error_type = "timeout_error"
        retryable = True
    elif "name or service not known" in lowered or "getaddrinfo" in lowered or "no route to host" in lowered or "network is unreachable" in lowered:
        status = "unavailable"
        category = "connectivity"
        error_type = "dns_error" if "getaddrinfo" in lowered or "name or service not known" in lowered else "network_error"
        retryable = True
    elif "connection refused" in lowered or "connection reset" in lowered or "could not connect" in lowered:
        status = "unavailable"
        category = "connectivity"
        error_type = "network_error"
        retryable = True
    elif retryable:
        status = "degraded"
        category = "sync"
        error_type = "unknown_error"

    return {
        "status": status,
        "category": category,
        "error_type": error_type,
        "retryable": retryable,
        "severity": severity,
        "message": message,
    }


def open_breaker(snapshot: IntegrationHealthSnapshot, *, threshold: int = DEFAULT_BREAKER_THRESHOLD, open_seconds: int = DEFAULT_BREAKER_OPEN_SECONDS) -> IntegrationHealthSnapshot:
    if snapshot.consecutive_failures < threshold:
        snapshot.breaker_state = "closed"
        snapshot.breaker_open_until_at = None
        return snapshot
    snapshot.breaker_state = "open"
    snapshot.breaker_open_until_at = snapshot.checked_at + timedelta(seconds=max(int(open_seconds or 1), 1))
    return snapshot


def close_breaker(snapshot: IntegrationHealthSnapshot) -> IntegrationHealthSnapshot:
    snapshot.breaker_state = "closed"
    snapshot.breaker_open_until_at = None
    return snapshot


__all__ = [
    "DEFAULT_BREAKER_OPEN_SECONDS",
    "DEFAULT_BREAKER_THRESHOLD",
    "IntegrationHealthSnapshot",
    "build_retryable_predicate",
    "classify_integration_issue",
    "close_breaker",
    "get_integration_health_details",
    "get_integration_health",
    "is_breaker_open",
    "now_utc",
    "open_breaker",
    "retry_with_backoff",
    "upsert_integration_health",
]
