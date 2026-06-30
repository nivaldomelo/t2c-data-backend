from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Depends, HTTPException, Request, Response, status
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.db import get_db
from t2c_data.core.external_auth import get_external_api_key
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.platform.api_keys import ApiKeyAuthResult
from t2c_data.models.platform import ApiRateLimitBucket


DEFAULT_ROUTE_LIMITS: dict[str, int] = {
    "external.catalog": 600,
    "external.explorer": 300,
    "external.governance": 240,
    "external.certification": 240,
    "external.dq": 240,
    "external.incidents": 240,
    "external.lineage": 180,
    "external.platform": 240,
    "external.tags": 300,
    "external.glossary": 300,
    "external.ping": 600,
    "external.default": 600,
}


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    reset_at: datetime
    bucket_start: datetime


def _normalize_path(path: str) -> str:
    return (path or "").strip().lower()


def _external_route_group(path: str) -> str:
    normalized = _normalize_path(path)
    if normalized.startswith("/api/v1/external/"):
        tail = normalized.replace("/api/v1/external/", "", 1)
    elif normalized.startswith("/api/external/"):
        tail = normalized.replace("/api/external/", "", 1)
    elif normalized.startswith("/external/"):
        tail = normalized.replace("/external/", "", 1)
    else:
        tail = normalized.lstrip("/")
    if not tail:
        return "external.default"
    if tail.startswith("catalog"):
        return "external.catalog"
    if tail.startswith("explorer"):
        return "external.explorer"
    if tail.startswith("governance"):
        return "external.governance"
    if tail.startswith("certification"):
        return "external.certification"
    if tail.startswith("dq"):
        return "external.dq"
    if tail.startswith("incidents"):
        return "external.incidents"
    if tail.startswith("lineage"):
        return "external.lineage"
    if tail.startswith("platform"):
        return "external.platform"
    if tail.startswith("tags"):
        return "external.tags"
    if tail.startswith("glossary"):
        return "external.glossary"
    if tail.startswith("ping"):
        return "external.ping"
    return "external.default"


def _rate_limit_overrides() -> dict[str, int]:
    raw = (settings.external_api_rate_limit_overrides_json or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return {str(k): int(v) for k, v in parsed.items() if str(k).strip()}
    return {}


def _resolve_limit(route_group: str) -> int:
    overrides = _rate_limit_overrides()
    if route_group in overrides:
        return max(1, int(overrides[route_group]))
    if "external.default" in overrides:
        return max(1, int(overrides["external.default"]))
    return max(1, int(DEFAULT_ROUTE_LIMITS.get(route_group, settings.external_api_rate_limit_default_per_window)))


def _bucket_start(now: datetime, window_seconds: int) -> datetime:
    window = max(1, int(window_seconds))
    seconds = int(now.timestamp())
    bucket_epoch = seconds - (seconds % window)
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)


def _rate_limit_decision(
    session: Session,
    *,
    api_key_id: int | None,
    route_group: str,
    window_seconds: int,
    limit: int,
) -> RateLimitDecision:
    now = datetime.now(timezone.utc)
    bucket_start = _bucket_start(now, window_seconds)
    reset_at = bucket_start + timedelta(seconds=window_seconds)

    if session.get_bind().dialect.name == "postgresql":
        stmt = insert(ApiRateLimitBucket).values(
            api_key_id=api_key_id,
            route_group=route_group,
            window_seconds=window_seconds,
            bucket_start=bucket_start,
            counter=1,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["api_key_id", "route_group", "window_seconds", "bucket_start"],
            set_={"counter": ApiRateLimitBucket.counter + 1},
        ).returning(ApiRateLimitBucket.counter)
        counter = session.execute(stmt).scalar_one()
        session.commit()
    else:
        row = session.scalar(
            select(ApiRateLimitBucket).where(
                ApiRateLimitBucket.api_key_id == api_key_id,
                ApiRateLimitBucket.route_group == route_group,
                ApiRateLimitBucket.window_seconds == window_seconds,
                ApiRateLimitBucket.bucket_start == bucket_start,
            )
        )
        if row is None:
            row = ApiRateLimitBucket(
                api_key_id=api_key_id,
                route_group=route_group,
                window_seconds=window_seconds,
                bucket_start=bucket_start,
                counter=1,
            )
            session.add(row)
        else:
            row.counter = int(row.counter or 0) + 1
        session.commit()
        counter = int(row.counter or 0)

    remaining = max(0, int(limit) - int(counter))
    return RateLimitDecision(
        allowed=counter <= limit,
        limit=limit,
        remaining=remaining,
        reset_at=reset_at,
        bucket_start=bucket_start,
    )


def enforce_external_rate_limit(
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
    result: ApiKeyAuthResult = Depends(get_external_api_key),
) -> RateLimitDecision:
    if not settings.external_api_rate_limit_enabled:
        return RateLimitDecision(
            allowed=True,
            limit=0,
            remaining=0,
            reset_at=datetime.now(timezone.utc),
            bucket_start=datetime.now(timezone.utc),
        )
    route_group = _external_route_group(request.url.path)
    window_seconds = max(1, int(settings.external_api_rate_limit_window_seconds))
    limit = _resolve_limit(route_group)
    decision = _rate_limit_decision(
        db,
        api_key_id=int(result.key.id) if result.key and result.key.id is not None else None,
        route_group=route_group,
        window_seconds=window_seconds,
        limit=limit,
    )
    response.headers["X-RateLimit-Limit"] = str(decision.limit)
    response.headers["X-RateLimit-Remaining"] = str(decision.remaining)
    response.headers["X-RateLimit-Reset"] = str(int(decision.reset_at.timestamp()))
    if not decision.allowed:
        runtime_metrics.rate_limit_hit(route_group=route_group)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit excedido para esta credencial.",
        )
    return decision


__all__ = [
    "RateLimitDecision",
    "enforce_external_rate_limit",
]
