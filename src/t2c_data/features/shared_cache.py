from __future__ import annotations

import json
from hashlib import blake2s
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Callable, TypeVar

from sqlalchemy.engine import URL, make_url

from t2c_data.core.redaction import is_sensitive_key, redact_sensitive_string

T = TypeVar("T")


@dataclass(slots=True)
class CachedValue:
    expires_at: datetime
    payload: Any


_CACHE_LOCK = Lock()
_CACHE: dict[tuple[str, tuple[object, ...]], CachedValue] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _stable_hash(value: str) -> str:
    return blake2s(value.encode("utf-8"), digest_size=12).hexdigest()


def _extract_bind_url(target: Any) -> Any | None:
    if target is None:
        return None
    if isinstance(target, URL):
        return target
    if hasattr(target, "render_as_string") or hasattr(target, "drivername"):
        return target
    url = getattr(target, "url", None)
    if url is not None:
        return url
    get_bind = getattr(target, "get_bind", None)
    if callable(get_bind):
        bind = get_bind()
        if bind is not None:
            engine = getattr(bind, "engine", bind)
            return getattr(engine, "url", None)
    engine = getattr(target, "engine", None)
    if engine is not None:
        return getattr(engine, "url", None)
    return None


def safe_connection_label(target: Any) -> str:
    url = _extract_bind_url(target)
    if url is None:
        return "bind:unknown"
    try:
        rendered = url.render_as_string(hide_password=True)
    except TypeError:
        rendered = str(url)
    except Exception:  # noqa: BLE001
        rendered = str(url)
    return redact_sensitive_string(rendered)


def safe_connection_fingerprint(target: Any) -> str:
    url = _extract_bind_url(target)
    if url is None:
        return "connfp:unknown"

    normalized_url: URL | None = None
    if isinstance(url, URL):
        normalized_url = url
    else:
        try:
            normalized_url = make_url(safe_connection_label(url))
        except Exception:  # noqa: BLE001
            normalized_url = None

    if normalized_url is None:
        return f"connfp:{_stable_hash(safe_connection_label(url))}"

    safe_query = {
        str(key): [str(item) for item in value] if isinstance(value, (list, tuple)) else str(value)
        for key, value in sorted((normalized_url.query or {}).items())
        if not is_sensitive_key(str(key))
    }
    payload = json.dumps(
        {
            "drivername": normalized_url.drivername or "",
            "host": normalized_url.host or "",
            "port": normalized_url.port or "",
            "database": normalized_url.database or "",
            "username_hash": _stable_hash(normalized_url.username or ""),
            "query": safe_query,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"connfp:{_stable_hash(payload)}"


def session_cache_key(session: Any) -> str:
    get_bind = getattr(session, "get_bind", None)
    if not callable(get_bind):
        return "bind:unknown"
    bind = get_bind()
    if bind is None:
        return "bind:unknown"
    engine = getattr(bind, "engine", bind)
    url = getattr(engine, "url", None)
    if url is None:
        return f"bind:{id(engine)}"
    return f"{safe_connection_fingerprint(engine)}|{id(engine)}"


def get_cached_value(namespace: str, key: tuple[object, ...], *, now: datetime | None = None) -> Any | None:
    current_time = now or _now_utc()
    cache_key = (namespace, key)
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached is None or cached.expires_at <= current_time:
            return None
        return cached.payload


def set_cached_value(
    namespace: str,
    key: tuple[object, ...],
    payload: Any,
    *,
    ttl_seconds: int,
    now: datetime | None = None,
) -> Any:
    current_time = now or _now_utc()
    cache_key = (namespace, key)
    with _CACHE_LOCK:
        _CACHE[cache_key] = CachedValue(
            expires_at=current_time + timedelta(seconds=max(int(ttl_seconds), 1)),
            payload=payload,
        )
    return payload


def get_or_set_cached_value(
    namespace: str,
    key: tuple[object, ...],
    *,
    ttl_seconds: int,
    loader: Callable[[], T],
    now: datetime | None = None,
) -> T:
    cached = get_cached_value(namespace, key, now=now)
    if cached is not None:
        return cached
    payload = loader()
    return set_cached_value(namespace, key, payload, ttl_seconds=ttl_seconds, now=now)


__all__ = [
    "CachedValue",
    "get_cached_value",
    "get_or_set_cached_value",
    "safe_connection_fingerprint",
    "safe_connection_label",
    "session_cache_key",
    "set_cached_value",
]
