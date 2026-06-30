from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.network import get_request_client_ip
from t2c_data.features.platform.sensitive_data import redact_sensitive_metadata
from t2c_data.models.auth import User, UserAccessEvent, UserSession
from t2c_data.services.audit import write_audit_log_sync

logger = logging.getLogger(__name__)

_SESSION_HEARTBEAT_MIN_SECONDS = 60
_PAGE_DENYLIST = (
    "/api/health",
    "/health",
    "/api/docs",
    "/api/redoc",
    "/api/openapi.json",
    "/favicon.ico",
)
_PAGE_ALLOWLIST_PREFIXES = (
    "/",
    "/dashboard",
    "/explorer",
    "/datalakes",
    "/data-quality",
    "/governance",
    "/privacy-access",
    "/certification",
    "/incidents",
    "/owners",
    "/data-sources",
    "/datasources",
    "/admin",
    "/ops",
    "/search",
    "/lineage",
    "/me",
    "/audit",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware_datetime(value: datetime | None, fallback: datetime) -> datetime:
    if value is None:
        return fallback
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _safe_hash(value: str | None) -> str | None:
    text = _normalize(value)
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _parse_user_agent(user_agent: str | None) -> dict[str, str | None]:
    ua = _normalize(user_agent) or ""
    lowered = ua.lower()
    browser = None
    if "firefox" in lowered:
        browser = "Firefox"
    elif "edg/" in lowered or "edge/" in lowered:
        browser = "Edge"
    elif "chrome" in lowered and "chromium" not in lowered:
        browser = "Chrome"
    elif "safari" in lowered and "chrome" not in lowered:
        browser = "Safari"
    os_name = None
    if "windows" in lowered:
        os_name = "Windows"
    elif "mac os" in lowered or "macintosh" in lowered:
        os_name = "macOS"
    elif "linux" in lowered and "android" not in lowered:
        os_name = "Linux"
    elif "android" in lowered:
        os_name = "Android"
    elif "iphone" in lowered or "ipad" in lowered or "ios" in lowered:
        os_name = "iOS"
    device_type = "mobile" if any(token in lowered for token in ("mobile", "android", "iphone", "ipad")) else "desktop"
    return {"browser": browser, "os": os_name, "device_type": device_type}


def redact_activity_metadata(value: Any) -> Any:
    redacted = redact_sensitive_metadata(value)
    if isinstance(redacted, dict):
        for key, item in list(redacted.items()):
            lowered = str(key).lower()
            if lowered in {"cpf", "cnpj", "email", "phone", "document"} and item not in (None, "[masked]", "[redacted]"):
                redacted[key] = _safe_hash(str(item))
    if isinstance(redacted, list):
        return redacted[:20]
    if isinstance(redacted, str) and len(redacted) > 512:
        return f"{redacted[:512]}...[truncated]"
    return redacted


def build_resource_context(
    *,
    resource_type: str | None = None,
    resource_id: str | int | None = None,
    resource_fqn: str | None = None,
    datasource_id: int | None = None,
    schema_name: str | None = None,
    table_id: int | None = None,
    table_name: str | None = None,
    column_id: int | None = None,
    column_name: str | None = None,
    sensitivity_level: str | None = None,
    has_personal_data: bool = False,
    has_sensitive_data: bool = False,
    privacy_classification: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "resource_type": _normalize(resource_type),
        "resource_id": None if resource_id is None else str(resource_id),
        "resource_fqn": _normalize(resource_fqn),
        "datasource_id": datasource_id,
        "schema_name": _normalize(schema_name),
        "table_id": table_id,
        "table_name": _normalize(table_name),
        "column_id": column_id,
        "column_name": _normalize(column_name),
        "sensitivity_level": _normalize(sensitivity_level),
        "has_personal_data": bool(has_personal_data),
        "has_sensitive_data": bool(has_sensitive_data),
        "privacy_classification": _normalize(privacy_classification),
        "metadata_json": redact_activity_metadata(metadata or {}),
    }


def _lookup_session(db: Session, *, user_id: int | None, jti: str | None) -> UserSession | None:
    if user_id is None or not jti:
        return None
    return db.scalar(select(UserSession).where(UserSession.user_id == user_id, UserSession.jti == jti))


def record_session_start(
    db: Session,
    *,
    user: User,
    jti: str,
    ip_address: str | None,
    user_agent: str | None,
    auth_method: str = "password",
    mfa_used: bool = False,
    success: bool = True,
    failure_reason: str | None = None,
    expires_at: datetime | None = None,
) -> UserSession:
    now = _now()
    parsed_ua = _parse_user_agent(user_agent)
    session = UserSession(
        user_id=user.id,
        jti=jti,
        started_at=now,
        last_seen_at=now,
        ended_at=None,
        duration_seconds=None,
        end_reason=None,
        expires_at=expires_at or now,
        revoked_at=None,
        ip_address=ip_address,
        user_agent=_normalize(user_agent),
        device_type=parsed_ua["device_type"],
        browser=parsed_ua["browser"],
        os=parsed_ua["os"],
        country=None,
        city=None,
        auth_method=_normalize(auth_method),
        mfa_used=bool(mfa_used),
        success=bool(success),
        failure_reason=_normalize(failure_reason),
    )
    db.add(session)
    db.flush()
    return session


def record_session_heartbeat(
    db: Session,
    *,
    user: User | None = None,
    user_id: int | None = None,
    session_jti: str | None,
    user_agent: str | None,
    ip_address: str | None,
    force: bool = False,
) -> UserSession | None:
    resolved_user_id = user_id if user_id is not None else getattr(user, "id", None)
    if resolved_user_id is None or not session_jti:
        return None
    session = _lookup_session(db, user_id=resolved_user_id, jti=session_jti)
    if session is None:
        return None
    now = _now()
    last_seen = _aware_datetime(session.last_seen_at or session.started_at, now)
    if not force and (now - last_seen).total_seconds() < _SESSION_HEARTBEAT_MIN_SECONDS:
        return session
    session.last_seen_at = now
    if ip_address and not session.ip_address:
        session.ip_address = ip_address
    if user_agent and not session.user_agent:
        parsed_ua = _parse_user_agent(user_agent)
        session.user_agent = _normalize(user_agent)
        session.device_type = parsed_ua["device_type"]
        session.browser = parsed_ua["browser"]
        session.os = parsed_ua["os"]
    db.add(session)
    return session


def record_session_end(
    db: Session,
    *,
    user: User | None,
    session_jti: str | None,
    end_reason: str,
) -> UserSession | None:
    if user is None or not session_jti:
        return None
    session = _lookup_session(db, user_id=user.id, jti=session_jti)
    if session is None:
        return None
    now = _now()
    session.ended_at = now
    session.end_reason = _normalize(end_reason)
    session.revoked_at = now if end_reason == "revoked" else session.revoked_at
    started = _aware_datetime(session.started_at, now)
    session.duration_seconds = max(int((now - started).total_seconds()), 0)
    db.add(session)
    return session


def should_record_access_event(route_path: str | None, method: str | None) -> bool:
    path = _normalize(route_path) or ""
    if not path:
        return False
    if any(path == item or path.startswith(f"{item}/") for item in _PAGE_DENYLIST):
        return False
    if method and method.upper() in {"OPTIONS", "HEAD"}:
        return False
    if path.startswith("/api/static") or path.startswith("/_next"):
        return False
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in _PAGE_ALLOWLIST_PREFIXES)


def infer_page_key(route_path: str | None) -> str | None:
    path = _normalize(route_path) or ""
    if not path:
        return None
    if path.startswith("/explorer/data-journey"):
        return "data_journey"
    if path.startswith("/explorer"):
        return "explorer"
    if path.startswith("/data-quality/observability") or path.startswith("/data-quality/rules") or path.startswith("/data-quality"):
        return "data_quality"
    if path.startswith("/privacy-access"):
        return "privacy"
    if path.startswith("/certification"):
        return "certification"
    if path.startswith("/incidents"):
        return "incidents"
    if path.startswith("/data-owners"):
        return "owners"
    if path.startswith("/datasources"):
        return "data_sources"
    if path.startswith("/datalakes"):
        return "data_lake"
    if path.startswith("/ops"):
        return "ops"
    if path.startswith("/lineage"):
        return "lineage"
    if path.startswith("/admin"):
        return "admin_users"
    if path.startswith("/me"):
        return "profile"
    if path.startswith("/search"):
        return "search"
    if path.startswith("/audit"):
        return "audit"
    return None


def record_access_event(
    db: Session,
    *,
    user: User | None,
    session_jti: str | None,
    event_type: str,
    route_path: str | None,
    method: str | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    resource_id: str | int | None = None,
    resource_fqn: str | None = None,
    datasource_id: int | None = None,
    schema_name: str | None = None,
    table_id: int | None = None,
    table_name: str | None = None,
    column_id: int | None = None,
    column_name: str | None = None,
    sensitivity_level: str | None = None,
    has_personal_data: bool = False,
    has_sensitive_data: bool = False,
    privacy_classification: str | None = None,
    metadata: dict[str, Any] | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> UserAccessEvent | None:
    if user is None or not session_jti:
        return None
    session = _lookup_session(db, user_id=user.id, jti=session_jti)
    if session is None:
        return None
    normalized_route = _normalize(route_path)
    if not normalized_route:
        return None
    if not should_record_access_event(normalized_route, method):
        return None
    event = UserAccessEvent(
        user_id=user.id,
        session_id=session.id,
        event_type=_normalize(event_type) or "api_access",
        page_key=infer_page_key(normalized_route),
        route_path=normalized_route,
        http_method=_normalize(method),
        resource_type=_normalize(resource_type),
        resource_id=None if resource_id is None else str(resource_id),
        resource_fqn=_normalize(resource_fqn),
        datasource_id=datasource_id,
        schema_name=_normalize(schema_name),
        table_id=table_id,
        table_name=_normalize(table_name),
        column_id=column_id,
        column_name=_normalize(column_name),
        action=_normalize(action),
        sensitivity_level=_normalize(sensitivity_level),
        has_personal_data=bool(has_personal_data),
        has_sensitive_data=bool(has_sensitive_data),
        privacy_classification=_normalize(privacy_classification),
        metadata_json=redact_activity_metadata(metadata or {}),
        ip_address=ip_address,
        user_agent=_normalize(user_agent),
        request_id=_normalize(request_id),
        correlation_id=_normalize(correlation_id),
    )
    db.add(event)
    return event


def record_page_view(
    db: Session,
    *,
    user: User | None,
    session_jti: str | None,
    route_path: str | None,
    page_key: str | None = None,
    metadata: dict[str, Any] | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> UserAccessEvent | None:
    return record_access_event(
        db,
        user=user,
        session_jti=session_jti,
        event_type="page_view",
        route_path=route_path,
        method="GET",
        action="view",
        resource_type="page",
        resource_id=page_key or infer_page_key(route_path),
        resource_fqn=route_path,
        metadata=metadata,
        request_id=request_id,
        correlation_id=correlation_id,
        ip_address=ip_address,
        user_agent=user_agent,
    )


def record_change_event(
    db: Session,
    *,
    action: str,
    user: User | None,
    session_jti: str | None,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    parent_entity_type: str | None = None,
    parent_entity_id: str | int | None = None,
    source_module: str | None = None,
    before: Any = None,
    after: Any = None,
    metadata: dict[str, Any] | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> None:
    write_audit_log_sync(
        db,
        action=action,
        user_id=user.id if user else None,
        user_email=getattr(user, "email", None),
        actor_name=getattr(user, "name", None) or getattr(user, "full_name", None),
        ip=ip_address,
        user_agent=user_agent,
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module=source_module,
        before=before,
        after=after,
        metadata=redact_activity_metadata(metadata or {}),
        request_id=request_id,
        correlation_id=correlation_id,
    )
