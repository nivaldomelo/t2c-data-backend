from __future__ import annotations

from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.network import get_request_client_ip
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.platform.alerting import emit_api_key_abuse_alert
from t2c_data.features.platform.api_keys import ApiKeyAuthResult, ensure_scopes, is_api_key_ip_allowed, resolve_api_key_from_token
from t2c_data.models.auth import Role, User
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync


def _extract_api_key(request: Request) -> str | None:
    key = request.headers.get("X-API-Key")
    if key:
        return key
    return None


def _external_user() -> User:
    user = User(email="external@t2c.local", name="External API")
    role = Role(name="viewer")
    user.roles = [role]
    return user


def get_external_api_key(
    request: Request,
    db: Session = Depends(get_db),
) -> ApiKeyAuthResult:
    token = _extract_api_key(request)
    if not token:
        runtime_metrics.api_auth_event(outcome="missing")
        try:
            write_audit_log_sync(
                db,
                action="platform.api_key.auth_failed",
                field_name="missing",
                source_module="external_api",
                ip=get_request_client_ip(request),
                user_agent=request.headers.get("user-agent"),
                metadata={"outcome": "missing", "path": request.url.path, "method": request.method},
                **request_audit_kwargs(request, None),
            )
            emit_api_key_abuse_alert(db, request=request, outcome="missing", api_key_public_id=None)
            db.commit()
        except Exception:
            db.rollback()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key ausente")
    result = resolve_api_key_from_token(token, db)
    api_key = result.key
    client_ip = get_request_client_ip(request)
    if not is_api_key_ip_allowed(api_key, client_ip):
        runtime_metrics.api_auth_event(outcome="ip_denied")
        try:
            write_audit_log_sync(
                db,
                action="platform.api_key.auth_failed",
                user_id=api_key.created_by_user_id,
                entity_type="platform_api_key",
                entity_id=api_key.id,
                field_name="ip_denied",
                source_module="external_api",
                ip=client_ip,
                user_agent=request.headers.get("user-agent"),
                metadata={"outcome": "ip_denied", "api_key_public_id": api_key.public_id, "path": request.url.path, "method": request.method},
                **request_audit_kwargs(request, None),
            )
            emit_api_key_abuse_alert(db, request=request, outcome="ip_denied", api_key_public_id=api_key.public_id)
            db.commit()
        except Exception:
            db.rollback()
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="API key não autorizada para este IP")
    api_key.last_used_at = datetime.now(timezone.utc)
    api_key.last_used_ip = client_ip
    api_key.last_used_user_agent = request.headers.get("user-agent")
    api_key.usage_count = int(api_key.usage_count or 0) + 1
    request.state.current_api_key = api_key
    request.state.current_api_key_data = {
        "id": api_key.id,
        "public_id": api_key.public_id,
        "name": api_key.name,
        "token_prefix": api_key.token_prefix,
        "status": api_key.status,
        "environment": api_key.environment,
        "usage_count": int(api_key.usage_count or 0),
        "scope_count": len(api_key.scopes_json or []),
        "allowed_ips_count": len(api_key.allowed_ips_json or []),
    }
    request.state.external_user = _external_user()
    try:
        write_audit_log_sync(
            db,
            action="platform.api_key.used",
            entity_type="platform_api_key",
            entity_id=api_key.id,
            actor_name="External API",
            metadata={
                "api_key_public_id": api_key.public_id,
                "api_key_name": api_key.name,
                "api_key_environment": api_key.environment,
                "api_key_status": api_key.status,
                "api_key_usage_count": int(api_key.usage_count or 0),
                "api_key_scope_count": len(api_key.scopes_json or []),
                "api_key_allowed_ips_count": len(api_key.allowed_ips_json or []),
            },
            source_module="external_api",
            is_sensitive_change=True,
            sensitive_category="credential",
            **request_audit_kwargs(request, None),
        )
        db.commit()
    except Exception:
        db.rollback()
    runtime_metrics.api_auth_event(outcome="success")
    return result


def require_api_key_scopes(*required_scopes: str):
    def _dependency(
        request: Request,
        db: Session = Depends(get_db),
        result: ApiKeyAuthResult = Depends(get_external_api_key),
    ) -> ApiKeyAuthResult:
        try:
            ensure_scopes(result, list(required_scopes))
        except HTTPException:
            runtime_metrics.api_auth_event(outcome="scope_denied")
            try:
                write_audit_log_sync(
                    db,
                    action="platform.api_key.auth_failed",
                    user_id=result.key.created_by_user_id,
                    entity_type="platform_api_key",
                    entity_id=result.key.id,
                    field_name="scope_denied",
                    source_module="external_api",
                    ip=get_request_client_ip(request),
                    user_agent=request.headers.get("user-agent"),
                    metadata={
                        "outcome": "scope_denied",
                        "api_key_public_id": result.key.public_id,
                        "required_scopes": list(required_scopes),
                        "path": request.url.path,
                        "method": request.method,
                    },
                    **request_audit_kwargs(request, None),
                )
                emit_api_key_abuse_alert(
                    db,
                    request=request,
                    outcome="scope_denied",
                    api_key_public_id=result.key.public_id,
                )
                db.commit()
            except Exception:
                db.rollback()
            raise
        return result

    return _dependency
