from __future__ import annotations

import re
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.security import decode_token, decode_token_payload
from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.features.platform.alerting import emit_permission_denied_alert
from t2c_data.models.access_control import AccessGroup
from t2c_data.models.auth import Role, User, UserSession
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")


def _normalize_api_scope_path(path: str) -> str:
    if path.startswith("/api/v1"):
        normalized = path[len("/api/v1") :]
    elif path.startswith("/api"):
        normalized = path[len("/api") :]
    else:
        normalized = path
    return normalized or "/"


def _authentication_unavailable_message(exc: SQLAlchemyError) -> str:
    detail = str(exc).strip()
    lowered = detail.lower()
    if "password authentication failed" in lowered or "invalid password" in lowered or "access denied" in lowered:
        return (
            "Serviço de autenticação indisponível no momento. "
            "A conexão com o banco principal falhou na autenticação. "
            "Em clusters com volume persistido, a senha atual pode ser diferente da senha declarada no compose."
        )
    if "connection refused" in lowered or "could not connect to server" in lowered:
        return "Serviço de autenticação indisponível no momento. O banco principal não respondeu à conexão."
    if "timeout" in lowered or "timed out" in lowered:
        return "Serviço de autenticação indisponível no momento. A conexão com o banco principal excedeu o tempo limite."
    if "could not translate host name" in lowered or "name or service not known" in lowered or "getaddrinfo failed" in lowered:
        return "Serviço de autenticação indisponível no momento. O host do banco principal não foi encontrado."
    return "Serviço de autenticação indisponível no momento. Não foi possível consultar o banco principal."


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_profile_path(path: str) -> bool:
    return path == "/me" or path.startswith("/me/") or path == "/auth/logout"


def _is_editor_blocked_path(path: str) -> bool:
    if path.startswith("/admin/governance"):
        return False
    # Editors manage all content and operations but never the datasource connections
    # nor the admin area (users/roles/permissions/API keys). Audit is readable by editors.
    blocked_prefixes = ("/datasources", "/admin")
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in blocked_prefixes)


# Operator/runtime actions (trigger scans, backups, platform jobs/automations) must stay
# admin-only. The authoritative check is each route's require_roles("admin"); this central
# guard is defense-in-depth so a new mutating route under these prefixes is not silently
# reachable by editor via the path-prefix default-allow.
_OPERATOR_RUNTIME_PREFIXES = (
    "/scan-runs",
    "/operations",
    "/platform/jobs",
    "/platform/automations",
    "/platform/actions",
    "/platform/read-models",
)


def _is_operator_runtime_path(path: str) -> bool:
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in _OPERATOR_RUNTIME_PREFIXES)


def _is_viewer_allowed_read_path(path: str) -> bool:
    # Read-only data-lake CATALOG browser (/datalakes) — only the catalog/table read
    # endpoints, NOT the connection console (/integrations/data-lake/connections...).
    if path == "/integrations/data-lake/catalog" or path.startswith("/integrations/data-lake/tables"):
        return True
    allowed_prefixes = (
        "/home",
        "/metrics",
        "/catalog",
        "/certification",
        "/privacy-access",
        "/lineage",
        "/tables",
        "/search",
        "/ping",
    )
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in allowed_prefixes)


def _is_viewer_allowed_write_path(path: str) -> bool:
    return (
        path in {"/search/track-query", "/search/track-click", "/search/favorites", "/assistant/explain"}
        or path.startswith("/search/favorites/")
        or path.startswith("/assistant/explain/")
    )


def _is_asset_owner_write_path(path: str) -> bool:
    # Owner/steward-only mutation surface for the data_owner role: PATCH /tables/{id}/owner.
    return bool(re.fullmatch(r"/tables/\d+/owner", path))


def enforce_role_scope_for_request(current_user: User, method: str, path: str) -> None:
    roles = user_role_names(current_user)
    if is_admin_role(roles):
        return

    normalized_path = _normalize_api_scope_path(path)
    http_method = method.upper()

    # Profile endpoints are available to all roles for self-service actions.
    if _is_profile_path(normalized_path):
        return

    if normalized_path.startswith("/assistant"):
        if "viewer" in roles:
            if http_method == "POST" and _is_viewer_allowed_write_path(normalized_path):
                return
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Viewer has read-only access")
        if "stewardship" in roles or "data_owner" in roles or "editor" in roles:
            return

    if "stewardship" in roles or "data_owner" in roles:
        # Both roles read everything except the admin area and datasources (datasource
        # connection metadata is admin-only). Defense-in-depth: agrees with the permission
        # layer (datasource:read é admin-only), evitando vazamento se uma rota GET nova
        # sob /datasources for adicionada sem require_permission.
        if normalized_path.startswith("/admin") or normalized_path.startswith("/datasources"):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role scope")
        if http_method == "GET":
            return
        # Both roles may act on the stewardship review queues.
        if normalized_path.startswith("/stewardship"):
            return
        # Data owner may additionally reassign the owner/steward of an asset.
        if "data_owner" in roles and _is_asset_owner_write_path(normalized_path):
            return
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role scope")

    if "editor" in roles:
        if normalized_path.startswith("/admin/governance"):
            if http_method != "GET":
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role scope")
            return
        if _is_editor_blocked_path(normalized_path):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role scope")
        # Editors may run operational actions (scans, jobs, ops). Genuinely admin-only
        # operations (backups, API keys, visibility rules) keep their own require_roles("admin").
        if normalized_path.startswith("/stewardship") and http_method != "GET":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role scope")
        return

    if "viewer" in roles:
        if http_method != "GET" and not _is_viewer_allowed_write_path(normalized_path):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Viewer has read-only access")
        if http_method == "GET" and not _is_viewer_allowed_read_path(normalized_path):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Viewer cannot access this area")
        return

    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role is not allowed")


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
    token: str = Depends(oauth2_scheme),
) -> User:
    token_payload = decode_token_payload(token)
    email = token_payload.get("sub") if token_payload else None
    if not email:
        fallback_email = decode_token(token)
        if isinstance(fallback_email, str):
            fallback_email = fallback_email.strip()
        email = fallback_email or None
    if not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    try:
        user = db.scalar(
            select(User)
            .options(selectinload(User.roles).selectinload(Role.permissions))
            .options(selectinload(User.access_grants))
            .options(selectinload(User.access_groups).selectinload(AccessGroup.grants))
            .where(User.email == email)
        )
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_authentication_unavailable_message(exc),
        ) from exc
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive or missing")
    token_version = token_payload.get("tv") if token_payload else None
    if int(token_version or 0) != int(getattr(user, "token_version", 0) or 0):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired or invalid")
    session_jti = token_payload.get("jti") if token_payload else None
    if isinstance(session_jti, str) and session_jti.strip():
        session = db.scalar(
            select(UserSession).where(
                UserSession.jti == session_jti.strip(),
                UserSession.user_id == user.id,
            )
        )
        session_expires_at = _as_utc(getattr(session, "expires_at", None))
        if (
            session is None
            or session.revoked_at is not None
            or session_expires_at is None
            or session_expires_at <= datetime.now(timezone.utc)
        ):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired or invalid")
        request.state.current_user_session = session
        request.state.current_user_session_id = getattr(session, "id", None)
        request.state.current_user_session_jti = session_jti.strip()
    request.state.current_user = user
    request.state.current_user_id = user.id
    request.state.current_user_name = user.name or user.full_name
    request.state.current_user_email = user.email
    enforce_role_scope_for_request(user, request.method, request.url.path)
    return user


def require_roles(*allowed: str):
    def _dependency(current_user: User = Depends(get_current_user)) -> User:
        role_names = user_role_names(current_user)
        allowed_set = set(allowed)
        if "viewer" in allowed_set:
            allowed_set.update({"stewardship", "data_owner"})
        if not role_names.intersection(allowed_set):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role")
        return current_user

    return _dependency


def require_permission(permission_name: str):
    def _dependency(request: Request = None, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)) -> User:
        role_names = user_role_names(current_user)
        if is_admin_role(role_names):
            return current_user

        granted = {perm.name for role in current_user.roles for perm in role.permissions}
        if permission_name in granted:
            return current_user

        if permission_name.endswith(":read") and permission_name != "datasource:read" and "*:read" in granted:
            return current_user

        if request is not None and hasattr(db, "commit") and hasattr(db, "rollback"):
            try:
                write_audit_log_sync(
                    db,
                    action="platform.permission.denied",
                    entity_type="permission",
                    entity_id=permission_name,
                    field_name=permission_name,
                    source_module="platform",
                    metadata={"permission_name": permission_name, "path": request.url.path, "method": request.method},
                    # request_audit_kwargs já provê user_id/user_email/ip/user_agent —
                    # passá-los também gerava TypeError (duplicate keyword) engolido pelo except,
                    # deixando o audit/alerta de permissão negada SEM efeito.
                    **request_audit_kwargs(request, current_user),
                )
                emit_permission_denied_alert(db, request=request, current_user=current_user, permission_name=permission_name)
                db.commit()
            except Exception:
                db.rollback()
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permission")

    return _dependency
