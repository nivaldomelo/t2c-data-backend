from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import settings
from t2c_data.core.db import get_db
from t2c_data.core.deps import oauth2_scheme
from t2c_data.core.network import get_request_client_ip
from t2c_data.core.secret_store import decrypt_secret_mapping
from t2c_data.core.security import create_access_token, decode_token_payload, verify_password, verify_totp_code
from t2c_data.features.auth.password_policy import password_expiry_status
from t2c_data.models.auth import Role, User
from t2c_data.models.audit import AuditLog
from t2c_data.schemas.auth import LoginRequest, LoginResponse, LogoutResponse
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync
from t2c_data.services.user_activity_tracker import record_session_end, record_session_start

router = APIRouter(prefix="/auth", tags=["auth"])


def _login_rate_limit_window_cutoff() -> datetime:
    window_seconds = max(1, settings.auth_rate_limit_window_seconds)
    return datetime.now(timezone.utc) - timedelta(seconds=window_seconds)


def _check_login_rate_limit(db: Session, _request: Request, email: str) -> None:
    max_attempts = max(1, settings.auth_rate_limit_attempts)
    normalized_email = email.strip().lower()
    conditions = [
        AuditLog.action == "login_failed",
        AuditLog.created_at >= _login_rate_limit_window_cutoff(),
    ]
    current_ip = get_request_client_ip(_request)
    if current_ip:
        conditions.append((AuditLog.ip == current_ip) | (func.lower(AuditLog.user_email) == normalized_email))
    else:
        conditions.append(func.lower(AuditLog.user_email) == normalized_email)
    attempts = db.scalar(select(func.count(AuditLog.id)).where(*conditions)) or 0
    if attempts >= max_attempts:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again later.",
        )


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, request: Request, db: Session = Depends(get_db)) -> LoginResponse:
    normalized_email = payload.email.strip().lower()
    _check_login_rate_limit(db, request, normalized_email)
    user = db.scalar(
        select(User)
        .options(selectinload(User.roles).selectinload(Role.permissions))
        .where(func.lower(User.email) == normalized_email)
    )
    if not user or not verify_password(payload.password, user.password_hash):
        audit_kwargs = request_audit_kwargs(request)
        audit_kwargs.pop("user_email", None)
        write_audit_log_sync(
            db,
            action="login_failed",
            entity_type="auth",
            entity_id=payload.email,
            user_email=normalized_email,
            metadata={"email": normalized_email, "reason": "invalid_credentials"},
            status_code=status.HTTP_401_UNAUTHORIZED,
            **audit_kwargs,
        )
        db.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    _MFA_LOCK_DETAIL = (
        "Conta bloqueada por falta de autenticação de duplo fator. "
        "Solicite o desbloqueio a um administrador."
    )
    if bool(getattr(user, "mfa_locked", False)):
        audit_kwargs = request_audit_kwargs(request, user)
        audit_kwargs.pop("user_email", None)
        write_audit_log_sync(
            db,
            action="login_failed",
            entity_type="auth",
            entity_id=user.email,
            user_email=user.email,
            metadata={"email": user.email, "reason": "mfa_locked"},
            status_code=status.HTTP_403_FORBIDDEN,
            **audit_kwargs,
        )
        db.commit()
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=_MFA_LOCK_DETAIL)

    pwd_status = password_expiry_status(user)
    if pwd_status.expired:
        audit_kwargs = request_audit_kwargs(request, user)
        audit_kwargs.pop("user_email", None)
        write_audit_log_sync(
            db,
            action="login_failed",
            entity_type="auth",
            entity_id=user.email,
            user_email=user.email,
            metadata={"email": user.email, "reason": "password_expired"},
            status_code=status.HTTP_403_FORBIDDEN,
            **audit_kwargs,
        )
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "Acesso bloqueado: sua senha expirou (troca obrigatória a cada 90 dias). "
                "Solicite a liberação a um administrador."
            ),
        )
    password_warning: str | None = None
    if pwd_status.warning:
        password_warning = (
            f"Sua senha expira em {pwd_status.days_remaining} dia(s). "
            "Altere-a no seu perfil para não perder o acesso."
        )

    mfa_enabled = bool(getattr(user, "mfa_enabled", False))
    mfa_grace_remaining: int | None = None
    mfa_warning: str | None = None

    if mfa_enabled:
        secret_payload = decrypt_secret_mapping(getattr(user, "mfa_secret_encrypted", None))
        mfa_secret = secret_payload.get("secret") if secret_payload else None
        if not mfa_secret:
            audit_kwargs = request_audit_kwargs(request, user)
            audit_kwargs.pop("user_email", None)
            write_audit_log_sync(
                db,
                action="login_failed",
                entity_type="auth",
                entity_id=user.email,
                user_email=user.email,
                metadata={"email": user.email, "reason": "mfa_configuration_missing"},
                status_code=status.HTTP_401_UNAUTHORIZED,
                **audit_kwargs,
            )
            db.commit()
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="MFA is not configured")
        mfa_code = (payload.mfa_code or "").strip()
        if not mfa_code:
            audit_kwargs = request_audit_kwargs(request, user)
            audit_kwargs.pop("user_email", None)
            write_audit_log_sync(
                db,
                action="login_failed",
                entity_type="auth",
                entity_id=user.email,
                user_email=user.email,
                metadata={"email": user.email, "reason": "mfa_required"},
                status_code=status.HTTP_401_UNAUTHORIZED,
                **audit_kwargs,
            )
            db.commit()
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="MFA required")
        if not verify_totp_code(mfa_secret, mfa_code):
            audit_kwargs = request_audit_kwargs(request, user)
            audit_kwargs.pop("user_email", None)
            write_audit_log_sync(
                db,
                action="login_failed",
                entity_type="auth",
                entity_id=user.email,
                user_email=user.email,
                metadata={"email": user.email, "reason": "invalid_mfa_code"},
                status_code=status.HTTP_401_UNAUTHORIZED,
                **audit_kwargs,
            )
            db.commit()
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid MFA code")
    else:
        grace_limit = int(getattr(settings, "mfa_grace_logins", 3) or 0)
        used = int(getattr(user, "mfa_grace_logins_used", 0) or 0)
        if used >= grace_limit:
            user.mfa_locked = True
            user.mfa_locked_at = datetime.now(timezone.utc)
            audit_kwargs = request_audit_kwargs(request, user)
            audit_kwargs.pop("user_email", None)
            write_audit_log_sync(
                db,
                action="login_failed",
                entity_type="auth",
                entity_id=user.email,
                user_email=user.email,
                metadata={"email": user.email, "reason": "mfa_grace_exhausted", "grace_limit": grace_limit},
                status_code=status.HTTP_403_FORBIDDEN,
                **audit_kwargs,
            )
            db.commit()
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Conta bloqueada: o período de carência sem autenticação de duplo fator terminou. "
                    "Solicite o desbloqueio a um administrador."
                ),
            )
        user.mfa_grace_logins_used = used + 1
        mfa_grace_remaining = max(grace_limit - user.mfa_grace_logins_used, 0)
        db.add(user)
        mfa_warning = (
            "Configure a autenticação de duplo fator (Google Authenticator) no seu perfil. "
            f"Você tem {mfa_grace_remaining} acesso(s) restante(s) sem MFA antes do bloqueio."
        )

    session_jti = uuid4().hex
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    record_session_start(
        db,
        user=user,
        jti=session_jti,
        ip_address=get_request_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        expires_at=expires_at,
    )
    token = create_access_token(
        subject=user.email,
        token_version=int(getattr(user, "token_version", 0) or 0),
        session_jti=session_jti,
    )
    permissions = sorted({perm.name for role in user.roles for perm in role.permissions})
    audit_kwargs = request_audit_kwargs(request, user)
    audit_kwargs.pop("user_email", None)
    write_audit_log_sync(
        db,
        action="login_success",
        entity_type="user",
        entity_id=user.id,
        user_email=user.email,
        metadata={"email": user.email, "roles": [role.name for role in user.roles]},
        status_code=status.HTTP_200_OK,
        **audit_kwargs,
    )
    db.commit()
    return LoginResponse(
        access_token=token,
        roles=[role.name for role in user.roles],
        permissions=permissions,
        mfa_enabled=mfa_enabled,
        mfa_grace_remaining=mfa_grace_remaining,
        mfa_warning=mfa_warning,
        password_expires_at=pwd_status.expires_at,
        password_days_remaining=pwd_status.days_remaining,
        password_warning=password_warning,
    )


@router.post("/logout", response_model=LogoutResponse)
def logout(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> LogoutResponse:
    payload = decode_token_payload(token) or {}
    email = (payload.get("sub") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    current_user = db.scalar(
        select(User)
        .options(selectinload(User.roles).selectinload(Role.permissions))
        .where(func.lower(User.email) == email)
    )
    if not current_user or not current_user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive or missing")
    session_jti = payload.get("jti")
    revoked_session = False
    if isinstance(session_jti, str) and session_jti.strip():
        session = record_session_end(
            db,
            user=current_user,
            session_jti=session_jti.strip(),
            end_reason="logout",
        )
        if session is not None and session.ended_at is not None:
            session.revoked_at = session.revoked_at or session.ended_at
            db.add(session)
            db.commit()
            revoked_session = True
            return LogoutResponse(ok=True, revoked_session=True)

    current_user.token_version = int(getattr(current_user, "token_version", 0) or 0) + 1
    db.add(current_user)
    db.commit()
    return LogoutResponse(ok=True, revoked_session=revoked_session)
