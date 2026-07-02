from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import get_current_user
from t2c_data.features.auth.password_policy import password_expiry_status
from t2c_data.core.secret_store import decrypt_secret_mapping, encrypt_secret_mapping
from t2c_data.core.security import (
    build_totp_provisioning_uri,
    find_totp_counter,
    generate_totp_secret,
    validate_password_policy,
    verify_password,
    verify_totp_code,
    hash_password,
)
from t2c_data.features.notifications import (
    forward_user_inbox_notification,
    get_user_inbox,
    get_user_inbox_summary,
    get_inbox_notification_payload,
    get_user_notification_preferences_payload,
    mark_user_inbox_notification_archived,
    mark_user_inbox_notification_read,
    mark_user_inbox_notification_unread,
    search_inbox_forward_recipients,
    update_user_notification_preferences,
)
from t2c_data.models.auth import User
from t2c_data.schemas.me import (
    ChangePasswordRequest,
    ChangePasswordResponse,
    InboxListOut,
    InboxNotificationOut,
    InboxRecipientOut,
    InboxSummaryOut,
    ForwardInboxNotificationRequest,
    MfaActionResponse,
    MfaDisableRequest,
    MfaStatusResponse,
    MfaVerifyRequest,
    MeResponse,
    NotificationPreferenceResponse,
    NotificationPreferenceUpdateRequest,
    ThemeUpdateRequest,
    ThemeUpdateResponse,
)
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync

router = APIRouter(prefix="/me", tags=["me"])

MFA_ISSUER = "t2c_data"


def _mfa_secret_value(user: User) -> str | None:
    secret_payload = decrypt_secret_mapping(getattr(user, "mfa_secret_encrypted", None))
    secret = secret_payload.get("secret") if secret_payload else None
    return secret or None


def _mfa_status_payload(user: User) -> MfaStatusResponse:
    secret = _mfa_secret_value(user)
    enabled = bool(getattr(user, "mfa_enabled", False))
    payload: dict[str, object] = {
        "enabled": enabled,
        "setup_pending": bool(secret and not enabled),
        "issuer": MFA_ISSUER,
        "account_name": user.email,
        "updated_at": getattr(user, "updated_at", None),
    }
    if secret and not enabled:
        payload["manual_secret"] = secret
        payload["otpauth_uri"] = build_totp_provisioning_uri(secret, user.email, issuer=MFA_ISSUER)
    return MfaStatusResponse(**payload)


def _mfa_action_payload(user: User, message: str) -> MfaActionResponse:
    return MfaActionResponse(message=message, **_mfa_status_payload(user).model_dump())


@router.get("", response_model=MeResponse)
def get_me(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MeResponse:
    roles = sorted({role.name for role in current_user.roles})
    is_admin = "admin" in roles
    if is_admin:
        permissions = ["*"]
    else:
        permissions = sorted({perm.name for role in current_user.roles for perm in role.permissions})
    inbox_summary = get_user_inbox_summary(db, user=current_user)
    pwd_status = password_expiry_status(current_user)
    return MeResponse(
        id=current_user.id,
        name=current_user.name or current_user.full_name,
        email=current_user.email,
        roles=roles,
        permissions=permissions,
        is_admin=is_admin,
        unread_notifications=int(inbox_summary["unread"]),
        password_changed_at=pwd_status.changed_at,
        password_expires_at=pwd_status.expires_at,
        password_days_remaining=pwd_status.days_remaining,
        ui_theme=getattr(current_user, "ui_theme", None) or "atual",
    )


ALLOWED_THEMES = {"atual", "teal", "corporate", "minimal"}


@router.put("/theme", response_model=ThemeUpdateResponse)
def update_theme(
    payload: ThemeUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ThemeUpdateResponse:
    theme = (payload.theme or "atual").strip().lower()
    if theme not in ALLOWED_THEMES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Tema inválido")
    current_user.ui_theme = theme
    db.add(current_user)
    db.commit()
    return ThemeUpdateResponse(ui_theme=theme)


@router.post("/change-password", response_model=ChangePasswordResponse)
def change_password(
    payload: ChangePasswordRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ChangePasswordResponse:
    if not verify_password(payload.current_password, current_user.password_hash):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Current password is invalid")
    try:
        validate_password_policy(payload.new_password)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    before = {"token_version": int(getattr(current_user, "token_version", 0) or 0)}
    current_user.password_hash = hash_password(payload.new_password)
    current_user.token_version = int(getattr(current_user, "token_version", 0) or 0) + 1
    current_user.password_changed_at = datetime.now(timezone.utc)
    db.add(current_user)
    db.commit()
    audit_kwargs = request_audit_kwargs(request, current_user)
    audit_kwargs.pop("user_email", None)
    write_audit_log_sync(
        db,
        action="password.changed",
        entity_type="user",
        entity_id=current_user.id,
        user_email=current_user.email,
        before=before,
        after={"token_version": int(current_user.token_version or 0)},
        metadata={"password_rotated": True},
        status_code=status.HTTP_200_OK,
        source_module="me",
        is_sensitive_change=True,
        sensitive_category="credential",
        **audit_kwargs,
    )
    db.commit()
    return ChangePasswordResponse(ok=True, message="Password changed successfully")


@router.get("/mfa", response_model=MfaStatusResponse)
def get_mfa_status(
    current_user: User = Depends(get_current_user),
) -> MfaStatusResponse:
    return _mfa_status_payload(current_user)


@router.post("/mfa/setup", response_model=MfaActionResponse)
def setup_mfa(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MfaActionResponse:
    if bool(getattr(current_user, "mfa_enabled", False)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="MFA is already enabled")

    secret = generate_totp_secret()
    current_user.mfa_secret_encrypted = encrypt_secret_mapping({"secret": secret})
    db.add(current_user)
    db.commit()
    audit_kwargs = request_audit_kwargs(request, current_user)
    audit_kwargs.pop("user_email", None)
    write_audit_log_sync(
        db,
        action="mfa.setup_started",
        entity_type="user",
        entity_id=current_user.id,
        user_email=current_user.email,
        metadata={"issuer": MFA_ISSUER, "account_name": current_user.email},
        status_code=status.HTTP_200_OK,
        **audit_kwargs,
    )
    db.commit()
    db.refresh(current_user)
    return _mfa_action_payload(current_user, "MFA setup ready. Scan the secret with Google Authenticator and confirm the code.")


@router.post("/mfa/verify", response_model=MfaActionResponse)
def verify_mfa(
    payload: MfaVerifyRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MfaActionResponse:
    if bool(getattr(current_user, "mfa_enabled", False)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="MFA is already enabled")

    secret = _mfa_secret_value(current_user)
    if not secret:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="MFA setup has not been started")
    matched_counter = find_totp_counter(secret, payload.code)
    if matched_counter is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid MFA code")

    current_user.mfa_enabled = True
    # Anti-replay: consome o contador usado no enrollment para não ser reusado no login.
    current_user.mfa_last_counter = matched_counter
    # Enrolling within the grace window clears any pending lock/grace state.
    current_user.mfa_locked = False
    current_user.mfa_locked_at = None
    current_user.mfa_grace_logins_used = 0
    db.add(current_user)
    db.commit()
    audit_kwargs = request_audit_kwargs(request, current_user)
    audit_kwargs.pop("user_email", None)
    write_audit_log_sync(
        db,
        action="mfa.enabled",
        entity_type="user",
        entity_id=current_user.id,
        user_email=current_user.email,
        metadata={"issuer": MFA_ISSUER, "account_name": current_user.email},
        status_code=status.HTTP_200_OK,
        **audit_kwargs,
    )
    db.commit()
    db.refresh(current_user)
    return _mfa_action_payload(current_user, "MFA enabled successfully.")


@router.post("/mfa/disable", response_model=MfaActionResponse)
def disable_mfa(
    payload: MfaDisableRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MfaActionResponse:
    if not verify_password(payload.current_password, current_user.password_hash):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Current password is invalid")
    if not bool(getattr(current_user, "mfa_enabled", False)) and not _mfa_secret_value(current_user):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="MFA is not enabled")

    current_user.mfa_enabled = False
    current_user.mfa_secret_encrypted = None
    db.add(current_user)
    db.commit()
    audit_kwargs = request_audit_kwargs(request, current_user)
    audit_kwargs.pop("user_email", None)
    write_audit_log_sync(
        db,
        action="mfa.disabled",
        entity_type="user",
        entity_id=current_user.id,
        user_email=current_user.email,
        metadata={"issuer": MFA_ISSUER, "account_name": current_user.email},
        status_code=status.HTTP_200_OK,
        **audit_kwargs,
    )
    db.commit()
    db.refresh(current_user)
    return _mfa_action_payload(current_user, "MFA disabled successfully.")


@router.get("/notification-preferences", response_model=NotificationPreferenceResponse)
def get_notification_preferences(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> NotificationPreferenceResponse:
    return NotificationPreferenceResponse(**get_user_notification_preferences_payload(db, current_user))


@router.put("/notification-preferences", response_model=NotificationPreferenceResponse)
def update_notification_preferences(
    payload: NotificationPreferenceUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> NotificationPreferenceResponse:
    return NotificationPreferenceResponse(**update_user_notification_preferences(db, user=current_user, payload=payload))


@router.get("/inbox", response_model=InboxListOut)
def list_inbox(
    state: str | None = None,
    category: str | None = None,
    page: int = 1,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InboxListOut:
    return InboxListOut(
        **get_user_inbox(
            db,
            user=current_user,
            state_filter=state,
            category=category,
            page=max(1, page),
            limit=max(1, min(limit, 200)),
        )
    )


@router.get("/inbox/summary", response_model=InboxSummaryOut)
def inbox_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InboxSummaryOut:
    return InboxSummaryOut(**get_user_inbox_summary(db, user=current_user))


@router.get("/inbox/recipients", response_model=list[InboxRecipientOut])
def inbox_recipients(
    q: str | None = None,
    limit: int = 20,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[InboxRecipientOut]:
    recipients = search_inbox_forward_recipients(db, q=q, limit=limit, exclude_user_id=current_user.id)
    return [InboxRecipientOut(**recipient) for recipient in recipients]


@router.post("/inbox/{notification_id}/read", response_model=InboxNotificationOut)
def mark_inbox_read(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InboxNotificationOut:
    try:
        return InboxNotificationOut(**mark_user_inbox_notification_read(db, user=current_user, notification_id=notification_id))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/inbox/{notification_id}/unread", response_model=InboxNotificationOut)
def mark_inbox_unread(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InboxNotificationOut:
    try:
        return InboxNotificationOut(**mark_user_inbox_notification_unread(db, user=current_user, notification_id=notification_id))
    except ValueError as exc:
        detail = str(exc)
        if "yourself" in detail.lower():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from exc
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc


@router.post("/inbox/{notification_id}/archive", response_model=InboxNotificationOut)
def mark_inbox_archived(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InboxNotificationOut:
    try:
        return InboxNotificationOut(**mark_user_inbox_notification_archived(db, user=current_user, notification_id=notification_id))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/inbox/{notification_id}/forward", response_model=InboxNotificationOut)
def forward_inbox_notification(
    notification_id: int,
    payload: ForwardInboxNotificationRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> InboxNotificationOut:
    try:
        forwarded = forward_user_inbox_notification(
            db,
            user=current_user,
            notification_id=notification_id,
            recipient_user_id=payload.recipient_user_id,
        )
        db.commit()
        db.refresh(forwarded)
        write_audit_log_sync(
            db,
            action="inbox.forward",
            entity_type="user_inbox_notification",
            entity_id=forwarded.id,
            parent_entity_type="user_inbox_notification",
            parent_entity_id=notification_id,
            metadata={
                "recipient_user_id": payload.recipient_user_id,
                "forwarded_from_notification_id": notification_id,
            },
            **request_audit_kwargs(request, current_user),
        )
        db.commit()
        return InboxNotificationOut(**get_inbox_notification_payload(forwarded))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
