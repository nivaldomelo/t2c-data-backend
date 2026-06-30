from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.core.security import hash_password, validate_password_policy
from t2c_data.features.admin.access_control_support import apply_user_access_scope_updates
from t2c_data.features.admin.api_support import (
    apply_user_updates,
    get_user_or_404,
    list_users_out,
    user_out,
    validate_user_email_available,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.models.auth import Role, User
from t2c_data.schemas.admin import UserCreate, UserOut, UserUpdate
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter()


@router.get("/users", response_model=PageOut[UserOut])
def list_users(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    _: User = Depends(require_roles("admin")),
) -> PageOut[UserOut]:
    return paginate_items(list_users_out(db), page=page, page_size=page_size)


@router.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_user(
    payload: UserCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> UserOut:
    validate_user_email_available(db, payload.email)

    roles = []
    if payload.role_ids:
        roles = db.scalars(select(Role).where(Role.id.in_(payload.role_ids))).all()

    try:
        validate_password_policy(payload.password)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    user = User(
        email=payload.email,
        name=payload.name or payload.full_name,
        full_name=payload.full_name or payload.name,
        password_hash=hash_password(payload.password),
        token_version=0,
        is_active=payload.is_active,
    )
    user.roles = list(roles)
    db.add(user)
    db.flush()
    apply_user_access_scope_updates(db, user, payload.model_dump(exclude_unset=True))
    db.commit()
    db.refresh(user)
    write_audit_log_sync(
        db,
        action="admin.user.create",
        entity_type="user",
        entity_id=user.id,
        after=serialize_model(user),
        metadata={
            "roles": [r.id for r in user.roles],
            "access_group_ids": [group.id for group in getattr(user, "access_groups", []) or []],
            "data_scope_grant_ids": [grant.id for grant in getattr(user, "access_grants", []) or []],
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return user_out(user)


@router.put("/users/{user_id}", response_model=UserOut)
def update_user(
    user_id: int,
    payload: UserUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> UserOut:
    user = get_user_or_404(db, user_id)
    before = serialize_model(user)

    updates = payload.model_dump(exclude_unset=True)
    if "email" in updates and updates["email"] != user.email:
        validate_user_email_available(db, updates["email"], exclude_user_id=user_id)
    try:
        apply_user_updates(db, user, updates)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    db.add(user)
    db.commit()
    db.refresh(user)
    write_audit_log_sync(
        db,
        action="admin.user.update",
        entity_type="user",
        entity_id=user.id,
        before=before,
        after=serialize_model(user),
        metadata={
            "roles": [r.id for r in user.roles],
            "access_group_ids": [group.id for group in getattr(user, "access_groups", []) or []],
            "data_scope_grant_ids": [grant.id for grant in getattr(user, "access_grants", []) or []],
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return user_out(user)


@router.delete("/users/{user_id}", response_model=None, status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> Response:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if user.id == current_user.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete current user")
    before = serialize_model(user)
    db.delete(user)
    db.commit()
    write_audit_log_sync(
        db,
        action="admin.user.delete",
        entity_type="user",
        entity_id=user_id,
        before=before,
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/users/{user_id}/mfa/unlock", response_model=UserOut)
def unlock_user_mfa(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> UserOut:
    """Unblock a user that was locked for not enrolling MFA, resetting the grace window."""
    user = get_user_or_404(db, user_id)
    before = serialize_model(user)
    user.mfa_locked = False
    user.mfa_locked_at = None
    user.mfa_grace_logins_used = 0
    db.add(user)
    db.commit()
    db.refresh(user)
    write_audit_log_sync(
        db,
        action="admin.user.mfa_unlock",
        entity_type="user",
        entity_id=user.id,
        before=before,
        after=serialize_model(user),
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return user_out(user)


@router.post("/users/{user_id}/password/unlock", response_model=UserOut)
def unlock_user_password(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> UserOut:
    """Release a user blocked by an expired password, restarting the 90-day window."""
    from datetime import datetime, timezone

    user = get_user_or_404(db, user_id)
    before = serialize_model(user)
    user.password_changed_at = datetime.now(timezone.utc)
    db.add(user)
    db.commit()
    db.refresh(user)
    write_audit_log_sync(
        db,
        action="admin.user.password_unlock",
        entity_type="user",
        entity_id=user.id,
        before=before,
        after=serialize_model(user),
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return user_out(user)
