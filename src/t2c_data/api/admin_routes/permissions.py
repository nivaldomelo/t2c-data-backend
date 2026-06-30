from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.admin.api_support import (
    apply_permission_updates,
    get_permission_or_404,
    list_permissions_out,
    permission_out,
    validate_permission_name_available,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.models.auth import Permission, Role, User
from t2c_data.schemas.admin import PermissionCreate, PermissionOut, PermissionUpdate
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter()


@router.get("/permissions", response_model=PageOut[PermissionOut])
def list_permissions(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    _: User = Depends(require_roles("admin")),
) -> PageOut[PermissionOut]:
    return paginate_items(list_permissions_out(db), page=page, page_size=page_size)


@router.post("/permissions", response_model=PermissionOut, status_code=status.HTTP_201_CREATED)
def create_permission(
    payload: PermissionCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> PermissionOut:
    validate_permission_name_available(db, payload.name)
    permission = Permission(name=payload.name, description=payload.description)
    db.add(permission)
    db.commit()
    db.refresh(permission)
    write_audit_log_sync(
        db,
        action="admin.permission.create",
        entity_type="permission",
        entity_id=permission.id,
        after=serialize_model(permission),
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return permission_out(permission)


@router.put("/permissions/{permission_id}", response_model=PermissionOut)
def update_permission(
    permission_id: int,
    payload: PermissionUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> PermissionOut:
    permission = get_permission_or_404(db, permission_id)
    before = serialize_model(permission)
    updates = payload.model_dump(exclude_unset=True)
    if "name" in updates and updates["name"] != permission.name:
        validate_permission_name_available(db, updates["name"], exclude_permission_id=permission_id)
    apply_permission_updates(permission, updates)
    db.add(permission)
    db.commit()
    db.refresh(permission)
    write_audit_log_sync(
        db,
        action="admin.permission.update",
        entity_type="permission",
        entity_id=permission.id,
        before=before,
        after=serialize_model(permission),
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return permission_out(permission)


@router.delete("/permissions/{permission_id}", response_model=None, status_code=status.HTTP_204_NO_CONTENT)
def delete_permission(
    permission_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> Response:
    permission = get_permission_or_404(db, permission_id)
    used = db.scalar(
        select(Role.id)
        .join(Role.permissions)
        .where(Permission.id == permission_id)
        .limit(1)
    )
    if used is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Permission is in use by a role")
    before = serialize_model(permission)
    db.delete(permission)
    db.commit()
    write_audit_log_sync(
        db,
        action="admin.permission.delete",
        entity_type="permission",
        entity_id=permission_id,
        before=before,
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
