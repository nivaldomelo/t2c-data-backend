from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.admin.api_support import (
    apply_role_updates,
    get_role_or_404,
    list_roles_out,
    role_out,
    validate_role_name_available,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.models.auth import Permission, Role, User
from t2c_data.schemas.admin import RoleCreate, RoleOut, RoleUpdate
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter()

# Built-in roles whose name the RBAC checks rely on (require_roles("admin"), etc.).
# Renaming or deleting them would break access control, so they are protected.
PROTECTED_ROLE_NAMES = {"admin", "editor", "viewer", "stewardship", "data_owner"}


@router.get("/roles", response_model=PageOut[RoleOut])
def list_roles(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    _: User = Depends(require_roles("admin")),
) -> PageOut[RoleOut]:
    return paginate_items(list_roles_out(db), page=page, page_size=page_size)


@router.post("/roles", response_model=RoleOut, status_code=status.HTTP_201_CREATED)
def create_role(
    payload: RoleCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> RoleOut:
    validate_role_name_available(db, payload.name)
    role = Role(name=payload.name, description=payload.description)
    if payload.permission_ids:
        permissions = db.scalars(select(Permission).where(Permission.id.in_(payload.permission_ids))).all()
        role.permissions = list(permissions)
    db.add(role)
    db.commit()
    db.refresh(role)
    write_audit_log_sync(
        db,
        action="admin.role.create",
        entity_type="role",
        entity_id=role.id,
        after=serialize_model(role),
        metadata={"permission_ids": [p.id for p in role.permissions]},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return role_out(role)


@router.put("/roles/{role_id}", response_model=RoleOut)
def update_role(
    role_id: int,
    payload: RoleUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> RoleOut:
    role = get_role_or_404(db, role_id)
    before = serialize_model(role)

    updates = payload.model_dump(exclude_unset=True)
    if "name" in updates and updates["name"] != role.name:
        if role.name in PROTECTED_ROLE_NAMES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Protected role cannot be renamed",
            )
        validate_role_name_available(db, updates["name"], exclude_role_id=role_id)
    apply_role_updates(db, role, updates)

    db.add(role)
    db.commit()
    db.refresh(role)
    write_audit_log_sync(
        db,
        action="admin.role.update",
        entity_type="role",
        entity_id=role.id,
        before=before,
        after=serialize_model(role),
        metadata={"permission_ids": [p.id for p in role.permissions]},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return role_out(role)


@router.delete("/roles/{role_id}", response_model=None, status_code=status.HTTP_204_NO_CONTENT)
def delete_role(
    role_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> Response:
    role = get_role_or_404(db, role_id)
    if role.name in PROTECTED_ROLE_NAMES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Protected role cannot be deleted")
    before = serialize_model(role)
    db.delete(role)
    db.commit()
    write_audit_log_sync(
        db,
        action="admin.role.delete",
        entity_type="role",
        entity_id=role_id,
        before=before,
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
