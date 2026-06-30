from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.admin.access_control_support import access_group_out, apply_user_access_scope_updates, data_scope_grant_out
from t2c_data.features.auth.password_policy import password_expiry_status
from t2c_data.core.security import hash_password, validate_password_policy
from t2c_data.models.access_control import AccessGroup, DataAccessGrant
from t2c_data.models.auth import Permission, Role, User
from t2c_data.schemas.admin import PermissionOut, RoleOut, UserOut


def permission_out(permission: Permission) -> PermissionOut:
    return PermissionOut.model_validate(permission)


def role_out(role: Role) -> RoleOut:
    return RoleOut(
        id=role.id,
        name=role.name,
        description=role.description,
        permissions=[permission_out(p) for p in sorted(role.permissions, key=lambda item: item.name)],
        created_at=role.created_at,
        updated_at=role.updated_at,
    )


def user_out(user: User) -> UserOut:
    display_name = user.name or user.full_name
    return UserOut(
        id=user.id,
        email=user.email,
        name=display_name,
        full_name=user.full_name,
        is_active=user.is_active,
        mfa_enabled=bool(getattr(user, "mfa_enabled", False)),
        mfa_locked=bool(getattr(user, "mfa_locked", False)),
        mfa_grace_logins_used=int(getattr(user, "mfa_grace_logins_used", 0) or 0),
        password_expires_at=password_expiry_status(user).expires_at,
        password_expired=password_expiry_status(user).expired,
        roles=[role_out(r) for r in sorted(user.roles, key=lambda item: item.name)],
        access_group_ids=[group.id for group in sorted(getattr(user, "access_groups", []) or [], key=lambda item: item.id)],
        access_groups=[access_group_out(group) for group in sorted(getattr(user, "access_groups", []) or [], key=lambda item: item.id)],
        data_scope_grants=[data_scope_grant_out(grant) for grant in sorted(getattr(user, "access_grants", []) or [], key=lambda item: item.id)],
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


def list_users_out(db: Session) -> list[UserOut]:
    users = db.scalars(
        select(User)
        .options(selectinload(User.roles).selectinload(Role.permissions))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.users))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.grants).selectinload(DataAccessGrant.datasource))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.grants).selectinload(DataAccessGrant.schema))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.grants).selectinload(DataAccessGrant.table))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.user))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.datasource))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.schema))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.table))
        .order_by(User.id)
    ).all()
    return [user_out(user) for user in users]


def list_roles_out(db: Session) -> list[RoleOut]:
    roles = db.scalars(select(Role).options(selectinload(Role.permissions)).order_by(Role.id)).all()
    return [role_out(role) for role in roles]


def list_permissions_out(db: Session) -> list[PermissionOut]:
    permissions = db.scalars(select(Permission).order_by(Permission.id)).all()
    return [permission_out(permission) for permission in permissions]


def get_user_or_404(db: Session, user_id: int) -> User:
    user = db.scalar(
        select(User)
        .options(selectinload(User.roles).selectinload(Role.permissions))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.users))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.grants).selectinload(DataAccessGrant.datasource))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.grants).selectinload(DataAccessGrant.schema))
        .options(selectinload(User.access_groups).selectinload(AccessGroup.grants).selectinload(DataAccessGrant.table))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.user))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.datasource))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.schema))
        .options(selectinload(User.access_grants).selectinload(DataAccessGrant.table))
        .where(User.id == user_id)
    )
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


def get_role_or_404(db: Session, role_id: int) -> Role:
    role = db.scalar(select(Role).options(selectinload(Role.permissions)).where(Role.id == role_id))
    if not role:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")
    return role


def get_permission_or_404(db: Session, permission_id: int) -> Permission:
    permission = db.get(Permission, permission_id)
    if not permission:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Permission not found")
    return permission


def validate_user_email_available(db: Session, email: str, *, exclude_user_id: int | None = None) -> None:
    query = select(User).where(User.email == email)
    if exclude_user_id is not None:
        query = query.where(User.id != exclude_user_id)
    if db.scalar(query):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already exists")


def validate_role_name_available(db: Session, name: str, *, exclude_role_id: int | None = None) -> None:
    query = select(Role).where(Role.name == name)
    if exclude_role_id is not None:
        query = query.where(Role.id != exclude_role_id)
    if db.scalar(query):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Role already exists")


def validate_permission_name_available(db: Session, name: str, *, exclude_permission_id: int | None = None) -> None:
    query = select(Permission).where(Permission.name == name)
    if exclude_permission_id is not None:
        query = query.where(Permission.id != exclude_permission_id)
    if db.scalar(query):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Permission already exists")


def apply_user_updates(db: Session, user: User, updates: dict) -> None:
    if "password" in updates and updates["password"]:
        password = updates.pop("password")
        validate_password_policy(password)
        user.password_hash = hash_password(password)
        user.token_version = int(getattr(user, "token_version", 0) or 0) + 1
        # Resetting the password restarts the 90-day rotation window (and unblocks).
        user.password_changed_at = datetime.now(timezone.utc)
    if "role_ids" in updates:
        role_ids = updates.pop("role_ids") or []
        roles = db.scalars(select(Role).where(Role.id.in_(role_ids))).all() if role_ids else []
        user.roles = list(roles)
    apply_user_access_scope_updates(db, user, updates)
    if "name" in updates and updates["name"] is not None and "full_name" not in updates:
        updates["full_name"] = updates["name"]
    if "full_name" in updates and updates["full_name"] is not None and "name" not in updates:
        updates["name"] = updates["full_name"]
    # Never allow mass-assignment of sensitive/privilege fields, even if the schema grows.
    protected_fields = {
        "id",
        "password_hash",
        "token_version",
        "mfa_secret_encrypted",
        "mfa_enabled",
        "is_superuser",
        "created_at",
        "roles",
        "access_grants",
        "access_groups",
    }
    for key, value in updates.items():
        if key in protected_fields:
            continue
        setattr(user, key, value)


def apply_role_updates(db: Session, role: Role, updates: dict) -> None:
    if "permission_ids" in updates:
        permission_ids = updates.pop("permission_ids") or []
        permissions = db.scalars(select(Permission).where(Permission.id.in_(permission_ids))).all() if permission_ids else []
        role.permissions = list(permissions)
    for key, value in updates.items():
        setattr(role, key, value)


def apply_permission_updates(permission: Permission, updates: dict) -> None:
    for key, value in updates.items():
        setattr(permission, key, value)
