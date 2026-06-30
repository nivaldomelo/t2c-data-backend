from __future__ import annotations

import logging

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.security import hash_password
from t2c_data.models.auth import Permission, Role, User

logger = logging.getLogger(__name__)


def rbac_tables_exist(session: Session) -> bool:
    role_reg = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.roles"},
    ).scalar_one()
    perm_reg = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.permissions"},
    ).scalar_one()
    user_reg = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.users"},
    ).scalar_one()
    return role_reg is not None and perm_reg is not None and user_reg is not None


def _get_or_create_role(session: Session, name: str, description: str) -> Role:
    role = session.scalar(select(Role).where(Role.name == name))
    if role is None:
        role = Role(name=name, description=description)
        session.add(role)
        session.flush()
    return role


def _get_or_create_user(session: Session, email: str, name: str, password: str) -> User:
    user = session.scalar(select(User).where(User.email == email))
    if user is None:
        user = User(
            email=email,
            name=name,
            full_name=name,
            password_hash=hash_password(password),
            token_version=0,
            is_active=True,
        )
        session.add(user)
        session.flush()
    else:
        if not user.name and name:
            user.name = name
        if not user.full_name and name:
            user.full_name = name
    return user


def _get_or_create_permission(session: Session, name: str, description: str | None = None) -> Permission:
    permission = session.scalar(select(Permission).where(Permission.name == name))
    if permission is None:
        permission = Permission(name=name, description=description)
        session.add(permission)
        session.flush()
    return permission


def _ensure_core_rbac_seed(session: Session) -> dict[str, Role]:
    roles = {
        "admin": "Full permissions",
        "editor": "Can edit metadata",
        "viewer": "Read only",
        "stewardship": "Stewardship approvals and review",
        "data_owner": "Data owner approvals and review",
    }
    return {name: _get_or_create_role(session, name, desc) for name, desc in roles.items()}


def _ensure_default_permissions(session: Session) -> dict[str, Permission]:
    permissions = {
        "admin:access": "Access admin area",
        "admin.user_audit.read": "Read user audit trail",
        "admin.user_audit.export": "Export user audit trail",
        "admin.user_audit.sensitive_read": "Read sensitive user audit trail",
        "admin.user_audit.change_read": "Read user change audit trail",
        "audit:export": "Export audit and access history",
        "catalog:export": "Export curated catalog spreadsheets",
        "certification:export": "Export certification queues and events",
        "dq.export": "Export data quality reports",
        "datasource:read": "Read datasources",
        "datasource:write": "Create/update/delete datasources",
        "asset.owner:write": "Assign owner/steward of tables and assets",
        "stewardship:approve": "Approve stewardship/governance review items",
        "stewardship:reject": "Reject stewardship/governance review items",
        "glossary:export": "Export glossary spreadsheets",
        "governance:export": "Export governance and ownership reports",
        "incidents.export": "Export incident reports",
        "integrations.export": "Export integration bundles",
        "io:export": "Export governance compatibility bundles",
        "lineage:export": "Export lineage spreadsheets",
        "owners.export": "Export ownership reports",
        "ops.export": "Export operational cockpit reports",
        "api_keys.export": "Export API key inventories",
        "privacy_access:export": "Export privacy review datasets",
        "tag:export": "Export tag spreadsheets",
        "sensitive:read": "Read sensitive data under ABAC policy",
        "sensitive:export": "Export sensitive data under ABAC policy",
        "user:read": "Read users",
        "user:manage": "Manage users",
        "role:manage": "Manage roles",
        "permission:manage": "Manage permissions",
        "*:read": "Read-only wildcard",
    }
    return {name: _get_or_create_permission(session, name, description) for name, description in permissions.items()}


def ensure_installation_seed(session: Session, *, create_viewer: bool = False, commit: bool = True) -> None:
    role_objs = _ensure_core_rbac_seed(session)
    permission_objs = _ensure_default_permissions(session)

    admin = _get_or_create_user(
        session=session,
        email=settings.bootstrap_admin_email,
        name=settings.bootstrap_admin_name,
        password=settings.bootstrap_admin_password,
    )
    if role_objs["admin"] not in admin.roles:
        admin.roles.append(role_objs["admin"])

    admin_perms = set(permission_objs.values())
    viewer_perms = {permission_objs["*:read"], permission_objs["user:read"]}
    editor_perms = {
        # Editors edit metadata, so they share the viewer's read baseline (*:read)
        # in addition to their export/owner-edit capabilities.
        permission_objs["*:read"],
        permission_objs["user:read"],
        permission_objs["asset.owner:write"],
        permission_objs["audit:export"],
        permission_objs["certification:export"],
        permission_objs["catalog:export"],
        permission_objs["glossary:export"],
        permission_objs["governance:export"],
        permission_objs["incidents.export"],
        permission_objs["integrations.export"],
        permission_objs["io:export"],
        permission_objs["lineage:export"],
        permission_objs["owners.export"],
        permission_objs["ops.export"],
        permission_objs["api_keys.export"],
        permission_objs["privacy_access:export"],
        permission_objs["tag:export"],
        permission_objs["dq.export"],
    }
    # Stewardship mirrors the viewer's read access (*:read + user:read) and can only
    # decide review-queue items. It must NOT carry an explicit datasource:read.
    stewardship_perms = {
        permission_objs["*:read"],
        permission_objs["user:read"],
        permission_objs["stewardship:approve"],
        permission_objs["stewardship:reject"],
    }
    # Data owner mirrors the viewer's read access, assigns asset owners and decides the
    # stewardship review queues. It must NOT carry datasource:read (admin-only).
    data_owner_perms = {
        permission_objs["*:read"],
        permission_objs["user:read"],
        permission_objs["asset.owner:write"],
        permission_objs["stewardship:approve"],
        permission_objs["stewardship:reject"],
    }

    for perm in admin_perms:
        if perm not in role_objs["admin"].permissions:
            role_objs["admin"].permissions.append(perm)
    for perm in viewer_perms:
        if perm not in role_objs["viewer"].permissions:
            role_objs["viewer"].permissions.append(perm)
    for perm in editor_perms:
        if perm not in role_objs["editor"].permissions:
            role_objs["editor"].permissions.append(perm)
    for perm in stewardship_perms:
        if perm not in role_objs["stewardship"].permissions:
            role_objs["stewardship"].permissions.append(perm)
    for perm in data_owner_perms:
        if perm not in role_objs["data_owner"].permissions:
            role_objs["data_owner"].permissions.append(perm)

    if create_viewer:
        viewer = _get_or_create_user(
            session=session,
            email=settings.viewer_email,
            name="Andromeda Viewer",
            password=settings.viewer_password,
        )
        if role_objs["viewer"] not in viewer.roles:
            viewer.roles.append(role_objs["viewer"])

        # Backward compatibility with older seeded login.
        viewer_legacy = _get_or_create_user(
            session=session,
            email="viewer@andromeda.com",
            name="Andromeda Viewer",
            password=settings.viewer_password,
        )
        if role_objs["viewer"] not in viewer_legacy.roles:
            viewer_legacy.roles.append(role_objs["viewer"])

    if commit:
        session.commit()
    logger.info("Installation seed applied (roles/users ensured create_viewer=%s)", create_viewer)


def ensure_dev_seed(session: Session) -> None:
    """
    Seed default roles/users for local development.
    Idempotent:
    - role exists -> keep
    - user exists -> keep
    - user without expected role -> assign role
    """
    ensure_installation_seed(session, create_viewer=True, commit=True)


def run_startup_seed_if_enabled(session: Session) -> None:
    if settings.env.lower() != "dev" or not settings.enable_db_seed:
        return
    if not rbac_tables_exist(session):
        logger.warning("DB seed skipped: RBAC tables not found in schema %s", settings.db_schema)
        return
    ensure_dev_seed(session)
