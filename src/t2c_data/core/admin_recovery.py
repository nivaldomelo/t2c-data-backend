from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import settings
from t2c_data.core.security import hash_password, validate_password_policy
from t2c_data.models.auth import Role, User
from t2c_data.seed import ensure_installation_seed

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BootstrapAdminRecoveryResult:
    email: str
    created: bool
    reactivated: bool
    hash_scheme: str


def _hash_scheme(password_hash: str) -> str:
    parts = password_hash.split("$", 2)
    if len(parts) > 1 and parts[1]:
        return parts[1]
    return "unknown"


def reset_bootstrap_admin_password(
    session: Session,
    *,
    email: str | None = None,
    password: str | None = None,
    ensure_seed: bool = True,
    commit: bool = True,
) -> BootstrapAdminRecoveryResult:
    target_email = (email or settings.bootstrap_admin_email).strip().lower()
    target_password = password or settings.bootstrap_admin_password
    validate_password_policy(target_password)

    existing_user = session.scalar(select(User).where(func.lower(User.email) == target_email))
    existed_before = existing_user is not None

    # Ensure the admin RBAC seed exists before touching the user row when requested.
    if ensure_seed:
        ensure_installation_seed(session, create_viewer=False, commit=False)

    user = session.scalar(select(User).options(selectinload(User.roles)).where(func.lower(User.email) == target_email))
    created = False
    reactivated = False

    if user is None:
        user = User(
            email=target_email,
            name=settings.bootstrap_admin_name,
            full_name=settings.bootstrap_admin_name,
            password_hash=hash_password(target_password),
            token_version=0,
            is_active=True,
        )
        session.add(user)
        session.flush()
        created = True
    else:
        user.email = target_email
        user.password_hash = hash_password(target_password)
        user.token_version = int(getattr(user, "token_version", 0) or 0) + 1
        reactivated = not user.is_active
        user.is_active = True
        if not user.name:
            user.name = settings.bootstrap_admin_name
        if not user.full_name:
            user.full_name = settings.bootstrap_admin_name

    admin_role = session.scalar(select(Role).where(Role.name == "admin"))
    if admin_role is not None and admin_role not in user.roles:
        user.roles.append(admin_role)

    if commit:
        session.commit()

    logger.info(
        "bootstrap admin password reset email=%s created=%s reactivated=%s active=%s hash_scheme=%s",
        user.email,
        created or not existed_before,
        reactivated,
        user.is_active,
        _hash_scheme(user.password_hash),
    )
    return BootstrapAdminRecoveryResult(
        email=user.email,
        created=created or not existed_before,
        reactivated=reactivated,
        hash_scheme=_hash_scheme(user.password_hash),
    )
