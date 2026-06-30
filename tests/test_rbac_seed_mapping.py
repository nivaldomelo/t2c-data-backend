"""Validates the seeded role -> permission mapping for the refined RBAC model."""

from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.config import settings
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.seed import ensure_installation_seed


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def _perms(db: Session, role_name: str) -> set[str]:
    role = db.scalar(select(Role).where(Role.name == role_name))
    assert role is not None, f"role {role_name} not seeded"
    return {permission.name for permission in role.permissions}


def test_seeded_role_permissions_match_refined_model() -> None:
    db = _build_session()
    ensure_installation_seed(db, commit=True)

    viewer = _perms(db, "viewer")
    stewardship = _perms(db, "stewardship")
    data_owner = _perms(db, "data_owner")
    editor = _perms(db, "editor")

    # Viewer is the read baseline.
    assert viewer == {"*:read", "user:read"}

    # Stewardship mirrors the viewer's reads and only decides review queues.
    assert "*:read" in stewardship and "user:read" in stewardship
    assert "stewardship:approve" in stewardship and "stewardship:reject" in stewardship
    # Acceptance criterion: NO explicit datasource:read on stewardship.
    assert "datasource:read" not in stewardship
    assert "datasource:write" not in stewardship
    assert "asset.owner:write" not in stewardship

    # Data owner mirrors viewer reads, reassigns asset owners and decides review queues.
    assert "*:read" in data_owner and "user:read" in data_owner
    assert "asset.owner:write" in data_owner
    assert "stewardship:approve" in data_owner and "stewardship:reject" in data_owner
    # Datasource access is admin-only: data_owner must not carry it.
    assert "datasource:read" not in data_owner
    assert "datasource:write" not in data_owner

    # Neither role gains administrative powers.
    for elevated in ("user:manage", "role:manage", "permission:manage", "admin:access", "datasource:write", "datasource:read"):
        assert elevated not in stewardship
        assert elevated not in data_owner

    # Editor keeps read parity with the viewer, reassigns asset owners and exports audit.
    assert "*:read" in editor
    assert "asset.owner:write" in editor
    assert "audit:export" in editor
    # ...but never datasource connection management nor the admin area.
    for elevated in ("user:manage", "role:manage", "permission:manage", "admin:access", "datasource:read", "datasource:write"):
        assert elevated not in editor


def test_fresh_install_creates_all_roles_and_bootstrap_admin() -> None:
    """A fresh install (the migration calls this) must yield every core role and the admin."""
    db = _build_session()
    ensure_installation_seed(db, create_viewer=False, commit=True)

    role_names = {r.name for r in db.scalars(select(Role)).all()}
    assert {"admin", "editor", "viewer", "stewardship", "data_owner"} <= role_names

    admin_user = db.scalar(select(User).where(User.email == settings.bootstrap_admin_email))
    assert admin_user is not None, "bootstrap admin user not created"
    assert "admin" in {r.name for r in admin_user.roles}

    # create_viewer=False must not seed demo viewer accounts (production posture).
    assert db.scalar(select(User).where(User.email == settings.viewer_email)) is None
