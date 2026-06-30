from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")
os.environ.setdefault("INITIAL_ADMIN_NAME", "Reset Admin")
os.environ.setdefault("INITIAL_ADMIN_EMAIL", "admin@andromeda.com")
os.environ.setdefault("INITIAL_ADMIN_PASSWORD", "reset-admin-pass")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.admin_recovery import reset_bootstrap_admin_password
from t2c_data.core.config import settings
from t2c_data.core.security import verify_password
from t2c_data.models.auth import Permission, Role, User, role_permission, user_role


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session_factory():
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

    with engine.begin() as conn:
        Role.__table__.create(bind=conn)
        Permission.__table__.create(bind=conn)
        User.__table__.create(bind=conn)
        user_role.create(bind=conn)
        role_permission.create(bind=conn)

    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def test_reset_bootstrap_admin_password_updates_existing_user_and_activates(monkeypatch) -> None:
    session_factory = _build_session_factory()
    monkeypatch.setattr(settings, "initial_admin_password", "Reset-Admin-2026!")

    with session_factory() as session:
        admin = User(
            email="admin@andromeda.com",
            name="Old Admin",
            full_name="Old Admin",
            password_hash="old-hash",
            is_active=False,
        )
        session.add(admin)
        session.commit()

    with session_factory() as session:
        result = reset_bootstrap_admin_password(session, commit=True)
        updated = session.scalar(select(User).where(User.email == settings.bootstrap_admin_email))

    assert result.email == settings.bootstrap_admin_email
    assert result.created is False
    assert result.reactivated is True
    assert updated is not None
    assert updated.is_active is True
    assert verify_password(settings.bootstrap_admin_password, updated.password_hash)


def test_reset_bootstrap_admin_password_creates_missing_user(monkeypatch) -> None:
    session_factory = _build_session_factory()
    monkeypatch.setattr(settings, "initial_admin_password", "Reset-Admin-2026!")

    with session_factory() as session:
        result = reset_bootstrap_admin_password(session, commit=True)
        created = session.scalar(select(User).where(User.email == settings.bootstrap_admin_email))

    assert result.created is True
    assert created is not None
    assert created.is_active is True
    assert verify_password(settings.bootstrap_admin_password, created.password_hash)
