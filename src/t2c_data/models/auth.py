from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Table, Text, UniqueConstraint, func
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin

user_role = Table(
    "user_role",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
)

role_permission = Table(
    "role_permissions",
    Base.metadata,
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
    Column("permission_id", Integer, ForeignKey("permissions.id", ondelete="CASCADE"), primary_key=True),
)

user_access_group = Table(
    "user_access_groups",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("group_id", Integer, ForeignKey("access_groups.id", ondelete="CASCADE"), primary_key=True),
)


class User(TimestampMixin, Base):
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("email", name="uq_users_email"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str | None] = mapped_column(String(255))
    full_name: Mapped[str | None] = mapped_column(String(255))
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    token_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    mfa_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    mfa_secret_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Grace logins used WITHOUT MFA enrolled; once the limit is reached the user is
    # locked until an admin unlocks them (or they enroll MFA within the grace window).
    mfa_grace_logins_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    mfa_locked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    mfa_locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Último contador TOTP aceito (anti-replay): rejeita código cujo contador <= este.
    mfa_last_counter: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # When the password was last set; access is blocked once it is older than the
    # configured max age (default 90 days) until rotated or released by an admin.
    password_changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    ui_theme: Mapped[str] = mapped_column(String(30), nullable=False, default="atual", server_default="atual")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    allowed_domains: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    allowed_environments: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    roles: Mapped[list[Role]] = relationship("Role", secondary=user_role, back_populates="users")
    access_groups: Mapped[list[AccessGroup]] = relationship("AccessGroup", secondary=user_access_group, back_populates="users")
    access_grants: Mapped[list[DataAccessGrant]] = relationship("DataAccessGrant", back_populates="user", cascade="all, delete-orphan")
    sessions: Mapped[list[UserSession]] = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")
    access_events: Mapped[list[UserAccessEvent]] = relationship("UserAccessEvent", back_populates="user", cascade="all, delete-orphan")


class Role(TimestampMixin, Base):
    __tablename__ = "roles"
    __table_args__ = (UniqueConstraint("name", name="uq_roles_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    description: Mapped[str | None] = mapped_column(String(255))

    users: Mapped[list[User]] = relationship("User", secondary=user_role, back_populates="roles")
    permissions: Mapped[list[Permission]] = relationship(
        "Permission",
        secondary=role_permission,
        back_populates="roles",
    )


class Permission(TimestampMixin, Base):
    __tablename__ = "permissions"
    __table_args__ = (UniqueConstraint("name", name="uq_permissions_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(String(255))

    roles: Mapped[list[Role]] = relationship(
        "Role",
        secondary=role_permission,
        back_populates="permissions",
    )


class UserSession(TimestampMixin, Base):
    __tablename__ = "user_sessions"
    __table_args__ = (
        UniqueConstraint("jti", name="uq_user_sessions_jti"),
        Index("ix_user_sessions_user_started_at", "user_id", "started_at"),
        Index("ix_user_sessions_last_seen_at", "last_seen_at"),
        Index("ix_user_sessions_ended_at", "ended_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    jti: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_reason: Mapped[str | None] = mapped_column(String(30), nullable=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    ip_address: Mapped[str | None] = mapped_column(String(64))
    user_agent: Mapped[str | None] = mapped_column(String(255))
    device_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    browser: Mapped[str | None] = mapped_column(String(80), nullable=True)
    os: Mapped[str | None] = mapped_column(String(80), nullable=True)
    country: Mapped[str | None] = mapped_column(String(80), nullable=True)
    city: Mapped[str | None] = mapped_column(String(80), nullable=True)
    auth_method: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    mfa_used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    success: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    user: Mapped[User] = relationship("User", back_populates="sessions")


class UserAccessEvent(TimestampMixin, Base):
    __tablename__ = "user_access_events"
    __table_args__ = (
        Index("ix_user_access_events_created_at", "created_at"),
        Index("ix_user_access_events_user_created_at", "user_id", "created_at"),
        Index("ix_user_access_events_session_created_at", "session_id", "created_at"),
        Index("ix_user_access_events_event_type_created_at", "event_type", "created_at"),
        Index("ix_user_access_events_page_key_created_at", "page_key", "created_at"),
        Index("ix_user_access_events_resource_type_resource_id", "resource_type", "resource_id"),
        Index("ix_user_access_events_datasource_schema_table", "datasource_id", "schema_name", "table_id"),
        Index("ix_user_access_events_sensitivity_level", "sensitivity_level"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    session_id: Mapped[int | None] = mapped_column(ForeignKey("user_sessions.id", ondelete="SET NULL"), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    page_key: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    route_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    http_method: Mapped[str | None] = mapped_column(String(10), nullable=True)
    resource_type: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    resource_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    resource_fqn: Mapped[str | None] = mapped_column(String(1000), nullable=True, index=True)
    datasource_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    table_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    action: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    sensitivity_level: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    has_personal_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    has_sensitive_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    privacy_classification: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    ip_address: Mapped[str | None] = mapped_column(INET, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)

    user: Mapped[User] = relationship("User", back_populates="access_events")
