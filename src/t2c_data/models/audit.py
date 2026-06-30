from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from t2c_data.models.base import Base


class AuditLog(Base):
    __tablename__ = "audit_log"
    __table_args__ = (
        Index("ix_audit_log_created_at", "created_at"),
        Index("ix_audit_log_action_created_at", "action", "created_at"),
        Index("ix_audit_log_action_user_email_created_at", "action", "user_email", "created_at"),
        Index("ix_audit_log_entity_created_at", "entity_type", "entity_id", "created_at"),
        Index("ix_audit_log_source_created_at", "source_module", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), default=func.now()
    )
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    actor_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip: Mapped[str | None] = mapped_column(INET, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    parent_entity_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    parent_entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    change_set_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    change_type: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    field_name: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    source_module: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    is_sensitive_change: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false", index=True)
    sensitive_category: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    route: Mapped[str | None] = mapped_column(Text, nullable=True)
    method: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    before_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    after_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)


class AuditLogArchive(Base):
    __tablename__ = "audit_log_archive"
    __table_args__ = (
        Index("ix_audit_log_archive_created_at", "created_at"),
        Index("ix_audit_log_archive_action_created_at", "action", "created_at"),
        Index("ix_audit_log_archive_entity_created_at", "entity_type", "entity_id", "created_at"),
        Index("ix_audit_log_archive_source_created_at", "source_module", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    actor_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip: Mapped[str | None] = mapped_column(INET, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    parent_entity_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    parent_entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    change_set_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    change_type: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    field_name: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    source_module: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    is_sensitive_change: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false", index=True)
    sensitive_category: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    route: Mapped[str | None] = mapped_column(Text, nullable=True)
    method: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    before_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    after_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)


class AccessLog(Base):
    __tablename__ = "access_log"
    __table_args__ = (
        Index("ix_access_log_created_at", "created_at"),
        Index("ix_access_log_api_version_created_at", "api_version", "created_at"),
        Index("ix_access_log_module_created_at", "module_name", "created_at"),
        Index("ix_access_log_route_created_at", "route", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), default=func.now()
    )
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    actor_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip: Mapped[str | None] = mapped_column(INET, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    route: Mapped[str] = mapped_column(Text, nullable=False)
    method: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    api_version: Mapped[str] = mapped_column(Text, nullable=False, server_default="v1")
    module_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)


class AccessLogArchive(Base):
    __tablename__ = "access_log_archive"
    __table_args__ = (
        Index("ix_access_log_archive_created_at", "created_at"),
        Index("ix_access_log_archive_api_version_created_at", "api_version", "created_at"),
        Index("ix_access_log_archive_module_created_at", "module_name", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    actor_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip: Mapped[str | None] = mapped_column(INET, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    route: Mapped[str] = mapped_column(Text, nullable=False)
    method: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    api_version: Mapped[str] = mapped_column(Text, nullable=False, server_default="v1")
    module_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
