from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.auth import User
from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class UserNotificationPreference(TimestampMixin, Base):
    __tablename__ = "user_notification_preferences"
    __table_args__ = (
        UniqueConstraint("user_id", name="uq_user_notification_preferences_user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    in_app_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    email_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    governance_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    stewardship_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    operational_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    only_assigned_items: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    daily_digest_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    last_daily_digest_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_daily_digest_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_daily_digest_status: Mapped[str | None] = mapped_column(String(20), nullable=True)

    user = relationship("User", foreign_keys=[user_id])


class UserInboxNotification(TimestampMixin, Base):
    __tablename__ = "user_inbox_notifications"
    __table_args__ = (
        UniqueConstraint("user_id", "dedupe_key", name="uq_user_inbox_notifications_user_dedupe"),
        Index("ix_user_inbox_notifications_user_state", "user_id", "state"),
        Index("ix_user_inbox_notifications_category", "category"),
        Index("ix_user_inbox_notifications_due_delivery", "delivery_state", "next_delivery_at"),
        Index("ix_user_inbox_notifications_forwarded_from", "forwarded_from_notification_id"),
        Index("ix_user_inbox_notifications_forwarded_by", "forwarded_by_user_id"),
        Index("ix_user_inbox_notifications_created", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", server_default="medium")
    source_module: Mapped[str] = mapped_column(String(40), nullable=False)
    source_entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    source_entity_id: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    href: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(String(20), nullable=False, default="unread", server_default="unread")
    delivery_state: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default="pending")
    context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    forwarded_from_notification_id: Mapped[int | None] = mapped_column(
        ForeignKey("user_inbox_notifications.id", ondelete="SET NULL"),
        nullable=True,
    )
    forwarded_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    forwarded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_notified_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_delivery_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    read_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    archived_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivery_channels_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    user = relationship("User", foreign_keys=lambda: [UserInboxNotification.user_id])
    forwarded_from_notification = relationship(
        "UserInboxNotification",
        remote_side=lambda: [UserInboxNotification.id],
        foreign_keys=lambda: [UserInboxNotification.forwarded_from_notification_id],
    )
    forwarded_by_user = relationship("User", foreign_keys=lambda: [UserInboxNotification.forwarded_by_user_id])

