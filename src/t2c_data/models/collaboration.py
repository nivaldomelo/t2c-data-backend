from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class CollaborationComment(TimestampMixin, Base):
    __tablename__ = "collaboration_comments"
    __table_args__ = (
        Index("ix_collaboration_comments_entity", "entity_type", "entity_id"),
        Index("ix_collaboration_comments_task_id", "task_id"),
        Index("ix_collaboration_comments_author_user_id", "author_user_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    entity_label: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[int | None] = mapped_column(
        ForeignKey("collaboration_tasks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    parent_comment_id: Mapped[int | None] = mapped_column(
        ForeignKey("collaboration_comments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    comment_kind: Mapped[str] = mapped_column(String(40), nullable=False, default="comment", server_default="comment")
    body: Mapped[str] = mapped_column(Text, nullable=False)
    visibility_scope: Mapped[str] = mapped_column(String(24), nullable=False, default="collaboration", server_default="collaboration")
    is_resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    author_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    author_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    author_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    resolved_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    context_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    author_user = relationship("User", foreign_keys=[author_user_id])
    resolved_by_user = relationship("User", foreign_keys=[resolved_by_user_id])
    task = relationship("CollaborationTask", back_populates="comments")
    parent_comment = relationship("CollaborationComment", remote_side=lambda: [CollaborationComment.id])


class CollaborationTask(TimestampMixin, Base):
    __tablename__ = "collaboration_tasks"
    __table_args__ = (
        Index("ix_collaboration_tasks_entity", "entity_type", "entity_id"),
        Index("ix_collaboration_tasks_status", "status"),
        Index("ix_collaboration_tasks_assigned_to", "assigned_to_user_id"),
        Index("ix_collaboration_tasks_task_type", "task_type"),
        Index("ix_collaboration_tasks_due_at", "due_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    entity_label: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(220), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_type: Mapped[str] = mapped_column(String(60), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="open", server_default="open")
    priority: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", server_default="medium")
    responsibility_role: Mapped[str | None] = mapped_column(String(80), nullable=True)
    assigned_to_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    assigned_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    linked_request_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    context_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    assigned_to_user = relationship("User", foreign_keys=[assigned_to_user_id])
    assigned_by_user = relationship("User", foreign_keys=[assigned_by_user_id])
    completed_by_user = relationship("User", foreign_keys=[completed_by_user_id])
    comments = relationship(
        "CollaborationComment",
        back_populates="task",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    events = relationship(
        "CollaborationEvent",
        back_populates="task",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by=lambda: CollaborationEvent.created_at.asc(),
    )


class CollaborationEvent(TimestampMixin, Base):
    __tablename__ = "collaboration_events"
    __table_args__ = (
        Index("ix_collaboration_events_entity", "entity_type", "entity_id"),
        Index("ix_collaboration_events_event_type", "event_type"),
        Index("ix_collaboration_events_task_id", "task_id"),
        Index("ix_collaboration_events_comment_id", "comment_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(60), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_from: Mapped[str | None] = mapped_column(String(24), nullable=True)
    status_to: Mapped[str | None] = mapped_column(String(24), nullable=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    actor_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    actor_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    comment_id: Mapped[int | None] = mapped_column(
        ForeignKey("collaboration_comments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    task_id: Mapped[int | None] = mapped_column(
        ForeignKey("collaboration_tasks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    payload_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    actor_user = relationship("User", foreign_keys=[actor_user_id])
    comment = relationship("CollaborationComment")
    task = relationship("CollaborationTask", back_populates="events")

