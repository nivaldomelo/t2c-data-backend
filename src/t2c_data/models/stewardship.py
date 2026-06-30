from __future__ import annotations

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class StewardshipRequest(TimestampMixin, Base):
    __tablename__ = "stewardship_requests"
    __table_args__ = (
        Index("ix_stewardship_requests_status", "status"),
        Index("ix_stewardship_requests_request_type", "request_type"),
        Index("ix_stewardship_requests_table_id", "table_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True)
    request_type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default="pending")
    request_origin: Mapped[str] = mapped_column(String(40), nullable=False, default="manual", server_default="manual")
    requested_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    approver_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    decided_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    requester_comment: Mapped[str | None] = mapped_column(Text)
    decision_comment: Mapped[str | None] = mapped_column(Text)
    current_value_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    proposed_value_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    decided_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))

    table = relationship("TableEntity")
    requested_by_user = relationship("User", foreign_keys=[requested_by_user_id])
    approver_user = relationship("User", foreign_keys=[approver_user_id])
    decided_by_user = relationship("User", foreign_keys=[decided_by_user_id])
    events: Mapped[list["StewardshipRequestEvent"]] = relationship(
        "StewardshipRequestEvent",
        back_populates="request",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="StewardshipRequestEvent.created_at",
    )


class StewardshipRequestEvent(TimestampMixin, Base):
    __tablename__ = "stewardship_request_events"
    __table_args__ = (
        Index("ix_stewardship_request_events_request_id", "stewardship_request_id"),
        Index("ix_stewardship_request_events_event_type", "event_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    stewardship_request_id: Mapped[int] = mapped_column(
        ForeignKey("stewardship_requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    comment: Mapped[str | None] = mapped_column(Text)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    request: Mapped[StewardshipRequest] = relationship("StewardshipRequest", back_populates="events")
    actor_user = relationship("User", foreign_keys=[actor_user_id])
