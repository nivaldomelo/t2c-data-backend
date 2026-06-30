from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class IntegrationHealth(TimestampMixin, Base):
    __tablename__ = "integration_health"
    __table_args__ = (
        UniqueConstraint("integration_name", name="uq_integration_health_name"),
        Index("ix_integration_health_status", "status"),
        Index("ix_integration_health_category", "category"),
        Index("ix_integration_health_checked_at", "checked_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    integration_name: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="unavailable")
    status_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason_code: Mapped[str | None] = mapped_column(String(80), nullable=True)
    category: Mapped[str | None] = mapped_column(String(40), nullable=True)
    base_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(160), nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    details_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    breaker_state: Mapped[str] = mapped_column(String(20), nullable=False, default="closed")
    breaker_open_until_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    history: Mapped[list["IntegrationHealthHistory"]] = relationship(
        "IntegrationHealthHistory",
        back_populates="health",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class IntegrationHealthHistory(TimestampMixin, Base):
    __tablename__ = "integration_health_history"
    __table_args__ = (
        Index("ix_integration_health_history_integration_checked_at", "integration_name", "checked_at"),
        Index("ix_integration_health_history_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    integration_health_id: Mapped[int] = mapped_column(
        ForeignKey("integration_health.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    integration_name: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    status_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason_code: Mapped[str | None] = mapped_column(String(80), nullable=True)
    category: Mapped[str | None] = mapped_column(String(40), nullable=True)
    base_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(160), nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    details_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    breaker_state: Mapped[str] = mapped_column(String(20), nullable=False, default="closed")
    breaker_open_until_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    health: Mapped["IntegrationHealth"] = relationship("IntegrationHealth", back_populates="history")


__all__ = ["IntegrationHealth", "IntegrationHealthHistory"]
