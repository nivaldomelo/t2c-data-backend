from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime as SADateTime, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class OperationalFailureTaxonomy(Base):
    __tablename__ = "operational_failure_taxonomy"

    code: Mapped[str] = mapped_column(String(80), primary_key=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    default_severity: Mapped[str] = mapped_column(String(30), nullable=False, default="medium", server_default="medium")
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    source_group: Mapped[str | None] = mapped_column(String(120))

    events: Mapped[list["OperationalFailureEvent"]] = relationship(
        "OperationalFailureEvent",
        back_populates="taxonomy",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class OperationalFailureEvent(TimestampMixin, Base):
    __tablename__ = "operational_failure_events"
    __table_args__ = (
        UniqueConstraint("source", "external_reference", name="uq_operational_failure_source_reference"),
        Index("ix_operational_failure_events_occurred_at", "occurred_at"),
        Index("ix_operational_failure_events_source_occurred_at", "source", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(SADateTime(timezone=True), nullable=False)
    category_code: Mapped[str] = mapped_column(
        ForeignKey("operational_failure_taxonomy.code", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    severity: Mapped[str] = mapped_column(String(30), nullable=False, default="medium", server_default="medium")
    source: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    error_type: Mapped[str | None] = mapped_column(String(160))
    message: Mapped[str] = mapped_column(Text, nullable=False)
    context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    retryable: Mapped[bool | None] = mapped_column(Boolean)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), index=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), index=True)
    scheduler_name: Mapped[str | None] = mapped_column(String(120))
    job_name: Mapped[str | None] = mapped_column(String(160))
    route: Mapped[str | None] = mapped_column(String(240))
    external_reference: Mapped[str | None] = mapped_column(String(200))

    taxonomy: Mapped[OperationalFailureTaxonomy] = relationship("OperationalFailureTaxonomy", back_populates="events")


class BackupExecution(TimestampMixin, Base):
    __tablename__ = "backup_executions"
    __table_args__ = (
        Index("ix_backup_executions_scope_started_at", "scope", "started_at"),
        Index("ix_backup_executions_status_started_at", "status", "started_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    scope: Mapped[str] = mapped_column(String(80), nullable=False, default="platform", server_default="platform")
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime] = mapped_column(SADateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(SADateTime(timezone=True))
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    retention_days: Mapped[int | None] = mapped_column(Integer)
    storage_uri: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    trigger_source: Mapped[str] = mapped_column(String(40), nullable=False, default="manual", server_default="manual")
    triggered_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    metadata_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


__all__ = [
    "BackupExecution",
    "OperationalFailureEvent",
    "OperationalFailureTaxonomy",
]
