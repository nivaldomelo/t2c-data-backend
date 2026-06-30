from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, Table, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


datasource_scan_schedule_recipients = Table(
    "datasource_scan_schedule_recipients",
    Base.metadata,
    Column("schedule_id", ForeignKey("t2c_data.datasource_scan_schedules.id", ondelete="CASCADE"), primary_key=True),
    Column("user_id", ForeignKey("t2c_data.users.id", ondelete="CASCADE"), primary_key=True),
    schema="t2c_data",
)


class DataSourceScanSchedulerStatus(TimestampMixin, Base):
    __tablename__ = "datasource_scan_scheduler_status"
    __table_args__ = {"schema": "t2c_data"}

    id: Mapped[int] = mapped_column(primary_key=True)
    scheduler_name: Mapped[str] = mapped_column(String(80), nullable=False, default="datasource_scan", server_default="datasource_scan")
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="worker", server_default="worker")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    last_started_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_heartbeat_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_success_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_failure_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_run_summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DataSourceScanSchedule(TimestampMixin, Base):
    __tablename__ = "datasource_scan_schedules"
    __table_args__ = {"schema": "t2c_data"}

    id: Mapped[int] = mapped_column(primary_key=True)
    datasource_id: Mapped[int] = mapped_column(ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    schedule_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual", index=True)
    schedule_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true", index=True)
    schedule_every_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    schedule_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    schedule_day_of_week: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_day_of_month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_anchor_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_status: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    schedule_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    schedule_next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    schedule_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    datasource: Mapped["DataSource"] = relationship("DataSource")
    notification_recipients: Mapped[list["User"]] = relationship(
        "User",
        secondary=datasource_scan_schedule_recipients,
        lazy="selectin",
    )
