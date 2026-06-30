from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from t2c_data.schemas.dq_rules import ScheduleMode


class DataSourceScanScheduleRecipientOut(BaseModel):
    id: int
    display_name: str
    email: str


class DataSourceScanScheduleCreate(BaseModel):
    datasource_id: int
    schedule_mode: ScheduleMode = "manual"
    schedule_enabled: bool = True
    schedule_every_minutes: int | None = None
    schedule_time: str | None = None
    schedule_day_of_week: int | None = None
    schedule_day_of_month: int | None = None
    schedule_anchor_date: datetime | None = None
    recipient_user_ids: list[int] = Field(default_factory=list)


class DataSourceScanScheduleUpdate(DataSourceScanScheduleCreate):
    pass


class DataSourceScanScheduleOut(BaseModel):
    id: int
    datasource_id: int
    datasource_name: str
    datasource_type: str
    schedule_mode: ScheduleMode
    schedule_enabled: bool
    schedule_every_minutes: int | None = None
    schedule_time: str | None = None
    schedule_day_of_week: int | None = None
    schedule_day_of_month: int | None = None
    schedule_anchor_date: datetime | None = None
    schedule_last_run_at: datetime | None = None
    schedule_last_started_at: datetime | None = None
    schedule_last_finished_at: datetime | None = None
    schedule_last_status: str | None = None
    schedule_last_error: str | None = None
    schedule_next_run_at: datetime | None = None
    schedule_summary: str | None = None
    notification_recipients: list[DataSourceScanScheduleRecipientOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class DataSourceScanSchedulerStatusOut(BaseModel):
    scheduler_name: str
    mode: str
    is_enabled: bool
    health: str
    last_started_at: str | None
    last_heartbeat_at: str | None
    last_success_at: str | None
    last_failure_at: str | None
    last_error: str | None
    last_run_summary: dict[str, object]
    scheduled_sources_total: int
    next_expected_run_at: str | None


class DataSourceScheduleUserOption(BaseModel):
    id: int
    display_name: str
    email: str

