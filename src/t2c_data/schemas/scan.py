from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ScanRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    datasource_id: int
    status: str
    started_by: int | None
    summary: dict
    created_at: datetime
    updated_at: datetime


class ScanRunDetailOut(BaseModel):
    id: int
    datasource_id: int
    datasource_name: str | None = None
    status: str
    execution_engine: str | None = None
    spark_master_url: str | None = None
    spark_application_id: str | None = None
    spark_driver_id: str | None = None
    spark_logs_path: str | None = None
    spark_logs_url: str | None = None
    failure_stage: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_detail: str | None = None
    error_stacktrace: str | None = None
    submitted_at: datetime | None = None
    running_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: int | None = None
    discovery: dict[str, int] = Field(default_factory=dict)
    row_counts: dict[str, Any] = Field(default_factory=dict)
    snapshots: int | None = None
    diffs: int | None = None
    legacy_status: str | None = None
    summary: dict[str, Any] = Field(default_factory=dict)


class ScanDiffOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    scan_run_id: int
    entity_type: str
    entity_key: str
    diff_type: str
    old_hash: str | None
    new_hash: str | None
    details: str | None
    created_at: datetime
