from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class BackupExecutionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    scope: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    retention_days: int | None
    storage_uri: str | None
    size_bytes: int | None
    error_message: str | None
    trigger_source: str
    triggered_by_user_id: int | None
    metadata_json: dict | None = None


class OperationalFailureEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    occurred_at: datetime
    category_code: str
    severity: str
    source: str
    error_type: str | None
    message: str
    retryable: bool | None
    table_id: int | None
    datasource_id: int | None
    scheduler_name: str | None
    job_name: str | None
    route: str | None
    external_reference: str | None
    context_json: dict | None = None


class OperationalFailureTaxonomyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    name: str
    description: str | None = None
    default_severity: str
    retryable: bool
    source_group: str | None = None
