from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field


class IngestionPipelineRefOut(BaseModel):
    pipeline_id: str | None = None
    pipeline_name: str | None = None
    dag_id: str | None = None
    task_name: str | None = None
    load_type: str | None = None
    load_type_label: str | None = None
    source_connection: str | None = None
    source_database: str | None = None
    source_table: str | None = None
    target_schema: str | None = None
    target_table: str | None = None
    latest_status: str | None = None
    latest_status_label: str | None = None
    watermark_value: str | None = None
    watermark_column: str | None = None
    watermark_type: str | None = None
    last_success_at: datetime | None = None
    last_execution_started_at: datetime | None = None
    last_execution_finished_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_error: str | None = None
    rows_processed: int | None = None
    pipeline_history_href: str | None = None
    airflow_dag_href: str | None = None
    airflow_task_href: str | None = None
    is_primary: bool = False


class IngestionTableSummaryOut(BaseModel):
    linked: bool
    state: str
    message: str | None = None
    table_schema: str
    table_name: str
    pipeline_count: int = 0
    primary_pipeline: IngestionPipelineRefOut | None = None
    pipelines: list[IngestionPipelineRefOut] = Field(default_factory=list)


class IngestionStabilityPointOut(BaseModel):
    execution_id: str
    occurred_at: datetime | None = None
    status_label: str
    success: bool = False
    rows_written: int | None = None


class IngestionStabilitySummaryOut(BaseModel):
    window_runs: int = 0
    success_rate_pct: float = 0
    failed_runs: int = 0
    recurrent_degradation: bool = False
    currently_stale: bool = False
    current_status_label: str | None = None
    points: list[IngestionStabilityPointOut] = Field(default_factory=list)


class IngestionExecutionOut(BaseModel):
    execution_id: str
    pipeline_id: str | None = None
    pipeline_name: str | None = None
    dag_id: str | None = None
    airflow_dag_href: str | None = None
    airflow_run_href: str | None = None
    status: str | None = None
    status_label: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: int | None = None
    rows_extracted: int | None = None
    rows_written: int | None = None
    rows_upserted: int | None = None
    watermark_before: str | None = None
    watermark_after: str | None = None
    error_message: str | None = None


class IngestionExecutionPageOut(BaseModel):
    linked: bool
    state: str
    message: str | None = None
    table_schema: str
    table_name: str
    page: int
    page_size: int
    total: int
    items: list[IngestionExecutionOut] = Field(default_factory=list)


class IngestionLogOut(BaseModel):
    log_id: str
    execution_id: str
    occurred_at: datetime | None = None
    step: str | None = None
    level: str | None = None
    message: str | None = None
    stacktrace: str | None = None


class IngestionExecutionLogsOut(BaseModel):
    execution_id: str
    page: int
    page_size: int
    total: int
    items: list[IngestionLogOut] = Field(default_factory=list)


class IngestionTableDetailOut(BaseModel):
    summary: IngestionTableSummaryOut
    executions: IngestionExecutionPageOut
    stability: IngestionStabilitySummaryOut | None = None
    history: list["IngestionStabilityHistoryPointOut"] = Field(default_factory=list)


class IngestionStabilityHistoryPointOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    bucket_start_at: datetime
    pipeline_name: str | None = None
    dag_id: str | None = None
    task_name: str | None = None
    latest_status_label: str | None = None
    rows_processed: int | None = None
    last_success_at: datetime | None = None
    last_execution_finished_at: datetime | None = None
    window_runs: int = 0
    success_rate_pct: float = 0
    failed_runs: int = 0
    recurrent_degradation: bool = False
    currently_stale: bool = False


class IngestionOperationalOverviewItemOut(BaseModel):
    table_id: int | None = None
    schema_name: str | None = None
    table_name: str
    table_fqn: str
    pipeline_name: str | None = None
    dag_id: str | None = None
    task_name: str | None = None
    load_type: str | None = None
    load_type_label: str | None = None
    latest_status_label: str | None = None
    last_status: str | None = None
    last_success_at: datetime | None = None
    last_execution_finished_at: datetime | None = None
    last_run_started_at: datetime | None = None
    last_run_finished_at: datetime | None = None
    last_watermark: str | None = None
    watermark_value: str | None = None
    records_processed: int | None = None
    rows_processed: int | None = None
    observacao: str | None = None
    last_error: str | None = None
    pipeline_history_href: str | None = None
    airflow_dag_href: str | None = None
    airflow_task_href: str | None = None
    target_url: str | None = None


class IngestionOperationalOverviewOut(BaseModel):
    available: bool
    message: str | None = None
    generated_at: datetime | None = None
    pipelines_total: int = 0
    linked_tables: int = 0
    unmapped: int = 0
    degraded: int = 0
    failed: int = 0
    running: int = 0
    pending: int = 0
    stale: int = 0
    critical_stale: int = 0
    high_volume_failed: int = 0
    high_volume_failed_threshold_rows: int = 0
    stale_threshold_hours: int = 0
    items: list[IngestionOperationalOverviewItemOut] = Field(default_factory=list)
    unmapped_items: list[IngestionOperationalOverviewItemOut] = Field(default_factory=list)
    degraded_items: list[IngestionOperationalOverviewItemOut] = Field(default_factory=list)
    failed_items: list[IngestionOperationalOverviewItemOut] = Field(default_factory=list)
    critical_stale_items: list[IngestionOperationalOverviewItemOut] = Field(default_factory=list)
    high_volume_failed_items: list[IngestionOperationalOverviewItemOut] = Field(default_factory=list)
