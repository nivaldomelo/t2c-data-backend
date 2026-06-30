from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


ObservabilitySourceOrigin = Literal[
    "catalog",
    "datasource_scan",
    "ingestion",
    "airflow",
    "metabase",
    "data_lake",
    "dq",
    "privacy",
    "certification",
    "incident",
    "seed",
    "stale_scan",
    "unknown",
]


ObservabilityLinkedBy = Literal[
    "table_id",
    "datasource_schema_table",
    "canonical_asset_id",
    "fqn",
    "name_only",
    "metabase_sql",
    "airflow_dag",
    "ingestion_log",
    "scan_run_id",
    "unknown",
]


ObservabilityContextState = Literal["selected", "related", "out_of_scope", "unlinked", "stale"]


ObservabilityStatus = Literal["healthy", "attention", "critical", "unreadable", "late", "drift", "blocked"]


ObservabilityReliability = Literal["reliable", "reliable_with_reservations", "watch", "unreliable", "blocked"]


class ObservabilityTimelineEventOut(BaseModel):
    id: str
    type: Literal["arrival", "pipeline", "profiling", "validation", "incident", "alert", "reprocess", "certification"]
    at: datetime | None = None
    label: str
    description: str


class ObservabilityHistoryPointOut(BaseModel):
    label: str
    value: int


class ObservabilityStageDurationOut(BaseModel):
    stage: str
    duration_ms: int


class ObservabilityLayerErrorOut(BaseModel):
    layer: str
    message: str


class ObservabilityContextOut(BaseModel):
    datasource_id: int | None
    datasource_name: str
    scope: Literal["datasource", "global"] = "datasource"
    schema_name: str | None = None
    table_name: str | None = None


class ObservabilityAssetOut(BaseModel):
    table_id: int
    table_name: str
    datasource_id: int
    data_source: str
    domain: str
    layer: str
    criticality: str
    source_origin: ObservabilitySourceOrigin = "catalog"
    linked_by: ObservabilityLinkedBy = "table_id"
    linked_confidence: int = 100
    confidence: int = 100
    scan_run_id: int | None = None
    last_seen_at: datetime | None = None
    is_demo: bool = False
    context_state: ObservabilityContextState = "selected"
    freshness_status: ObservabilityStatus = "unreadable"
    volume_status: ObservabilityStatus = "unreadable"
    schema_status: ObservabilityStatus = "unreadable"
    pipeline_status: ObservabilityStatus = "unreadable"
    reliability_status: ObservabilityReliability = "watch"
    observability_score: int = 0
    quality_score: float | None = None
    last_arrival_at: datetime | None = None
    last_partition: str | None = None
    last_file_path: str | None = None
    last_source_row_at: datetime | None = None
    last_silver_load_at: datetime | None = None
    last_gold_load_at: datetime | None = None
    last_dw_load_at: datetime | None = None
    last_updated_at: datetime | None = None
    current_row_count: int | None = None
    expected_row_count: int | None = None
    historical_avg_row_count: int | None = None
    same_weekday_avg_row_count: int | None = None
    volume_change_pct: float | None = None
    schema_drift_detected: bool = False
    pipeline_failed: bool = False
    partial_failure_detected: bool = False
    critical_rules_total: int = 0
    critical_rules_passed: int = 0
    open_incidents_total: int = 0
    blocking_incidents_total: int = 0
    summary: str
    recommendation: str
    timeline_events: list[ObservabilityTimelineEventOut] = Field(default_factory=list)
    last_pipeline_run_at: datetime | None = None
    dag_name: str | None = None
    last_pipeline_status: str | None = None
    pipeline_duration_ms: int | None = None
    pipeline_attempts: int = 0
    stage_durations: list[ObservabilityStageDurationOut] = Field(default_factory=list)
    layer_errors: list[ObservabilityLayerErrorOut] = Field(default_factory=list)
    reprocess_count: int = 0
    backfill_count: int = 0
    slow_spark_jobs_count: int = 0
    gold_write_failures_count: int = 0
    last_error_message: str | None = None
    certification_valid: bool = False
    gold_newer_than_silver: bool = False
    silver_validated_before_gold: bool = False
    reliability_reasons: list[str] = Field(default_factory=list)
    volume_history: list[ObservabilityHistoryPointOut] = Field(default_factory=list)
    new_columns: list[str] = Field(default_factory=list)
    removed_columns: list[str] = Field(default_factory=list)
    altered_columns: list[str] = Field(default_factory=list)
    nulled_columns: list[str] = Field(default_factory=list)
    parquet_changes: list[str] = Field(default_factory=list)
    relational_changes: list[str] = Field(default_factory=list)
    drift_severity: str = "Sem drift"
    downstream_impact: str = "Sem impacto downstream registrado."


class ObservabilityRelatedSignalsOut(BaseModel):
    airflow: list[ObservabilityAssetOut] = Field(default_factory=list)
    certification: list[ObservabilityAssetOut] = Field(default_factory=list)
    data_lake: list[ObservabilityAssetOut] = Field(default_factory=list)
    dq: list[ObservabilityAssetOut] = Field(default_factory=list)
    datasource_scan: list[ObservabilityAssetOut] = Field(default_factory=list)
    incident: list[ObservabilityAssetOut] = Field(default_factory=list)
    ingestion: list[ObservabilityAssetOut] = Field(default_factory=list)
    metabase: list[ObservabilityAssetOut] = Field(default_factory=list)
    seed: list[ObservabilityAssetOut] = Field(default_factory=list)
    privacy: list[ObservabilityAssetOut] = Field(default_factory=list)
    stale_scan: list[ObservabilityAssetOut] = Field(default_factory=list)
    unknown: list[ObservabilityAssetOut] = Field(default_factory=list)


class ObservabilityDiagnosticsOut(BaseModel):
    selected_assets: int = 0
    out_of_scope_assets: int = 0
    related_signals: int = 0
    unlinked_signals: int = 0


class ObservabilitySummaryOut(BaseModel):
    total: int = 0
    healthy: int = 0
    attention: int = 0
    critical: int = 0
    out_of_sla: int = 0
    schema_drift: int = 0
    volume_anomaly: int = 0
    pipeline_failures: int = 0


class ObservabilityFilterOptionsOut(BaseModel):
    domains: list[str] = Field(default_factory=list)
    layers: list[str] = Field(default_factory=list)


class ObservabilityOverviewOut(BaseModel):
    context: ObservabilityContextOut
    items: list[ObservabilityAssetOut] = Field(default_factory=list)
    related_signals: ObservabilityRelatedSignalsOut = Field(default_factory=ObservabilityRelatedSignalsOut)
    out_of_scope_assets: list[ObservabilityAssetOut] = Field(default_factory=list)
    unlinked_signals: list[ObservabilityAssetOut] = Field(default_factory=list)
    page: int
    page_size: int
    total: int
    summary: ObservabilitySummaryOut = Field(default_factory=ObservabilitySummaryOut)
    diagnostics: ObservabilityDiagnosticsOut = Field(default_factory=ObservabilityDiagnosticsOut)
    filter_options: ObservabilityFilterOptionsOut = Field(default_factory=ObservabilityFilterOptionsOut)


class ObservabilityAssetDetailOut(ObservabilityAssetOut):
    dq_latest: dict[str, object] | None = None
    dq_artifacts: dict[str, object] | None = None
    ingestion_summary: dict[str, object] | None = None
    ingestion_detail: dict[str, object] | None = None
    metabase_consumption: dict[str, object] | None = None
    operational_context: dict[str, object] | None = None
