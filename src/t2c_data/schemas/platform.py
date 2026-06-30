from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PlatformMetricItemOut(BaseModel):
    label: str
    value: int


class PlatformAssetQueueItemOut(BaseModel):
    table_id: int
    table_name: str
    table_fqn: str
    target_url: str
    status_label: str | None = None
    last_success_at: datetime | None = None
    pipeline_history_href: str | None = None
    hint: str | None = None
    pipeline_name: str | None = None
    dag_id: str | None = None
    rows_processed: int | None = None
    airflow_dag_href: str | None = None
    airflow_task_href: str | None = None


class PlatformCorrelationPriorityOut(BaseModel):
    asset_id: int
    table_id: int
    asset_name: str
    qualified_name: str
    schema_name: str
    source_name: str
    has_operational_failure: bool
    has_dq_degradation: bool
    has_open_incident: bool
    priority_score: int
    correlation_type: str
    summary: str
    table_fqn: str | None = None
    total_clicks: int | None = None
    target_url: str | None = None


class PlatformFailureItemOut(BaseModel):
    id: int
    status: str
    created_at: datetime | None = None
    datasource_id: int | None = None
    table_id: int | None = None
    job_type: str | None = None
    table_fqn: str | None = None
    target_url: str | None = None


class PlatformIngestionItemOut(BaseModel):
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


class PlatformIngestionSummaryOut(BaseModel):
    available: bool
    message: str | None = None
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
    high_volume_failed_threshold_rows: int = 100000
    stale_threshold_hours: int = 72
    items: list[PlatformIngestionItemOut] = Field(default_factory=list)
    high_volume_failed_items: list[PlatformIngestionItemOut] = Field(default_factory=list)


class PlatformAnalyticsTopAssetOut(BaseModel):
    asset_id: int
    asset_type: str
    asset_name: str
    schema_name: str | None = None
    qualified_name: str
    source_name: str | None = None
    total_clicks: int
    entity_type: str | None = None
    entity_id: int | None = None
    count: int | None = None


class PlatformAnalyticsTrendPointOut(BaseModel):
    label: str
    search_queries: int = 0
    search_clicks: int = 0
    usage_events: int = 0
    explorer_page_views: int = 0
    incidents_page_views: int = 0
    certification_page_views: int = 0
    privacy_page_views: int = 0
    legacy_api_hits: int = 0


class PlatformAnalyticsSummaryOut(BaseModel):
    generated_at: datetime
    window_days: int
    search_queries: int
    search_clicks: int
    usage_events: int
    search_to_asset_conversion_pct: float
    dashboard_to_action_count: int
    campaign_to_update_count: int
    explorer_page_views: int = 0
    incidents_page_views: int = 0
    certification_page_views: int = 0
    privacy_page_views: int = 0
    legacy_api_hits: int = 0
    legacy_api_cutoff_window_days: int = 30
    managed_legacy_modules: list[str] = Field(default_factory=list)
    disabled_legacy_modules: list[str] = Field(default_factory=list)
    force_enabled_legacy_modules: list[str] = Field(default_factory=list)
    eligible_legacy_modules_to_disable: list[str] = Field(default_factory=list)
    top_modules: list[PlatformMetricItemOut] = Field(default_factory=list)
    top_events: list[PlatformMetricItemOut] = Field(default_factory=list)
    top_legacy_modules: list[PlatformMetricItemOut] = Field(default_factory=list)
    top_assets: list[PlatformAnalyticsTopAssetOut] = Field(default_factory=list)
    trend: list[PlatformAnalyticsTrendPointOut] = Field(default_factory=list)


class PlatformLegacyApiSurfaceItemOut(BaseModel):
    module: str
    legacy_prefixes: list[str] = Field(default_factory=list)
    canonical_prefixes: list[str] = Field(default_factory=list)
    hits_total: int = 0
    hits_in_window: int = 0
    last_hit_at: datetime | None = None
    managed: bool = True
    disabled: bool = False
    forced_enabled: bool = False
    physically_removed: bool = False
    sunset_status: str
    note: str


class PlatformLegacyApiSurfaceOut(BaseModel):
    window_days: int
    official_surface: str
    temporary_surface: str
    items: list[PlatformLegacyApiSurfaceItemOut] = Field(default_factory=list)


class PlatformSchedulerStatusOut(BaseModel):
    scheduler_name: str
    mode: str
    is_enabled: bool
    applicable: bool = True
    health: str
    last_started_at: str | None = None
    last_heartbeat_at: str | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    last_error: str | None = None
    last_run_summary: dict[str, object] = Field(default_factory=dict)


class IntegrationSyncJobRunIn(BaseModel):
    source: str = Field(min_length=1)
    job_type: str = Field(min_length=1)
    target_type: str | None = None
    target_id: int | None = None
    target_name: str | None = None
    trigger_mode: str = Field(default="manual")


class IntegrationSyncJobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    job_key: str
    source: str
    job_type: str
    target_type: str | None = None
    target_id: int | None = None
    target_name: str | None = None
    trigger_mode: str
    status: str
    queued_at: datetime | None = None
    started_at: datetime
    finished_at: datetime | None = None
    next_expected_run_at: datetime | None = None
    records_processed: int | None = None
    progress_pct: float | None = None
    correlation_id: str | None = None
    requested_by_user_id: int | None = None
    error: str | None = None
    context_json: dict[str, Any] | list | None = None
    result_summary_json: dict[str, Any] | list | None = None
    artifact_public_id: str | None = None
    artifact_filename: str | None = None
    artifact_content_type: str | None = None
    artifact_storage_path: str | None = None
    artifact_available_at: datetime | None = None
    artifact_expires_at: datetime | None = None
    artifact_size_bytes: int | None = None
    artifact_download_count: int = 0
    artifact_last_downloaded_at: datetime | None = None
    export_status_href: str | None = None
    export_download_href: str | None = None
    export_download_available: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None
    diagnostic_status: str | None = None
    diagnostic_severity: str | None = None
    diagnostic_label: str | None = None
    diagnostic_description: str | None = None
    diagnostic_impact: str | None = None
    diagnostic_recommended_action: str | None = None
    diagnostic_module: str | None = None
    diagnostic_probable_cause: str | None = None
    diagnostic_probable_cause_code: str | None = None
    diagnostic_evidence: str | None = None
    diagnostic_runbook_url: str | None = None
    diagnostic_correlation_id: str | None = None
    diagnostic_generated_at: datetime | None = None
    diagnostic_recurrence_count: int | None = None
    is_stalled: bool = False
    is_overdue_next_run: bool = False
    running_duration_seconds: int | None = None


class PlatformCockpitRecommendedActionOut(BaseModel):
    id: str
    title: str
    severity: str
    origin: str
    impact: str
    reason: str
    suggested_route: str | None = None
    primary_action_label: str
    secondary_action_label: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0


class PlatformCockpitRecommendedActionsOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[PlatformCockpitRecommendedActionOut] = Field(default_factory=list)


class PlatformCockpitQueueItemOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    id: str
    type: str
    category: str
    title: str
    subtitle: str | None = None
    severity: str
    status: str
    description: str
    asset_id: int | None = None
    asset_name: str | None = None
    connection: str | None = None
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    pipeline_name: str | None = None
    dag_id: str | None = None
    task_id: str | None = None
    recommended_action: str | None = None
    route: str | None = None
    updated_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PlatformCockpitQueuePageOut(BaseModel):
    generated_at: datetime
    category: str | None = None
    page: int = 1
    page_size: int = 25
    total: int = 0
    total_pages: int = 0
    has_more: bool = False
    items: list[PlatformCockpitQueueItemOut] = Field(default_factory=list)


class PlatformJobsStatusOut(BaseModel):
    generated_at: datetime
    total: int = 0
    queued: int = 0
    running: int = 0
    success: int = 0
    partial_success: int = 0
    failed: int = 0
    skipped: int = 0
    next_expected_run_at: datetime | None = None
    items: list[IntegrationSyncJobOut] = Field(default_factory=list)


class PlatformOperationalSourceOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    configured: bool
    schema_name: str = Field(alias="schema")
    source_kind: str | None = None
    has_url: bool
    has_host_parts: bool
    host: str | None = None
    port: int | None = None
    database: str | None = None
    user: str | None = None
    connect_timeout_seconds: int | None = None


class PlatformCockpitOut(BaseModel):
    generated_at: datetime
    runtime: dict[str, object]
    health: dict[str, int]
    correlation_priority: list[PlatformCorrelationPriorityOut] = Field(default_factory=list)
    queues: dict[str, list[PlatformAssetQueueItemOut]]
    recent_failures: dict[str, list[PlatformFailureItemOut]]
    ingestion: PlatformIngestionSummaryOut
    analytics: PlatformAnalyticsSummaryOut


class ReadModelRefreshOut(BaseModel):
    refreshed_at: datetime
    search_entries: int
    dashboard_entries: int
    mode: str = "full"


class AssetVisibilityRuleOut(BaseModel):
    id: int
    entity_type: str
    entity_id: int | None = None
    rule_scope: str
    match_value: str | None = None
    allowed_role: str | None = None
    allowed_user_id: int | None = None
    visibility_scope: str
    mask_sensitive_fields: bool = False
    reason: str | None = None
    is_active: bool
    created_at: datetime | None = None


class AssetVisibilityRuleIn(BaseModel):
    entity_type: str = "table"
    entity_id: int | None = None
    rule_scope: str = "asset"
    match_value: str | None = None
    allowed_role: str | None = None
    allowed_user_id: int | None = None
    visibility_scope: str = "full"
    mask_sensitive_fields: bool = False
    reason: str | None = None
    is_active: bool = True


class PlatformActionOut(BaseModel):
    ok: bool = True
    message: str
    target_id: int | None = None


class PlatformAutomationActionOut(BaseModel):
    key: str
    label: str
    description: str
    category: str
    category_label: str
    executable: bool = True
    destructive: bool = False
    suggestion_only: bool = False
    requires_target: bool = False
    target_types: list[str] = Field(default_factory=list)
    scope_kinds: list[str] = Field(default_factory=list)
    hints: list[str] = Field(default_factory=list)
    default_payload_json: dict[str, Any] | None = None


class PlatformAutomationRuleIn(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None
    status: str = Field(default="active")
    scope_kind: str = Field(default="asset")
    scope_value: str | None = None
    condition_kind: str = Field(min_length=1)
    condition_operator: str = Field(default="gte")
    threshold_value: int | None = None
    window_days: int = Field(default=7, ge=1, le=365)
    action_key: str = Field(min_length=1)
    action_target_json: dict[str, Any] | None = None
    execution_mode: str = Field(default="automatic")
    notify_owner: bool = True
    open_incident: bool = False
    schedule_enabled: bool = True
    notes: str | None = None


class PlatformAutomationRuleOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    status: str
    scope_kind: str
    scope_value: str | None = None
    condition_kind: str
    condition_operator: str
    threshold_value: int | None = None
    window_days: int
    action_key: str
    action_target_json: dict[str, Any] | list | None = None
    execution_mode: str
    notify_owner: bool
    open_incident: bool
    schedule_enabled: bool
    notes: str | None = None
    created_by_user_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_evaluated_at: datetime | None = None
    last_triggered_at: datetime | None = None
    last_triggered_status: str | None = None
    last_triggered_summary_json: dict[str, Any] | list | None = None
    execution_count: int = 0
    suggested_count: int = 0
    succeeded_count: int = 0
    failed_count: int = 0


class PlatformAutomationExecuteIn(BaseModel):
    action_key: str = Field(min_length=1)
    table_id: int | None = None
    datasource_id: int | None = None
    dq_rule_id: int | None = None
    delivery_id: int | None = None
    incident_id: int | None = None
    data_owner_id: int | None = None
    request_type: str | None = None
    scope_kind: str | None = None
    scope_value: str | None = None
    target_json: dict[str, Any] | None = None
    notes: str | None = None


class PlatformAutomationExecutionOut(BaseModel):
    id: int
    rule_id: int | None = None
    rule_name: str | None = None
    action_key: str
    action_label: str
    execution_mode: str
    status: str
    trigger_source: str
    scope_kind: str
    scope_value: str | None = None
    entity_type: str | None = None
    entity_id: int | None = None
    table_id: int | None = None
    datasource_id: int | None = None
    domain_name: str | None = None
    product_name: str | None = None
    target_json: dict[str, Any] | list | None = None
    input_json: dict[str, Any] | list | None = None
    result_json: dict[str, Any] | list | None = None
    impact_json: dict[str, Any] | list | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_by_user_id: int | None = None
    executed_by_user_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PlatformAutomationActionsOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[PlatformAutomationActionOut] = Field(default_factory=list)


class PlatformAutomationRulesOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[PlatformAutomationRuleOut] = Field(default_factory=list)


class PlatformAutomationExecutionsOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[PlatformAutomationExecutionOut] = Field(default_factory=list)


class PlatformAutomationEvaluationOut(BaseModel):
    generated_at: datetime
    rules_evaluated: int = 0
    suggestions_created: int = 0
    actions_executed: int = 0
    skipped: int = 0
    items: list[PlatformAutomationExecutionOut] = Field(default_factory=list)


class PlatformUsageEventIn(BaseModel):
    event_name: str
    module_name: str
    page_path: str | None = None
    entity_type: str | None = None
    entity_id: int | None = None
    target_url: str | None = None
    metadata: dict | None = None


class PlatformUsageEventOut(BaseModel):
    ok: bool = True


class PlatformDomainEventOut(BaseModel):
    id: int
    event_key: str
    category: str
    severity: str
    title: str
    summary: str | None = None
    source_module: str | None = None
    source_action: str | None = None
    entity_type: str | None = None
    entity_id: int | None = None
    table_id: int | None = None
    column_id: int | None = None
    datasource_id: int | None = None
    actor_user_id: int | None = None
    actor_name: str | None = None
    actor_email: str | None = None
    manual_mode: str = "unknown"
    correlation_key: str | None = None
    payload_json: dict | list | None = None
    occurred_at: datetime


class PlatformDomainEventsOut(BaseModel):
    generated_at: datetime
    total: int = 0
    limit: int = 100
    days: int = 30
    items: list[PlatformDomainEventOut] = Field(default_factory=list)


class PlatformSupportedEventOut(BaseModel):
    event_key: str
    display_name: str
    description: str
    category: str
    category_label: str
    supported: bool = True
    active: bool = True
    version: str = "v1"
    entity_types: list[str] = Field(default_factory=list)
    payload_summary: str | None = None
    payload_example_json: dict | list | None = None


class PlatformSupportedEventsOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[PlatformSupportedEventOut] = Field(default_factory=list)
