from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import re

from pydantic import BaseModel, ConfigDict, Field, field_validator

_S3_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")

from t2c_data.schemas.metabase import MetabaseObjectType, MetabaseSyncRunOut


IntegrationStatus = Literal[
    "inactive",
    "active",
    "degraded",
    "healthy",
    "running",
    "error",
    "empty",
    "not_configured",
    "unavailable",
    "misconfigured",
]


class IntegrationHealthFields(BaseModel):
    contract_version: str = "v1"
    integration_status: IntegrationStatus = "unavailable"
    status_message: str | None = None
    reason_code: str | None = None
    health_category: str | None = None
    checked_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    consecutive_failures: int = 0
    failure_count: int = 0
    latency_ms: int | None = None
    error_type: str | None = None
    error_summary: str | None = None
    breaker_state: str | None = None
    breaker_open_until_at: datetime | None = None


class IntegrationDimensionStatusOut(BaseModel):
    status: str = "unknown"
    checked_at: datetime | None = None
    message: str | None = None
    reason_code: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class IntegrationStatusContractOut(BaseModel):
    contract_version: str = "v1"
    source_name: str | None = None
    connectivity: IntegrationDimensionStatusOut = Field(default_factory=IntegrationDimensionStatusOut)
    operation: IntegrationDimensionStatusOut = Field(default_factory=IntegrationDimensionStatusOut)
    consumption: IntegrationDimensionStatusOut = Field(default_factory=IntegrationDimensionStatusOut)
    overall_status: Literal["healthy", "warning", "critical", "unknown"] = "unknown"
    overall_message: str | None = None
    checked_at: datetime | None = None


class AirflowIntegrationContextOut(IntegrationHealthFields):
    configured: bool = False
    enabled: bool = False
    available: bool = False
    operational_status: str = "unknown"
    message: str | None = None
    generated_at: datetime | None = None
    airflow_ui_base_url: str | None = None


class AirflowIntegrationDagSummaryOut(BaseModel):
    dag_id: str
    dag_display_name: str | None = None
    description: str | None = None
    is_active: bool = True
    is_paused: bool = False
    owner: str | None = None
    schedule_interval: str | None = None
    timetable_description: str | None = None
    next_dagrun_at: datetime | None = None
    has_import_errors: bool = False
    fileloc: str | None = None
    tags: list[str] = Field(default_factory=list)
    latest_run_pk: int | None = None
    latest_run_id: str | None = None
    latest_execution_at: datetime | None = None
    latest_state: str | None = None
    latest_duration_seconds: int | None = None
    recent_runs_count_24h: int = 0
    recent_failures_count_24h: int = 0
    updated_at: datetime | None = None


class AirflowIntegrationDagRunOut(BaseModel):
    dag_run_pk: int | None = None
    dag_id: str
    dag_display_name: str | None = None
    is_active: bool = True
    is_paused: bool = False
    run_id: str
    state: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration_seconds: int | None = None
    run_type: str | None = None
    execution_date: datetime | None = None
    logical_date: datetime | None = None
    queued_at: datetime | None = None
    external_trigger: bool | None = None
    data_interval_start: datetime | None = None
    data_interval_end: datetime | None = None
    last_scheduling_decision: datetime | None = None
    updated_at: datetime | None = None


class AirflowIntegrationTaskFailureOut(BaseModel):
    dag_id: str
    dag_display_name: str | None = None
    task_id: str
    run_id: str
    map_index: int | None = None
    state: str | None = None
    try_number: int | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration_seconds: int | None = None
    operator: str | None = None
    queue: str | None = None
    hostname: str | None = None
    unixname: str | None = None
    job_id: int | None = None
    queued_dttm: datetime | None = None
    updated_at: datetime | None = None
    task_display_name: str | None = None
    next_method: str | None = None
    next_kwargs: dict[str, Any] | None = None
    external_executor_id: str | None = None
    failure_at: datetime | None = None
    task_fail_count: int = 0
    last_task_fail_at: datetime | None = None
    log_event: str | None = None
    log_dttm: datetime | None = None
    log_extra: str | None = None
    log_try_number: int | None = None
    troubleshooting_context: str | None = None


class AirflowIntegrationOperationalOut(AirflowIntegrationContextOut):
    total_dags: int = 0
    active_dags: int = 0
    paused_dags: int = 0
    success_runs_24h: int = 0
    failed_runs_24h: int = 0
    task_failures_24h: int = 0
    latest_execution_at: datetime | None = None
    latest_failure_at: datetime | None = None
    latest_log_at: datetime | None = None
    updated_at: datetime | None = None


class AirflowIntegrationSummaryOut(AirflowIntegrationOperationalOut):
    status_contract: IntegrationStatusContractOut | None = None
    recent_runs: list[AirflowIntegrationDagRunOut] = Field(default_factory=list)
    recent_failures: list[AirflowIntegrationTaskFailureOut] = Field(default_factory=list)


class AirflowIntegrationPipelinesOut(AirflowIntegrationContextOut):
    status_contract: IntegrationStatusContractOut | None = None
    items: list[AirflowIntegrationDagSummaryOut] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 0
    total_pages: int = 0


class AirflowIntegrationFailuresOut(AirflowIntegrationContextOut):
    status_contract: IntegrationStatusContractOut | None = None
    items: list[AirflowIntegrationTaskFailureOut] = Field(default_factory=list)


class MetabaseIntegrationTopTableOut(BaseModel):
    table_id: int
    table_fqn: str
    table_name: str
    schema_name: str
    datasource_name: str
    direct_links_count: int = 0
    indirect_links_count: int = 0
    total_links_count: int = 0
    owner: str | None = None
    owner_email: str | None = None
    certification_status: str | None = None
    certification_readiness: int | None = None
    dq_score: float | None = None
    privacy_status: str | None = None
    privacy_signals: list[str] = Field(default_factory=list)
    incident_count: int | None = None
    linked_dashboards: int | None = None
    linked_questions: int | None = None
    linked_artifacts_total: int | None = None


class MetabaseArtifactLinkedTableOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    table_id: int
    full_name: str
    connection: str
    database: str
    schema_name: str = Field(alias="schema")
    table: str


class MetabaseArtifactReferencedTableOut(BaseModel):
    """A table used by an artifact, resolved from its query (name or MBQL id)."""

    full_name: str
    name: str
    schema_name: str | None = Field(default=None, alias="schema")
    metabase_table_id: str | None = None
    source: Literal["sql", "mbql"] = "sql"
    resolved: bool = True
    table_id: int | None = None
    catalog_full_name: str | None = None

    model_config = ConfigDict(populate_by_name=True)


class MetabaseArtifactLinkSummaryOut(BaseModel):
    object_type: MetabaseObjectType | Literal["all"] = "all"
    total_artifacts: int = 0
    linked_artifacts: int = 0
    partially_linked_artifacts: int = 0
    unlinked_artifacts: int = 0
    unknown_artifacts: int = 0
    coverage_percent: int = 0


class MetabaseIntegrationArtifactOut(BaseModel):
    object_id: int
    object_type: Literal["dashboard", "question", "collection"]
    metabase_id: str | None = None
    title: str
    description: str | None = None
    collection_name: str | None = None
    collection_external_id: str | None = None
    url: str | None = None
    archived: bool = False
    creator_name: str | None = None
    view_count: int | None = None
    linked_status: Literal["linked", "partially_linked", "unlinked", "unknown"] | None = None
    direct_links: int = 0
    indirect_links: int = 0
    linked_tables: list[MetabaseArtifactLinkedTableOut] = Field(default_factory=list)
    referenced_tables: list[MetabaseArtifactReferencedTableOut] = Field(default_factory=list)
    unresolved_references: list[str] = Field(default_factory=list)
    remote_updated_at: datetime | None = None
    last_synced_at: datetime | None = None
    last_seen_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class MetabaseArtifactCardOut(BaseModel):
    """A question/card that belongs to a dashboard."""

    object_id: int | None = None
    metabase_id: str | None = None
    title: str
    url: str | None = None
    viz_type: str | None = None
    linked_status: Literal["linked", "partially_linked", "unlinked", "unknown"] | None = None


class MetabaseArtifactDetailOut(MetabaseIntegrationArtifactOut):
    creator_name: str | None = None
    view_count: int | None = None
    query_type: str | None = None
    sql: str | None = None
    viz_type: str | None = None
    database_id: int | None = None
    cards: list[MetabaseArtifactCardOut] = Field(default_factory=list)


class MetabaseIntegrationRecommendationOut(BaseModel):
    severity: Literal["critical", "warning", "info"]
    title: str
    description: str
    reason: str | None = None
    action_label: str | None = None
    action_target: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class MetabaseIntegrationSyncNowIn(BaseModel):
    instance_id: int | None = Field(default=None, ge=1)
    force: bool = False


class MetabaseIntegrationSummaryOut(IntegrationHealthFields):
    configured: bool = False
    enabled: bool = False
    available: bool = False
    status_contract: IntegrationStatusContractOut | None = None
    sync_status: str | None = None
    message: str | None = None
    instance_id: int | None = None
    instance_name: str | None = None
    instance_base_url: str | None = None
    last_sync_at: datetime | None = None
    last_sync_message: str | None = None
    dashboards_count: int = 0
    questions_count: int = 0
    collections_count: int = 0
    direct_links_count: int = 0
    indirect_links_count: int = 0
    total_links_count: int = 0
    tables_with_consumption_count: int = 0
    recent_sync_runs: list[MetabaseSyncRunOut] = Field(default_factory=list)
    top_tables: list[MetabaseIntegrationTopTableOut] = Field(default_factory=list)
    recent_artifacts: list[MetabaseIntegrationArtifactOut] = Field(default_factory=list)
    top_dashboards: list[MetabaseIntegrationArtifactOut] = Field(default_factory=list)
    top_tables_enriched: list[MetabaseIntegrationTopTableOut] = Field(default_factory=list)
    link_coverage: MetabaseArtifactLinkSummaryOut | None = None
    artifact_link_summary: list[MetabaseArtifactLinkSummaryOut] = Field(default_factory=list)
    recommendations: list[MetabaseIntegrationRecommendationOut] = Field(default_factory=list)
    sync_health_notes: list[str] = Field(default_factory=list)


class MetabaseIntegrationHealthOut(IntegrationHealthFields):
    status: Literal["UP", "DOWN"] = "DOWN"
    configured: bool = False
    enabled: bool = False
    available: bool = False
    status_contract: IntegrationStatusContractOut | None = None
    instance_id: int | None = None
    instance_name: str | None = None
    instance_base_url: str | None = None
    message: str | None = None
    checked_at: datetime | None = None


DataLakeAuthType = Literal[
    "access_key_secret_key",
    "access_key_secret_key_session_token",
    "role_arn",
    "default_environment",
]


class DataLakeConnectionIn(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None
    bucket: str = Field(min_length=1)
    region: str = Field(min_length=1)
    prefix: str | None = None
    auth_type: DataLakeAuthType = "default_environment"
    freshness_sla_hours_default: int | None = None
    freshness_sla_hours_bronze: int | None = None
    freshness_sla_hours_silver: int | None = None
    freshness_sla_hours_gold: int | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    role_arn: str | None = None
    is_active: bool = True

    @field_validator("region")
    @classmethod
    def _validate_region(cls, value: str) -> str:
        # AWS region token; prevents host-injection/SSRF when building S3/STS endpoint URLs.
        from t2c_data.core.ssrf import SsrfValidationError, validate_aws_region

        try:
            return validate_aws_region(value)
        except SsrfValidationError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("bucket")
    @classmethod
    def _validate_bucket(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not _S3_BUCKET_RE.match(normalized):
            raise ValueError("bucket inválido: use o formato de nome de bucket S3 (a-z, 0-9, ponto e hífen).")
        return normalized


class DataLakeConnectionOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    bucket: str
    region: str
    prefix: str | None = None
    auth_type: DataLakeAuthType = "default_environment"
    freshness_sla_hours_default: int | None = None
    freshness_sla_hours_bronze: int | None = None
    freshness_sla_hours_silver: int | None = None
    freshness_sla_hours_gold: int | None = None
    aws_access_key_id: str | None = None
    role_arn: str | None = None
    aws_secret_access_key_configured: bool = False
    aws_session_token_configured: bool = False
    credentials_configured: bool = False
    last_test_status: str | None = None
    last_test_message: str | None = None
    last_test_at: datetime | None = None
    is_active: bool = True
    created_by_user_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DataLakeConnectionTestOut(BaseModel):
    ok: bool = True
    status: str
    message: str
    detail: str | None = None
    bucket: str
    region: str
    prefix: str | None = None
    latency_ms: int = 0
    tested_at: datetime
    bucket_accessible: bool = False
    prefix_accessible: bool = False
    prefix_object_count: int = 0
    parquet_files_count: int = 0
    bucket_prefixes: list[dict[str, Any]] = Field(default_factory=list)
    prefix_candidates: list[str] = Field(default_factory=list)
    prefix_suggestion: str | None = None
    prefix_diagnostics: list[str] = Field(default_factory=list)
    table_candidates: list[dict[str, Any]] = Field(default_factory=list)
    example_paths: list[str] = Field(default_factory=list)
    credentials_mode: str
    role_arn_used: str | None = None
    caller_identity_arn: str | None = None
    caller_identity_account: str | None = None
    caller_identity_userid: str | None = None


class DataLakeInventoryTableGovernanceIn(BaseModel):
    data_owner_id: int | None = Field(default=None, ge=1)
    domain_name: str | None = None
    description: str | None = None
    classification: str | None = None
    criticality: str | None = None
    is_monitored: bool = False


class DataLakeInventoryTableOut(BaseModel):
    id: int
    connection_id: int
    layer: str
    table_name: str
    path_base: str
    files_count: int = 0
    parquet_files_count: int = 0
    non_parquet_files_count: int = 0
    size_total_bytes: int = 0
    last_modified_at: datetime | None = None
    has_partitions: bool = False
    partition_pattern_detected: str | None = None
    status_scan: str = "unknown"
    data_last_scan_at: datetime | None = None
    freshness_sla_hours_override: int | None = None
    last_quality_score: float | None = None
    last_quality_evaluated_at: datetime | None = None
    data_owner_id: int | None = None
    domain_name: str | None = None
    description: str | None = None
    classification: str | None = None
    criticality: str | None = None
    is_monitored: bool = False
    governance_last_updated_at: datetime | None = None
    catalog_ready: bool = False
    governance_status: str = "unclassified"
    scan_run_id: int | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DataLakeScanScheduleIn(BaseModel):
    schedule_mode: str = "manual"
    schedule_enabled: bool = True
    schedule_every_minutes: int | None = None
    schedule_time: str | None = None
    schedule_day_of_week: int | None = None
    schedule_day_of_month: int | None = None
    schedule_anchor_date: datetime | None = None


class DataLakeScanScheduleOut(BaseModel):
    id: int
    connection_id: int
    schedule_mode: str
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
    created_by_user_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DataLakeInventoryScanRunOut(BaseModel):
    id: int
    connection_id: int
    status: str
    scanned_layers_count: int = 0
    discovered_tables_count: int = 0
    discovered_parquet_files_count: int = 0
    total_bytes: int = 0
    trigger_mode: str = "manual"
    schedule_id: int | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    scanned_by_user_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DataLakeInventorySummaryOut(BaseModel):
    connection_id: int
    connection_name: str
    total_tables: int = 0
    bronze_tables: int = 0
    silver_tables: int = 0
    gold_tables: int = 0
    total_parquet_files: int = 0
    total_bytes: int = 0
    tables_without_parquet: int = 0
    tables_without_recent_update: int = 0
    layers_detected: list[str] = Field(default_factory=list)
    last_scan_at: datetime | None = None
    latest_scan_status: str | None = None
    latest_scan_message: str | None = None
    latest_scan_run_id: int | None = None


class DataLakeInventoryPageOut(BaseModel):
    summary: DataLakeInventorySummaryOut
    latest_scan: DataLakeInventoryScanRunOut | None = None
    page: int = 1
    page_size: int = 25
    total: int = 0
    has_more: bool = False
    items: list[DataLakeInventoryTableOut] = Field(default_factory=list)


class DataLakeCatalogTableOut(BaseModel):
    id: int
    connection_id: int
    connection_name: str
    bucket: str
    region: str
    prefix: str | None = None
    layer: str
    table_name: str
    path_base: str
    files_count: int = 0
    parquet_files_count: int = 0
    non_parquet_files_count: int = 0
    size_total_bytes: int = 0
    last_modified_at: datetime | None = None
    has_partitions: bool = False
    partition_pattern_detected: str | None = None
    status_scan: str = "empty"
    data_last_scan_at: datetime | None = None
    freshness_sla_hours_override: int | None = None
    last_quality_score: float | None = None
    last_quality_evaluated_at: datetime | None = None
    data_owner_id: int | None = None
    domain_name: str | None = None
    description: str | None = None
    classification: str | None = None
    criticality: str | None = None
    is_monitored: bool = False
    governance_last_updated_at: datetime | None = None
    catalog_ready: bool = False
    governance_status: str = "unlinked"
    scan_run_id: int | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DataLakeCatalogSummaryOut(BaseModel):
    total_tables: int = 0
    bronze_tables: int = 0
    silver_tables: int = 0
    gold_tables: int = 0
    total_parquet_files: int = 0
    total_bytes: int = 0
    tables_without_parquet: int = 0
    tables_without_recent_update: int = 0
    active_connections: int = 0
    total_connections: int = 0
    layers_detected: list[str] = Field(default_factory=list)
    last_scan_at: datetime | None = None
    latest_scan_status: str | None = None
    latest_scan_message: str | None = None
    latest_scan_run_id: int | None = None


class DataLakeCatalogPageOut(BaseModel):
    summary: DataLakeCatalogSummaryOut
    page: int = 1
    page_size: int = 25
    total: int = 0
    has_more: bool = False
    items: list[DataLakeCatalogTableOut] = Field(default_factory=list)


class DataLakeInventoryScanOut(BaseModel):
    scan_run: DataLakeInventoryScanRunOut
    summary: DataLakeInventorySummaryOut
    job_id: int | None = None
    job_status: str | None = None
    correlation_id: str | None = None


class DataLakeConnectionOperationalLayerOut(BaseModel):
    layer: str
    tables_count: int = 0
    average_quality_score: float | None = None
    tables_without_recent_update: int = 0
    stale_tables_count: int = 0


class DataLakeOperationalIssueOut(BaseModel):
    key: str
    label: str
    tone: str = "neutral"
    detail: str | None = None
    recommended_action: str | None = None
    table_id: int | None = None
    table_name: str | None = None


class DataLakeOperationsSummaryOut(BaseModel):
    connection_id: int
    connection_name: str
    last_scan_at: datetime | None = None
    last_scan_duration_seconds: int | None = None
    last_scan_status: str | None = None
    last_scan_error: str | None = None
    tables_total: int = 0
    tables_scanned: int = 0
    tables_with_error: int = 0
    tables_without_parquet: int = 0
    tables_without_recent_update: int = 0
    tables_with_drift: int = 0
    average_quality_score: float | None = None
    layer_summaries: list[DataLakeConnectionOperationalLayerOut] = Field(default_factory=list)
    recent_scan_runs: list[DataLakeInventoryScanRunOut] = Field(default_factory=list)
    issues: list[DataLakeOperationalIssueOut] = Field(default_factory=list)


class DataLakeTroubleshootingOut(BaseModel):
    connection_id: int
    connection_name: str
    status: str = "ok"
    summary: str | None = None
    items: list[DataLakeOperationalIssueOut] = Field(default_factory=list)


class DataLakeTableDetailColumnOut(BaseModel):
    path: str
    name: str
    physical_type: str | None = None
    logical_type: str | None = None
    repetition_type: str | None = None
    nullable: bool = True
    is_suspicious: bool = False


class DataLakeTableDetailFileOut(BaseModel):
    key: str
    size_bytes: int = 0
    last_modified_at: datetime | None = None
    row_count: int | None = None
    schema_signature: str | None = None
    is_sample: bool = True


class DataLakeTableDetailSignalOut(BaseModel):
    key: str
    label: str
    tone: str = "neutral"
    detail: str | None = None


class DataLakeTableDetailScoreOut(BaseModel):
    key: str
    label: str
    score: float = 0.0
    tone: str = "neutral"
    detail: str | None = None


class DataLakeTableDetailErrorOut(BaseModel):
    bucket: str | None = None
    region: str | None = None
    key: str | None = None
    operation: str | None = None
    category: str = "unknown"
    status_code: int | None = None
    code: str | None = None
    message: str | None = None
    detail: str | None = None
    response_body: str | None = None


class DataLakeTableDetailHistoryOut(BaseModel):
    observed_at: datetime | None = None
    source_kind: str = "detail"
    freshness_status: str = "unknown"
    freshness_age_seconds: int | None = None
    freshness_sla_hours: int | None = None
    row_count: int | None = None
    row_count_method: str | None = None
    row_count_confidence: str | None = None
    size_total_bytes: int | None = None
    quality_score: float | None = None
    schema_variants_count: int = 0
    drift_detected: bool = False


class DataLakeTableFreshnessSlaIn(BaseModel):
    freshness_sla_hours_override: int | None = Field(default=None, ge=1)


class DataLakeTableDetailOut(BaseModel):
    inventory: DataLakeInventoryTableOut
    connection_id: int
    connection_name: str
    bucket: str
    region: str
    prefix: str | None = None
    sample_files: list[DataLakeTableDetailFileOut] = Field(default_factory=list)
    schema_status: str = "unavailable"
    schema_message: str | None = None
    schema_variants_count: int = 0
    row_count: int | None = None
    row_count_method: str | None = None
    row_count_confidence: str | None = None
    row_count_source_files: int = 0
    column_count: int = 0
    columns: list[DataLakeTableDetailColumnOut] = Field(default_factory=list)
    partitions: list[str] = Field(default_factory=list)
    last_modified_at: datetime | None = None
    freshness_age_seconds: int | None = None
    freshness_age_hours: float | None = None
    freshness_sla_hours: int | None = None
    freshness_status: str = "unknown"
    freshness_detail: str | None = None
    quality_score: float | None = None
    quality_breakdown: list[DataLakeTableDetailScoreOut] = Field(default_factory=list)
    quality_signals: list[DataLakeTableDetailSignalOut] = Field(default_factory=list)
    operational_signals: list[DataLakeTableDetailSignalOut] = Field(default_factory=list)
    history: list[DataLakeTableDetailHistoryOut] = Field(default_factory=list)
    technical_errors: list[DataLakeTableDetailErrorOut] = Field(default_factory=list)
    technical_notes: list[str] = Field(default_factory=list)


class DataLakeTableFileOut(BaseModel):
    key: str
    size_bytes: int = 0
    last_modified_at: datetime | None = None
    is_parquet: bool = False
    file_type: str = "unknown"
    relative_path: str | None = None


class DataLakeTableFilesPageOut(BaseModel):
    page: int = 1
    page_size: int = 25
    total: int = 0
    has_more: bool = False
    items: list[DataLakeTableFileOut] = Field(default_factory=list)


class AirflowIntegrationHealthOut(IntegrationHealthFields):
    status: Literal["UP", "DOWN"] = "DOWN"
    configured: bool = False
    enabled: bool = False
    available: bool = False
    status_contract: IntegrationStatusContractOut | None = None
    message: str | None = None
    airflow_ui_base_url: str | None = None
