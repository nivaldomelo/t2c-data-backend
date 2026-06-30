from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from t2c_data.schemas.catalog import TreeDatasourceChildrenOut, TreeDatasourceOut, TreeTableOut


class DQRunRequest(BaseModel):
    table_id: int | None = None
    datasource_id: int | None = None
    max_tables: int = 50
    execution_engine: Literal["spark"] = "spark"


class DQRunOut(BaseModel):
    run_ids: list[int]
    processed_tables: int
    status: str
    execution_engine: str = "spark"


class DQSparkProfilingRunRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    scope: str = "table"  # table | schema
    table_id: int | None = None
    table_fqn: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    datasource_id: int | None = None
    limit: int = Field(default=200, ge=1, le=5000)
    concurrency: int = Field(default=5, ge=1, le=20)
    include_tables: list[str] = Field(default_factory=list)
    exclude_tables: list[str] = Field(default_factory=list)
    sample_fraction: float | None = Field(default=None, gt=0, le=1)
    columns: list[str] = Field(default_factory=list)
    execution_engine: Literal["spark"] = "spark"


class DQSparkBatchProfilingRunRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    scope: Literal["datasource", "schema", "tables"] = Field(default="schema", alias="scope_type")
    datasource_id: int | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    table_ids: list[int] = Field(default_factory=list)
    limit: int = Field(default=200, ge=1, le=5000)
    concurrency: int = Field(default=5, ge=1, le=20)
    include_tables: list[str] = Field(default_factory=list)
    exclude_tables: list[str] = Field(default_factory=list)
    sample_fraction: float | None = Field(default=None, gt=0, le=1)
    columns: list[str] = Field(default_factory=list)
    execution_engine: Literal["spark"] = "spark"


class DQSparkRulesRunRequest(BaseModel):
    table_id: int | None = None
    table_fqn: str | None = None
    rule_ids: list[int] = Field(default_factory=list)
    execution_engine: Literal["spark"] = "spark"


class DQJobRunOut(BaseModel):
    id: int
    job_type: str
    status: str
    execution_engine: str
    dq_run_id: int | None = None
    profiling_schedule_id: int | None = None
    table_id: int | None = None
    table_fqn: str | None = None
    datasource_id: int | None = None
    requested_by_user_id: int | None = None
    requested_by_user_name: str | None = None
    requested_by_user_email: str | None = None
    trigger_source: str | None = None
    spark_app_id: str | None = None
    spark_master_url: str | None = None
    logs_path: str | None = None
    command: str | None = None
    stdout_log: str | None = None
    stderr_log: str | None = None
    result_json: dict | list | None = None
    error_message: str | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    log_tail: str | None = None
    violations_count: int | None = None
    created_at: datetime
    updated_at: datetime


class DQProfilingLaunchOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    run_id: int
    scope: str
    schema_name: str | None = Field(default=None, alias="schema")
    table_fqn: str | None = None
    tables_total: int = 0
    status: str
    execution_engine: str = "spark"
    job_run_id: int | None = None


class DQProfilingScheduleRecipientOut(BaseModel):
    id: int
    display_name: str
    email: str


class DQProfilingScheduleCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    scope: Literal["table", "schema", "datasource", "tables"] = Field(default="table", alias="scope_type")
    name: str | None = None
    table_id: int | None = None
    datasource_id: int | None = None
    schema_name: str | None = None
    table_ids: list[int] = Field(default_factory=list)
    execution_engine: Literal["spark"] = "spark"
    schedule_mode: str = "manual"
    schedule_enabled: bool = True
    schedule_every_minutes: int | None = None
    schedule_time: str | None = None
    schedule_timezone: str | None = None
    schedule_day_of_week: int | None = None
    schedule_day_of_month: int | None = None
    schedule_anchor_date: datetime | None = None
    recipient_user_ids: list[int] = Field(default_factory=list)
    schema_limit: int | None = None
    schema_concurrency: int | None = None
    schema_sample_fraction: float | None = None
    schema_include_tables_json: list[str] = Field(default_factory=list)
    schema_exclude_tables_json: list[str] = Field(default_factory=list)
    schema_columns_json: list[str] = Field(default_factory=list)


class DQProfilingScheduleUpdate(DQProfilingScheduleCreate):
    pass


class DQProfilingScheduleOut(BaseModel):
    id: int
    scope: Literal["table", "schema", "datasource", "tables"]
    name: str | None = None
    table_id: int | None = None
    datasource_id: int | None = None
    schema_name: str | None = None
    table_ids: list[int] = Field(default_factory=list)
    table_fqn: str | None = None
    target_label: str
    execution_engine: Literal["spark"]
    schedule_mode: str
    schedule_enabled: bool
    schedule_every_minutes: int | None = None
    schedule_time: str | None = None
    schedule_timezone: str | None = None
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
    schema_limit: int | None = None
    schema_concurrency: int | None = None
    schema_sample_fraction: float | None = None
    schema_include_tables_json: list[str] = Field(default_factory=list)
    schema_exclude_tables_json: list[str] = Field(default_factory=list)
    schema_columns_json: list[str] = Field(default_factory=list)
    notification_recipients: list[DQProfilingScheduleRecipientOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class DQProfilingSchedulerStatusOut(BaseModel):
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
    scheduled_profiles_total: int
    next_expected_run_at: str | None


class DQRunProgressOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int
    scope: str
    schema_name: str | None = Field(default=None, alias="schema")
    status: str
    execution_engine: str
    datasource_id: int | None = None
    table_id: int | None = None
    parent_run_id: int | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    spark_app_id: str | None = None
    log_tail: str | None = None
    total_items: int = 0
    queued_items: int = 0
    running_items: int = 0
    success_items: int = 0
    failed_items: int = 0


class DQRunItemOut(BaseModel):
    id: int
    parent_run_id: int | None = None
    table_id: int | None = None
    table_fqn: str | None = None
    status: str
    execution_engine: str
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    spark_app_id: str | None = None
    log_tail: str | None = None


class DQProfilingExecutionItemOut(BaseModel):
    id: int
    parent_run_id: int | None = None
    scope: str
    datasource_id: int | None = None
    datasource_name: str | None = None
    schema_name: str | None = None
    table_id: int | None = None
    table_fqn: str | None = None
    status: str
    execution_engine: str
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    spark_app_id: str | None = None
    log_tail: str | None = None
    trigger_source: str | None = None
    row_count: int | None = None
    completeness_pct_avg: float | None = None
    dq_score: float | None = None
    duplicates_count: int | None = None
    failed_rules_count: int | None = None
    observation: str | None = None
    profile_summary: dict[str, object] | None = None
    profiling_intelligence: dict[str, object] | None = None
    profiling_mode: str | None = None
    watermark_column: str | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None


class DQProfilingExecutionSummaryOut(BaseModel):
    id: int
    parent_run_id: int | None = None
    scope: str
    datasource_id: int | None = None
    datasource_name: str | None = None
    schema_name: str | None = None
    table_id: int | None = None
    table_fqn: str | None = None
    status: str
    execution_engine: str
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    spark_app_id: str | None = None
    log_tail: str | None = None
    trigger_source: str | None = None
    total_items: int = 0
    queued_items: int = 0
    running_items: int = 0
    success_items: int = 0
    failed_items: int = 0
    row_count: int | None = None
    completeness_pct_avg: float | None = None
    dq_score: float | None = None
    duplicates_count: int | None = None
    failed_rules_count: int | None = None
    observation: str | None = None
    profile_summary: dict[str, object] | None = None
    profiling_intelligence: dict[str, object] | None = None
    profiling_mode: str | None = None
    watermark_column: str | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None


class DQProfilingExecutionDetailOut(DQProfilingExecutionSummaryOut):
    items: list[DQProfilingExecutionItemOut] = Field(default_factory=list)


class DQProfilingExecutionPageOut(BaseModel):
    items: list[DQProfilingExecutionSummaryOut] = Field(default_factory=list)
    total: int = 0
    limit: int = 10
    offset: int = 0


class DQProfilingTableSettingOut(BaseModel):
    table_id: int
    table_fqn: str | None = None
    start_date: datetime | None = None
    watermark_column: str | None = None
    detected_watermark_column: str | None = None
    effective_watermark_column: str | None = None
    has_previous_success: bool = False
    updated_at: datetime | None = None


class DQProfilingTableSettingIn(BaseModel):
    table_id: int = Field(ge=1)
    start_date: datetime | None = None
    watermark_column: str | None = None


class DQColumnMetricOut(BaseModel):
    column_name: str
    data_type: str
    null_count: int
    null_pct: float
    distinct_count: int
    min_value: str | None
    max_value: str | None


class DQColumnHistoryPointOut(BaseModel):
    run_id: int
    run_at: datetime
    null_count: int
    null_pct: float
    distinct_count: int
    min_value: str | None
    max_value: str | None


class DQTableSnapshotOut(BaseModel):
    run_id: int
    run_at: datetime
    execution_engine: str | None = None
    spark_app_id: str | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    log_tail: str | None = None
    row_count: int
    completeness_pct_avg: float
    dq_score: float
    effective_dq_score: float | None = None
    operational_penalty_points: int = 0
    operational_penalty_label: str | None = None
    operational_penalty_applied: bool = False
    operational_recurrent_degradation: bool = False
    duplicates_count: int
    failed_rules: int
    freshness_seconds: int
    columns: list[DQColumnMetricOut]
    observability: dict[str, object] = Field(default_factory=dict)


class DQHistoryPointOut(BaseModel):
    run_id: int
    run_at: datetime
    execution_engine: str | None = None
    dq_score: float
    completeness_pct_avg: float
    row_count: int
    freshness_seconds: int


class DQTableLatestOut(BaseModel):
    table_id: int
    table_fqn: str
    run_id: int
    run_at: datetime
    execution_engine: str | None = None
    spark_app_id: str | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    log_tail: str | None = None
    row_count: int
    completeness_pct_avg: float
    dq_score: float
    effective_dq_score: float | None = None
    operational_penalty_points: int = 0
    operational_penalty_label: str | None = None
    operational_penalty_applied: bool = False
    operational_recurrent_degradation: bool = False
    duplicates_count: int
    failed_rules: int
    freshness_seconds: int
    columns: list[DQColumnMetricOut]
    current: DQTableSnapshotOut
    previous: DQTableSnapshotOut | None = None
    history: list[DQHistoryPointOut] = Field(default_factory=list)
    column_history: dict[str, list[DQColumnHistoryPointOut]] = Field(default_factory=dict)
    observability: dict[str, object] = Field(default_factory=dict)


class DQObservabilityHistoryFiltersOut(BaseModel):
    artifact_type: str = "all"
    limit: int = 10
    metric_key: str | None = None
    column_name: str | None = None
    dimension_key: str | None = None
    event_type: str | None = None
    severity: str | None = None
    evidence_type: str | None = None
    origin: str | None = None
    status: str | None = None
    dq_run_id: int | None = None
    rule_run_id: int | None = None
    rule_id: int | None = None


class DQObservabilityHistoryOut(BaseModel):
    table_id: int
    table_fqn: str
    filters: DQObservabilityHistoryFiltersOut
    baselines: list[dict[str, object]] = Field(default_factory=list)
    events: list[dict[str, object]] = Field(default_factory=list)
    evidence_samples: list[dict[str, object]] = Field(default_factory=list)


class DQScorecardGroupOut(BaseModel):
    key: str
    label: str
    count: int
    avg_dq_score: float | None = None
    avg_trust_score: float | None = None
    avg_readiness_score: float | None = None
    rules_coverage_pct: float | None = None
    contract_coverage_pct: float | None = None
    open_incidents: int = 0
    critical_incidents: int = 0
    tables_without_rules: int = 0
    critical_tables_without_rules: int = 0
    contract_breaking: int = 0
    contract_warning: int = 0
    tone: str = "neutral"


class DQScorecardRuleOut(BaseModel):
    key: str
    name: str
    table_fqn: str
    severity: str
    status: str
    violations_count: int = 0
    last_run_at: datetime | None = None
    open_incident_id: int | None = None
    tone: str = "neutral"


class DQScorecardAssetOut(BaseModel):
    table_id: int
    table_fqn: str
    table_name: str
    domain_name: str | None = None
    owner_name: str | None = None
    dq_score: float | None = None
    trust_score: int | None = None
    readiness_score: int | None = None
    documentation_score: int | None = None
    certification_status: str | None = None
    criticality: str | None = None
    active_rules: int = 0
    open_incidents: int = 0
    critical_open_incidents: int = 0
    contract_status: str | None = None
    contract_validation_status: str | None = None
    contract_issues: int | None = None
    rule_coverage_pct: float | None = None
    trust_label: str | None = None
    trust_tone: str | None = None
    reasons: list[str] = Field(default_factory=list)


class DQPlatformScorecardTotalsOut(BaseModel):
    tables: int = 0
    with_metrics: int = 0
    avg_dq_score: float | None = None
    avg_trust_score: float | None = None
    avg_readiness_score: float | None = None
    avg_documentation_score: float | None = None
    active_rules: int = 0
    tables_with_rules: int = 0
    tables_without_rules: int = 0
    critical_tables_without_rules: int = 0
    sensitive_tables_without_rules: int = 0
    contracts_total: int = 0
    contracts_with_validation: int = 0
    failed_contract_validations: int = 0
    contract_coverage_pct: float | None = None
    breaking_contracts: int = 0
    warning_contracts: int = 0
    high_risk_tables: int = 0


class DQPlatformScorecardSummaryOut(BaseModel):
    generated_at: datetime
    scope_domain: str | None = None
    scope_owner: str | None = None
    scope_criticality: str | None = None
    totals: DQPlatformScorecardTotalsOut
    by_domain: list[DQScorecardGroupOut] = Field(default_factory=list)
    by_owner: list[DQScorecardGroupOut] = Field(default_factory=list)
    by_criticality: list[DQScorecardGroupOut] = Field(default_factory=list)
    failing_rules: list[DQScorecardRuleOut] = Field(default_factory=list)
    top_risks: list[DQScorecardAssetOut] = Field(default_factory=list)


class DQTreeDatasourceOut(TreeDatasourceOut):
    pass


class DQTreeDatasourceChildrenOut(TreeDatasourceChildrenOut):
    pass


class DQTreeTableOut(TreeTableOut):
    pass
