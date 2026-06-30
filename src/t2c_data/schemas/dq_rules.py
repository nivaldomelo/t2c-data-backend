from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

RuleType = Literal[
    "column_validation",
    "nullability",
    "domain",
    "uniqueness",
    "freshness",
    "column_comparison",
    "reconciliation",
]
RuleDimension = Literal["completude", "validade", "consistencia", "unicidade", "tempestividade", "acuracia"]
RuleCategory = Literal["technical", "business", "operational"]
RuleSeverity = Literal["critical", "high", "medium", "low"]
RuleRunStatus = Literal["pass", "fail", "error"]
ScheduleMode = Literal["manual", "interval", "daily", "weekly", "biweekly", "monthly"]
ExecutionEngine = Literal["spark"]
RuleLogic = Literal["AND", "OR"]
RuleValueType = Literal["number", "text", "date", "boolean", "list", "column", "none"]
RuleTimeUnit = Literal["hours", "days"]
RuleComparisonMetric = Literal["count", "sum"]


class DQRuleConditionIn(BaseModel):
    column: str = Field(min_length=1, max_length=255)
    operator: str = Field(min_length=1, max_length=80)
    value: Any | None = None
    value_to: Any | None = None
    values: list[Any] = Field(default_factory=list)
    compare_column: str | None = Field(default=None, max_length=255)
    value_type: RuleValueType | None = None
    time_unit: RuleTimeUnit | None = None


class DQRuleComparisonTargetIn(BaseModel):
    table_id: int | None = Field(default=None, ge=1)
    datasource_id: int | None = Field(default=None, ge=1)
    schema_name: str | None = Field(default=None, max_length=255)
    table_name: str | None = Field(default=None, max_length=255)
    table_fqn: str | None = Field(default=None, min_length=3, max_length=500)
    metric: RuleComparisonMetric = "count"
    column: str | None = Field(default=None, max_length=255)
    key_columns: list[str] = Field(default_factory=list)
    tolerance_abs: float | None = Field(default=None, ge=0)
    tolerance_pct: float | None = Field(default=None, ge=0)


class DQRuleBuilderTarget(BaseModel):
    table_id: int = Field(ge=1)
    datasource_id: int | None = Field(default=None, ge=1)
    schema_name: str | None = Field(default=None, max_length=255)
    table_name: str | None = Field(default=None, max_length=255)


class DQRuleBase(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    table_id: int = Field(ge=1)
    table_fqn: str | None = Field(default=None, min_length=3, max_length=500)
    execution_engine: ExecutionEngine = "spark"
    notification_recipient_user_id: int | None = None
    notification_recipient_user_ids: list[int] = Field(default_factory=list)
    schedule_mode: ScheduleMode | None = None
    schedule_enabled: bool = True
    schedule_every_minutes: int | None = Field(default=60, ge=1, le=10080)
    schedule_time: str | None = Field(default=None, max_length=5)
    schedule_day_of_week: int | None = Field(default=None, ge=0, le=6)
    schedule_day_of_month: int | None = Field(default=None, ge=1, le=31)
    schedule_anchor_date: date | None = None
    rule_type: RuleType = "column_validation"
    quality_dimension: RuleDimension | None = None
    rule_category: RuleCategory | None = None
    template_key: str | None = Field(default=None, max_length=120)
    severity: RuleSeverity = "medium"
    logic: RuleLogic = "AND"
    conditions: list[DQRuleConditionIn] = Field(default_factory=list)
    unique_columns: list[str] = Field(default_factory=list)
    comparison_target: DQRuleComparisonTargetIn | None = None
    is_active: bool = True


class DQRuleCreate(DQRuleBase):
    model_config = ConfigDict(extra="forbid")
    pass


class DQRuleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    table_id: int | None = Field(default=None, ge=1)
    table_fqn: str | None = Field(default=None, min_length=3, max_length=500)
    execution_engine: ExecutionEngine | None = None
    notification_recipient_user_id: int | None = None
    notification_recipient_user_ids: list[int] | None = None
    schedule_mode: ScheduleMode | None = None
    schedule_enabled: bool | None = None
    schedule_every_minutes: int | None = Field(default=None, ge=1, le=10080)
    schedule_time: str | None = Field(default=None, max_length=5)
    schedule_day_of_week: int | None = Field(default=None, ge=0, le=6)
    schedule_day_of_month: int | None = Field(default=None, ge=1, le=31)
    schedule_anchor_date: date | None = None
    rule_type: RuleType | None = None
    quality_dimension: RuleDimension | None = None
    rule_category: RuleCategory | None = None
    template_key: str | None = Field(default=None, max_length=120)
    severity: RuleSeverity | None = None
    logic: RuleLogic | None = None
    conditions: list[DQRuleConditionIn] | None = None
    unique_columns: list[str] | None = None
    comparison_target: DQRuleComparisonTargetIn | None = None
    is_active: bool | None = None


class DQUserOption(BaseModel):
    id: int
    display_name: str
    email: str


class DQRuleOut(BaseModel):
    id: int
    table_id: int | None = None
    datasource_id: int | None = None
    datasource_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    execution_engine: ExecutionEngine = "spark"
    rule_builder_version: int | None = None
    rule_definition_json: dict[str, Any] | None = None
    rule_summary: str | None = None
    quality_dimension: RuleDimension | None = None
    rule_category: RuleCategory | None = None
    template_key: str | None = None
    legacy_mode: bool = False
    notification_recipient_user_id: int | None = None
    notification_recipient_user_name: str | None = None
    notification_recipient_user_email: str | None = None
    notification_recipient_users: list[DQUserOption] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    created_by_user_id: int | None = None
    created_by_user_name: str | None = None
    created_by_user_email: str | None = None
    updated_by_user_id: int | None = None
    updated_by_user_name: str | None = None
    updated_by_user_email: str | None = None
    last_audit_action: str | None = None
    last_audit_at: datetime | None = None
    schedule_mode: ScheduleMode = "manual"
    schedule_enabled: bool = True
    schedule_every_minutes: int | None = None
    schedule_time: str | None = None
    schedule_day_of_week: int | None = None
    schedule_day_of_month: int | None = None
    schedule_anchor_date: date | None = None
    schedule_summary: str | None = None
    schedule_last_run_at: datetime | None = None
    schedule_next_run_at: datetime | None = None
    table_fqn: str
    name: str
    description: str | None = None
    rule_type: RuleType | str
    severity: RuleSeverity
    is_active: bool
    last_run_id: int | None = None
    last_run_status: str | None = None
    last_run_engine: str | None = None
    last_run_at: datetime | None = None
    last_violations_count: int = 0
    last_error_message: str | None = None
    last_job_run_id: int | None = None
    last_job_status: str | None = None
    last_job_engine: str | None = None
    last_job_duration_ms: int | None = None
    last_job_error_message: str | None = None
    last_job_log_tail: str | None = None
    last_job_spark_app_id: str | None = None
    last_job_requested_by_user_id: int | None = None
    last_job_requested_by_user_name: str | None = None
    last_job_requested_by_user_email: str | None = None
    last_job_trigger_source: str | None = None
    last_job_started_at: datetime | None = None
    last_job_finished_at: datetime | None = None
    last_rows_checked: int | None = None
    last_job_violations_count: int | None = None
    last_job_total_rules: int | None = None
    last_job_passed_rules: int | None = None
    last_job_failed_rules: int | None = None
    last_job_error_rules: int | None = None
    open_incident_id: int | None = None
    open_incident_status: str | None = None


class DQRuleRunOut(BaseModel):
    id: int
    rule_id: int
    status: RuleRunStatus
    execution_engine: str = "spark"
    violations_count: int
    sample_rows_json: list[dict] | None = None
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime


class DQRuleTestOut(BaseModel):
    valid: bool
    status: RuleRunStatus
    violations_count: int
    preview_rows: list[dict] = Field(default_factory=list)
    error_message: str | None = None


class DQRuleRunRequest(BaseModel):
    execution_engine: ExecutionEngine | None = "spark"


class DQRuleTableOption(BaseModel):
    table_id: int
    table_fqn: str


class DQRuleBuilderFieldOption(BaseModel):
    value: str
    label: str


class DQRuleBuilderTemplateOption(BaseModel):
    key: str
    label: str
    dimension: RuleDimension
    category: RuleCategory
    rule_type: RuleType
    description: str
    requires_comparison: bool = False


class DQRuleBuilderOptionsOut(BaseModel):
    category_options: list[DQRuleBuilderFieldOption]
    dimension_options: list[DQRuleBuilderFieldOption]
    logic_options: list[DQRuleBuilderFieldOption]
    rule_types: list[DQRuleBuilderFieldOption]
    severities: list[DQRuleBuilderFieldOption]
    templates: list[DQRuleBuilderTemplateOption]
    operators: dict[str, list[DQRuleBuilderFieldOption]]
    time_units: list[DQRuleBuilderFieldOption]


class DQSchedulerStatusOut(BaseModel):
    scheduler_name: str
    mode: str
    is_enabled: bool
    health: str
    last_started_at: str | None = None
    last_heartbeat_at: str | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    last_error: str | None = None
    last_run_summary: dict[str, object] = Field(default_factory=dict)
    scheduled_rules_total: int = 0
    next_expected_run_at: str | None = None
