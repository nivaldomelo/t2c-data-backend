from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator

from t2c_data.schemas.asset_context import AssetLinksOut
from t2c_data.schemas.tag import TagOut


class GovernanceReviewItemOut(BaseModel):
    table_id: int
    table_name: str
    table_fqn: str
    datasource_name: str
    database_name: str
    schema_name: str
    owner_name: str
    certification_status: str
    certification_status_label: str
    sensitivity_label: str
    owner_review_due: bool
    privacy_review_due: bool
    certification_review_due: bool
    last_review_at: datetime | None = None
    links: AssetLinksOut


class GovernanceScoreFactorOut(BaseModel):
    key: str
    label: str
    points: int
    max_points: int
    status: str
    detail: str


class GovernanceScoreOut(BaseModel):
    score: int
    max_score: int = 100
    label: str
    tone: str
    completed_factors: int
    partial_factors: int = 0
    total_factors: int
    summary: str
    factors: list[GovernanceScoreFactorOut] = Field(default_factory=list)


class GovernanceScoreWeightsOut(BaseModel):
    owner_defined: int = Field(ge=0, le=100)
    table_description_complete: int = Field(ge=0, le=100)
    column_description_complete: int = Field(ge=0, le=100)
    tags_applied: int = Field(ge=0, le=100)
    glossary_terms: int = Field(ge=0, le=100)
    dq_score: int = Field(ge=0, le=100)
    certification: int = Field(ge=0, le=100)
    incident_health: int = Field(ge=0, le=100)
    owner_review: int = Field(ge=0, le=100)
    privacy_review: int = Field(ge=0, le=100)
    certification_review: int = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_total(self) -> "GovernanceScoreWeightsOut":
        total = sum(
            getattr(self, key)
            for key in (
                "owner_defined",
                "table_description_complete",
                "column_description_complete",
                "tags_applied",
                "glossary_terms",
                "dq_score",
                "certification",
                "incident_health",
                "owner_review",
                "privacy_review",
                "certification_review",
            )
        )
        if total != 100:
            raise ValueError("A soma dos pesos do score de governança deve ser 100.")
        return self


class GovernanceReviewSummaryOut(BaseModel):
    generated_at: datetime
    owner_review_due: int
    privacy_review_due: int
    certification_review_due: int
    items: list[GovernanceReviewItemOut] = Field(default_factory=list)


class GovernanceCampaignItemOut(BaseModel):
    key: str
    label: str
    count: int
    completed_count: int
    progress_pct: float
    responsible: str
    hint: str
    href: str
    export_csv_href: str
    export_xlsx_href: str
    tone: str = "neutral"


class GovernanceCampaignsOut(BaseModel):
    generated_at: datetime
    total_assets: int
    items: list[GovernanceCampaignItemOut] = Field(default_factory=list)


class GovernanceReviewMarkOut(BaseModel):
    table_id: int
    review_type: str
    reviewed_at: datetime
    reviewed_by_user_id: int
    reviewed_by_name: str | None = None


class GovernanceCriticalChangeOut(BaseModel):
    id: int
    changed_at: datetime
    actor_name: str | None = None
    actor_email: str | None = None
    field_name: str | None = None
    change_type: str | None = None
    sensitive_category: str | None = None
    table_id: int | None = None
    table_name: str | None = None
    schema_name: str | None = None
    database_name: str | None = None
    datasource_name: str | None = None
    before_value: str | None = None
    after_value: str | None = None
    href: str | None = None


class GovernanceCriticalChangesOut(BaseModel):
    generated_at: datetime
    total: int
    items: list[GovernanceCriticalChangeOut] = Field(default_factory=list)


class GovernanceUserRefOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str | None = None
    email: str | None = None
    display_name: str | None = None
    is_active: bool | None = None


class AssetSlaIn(BaseModel):
    asset_type: str = Field(min_length=1)
    asset_id: int = Field(ge=1)
    sla_kind: str = Field(default="freshness", min_length=1)
    sla_hours: int = Field(ge=1, le=1_000_000)
    status: str = Field(default="active")
    source_kind: str = Field(default="manual")
    source_ref: str | None = None
    context_json: dict[str, object] | None = None


class AssetSlaOut(AssetSlaIn):
    id: int
    table_id: int | None = None
    column_id: int | None = None
    asset_name: str | None = None
    asset_fqn: str | None = None
    reviewed_by_user_id: int | None = None
    reviewed_by_user: GovernanceUserRefOut | None = None
    reviewed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class AssetSlaListOut(BaseModel):
    generated_at: datetime
    asset_type: str
    asset_id: int
    asset_name: str | None = None
    asset_fqn: str | None = None
    total: int = 0
    items: list[AssetSlaOut] = Field(default_factory=list)


class MetadataChangeRequestEventOut(BaseModel):
    id: int
    metadata_change_request_id: int
    event_type: str
    previous_status: str | None = None
    next_status: str | None = None
    actor_user_id: int | None = None
    actor_user: GovernanceUserRefOut | None = None
    comment: str | None = None
    payload_json: dict[str, object] | None = None
    created_at: datetime


class MetadataChangeRequestIn(BaseModel):
    asset_type: str = Field(min_length=1)
    asset_id: int = Field(ge=1)
    change_kind: str = Field(min_length=1)
    title: str = Field(min_length=1, max_length=240)
    description: str | None = None
    status: str = Field(default="draft")
    policy_rule_key: str | None = None
    recommendation_id: int | None = Field(default=None, ge=1)
    current_value_json: dict[str, object] | None = None
    proposed_value_json: dict[str, object] | None = None
    context_json: dict[str, object] | None = None


class MetadataChangeRequestTransitionIn(BaseModel):
    comment: str | None = None


class ColumnClassificationReviewIn(BaseModel):
    taxonomy_key: str = Field(min_length=1, max_length=80)
    source_kind: str = Field(default="manual", max_length=40)
    confidence_score: int = Field(default=100, ge=0, le=100)
    decision_status: str = Field(default="approved", max_length=30)
    notes: str | None = None
    evidence_json: dict[str, object] | None = None


class ColumnClassificationOut(BaseModel):
    id: int
    column_id: int
    taxonomy_key: str
    taxonomy_label: str
    taxonomy_group: str
    review_status: str
    source_kind: str
    confidence_score: int
    is_personal_data: bool
    is_sensitive_data: bool
    is_financial_data: bool
    is_operational_data: bool
    evidence_json: dict[str, object] | None = None
    notes: str | None = None
    reviewed_by_user_id: int | None = None
    reviewed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ColumnClassificationVersionOut(BaseModel):
    id: int
    column_id: int
    column_classification_id: int | None = None
    version_number: int
    decision_status: str
    taxonomy_key: str
    taxonomy_label: str
    taxonomy_group: str
    source_kind: str
    confidence_score: int
    is_personal_data: bool
    is_sensitive_data: bool
    is_financial_data: bool
    is_operational_data: bool
    evidence_json: dict[str, object] | None = None
    notes: str | None = None
    decided_by_user_id: int | None = None
    decided_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ColumnClassificationHistoryOut(BaseModel):
    generated_at: datetime
    column_id: int
    current: ColumnClassificationOut | None = None
    items: list[ColumnClassificationVersionOut] = Field(default_factory=list)


class MetadataChangeRequestOut(BaseModel):
    id: int
    request_key: str
    asset_type: str
    asset_id: int
    table_id: int | None = None
    column_id: int | None = None
    asset_name: str | None = None
    asset_fqn: str | None = None
    change_kind: str
    status: str
    status_label: str
    title: str
    description: str | None = None
    requested_by_user_id: int | None = None
    requested_by_user: GovernanceUserRefOut | None = None
    reviewed_by_user_id: int | None = None
    reviewed_by_user: GovernanceUserRefOut | None = None
    approved_by_user_id: int | None = None
    approved_by_user: GovernanceUserRefOut | None = None
    applied_by_user_id: int | None = None
    applied_by_user: GovernanceUserRefOut | None = None
    rejected_by_user_id: int | None = None
    rejected_by_user: GovernanceUserRefOut | None = None
    reviewed_at: datetime | None = None
    approved_at: datetime | None = None
    applied_at: datetime | None = None
    rejected_at: datetime | None = None
    policy_rule_key: str | None = None
    recommendation_id: int | None = None
    current_value_json: dict[str, object] | None = None
    proposed_value_json: dict[str, object] | None = None
    context_json: dict[str, object] | None = None
    apply_error: str | None = None
    can_review: bool = False
    can_approve: bool = False
    can_apply: bool = False
    can_reject: bool = False
    links: AssetLinksOut | None = None
    events: list[MetadataChangeRequestEventOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class MetadataChangeRequestListOut(BaseModel):
    generated_at: datetime
    total: int
    page: int
    page_size: int
    items: list[MetadataChangeRequestOut] = Field(default_factory=list)


class GovernanceSettingsOut(BaseModel):
    owner_review_interval_days: int
    privacy_review_interval_days: int
    sensitive_privacy_review_interval_days: int
    certification_review_interval_days: int
    certification_review_sla_days: int
    certification_revalidation_window_days: int
    audit_log_retention_days: int
    audit_log_archive_retention_days: int
    access_log_retention_days: int
    access_log_archive_retention_days: int
    platform_usage_event_retention_days: int
    search_result_click_retention_days: int
    legacy_api_cutoff_window_days: int
    legacy_api_disabled_modules: list[str] = Field(default_factory=list)
    legacy_api_force_enabled_modules: list[str] = Field(default_factory=list)
    stewardship_assignment_rules: list["StewardshipAssignmentRuleOut"] = Field(default_factory=list)
    governance_policy_rules: list["GovernancePolicyRuleOut"] = Field(default_factory=list)
    governance_score_weights: GovernanceScoreWeightsOut
    trust_score_domain_adjustments: dict[str, int] = Field(default_factory=dict)
    trust_score_criticality_adjustments: dict[str, int] = Field(default_factory=dict)
    governance_notifications_enabled: bool = True
    governance_notification_repeat_days: int = 7
    governance_notification_critical_repeat_hours: int = 24
    pipeline_failure_owner_sla_hours: int = 24
    platform_job_running_attention_minutes: int = 120
    platform_job_running_critical_hours: int = 24
    platform_job_next_expected_delay_minutes: int = 60
    platform_recent_success_window_hours: int = 72
    operational_high_volume_threshold_rows: int = 100000
    governance_high_usage_click_threshold: int = 20
    dq_operational_failure_penalty_points: int = 15
    dq_operational_stale_penalty_points: int = 8
    dq_operational_recurrent_penalty_points: int = 5
    airflow_ui_base_url: str | None = None
    updated_at: datetime | None = None


class GovernanceSettingsUpdate(BaseModel):
    owner_review_interval_days: int = Field(ge=1, le=3650)
    privacy_review_interval_days: int = Field(ge=1, le=3650)
    sensitive_privacy_review_interval_days: int = Field(ge=1, le=3650)
    certification_review_interval_days: int = Field(ge=1, le=3650)
    certification_review_sla_days: int = Field(ge=1, le=3650)
    certification_revalidation_window_days: int = Field(ge=1, le=3650)
    audit_log_retention_days: int = Field(ge=1, le=36500)
    audit_log_archive_retention_days: int = Field(ge=1, le=36500)
    access_log_retention_days: int = Field(ge=1, le=36500)
    access_log_archive_retention_days: int = Field(ge=1, le=36500)
    platform_usage_event_retention_days: int = Field(ge=1, le=36500)
    search_result_click_retention_days: int = Field(ge=1, le=36500)
    legacy_api_cutoff_window_days: int = Field(ge=1, le=3650)
    legacy_api_disabled_modules: list[str] = Field(default_factory=list)
    legacy_api_force_enabled_modules: list[str] = Field(default_factory=list)
    stewardship_assignment_rules: list["StewardshipAssignmentRuleIn"] = Field(default_factory=list)
    governance_policy_rules: list["GovernancePolicyRuleIn"] = Field(default_factory=list)
    governance_score_weights: GovernanceScoreWeightsOut
    trust_score_domain_adjustments: dict[str, int] = Field(default_factory=dict)
    trust_score_criticality_adjustments: dict[str, int] = Field(default_factory=dict)
    governance_notifications_enabled: bool = True
    governance_notification_repeat_days: int = Field(ge=1, le=3650)
    governance_notification_critical_repeat_hours: int = Field(ge=1, le=8760)
    pipeline_failure_owner_sla_hours: int = Field(ge=1, le=8760)
    platform_job_running_attention_minutes: int = Field(ge=1, le=10080)
    platform_job_running_critical_hours: int = Field(ge=1, le=168)
    platform_job_next_expected_delay_minutes: int = Field(ge=1, le=10080)
    platform_recent_success_window_hours: int = Field(ge=1, le=8760)
    operational_high_volume_threshold_rows: int = Field(ge=1, le=1_000_000_000)
    governance_high_usage_click_threshold: int = Field(ge=1, le=1_000_000_000)
    dq_operational_failure_penalty_points: int = Field(ge=0, le=100)
    dq_operational_stale_penalty_points: int = Field(ge=0, le=100)
    dq_operational_recurrent_penalty_points: int = Field(ge=0, le=100)
    airflow_ui_base_url: str | None = None


class RetentionTableSummaryOut(BaseModel):
    table_name: str
    hot_rows: int
    archived_rows: int = 0
    eligible_for_archive: int = 0
    eligible_for_purge: int = 0
    last_archived_count: int = 0
    last_purged_count: int = 0
    estimated_rows_per_day: float = 0
    projected_rows_30d: int = 0
    projected_hot_rows_at_retention: int = 0
    estimated_storage_mb: float | None = None
    projected_storage_mb_30d: float | None = None
    pressure_level: str = "normal"
    hot_retention_days: int | None = None
    archive_retention_days: int | None = None


class StewardshipAssignmentRuleIn(BaseModel):
    key: str | None = None
    request_type: str = Field(default="any")
    domain_name: str | None = None
    owner_area: str | None = None
    approver_user_id: int = Field(ge=1)
    priority: int = Field(default=100, ge=1, le=9999)
    is_active: bool = True


class StewardshipAssignmentRuleOut(BaseModel):
    key: str
    request_type: str
    domain_name: str | None = None
    owner_area: str | None = None
    approver_user_id: int
    approver_name: str | None = None
    approver_email: str | None = None
    priority: int = 100
    is_active: bool = True


class GovernancePolicyRuleIn(BaseModel):
    key: str | None = None
    name: str
    description: str | None = None
    trigger_key: str = Field(min_length=1)
    scope: str = Field(default="table")
    domain_name: str | None = None
    datasource_name: str | None = None
    criticality: str | None = None
    sensitivity_level: str | None = None
    min_trust_score: int | None = Field(default=None, ge=0, le=100)
    min_risk_score: int | None = Field(default=None, ge=0, le=100)
    min_search_clicks: int | None = Field(default=None, ge=0, le=1_000_000_000)
    severity: str = Field(default="medium")
    impact: str = Field(default="medium")
    sla_days: int | None = Field(default=None, ge=1, le=3650)
    action_key: str = Field(min_length=1)
    action_label: str = Field(min_length=1)
    recommendation_title: str = Field(min_length=1)
    recommendation_detail: str = Field(min_length=1)
    auto_create_recommendation: bool = True
    requires_owner: bool = False
    requires_classification: bool = False
    requires_dictionary: bool = False
    requires_active_dq: bool = False
    requires_sla: bool = False
    priority: int = Field(default=100, ge=1, le=9999)
    is_active: bool = True


class GovernancePolicyRuleOut(GovernancePolicyRuleIn):
    key: str


class GovernancePlaybookOut(BaseModel):
    key: str
    title: str
    description: str | None = None
    scope: str = "table"
    trigger_key: str
    domain_name: str | None = None
    datasource_name: str | None = None
    criticality: str | None = None
    sensitivity_level: str | None = None
    severity: str = "medium"
    impact: str = "medium"
    sla_days: int | None = None
    action_key: str
    action_label: str
    recommendation_title: str
    recommendation_detail: str
    auto_create_recommendation: bool = True
    requires_owner: bool = False
    requires_classification: bool = False
    requires_dictionary: bool = False
    requires_active_dq: bool = False
    requires_sla: bool = False
    priority: int = 100
    is_active: bool = True
    matched_recommendations: int = 0
    open_recommendations: int = 0
    last_matched_at: datetime | None = None
    recommended_actions: list[dict[str, object]] = Field(default_factory=list)


class GovernancePlaybooksOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[GovernancePlaybookOut] = Field(default_factory=list)


class GovernanceAssistantToolOut(BaseModel):
    key: str
    label: str
    description: str | None = None
    kind: str = "action"
    action: str | None = None
    confirmation_required: bool = True
    confirmation_label: str | None = None
    confirmation_hint: str | None = None
    severity: str | None = None
    impact: str | None = None
    confidence_score: int | None = None
    can_execute: bool = True


class GovernanceRetentionSummaryOut(BaseModel):
    generated_at: datetime
    items: list[RetentionTableSummaryOut] = Field(default_factory=list)


class GovernanceRecommendationSignalOut(BaseModel):
    key: str
    label: str
    value: str | None = None
    tone: str = "neutral"
    detail: str | None = None


class GovernanceRecommendationOut(BaseModel):
    id: int
    key: str
    recommendation_key: str
    policy_rule_key: str | None = None
    entity_type: str
    entity_id: int
    table_id: int
    table_name: str
    table_fqn: str
    column_id: int | None = None
    column_name: str | None = None
    datasource_id: int | None = None
    datasource_name: str
    database_name: str
    schema_name: str
    domain_name: str | None = None
    owner_name: str | None = None
    certification_status: str
    certification_status_label: str
    sensitivity_level: str | None = None
    sensitivity_label: str
    confidence_score: int = 0
    trust_score: int = 0
    trust_label: str | None = None
    trust_tone: str | None = None
    risk_score: int = 0
    risk_label: str = "Baixo risco"
    risk_tone: str = "neutral"
    severity: str
    severity_label: str
    impact: str
    impact_label: str
    status: str
    status_label: str
    action_key: str
    action_label: str
    due_at: datetime | None = None
    aging_days: int = 0
    context_value: str | None = None
    reason: str | None = None
    summary: str | None = None
    source_kind: str
    source_label: str
    priority: int = 0
    assistant_summary: str | None = None
    feedback_rating: str | None = None
    feedback_label: str | None = None
    feedback_tone: str = "neutral"
    feedback_note: str | None = None
    feedback_updated_at: datetime | None = None
    feedback_updated_by_user_id: int | None = None
    signals: list[GovernanceRecommendationSignalOut] = Field(default_factory=list)
    context: dict[str, object] = Field(default_factory=dict)
    links: AssetLinksOut
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None = None
    resolved_by_user_id: int | None = None
    resolution_action: str | None = None
    resolution_note: str | None = None


class GovernanceRecommendationFiltersOut(BaseModel):
    statuses: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    severities: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    impacts: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    sources: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    datasources: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    schemas: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    domains: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    owners: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)


class GovernanceRecommendationSummaryOut(BaseModel):
    open_recommendations: int = 0
    high_confidence: int = 0
    due_soon: int = 0
    policy_driven: int = 0
    applied_recently: int = 0
    dismissed_recently: int = 0


class GovernanceRecommendationsOut(BaseModel):
    generated_at: datetime
    total: int
    page: int
    page_size: int
    summary: GovernanceRecommendationSummaryOut = Field(default_factory=GovernanceRecommendationSummaryOut)
    filters: GovernanceRecommendationFiltersOut = Field(default_factory=GovernanceRecommendationFiltersOut)
    items: list[GovernanceRecommendationOut] = Field(default_factory=list)


class GovernanceRecommendationResolutionIn(BaseModel):
    recommendation_ids: list[int] = Field(default_factory=list, min_length=1, max_length=200)
    resolution_action: str = Field(min_length=1, max_length=40)
    resolution_note: str | None = None


class GovernanceRecommendationResolutionError(BaseModel):
    recommendation_id: int
    message: str


class GovernanceRecommendationResolutionOut(BaseModel):
    requested: int
    succeeded: int
    failed: int
    applied_ids: list[int] = Field(default_factory=list)
    failed_items: list[GovernanceRecommendationResolutionError] = Field(default_factory=list)


class ClassificationReviewBatchPromoteIn(BaseModel):
    table_ids: list[int] = Field(default_factory=list, min_length=1, max_length=200)


class ClassificationReviewBatchPromoteOut(BaseModel):
    generated_at: datetime
    requested_table_ids: list[int] = Field(default_factory=list)
    promoted_count: int = 0
    refresh_created: int = 0
    refresh_updated: int = 0
    refresh_reopened: int = 0
    refresh_resolved: int = 0
    refresh_purged: int = 0
    retention_days: int = 90


class GovernanceRecommendationContextOut(BaseModel):
    generated_at: datetime
    recommendation: GovernanceRecommendationOut
    assistant_summary: str
    assistant_tools: list[GovernanceAssistantToolOut] = Field(default_factory=list)
    policy_matches: list[dict[str, object]] = Field(default_factory=list)
    playbooks: list[GovernancePlaybookOut] = Field(default_factory=list)
    recent_events: list[dict[str, object]] = Field(default_factory=list)
    trust_history: list[dict[str, object]] = Field(default_factory=list)
    canonical_asset: dict[str, object] | None = None


class GovernanceRecommendationFeedbackIn(BaseModel):
    feedback_rating: str = Field(min_length=1, max_length=20)
    feedback_note: str | None = None


class GovernanceRecommendationFeedbackOut(BaseModel):
    recommendation_id: int
    recommendation_key: str
    feedback_rating: str | None = None
    feedback_label: str | None = None
    feedback_tone: str = "neutral"
    feedback_note: str | None = None
    feedback_updated_at: datetime | None = None
    feedback_updated_by_user_id: int | None = None
    message: str


class GovernanceAssistantActionIn(BaseModel):
    tool_key: str = Field(min_length=1, max_length=80)
    confirm: bool = False
    resolution_note: str | None = None


class GovernanceAssistantActionOut(BaseModel):
    ok: bool = True
    recommendation_id: int
    recommendation_key: str
    tool_key: str
    executed: bool = False
    message: str
    result: dict[str, object] = Field(default_factory=dict)


class GovernanceCampaignQueueItemOut(BaseModel):
    table_id: int
    table_name: str
    table_fqn: str
    datasource_name: str
    database_name: str
    schema_name: str
    owner_name: str
    certification_status: str
    certification_status_label: str
    sensitivity_label: str
    governance_score: GovernanceScoreOut
    last_review_at: datetime | None = None
    links: AssetLinksOut


class GovernanceCampaignQueueOut(BaseModel):
    generated_at: datetime
    campaign: GovernanceCampaignItemOut
    total: int
    page: int
    page_size: int
    items: list[GovernanceCampaignQueueItemOut] = Field(default_factory=list)


class ClassificationReviewFilterOptionOut(BaseModel):
    value: str
    label: str


class ClassificationReviewFiltersOut(BaseModel):
    kinds: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    entity_levels: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    review_statuses: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    sources: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    datasources: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    databases: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    schemas: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    domains: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    owners: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)
    tags: list[ClassificationReviewFilterOptionOut] = Field(default_factory=list)


class ClassificationReviewSignalOut(BaseModel):
    key: str
    label: str
    value: str | None = None
    tone: str = "neutral"
    detail: str | None = None


class ClassificationReviewTermPreviewOut(BaseModel):
    id: int
    name: str
    definition: str
    steward: str | None = None


class ClassificationReviewItemOut(BaseModel):
    key: str
    kind: str
    entity_level: str
    entity_type: str
    table_id: int
    table_name: str
    table_fqn: str
    column_id: int | None = None
    column_name: str | None = None
    datasource_id: int
    datasource_name: str
    database_name: str
    schema_name: str
    domain_name: str | None = None
    owner_name: str | None = None
    certification_status: str
    certification_status_label: str
    sensitivity_level: str | None = None
    sensitivity_label: str
    owner_defined: bool = False
    description_complete: bool = False
    dictionary_complete: bool = False
    classification_defined: bool = False
    tags_count: int = 0
    terms_count: int = 0
    readiness_score: int = 0
    governance_score: int = 0
    governance_label: str
    governance_tone: str
    trust_score: int = 0
    trust_label: str | None = None
    trust_tone: str | None = None
    dq_score: float | None = None
    has_personal_data: bool = False
    has_sensitive_personal_data: bool = False
    active_dq_violation: bool = False
    active_dq_rule_names: list[str] = Field(default_factory=list)
    critical_open_incidents: int = 0
    suggestion_tag_id: int | None = None
    suggestion_tag_name: str | None = None
    suggestion_tag_slug: str | None = None
    confidence_score: int | None = None
    inference_source: str | None = None
    inference_reason: str | None = None
    applied_automatically: bool | None = None
    review_status: str
    current_tags: list[TagOut] = Field(default_factory=list)
    table_tags: list[TagOut] = Field(default_factory=list)
    column_tags: list[TagOut] = Field(default_factory=list)
    current_terms: list[ClassificationReviewTermPreviewOut] = Field(default_factory=list)
    signals: list[ClassificationReviewSignalOut] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)
    links: AssetLinksOut
    created_at: datetime
    updated_at: datetime
    reviewed_at: datetime | None = None


class ClassificationReviewSummaryOut(BaseModel):
    pending_reviews: int
    high_confidence_reviews: int
    trust_at_risk: int
    probable_pii: int
    probable_sensitive: int
    conflicts: int
    critical_columns: int
    inheritance_pending: int
    reviewed_recently: int


class ClassificationReviewOut(BaseModel):
    generated_at: datetime
    total: int
    page: int
    page_size: int
    filters: ClassificationReviewFiltersOut = Field(default_factory=ClassificationReviewFiltersOut)
    summary: ClassificationReviewSummaryOut
    items: list[ClassificationReviewItemOut] = Field(default_factory=list)


class GovernancePendingBreakdownItemOut(BaseModel):
    key: str
    label: str
    count: int


class GovernancePendingFilterOptionOut(BaseModel):
    value: str
    label: str


class GovernancePendingFiltersOut(BaseModel):
    severities: list[GovernancePendingFilterOptionOut] = Field(default_factory=list)
    origins: list[GovernancePendingFilterOptionOut] = Field(default_factory=list)
    statuses: list[GovernancePendingFilterOptionOut] = Field(default_factory=list)
    owners: list[GovernancePendingFilterOptionOut] = Field(default_factory=list)
    datasources: list[GovernancePendingFilterOptionOut] = Field(default_factory=list)
    schemas: list[GovernancePendingFilterOptionOut] = Field(default_factory=list)
    domains: list[GovernancePendingFilterOptionOut] = Field(default_factory=list)


class GovernancePendingItemOut(BaseModel):
    key: str
    title: str
    description: str
    severity: str
    severity_label: str
    priority: int
    origin: str
    origin_label: str
    status: str
    status_label: str
    table_id: int
    table_name: str
    table_fqn: str
    datasource_name: str
    database_name: str
    schema_name: str
    domain_name: str | None = None
    owner_name: str
    data_owner_id: int | None = None
    detected_at: datetime
    aging_days: int = 0
    sla_days: int | None = None
    due_at: datetime | None = None
    sla_status: str | None = None
    sla_status_label: str | None = None
    governance_score: GovernanceScoreOut
    trust_score: int = 0
    trust_label: str | None = None
    trust_tone: str | None = None
    risk_score: int = 0
    risk_label: str = "Baixo risco"
    risk_tone: str = "neutral"
    risk_reason: str | None = None
    risk_components: list[str] = Field(default_factory=list)
    context_value: str | None = None
    action_label: str
    action_href: str
    links: AssetLinksOut


class GovernancePendingCampaignOut(BaseModel):
    group_by: str
    group_label: str
    value: str
    label: str
    count: int
    avg_governance_score: float
    lowest_governance_score: int
    governance_label: str
    governance_tone: str
    href: str
    hint: str


class GovernancePendingSummaryCardsOut(BaseModel):
    stewardship_pending: int = 0
    without_approver: int = 0
    reviews: int = 0
    certification: int = 0
    my_approval: int = 0
    my_queue: int = 0
    active_notifications: int = 0
    ready_to_resend: int = 0
    critical: int = 0
    operation: int = 0
    quality_incidents: int = 0
    trust_at_risk: int = 0


class GovernancePendingStewardshipSummaryOut(BaseModel):
    pending_total: int = 0
    awaiting_assignment: int = 0
    review_pending: int = 0
    certification_pending: int = 0
    my_approvals_pending: int = 0
    my_owner_queue: int = 0


class GovernanceNotificationOut(BaseModel):
    id: int
    dedupe_key: str
    rule_key: str
    channel: str
    status: str
    status_label: str
    severity: str
    severity_label: str
    origin: str
    title: str
    message: str
    entity_type: str
    table_id: int | None = None
    table_name: str | None = None
    table_fqn: str | None = None
    owner_name: str | None = None
    target_href: str | None = None
    context: dict[str, object] = Field(default_factory=dict)
    first_detected_at: datetime | None = None
    last_detected_at: datetime | None = None
    last_sent_at: datetime | None = None
    next_send_at: datetime | None = None
    resolved_at: datetime | None = None
    send_count: int = 0
    last_delivery_status: str | None = None
    last_delivery_error: str | None = None
    is_due: bool = False


class GovernanceNotificationSummaryOut(BaseModel):
    generated_at: datetime | None = None
    enabled: bool = True
    repeat_days: int = 7
    critical_repeat_hours: int = 24
    active_total: int = 0
    due_now_total: int = 0
    critical_total: int = 0
    review_total: int = 0
    operational_total: int = 0
    quality_total: int = 0
    incident_total: int = 0
    top_items: list[GovernanceNotificationOut] = Field(default_factory=list)


class GovernanceNotificationListOut(BaseModel):
    generated_at: datetime | None = None
    status: str
    total: int
    items: list[GovernanceNotificationOut] = Field(default_factory=list)


class GovernancePendingCenterOut(BaseModel):
    generated_at: datetime
    total: int
    page: int
    page_size: int
    export_csv_href: str
    export_xlsx_href: str
    summary_cards: GovernancePendingSummaryCardsOut = Field(default_factory=GovernancePendingSummaryCardsOut)
    summary: list[GovernancePendingBreakdownItemOut] = Field(default_factory=list)
    origins: list[GovernancePendingBreakdownItemOut] = Field(default_factory=list)
    campaigns: list[GovernancePendingCampaignOut] = Field(default_factory=list)
    stewardship: GovernancePendingStewardshipSummaryOut = Field(default_factory=GovernancePendingStewardshipSummaryOut)
    notifications: GovernanceNotificationSummaryOut = Field(default_factory=GovernanceNotificationSummaryOut)
    filters: GovernancePendingFiltersOut = Field(default_factory=GovernancePendingFiltersOut)
    risk_queue: list[GovernancePendingItemOut] = Field(default_factory=list)
    items: list[GovernancePendingItemOut] = Field(default_factory=list)


class GovernancePendingCenterSummaryOut(BaseModel):
    generated_at: datetime
    total: int
    export_csv_href: str
    export_xlsx_href: str
    summary_cards: GovernancePendingSummaryCardsOut = Field(default_factory=GovernancePendingSummaryCardsOut)
    summary: list[GovernancePendingBreakdownItemOut] = Field(default_factory=list)
    origins: list[GovernancePendingBreakdownItemOut] = Field(default_factory=list)
    campaigns: list[GovernancePendingCampaignOut] = Field(default_factory=list)
    stewardship: GovernancePendingStewardshipSummaryOut = Field(default_factory=GovernancePendingStewardshipSummaryOut)
    notifications: GovernanceNotificationSummaryOut = Field(default_factory=GovernanceNotificationSummaryOut)
    filters: GovernancePendingFiltersOut = Field(default_factory=GovernancePendingFiltersOut)


class GovernancePendingCenterSummaryLightOut(BaseModel):
    generated_at: datetime
    total: int
    export_csv_href: str
    export_xlsx_href: str
    summary_cards: GovernancePendingSummaryCardsOut = Field(default_factory=GovernancePendingSummaryCardsOut)
    summary: list[GovernancePendingBreakdownItemOut] = Field(default_factory=list)
    origins: list[GovernancePendingBreakdownItemOut] = Field(default_factory=list)
    campaigns: list[GovernancePendingCampaignOut] = Field(default_factory=list)
    stewardship: GovernancePendingStewardshipSummaryOut = Field(default_factory=GovernancePendingStewardshipSummaryOut)
    notifications: GovernanceNotificationSummaryOut = Field(default_factory=GovernanceNotificationSummaryOut)
    filters: GovernancePendingFiltersOut = Field(default_factory=GovernancePendingFiltersOut)


class GovernancePendingCenterCampaignsOut(BaseModel):
    generated_at: datetime
    total: int
    campaigns: list[GovernancePendingCampaignOut] = Field(default_factory=list)


class GovernancePendingCenterQueueOut(BaseModel):
    generated_at: datetime
    total: int
    page: int
    page_size: int
    export_csv_href: str
    export_xlsx_href: str
    risk_queue: list[GovernancePendingItemOut] = Field(default_factory=list)
    items: list[GovernancePendingItemOut] = Field(default_factory=list)
