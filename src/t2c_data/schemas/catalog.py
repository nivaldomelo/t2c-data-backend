from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from t2c_data.schemas.asset_context import AssetOperationalContextOut, DQIncidentSignalsOut
from t2c_data.schemas.data_owner import DataOwnerRefOut
from t2c_data.schemas.governance import GovernanceScoreOut
from t2c_data.schemas.ingestion import IngestionStabilitySummaryOut, IngestionTableSummaryOut
from t2c_data.schemas.metabase import MetabaseImpactSummaryOut
from t2c_data.schemas.tag import TagOut
from t2c_data.schemas.contracts import DataContractSummaryOut


class ColumnOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    data_owner_id: int | None = None
    name: str
    data_type: str
    is_primary_key: bool
    is_nullable: bool
    ordinal_position: int
    description_source: str | None
    description_manual: str | None
    external_id: str | None = None
    slug: str | None = None
    udt_name: str | None = None
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    column_default: str | None = None
    existing_comment: str | None = None
    dictionary_description: str | None = None
    dictionary_comment: str | None = None
    data_owner: DataOwnerRefOut | None = None
    owner_reviewed_by_user_id: int | None = None
    owner_reviewed_by_user_name: str | None = None
    owner_reviewed_by_user_email: str | None = None
    owner_reviewed_at: datetime | None = None
    tags: list[TagOut] = Field(default_factory=list)


class TableOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    schema_id: int
    data_owner_id: int | None
    name: str
    table_type: str
    description_source: str | None
    description_manual: str | None
    owner: str | None
    owner_email: str | None
    data_owner: DataOwnerRefOut | None = None
    lifecycle_status: str | None
    certification_status: str
    certification_criticality: str | None = None
    certification_badges: list[str] | None = None
    certification_notes: str | None = None
    certification_submitted_by_user_id: int | None = None
    certification_submitted_by_user_name: str | None = None
    certification_submitted_by_user_email: str | None = None
    certification_submitted_at: datetime | None = None
    certification_decided_by_user_id: int | None = None
    certification_decided_by_user_name: str | None = None
    certification_decided_by_user_email: str | None = None
    certification_decided_at: datetime | None = None
    certification_review_at: datetime | None = None
    certification_expires_at: datetime | None = None
    owner_reviewed_by_user_id: int | None = None
    owner_reviewed_by_user_name: str | None = None
    owner_reviewed_by_user_email: str | None = None
    owner_reviewed_at: datetime | None = None
    owner_review_due: bool = False
    owner_review_next_at: datetime | None = None
    sensitivity_level: str | None = None
    has_personal_data: bool
    has_sensitive_personal_data: bool
    legal_basis: str | None = None
    privacy_purpose: str | None = None
    retention_policy: str | None = None
    is_masked: bool
    external_sharing: bool
    access_scope: str | None = None
    access_roles: list[str] | None = None
    privacy_notes: str | None = None
    privacy_reviewed_by_user_id: int | None = None
    privacy_reviewed_by_user_name: str | None = None
    privacy_reviewed_by_user_email: str | None = None
    privacy_reviewed_at: datetime | None = None
    privacy_review_due: bool = False
    privacy_review_next_at: datetime | None = None
    certification_review_due: bool = False
    certification_next_review_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class TableDetailOut(TableOut):
    columns: list[ColumnOut]
    data_contract: DataContractSummaryOut | None = None
    row_count_metrics: Optional["TableRowCountMetricsOut"] = None
    metabase_impact: MetabaseImpactSummaryOut | None = None
    steward_user_id: int | None = None
    steward_name: str | None = None
    steward_email: str | None = None


class TableVolumeSnapshotOut(BaseModel):
    table_id: int
    datasource_id: int | None = None
    schema_id: int | None = None
    connection_name: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    fqn: str | None = None
    row_count: int | None = None
    measurement_type: str | None = None
    measurement_source: str | None = None
    status: str | None = None
    measured_at: datetime | None = None
    duration_ms: int | None = None
    error_message: str | None = None


class TableVolumeHistoryOut(BaseModel):
    items: list[TableVolumeSnapshotOut] = Field(default_factory=list)


class TableVolumeRunOut(BaseModel):
    total_tables: int
    succeeded: int
    failed: int
    skipped: int
    items: list[TableVolumeSnapshotOut] = Field(default_factory=list)


class TableRowCountMetricsOut(BaseModel):
    current_row_count: int | None = None
    previous_row_count: int | None = None
    snapshot_at: datetime | None = None
    previous_snapshot_at: datetime | None = None
    collection_method: str | None = None
    collection_status: str | None = None
    measured_at: datetime | None = None
    measurement_type: str | None = None
    measurement_source: str | None = None
    status: str | None = None
    error_message: str | None = None
    duration_ms: int | None = None
    growth_absolute: int | None = None
    growth_percent: float | None = None
    has_history: bool = False


TableDetailOut.model_rebuild()


class TablePatch(BaseModel):
    description_manual: str | None = None
    data_owner_id: int | None = None
    owner: str | None = None
    owner_email: str | None = None
    steward_user_id: int | None = None
    lifecycle_status: str | None = None
    sensitivity_level: str | None = None
    has_personal_data: bool | None = None
    has_sensitive_personal_data: bool | None = None
    legal_basis: str | None = None
    privacy_purpose: str | None = None
    retention_policy: str | None = None
    is_masked: bool | None = None
    external_sharing: bool | None = None
    access_scope: str | None = None
    access_roles: list[str] | None = None
    privacy_notes: str | None = None


class TableOwnerPatch(BaseModel):
    """Owner/steward-only mutation surface.

    Restricts edits to ownership/responsibility fields so the data_owner role can
    reassign owners without touching any other table metadata.
    """

    data_owner_id: int | None = None
    owner: str | None = None
    owner_email: str | None = None
    steward_user_id: int | None = None


class TableCertificationSummaryOut(BaseModel):
    id: int
    name: str
    schema_name: str
    database_name: str
    datasource_name: str
    owner: str | None
    owner_email: str | None
    data_owner_id: int | None = None
    data_owner: DataOwnerRefOut | None = None
    data_owner_is_active: bool | None = None
    certification_status: str
    certification_criticality: str | None = None
    certification_badges: list[str] | None = None
    certification_notes: str | None = None
    certification_status_source: str = "automatic"
    certification_status_rule: str = "automatic_readiness_not_eligible"
    certification_status_reason: str = "Prontidão abaixo do patamar mínimo para certificação."
    certification_submitted_by_user_id: int | None = None
    certification_submitted_by_user_name: str | None = None
    certification_submitted_by_user_email: str | None = None
    certification_submitted_at: datetime | None = None
    certification_decided_by_user_id: int | None = None
    certification_decided_by_user_name: str | None = None
    certification_decided_by_user_email: str | None = None
    certification_decided_at: datetime | None = None
    certification_review_at: datetime | None = None
    certification_expires_at: datetime | None = None
    certification_review_due: bool = False
    certification_next_review_at: datetime | None = None
    certification_sla_due_at: datetime | None = None
    certification_sla_status: str = "not_applicable"
    certification_sla_label: str = "Sem SLA ativo"
    certification_revalidation_required: bool = False
    certification_next_step: str | None = None
    active_dq_violation: bool = False
    active_dq_violation_count: int = 0
    active_dq_rule_names: list[str] = Field(default_factory=list)
    owner_reviewed_by_user_id: int | None = None
    owner_reviewed_by_user_name: str | None = None
    owner_reviewed_by_user_email: str | None = None
    owner_reviewed_at: datetime | None = None
    owner_review_due: bool = False
    owner_review_next_at: datetime | None = None
    certification_status_label: str
    trust_score: int | None = None
    trust_label: str | None = None
    trust_tone: str | None = None
    readiness_score: int
    readiness_completed: int
    readiness_total: int
    eligible_for_certification: bool
    checklist: list[dict[str, str | bool]]
    created_at: datetime
    updated_at: datetime


class CertificationTableFilterOptionOut(BaseModel):
    id: int
    name: str


class TableCertificationFiltersOut(BaseModel):
    owners: list[CertificationTableFilterOptionOut] = Field(default_factory=list)
    schemas: list[str] = Field(default_factory=list)
    databases: list[str] = Field(default_factory=list)


class TableCertificationPageOut(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[TableCertificationSummaryOut] = Field(default_factory=list)
    filters: TableCertificationFiltersOut = Field(default_factory=TableCertificationFiltersOut)


class TableCertificationBlockerSummaryOut(BaseModel):
    key: str
    label: str
    count: int
    percent: int
    description: str
    action: str


class TableCertificationPriorityItemOut(BaseModel):
    id: int
    name: str
    schema_name: str
    database_name: str
    datasource_name: str
    certification_status: str
    certification_status_label: str
    readiness_score: int
    readiness_completed: int
    readiness_total: int
    pending_criteria: int
    primary_blocker: str | None = None
    primary_blocker_detail: str | None = None
    next_step: str | None = None


class TableCertificationDistributionOut(BaseModel):
    key: str
    database_name: str
    schema_name: str
    total: int
    certified: int
    eligible: int
    not_eligible: int
    avg_readiness: int
    primary_blocker: str | None = None
    primary_blocker_count: int = 0


class TableCertificationSummaryMetricsOut(BaseModel):
    total: int
    certified: int
    eligible: int
    in_review: int
    rejected: int
    revalidation_pending: int
    not_eligible: int
    avg_readiness: int
    blockers: list[TableCertificationBlockerSummaryOut] = Field(default_factory=list)
    near_certification: list[TableCertificationPriorityItemOut] = Field(default_factory=list)
    most_blocked: list[TableCertificationPriorityItemOut] = Field(default_factory=list)
    distribution: list[TableCertificationDistributionOut] = Field(default_factory=list)


class CertificationGoalBase(BaseModel):
    name: str = Field(min_length=3, max_length=200)
    period_start: date
    period_end: date
    target_certified_assets: int = Field(default=0, ge=0)
    target_eligible_assets: int = Field(default=0, ge=0)
    target_reviewed_assets: int = Field(default=0, ge=0)
    target_revalidated_assets: int = Field(default=0, ge=0)
    scope_type: str = Field(default="global", min_length=3, max_length=40)
    scope_value: str | None = Field(default=None, max_length=255)
    owner: str | None = Field(default=None, max_length=160)
    status: str = Field(default="active", min_length=3, max_length=20)
    notes: str | None = None


class CertificationGoalCreate(CertificationGoalBase):
    pass


class CertificationGoalUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=3, max_length=200)
    period_start: date | None = None
    period_end: date | None = None
    target_certified_assets: int | None = Field(default=None, ge=0)
    target_eligible_assets: int | None = Field(default=None, ge=0)
    target_reviewed_assets: int | None = Field(default=None, ge=0)
    target_revalidated_assets: int | None = Field(default=None, ge=0)
    scope_type: str | None = Field(default=None, min_length=3, max_length=40)
    scope_value: str | None = Field(default=None, max_length=255)
    owner: str | None = Field(default=None, max_length=160)
    status: str | None = Field(default=None, min_length=3, max_length=20)
    notes: str | None = None


class CertificationGoalOut(CertificationGoalBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime


class CertificationGoalDailyProgressOut(BaseModel):
    date: date
    certified: int = 0
    eligible: int = 0
    reviewed: int = 0
    revalidated: int = 0
    accumulated_certified: int = 0


class CertificationGoalProgressMetricsOut(BaseModel):
    certified_assets: int = 0
    eligible_assets: int = 0
    reviewed_assets: int = 0
    revalidated_assets: int = 0
    decisions_assets: int = 0
    refusal_assets: int = 0
    current_certified_assets: int = 0
    current_eligible_assets: int = 0
    remaining_certified_assets: int = 0
    completion_percent: int = 0
    days_elapsed: int = 0
    days_remaining: int = 0
    required_daily_rate: float = 0
    current_daily_rate: float = 0
    projected_total: int = 0
    status: str = "no_data"
    status_label: str = "Sem dados suficientes"
    history_source: str = "legacy_dates_fallback"
    history_note: str | None = None


class CertificationRecommendationOut(BaseModel):
    title: str
    description: str
    priority: str
    action_label: str | None = None
    action_href: str | None = None


class CertificationGoalProgressOut(BaseModel):
    goal: CertificationGoalOut
    progress: CertificationGoalProgressMetricsOut
    daily: list[CertificationGoalDailyProgressOut] = Field(default_factory=list)
    blockers: list[TableCertificationBlockerSummaryOut] = Field(default_factory=list)
    recommendations: list[CertificationRecommendationOut] = Field(default_factory=list)


class CertificationDecisionEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    asset_id: int
    asset_name: str
    database_name: str
    schema_name: str
    table_name: str
    previous_status: str | None = None
    new_status: str
    previous_readiness: int | None = None
    new_readiness: int | None = None
    decision_type: str
    decision_source: str
    reviewer_user_id: int | None = None
    reviewer: str | None = None
    reviewer_email: str | None = None
    observation: str | None = None
    reason: str | None = None
    valid_until: datetime | None = None
    revalidation_due_at: datetime | None = None
    goal_id: int | None = None
    metadata_json: dict | None = None
    created_at: datetime


class CertificationDecisionEventPageOut(BaseModel):
    items: list[CertificationDecisionEventOut] = Field(default_factory=list)
    total: int
    page: int
    page_size: int


class TableSearchSuggestionOut(BaseModel):
    id: int
    name: str
    table_fqn: str
    datasource_name: str
    database_name: str
    schema_name: str
    table_type: str


class TableCertificationPatch(BaseModel):
    certification_status: str
    certification_criticality: str | None = None
    certification_badges: list[str] | None = None
    certification_notes: str | None = None
    certification_review_at: datetime | None = None
    certification_expires_at: datetime | None = None


class TableCertificationSubmitIn(BaseModel):
    certification_notes: str | None = None
    certification_review_at: datetime | None = None
    certification_expires_at: datetime | None = None


class TableCertificationDecisionIn(BaseModel):
    decision: str
    certification_criticality: str | None = None
    certification_badges: list[str] | None = None
    certification_notes: str | None = None
    certification_review_at: datetime | None = None
    certification_expires_at: datetime | None = None


class TreeDatasourceOut(BaseModel):
    id: int
    name: str
    db_type: str
    database: str


class TreeSchemaOut(BaseModel):
    id: int
    name: str


class TreeDatasourceChildrenOut(BaseModel):
    datasource_id: int
    database_id: int | None
    database: str
    schemas: list[TreeSchemaOut]


class TreeTableOut(BaseModel):
    id: int
    name: str
    kind: str
    governance_score: int | None = None
    governance_label: str | None = None
    governance_tone: str | None = None
    certification_status: str | None = None
    readiness_score: int | None = None
    trust_score: int | None = None
    trust_label: str | None = None
    trust_tone: str | None = None
    active_dq_violation: bool = False
    owner_defined: bool = False
    tags: list[TagOut] = Field(default_factory=list)


class TreeTablePageOut(BaseModel):
    page: int
    page_size: int
    total: int | None = None
    has_more: bool = False
    items: list[TreeTableOut] = Field(default_factory=list)


class TreeTableColumnsOut(BaseModel):
    id: int
    table_id: int
    data_owner_id: int | None = None
    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool
    ordinal_position: int
    external_id: str | None = None
    slug: str | None = None
    udt_name: str | None = None
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    column_default: str | None = None
    existing_comment: str | None = None
    description_source: str | None = None
    description_manual: str | None = None
    dictionary_description: str | None = None
    dictionary_comment: str | None = None
    data_owner: DataOwnerRefOut | None = None
    owner_reviewed_by_user_id: int | None = None
    owner_reviewed_by_user_name: str | None = None
    owner_reviewed_by_user_email: str | None = None
    owner_reviewed_at: datetime | None = None
    description: str | None
    tags: list[TagOut] = Field(default_factory=list)


class TreeTableColumnsPageOut(BaseModel):
    page: int
    page_size: int
    total: int
    has_more: bool = False
    items: list[TreeTableColumnsOut] = Field(default_factory=list)


class TableColumnSummaryOut(BaseModel):
    table_id: int
    total: int
    required: int
    nullable: int
    primary_keys: int
    documented: int
    commented: int
    preview: list[TreeTableColumnsOut] = Field(default_factory=list)


class ColumnDictionaryImportError(BaseModel):
    row_number: int
    slug: str | None = None
    message: str


class ColumnDictionaryImportResult(BaseModel):
    processed: int
    matched: int
    imported: int
    updated: int
    ignored: int
    rejected: int
    errors: list[ColumnDictionaryImportError] = Field(default_factory=list)
    touched_table_ids: list[int] = Field(default_factory=list)


class ExplorerSearchResultOut(BaseModel):
    match_type: str
    name: str
    datasource_id: int
    schema_id: int | None
    table_id: int | None
    column_name: str | None
    governance_score: int | None = None
    governance_label: str | None = None
    governance_tone: str | None = None
    certification_status: str | None = None
    readiness_score: int | None = None
    trust_score: int | None = None
    trust_label: str | None = None
    trust_tone: str | None = None
    active_dq_violation: bool = False
    owner_defined: bool = False


class TableLocatorOut(BaseModel):
    table_id: int
    datasource_id: int
    datasource_name: str
    database_id: int | None
    database_name: str
    schema_id: int
    schema_name: str
    table_name: str
    kind: str
    db_type: str


class TableCorrelationDQRuleOut(BaseModel):
    id: int
    name: str
    severity: str
    last_run_status: str | None = None
    last_violations_count: int = 0
    open_incident_id: int | None = None
    target_url: str


class TableCorrelationDQSummaryOut(BaseModel):
    dq_score: float | None = None
    failed_rules: int = 0
    freshness_seconds: int | None = None
    run_at: datetime | None = None
    correlated_rules: list[TableCorrelationDQRuleOut] = Field(default_factory=list)


class TableCorrelationIncidentItemOut(BaseModel):
    id: int
    title: str
    status: str
    severity: str
    severity_label: str
    source_type: str | None = None
    detected_at: datetime
    last_seen_at: datetime | None = None
    target_url: str


class TableCorrelationIncidentSummaryOut(BaseModel):
    open_count: int = 0
    critical_open_count: int = 0
    latest_open_incident_id: int | None = None
    latest_open_incident_title: str | None = None
    items: list[TableCorrelationIncidentItemOut] = Field(default_factory=list)


class TableCorrelationOperationalSLAOut(BaseModel):
    active: bool = False
    issue_type: str | None = None
    issue_label: str | None = None
    detected_at: datetime | None = None
    due_at: datetime | None = None
    aging_hours: int = 0
    sla_hours: int | None = None
    status: str = "within_sla"
    status_label: str = "Dentro do SLA"
    recurrent_degradation: bool = False


class TableOperationalIncidentPrefillOut(BaseModel):
    title: str
    description: str
    source_type: str
    source_ref_id: int
    evidence_json: dict[str, object] = Field(default_factory=dict)


class TableCorrelationSignalsOut(BaseModel):
    combined_attention: bool = False
    operational_failure: bool = False
    stale_pipeline: bool = False
    open_incident: bool = False
    dq_below_threshold: bool = False
    summary: str


class GovernanceScoreHistoryPointOut(BaseModel):
    bucket_date: datetime
    score: int
    label: str
    tone: str
    dq_score: float | None = None
    open_incidents: int = 0


class GovernanceScoreTrendOut(BaseModel):
    current_score: int
    baseline_score: int
    delta: int
    direction: str
    label: str
    tone: str
    history: list[GovernanceScoreHistoryPointOut] = Field(default_factory=list)


class TableCorrelationSummaryOut(BaseModel):
    table_id: int
    locator: TableLocatorOut
    operational_context: AssetOperationalContextOut | None = None
    ingestion: IngestionTableSummaryOut | None = None
    stability: IngestionStabilitySummaryOut | None = None
    governance_score: GovernanceScoreOut
    governance_trend: GovernanceScoreTrendOut | None = None
    dq: TableCorrelationDQSummaryOut = Field(default_factory=TableCorrelationDQSummaryOut)
    incident_signals: DQIncidentSignalsOut | None = None
    incidents: TableCorrelationIncidentSummaryOut = Field(default_factory=TableCorrelationIncidentSummaryOut)
    operational_sla: TableCorrelationOperationalSLAOut | None = None
    incident_prefill: TableOperationalIncidentPrefillOut | None = None
    signals: TableCorrelationSignalsOut
    asset_id: int | None = None
    asset_name: str | None = None
    qualified_name: str | None = None
    schema_name: str | None = None
    source_name: str | None = None
    has_operational_failure: bool = False
    has_dq_degradation: bool = False
    has_open_incident: bool = False
    priority_score: int = 0
    correlation_type: str | None = None
    summary: str | None = None
