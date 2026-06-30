from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from t2c_data.schemas.governance import GovernanceScoreOut


class DashboardKpiOut(BaseModel):
    key: str
    label: str
    value: float
    unit: str | None = None
    hint: str | None = None
    tone: str = "neutral"


class DashboardBreakdownItemOut(BaseModel):
    key: str
    label: str
    value: float
    tone: str | None = None


class DashboardCoverageItemOut(BaseModel):
    key: str
    label: str
    pct: float
    count: int
    total: int
    tone: str | None = None


class DashboardTrendPointOut(BaseModel):
    label: str
    value: float


class DashboardTableItemOut(BaseModel):
    table_id: int
    table_name: str
    table_fqn: str
    datasource_name: str
    database_name: str
    schema_name: str
    engine: str
    table_type: str
    dq_score: float | None = None
    completeness_pct_avg: float | None = None
    freshness_seconds: int | None = None
    open_incidents: int = 0
    critical_open_incidents: int = 0
    certification_status: str
    certification_criticality: str | None = None
    certification_badges: list[str] = Field(default_factory=list)
    owner_defined: bool = False
    owner_name: str | None = None
    dictionary_complete: bool = False
    description_complete: bool = False
    tags_count: int = 0
    terms_count: int = 0
    readiness_score: int = 0
    documentation_score: int = 0
    domain_name: str | None = None
    sensitivity_level: str | None = None
    last_review_at: datetime | None = None
    last_sync_at: datetime | None = None
    last_updated_at: datetime | None = None


class DashboardIncidentItemOut(BaseModel):
    id: int
    title: str
    entity_type: str
    severity: str
    status: str
    detected_at: datetime
    table_fqn: str | None = None
    airflow_dag_id: str | None = None


class DashboardSourceDistributionItemOut(BaseModel):
    datasource_id: int
    datasource_name: str
    engine: str
    engine_label: str
    database_name: str
    schema_count: int
    table_count: int
    served_tables: int
    certified_tables: int
    pending_tables: int
    is_active: bool
    status_key: str
    status_label: str
    status_tone: str


class DashboardSourceDistributionOut(BaseModel):
    total_sources: int = 0
    total_schemas: int = 0
    total_tables: int = 0
    served_tables: int = 0
    certified_tables: int = 0
    pending_tables: int = 0
    items: list[DashboardSourceDistributionItemOut] = Field(default_factory=list)


class DashboardSourcesSummaryOut(BaseModel):
    by_engine: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    by_datasource: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    lowest_governance: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    distribution: DashboardSourceDistributionOut = Field(default_factory=DashboardSourceDistributionOut)


class DashboardDocumentationSummaryOut(BaseModel):
    coverage: list[DashboardCoverageItemOut] = Field(default_factory=list)
    undocumented_tables: int = 0
    most_complete: list[DashboardTableItemOut] = Field(default_factory=list)
    least_complete: list[DashboardTableItemOut] = Field(default_factory=list)


class DashboardCertificationSummaryOut(BaseModel):
    by_status: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    by_criticality: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    by_badge: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    eligible_tables: int = 0
    pending_critical: int = 0


class DashboardGovernanceSummaryOut(BaseModel):
    coverage: list[DashboardCoverageItemOut] = Field(default_factory=list)


class DashboardDQSummaryOut(BaseModel):
    avg_score: float = 0.0
    below_minimum: int = 0
    without_metrics: int = 0
    score_bands: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    freshness_bands: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    worst_tables: list[DashboardTableItemOut] = Field(default_factory=list)
    trend: list[DashboardTrendPointOut] = Field(default_factory=list)


class DashboardIncidentsSummaryOut(BaseModel):
    total_open: int = 0
    critical_open: int = 0
    open_on_certified_assets: int = 0
    avg_open_age_hours: float = 0.0
    by_status: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    by_priority: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    top_items: list[DashboardIncidentItemOut] = Field(default_factory=list)


class DashboardAttentionSummaryOut(BaseModel):
    low_dq: list[DashboardTableItemOut] = Field(default_factory=list)
    no_owner: list[DashboardTableItemOut] = Field(default_factory=list)
    no_dictionary: list[DashboardTableItemOut] = Field(default_factory=list)
    eligible_not_certified: list[DashboardTableItemOut] = Field(default_factory=list)
    critical_incidents: list[DashboardTableItemOut] = Field(default_factory=list)
    rejected: list[DashboardTableItemOut] = Field(default_factory=list)
    restricted: list[DashboardTableItemOut] = Field(default_factory=list)


class DashboardSummaryOut(BaseModel):
    generated_at: datetime
    kpis: list[DashboardKpiOut] = Field(default_factory=list)
    certification: DashboardCertificationSummaryOut
    governance: DashboardGovernanceSummaryOut
    dq: DashboardDQSummaryOut
    incidents: DashboardIncidentsSummaryOut
    sources: DashboardSourcesSummaryOut
    documentation: DashboardDocumentationSummaryOut
    attention: DashboardAttentionSummaryOut


class DashboardExecutiveFilterOptionOut(BaseModel):
    value: str
    label: str
    datasource_id: int | None = None
    database_id: int | None = None
    schema_id: int | None = None


class DashboardExecutiveFiltersOut(BaseModel):
    domains: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)
    sources: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)
    databases: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)
    schemas: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)
    owners: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)
    certification_statuses: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)
    dq_bands: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)
    incident_options: list[DashboardExecutiveFilterOptionOut] = Field(default_factory=list)


class DashboardExecutiveAppliedFiltersOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    domain: str | None = None
    data_source_id: int | None = None
    source: str | None = None
    database: str | None = None
    schema_key: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    owner: str | None = None
    certification_status: str | None = None
    dq_band: str | None = None
    incidents: str | None = None
    q: str | None = None


class DashboardExecutiveScoreFactorOut(BaseModel):
    key: str
    label: str
    points: int
    applied: bool
    detail: str


class DashboardExecutiveLinksOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    explorer: str
    lineage: str
    data_quality: str
    certification: str
    incidents: str
    owners: str
    privacy: str
    audit: str
    datasource: str
    database: str
    schema_name: str = Field(alias="schema")


class DashboardExecutiveActionOut(BaseModel):
    key: str
    label: str
    description: str
    href: str
    category: str
    tone: str = "neutral"


class DashboardExecutiveAssetOut(BaseModel):
    table_id: int
    table_name: str
    table_fqn: str
    domain_name: str
    datasource_name: str
    database_name: str
    schema_name: str
    owner_name: str
    owner_defined: bool
    data_owner_is_active: bool | None = None
    governance_score: GovernanceScoreOut
    criticality_score: int
    criticality_label: str
    criticality_tone: str
    dq_score: float | None = None
    dq_status_label: str
    certification_status: str
    certification_status_label: str
    dictionary_complete: bool
    dictionary_status_label: str
    tags_count: int
    terms_count: int
    open_incidents: int
    critical_open_incidents: int
    last_review_at: datetime | None = None
    last_updated_at: datetime | None = None
    last_sync_at: datetime | None = None
    sensitivity_level: str | None = None
    sensitivity_label: str
    eligible_for_certification: bool
    owner_review_due: bool = False
    privacy_review_due: bool = False
    certification_review_due: bool = False
    score_factors: list[DashboardExecutiveScoreFactorOut] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)
    actions: list[DashboardExecutiveActionOut] = Field(default_factory=list)
    links: DashboardExecutiveLinksOut


class DashboardExecutiveTopCriticalOut(BaseModel):
    total: int
    items: list[DashboardExecutiveAssetOut] = Field(default_factory=list)


class DashboardExecutiveCertificationOut(BaseModel):
    certified: int
    eligible_not_certified: int
    not_eligible: int
    certified_pct: float
    eligible_not_certified_pct: float
    not_eligible_pct: float


class DashboardExecutiveGovernanceGapItemOut(BaseModel):
    key: str
    label: str
    count: int
    pct: float
    hint: str


class DashboardExecutiveGovernanceGapsOut(BaseModel):
    total_assets: int
    items: list[DashboardExecutiveGovernanceGapItemOut] = Field(default_factory=list)


class DashboardExecutiveGovernanceMaturityBandOut(BaseModel):
    key: str
    label: str
    count: int
    pct: float
    tone: str


class DashboardExecutiveGovernanceMaturityOut(BaseModel):
    avg_score: float
    bands: list[DashboardExecutiveGovernanceMaturityBandOut] = Field(default_factory=list)


class DashboardExecutiveStewardshipInboxGroupOut(BaseModel):
    key: str
    label: str
    count: int
    href: str


class DashboardExecutiveStewardshipOut(BaseModel):
    pending_total: int
    awaiting_assignment: int
    review_pending: int
    certification_pending: int
    my_approvals_pending: int = 0
    my_owner_queue: int = 0
    by_owner: list[DashboardExecutiveStewardshipInboxGroupOut] = Field(default_factory=list)
    by_approver: list[DashboardExecutiveStewardshipInboxGroupOut] = Field(default_factory=list)


class DashboardExecutiveDQOut(BaseModel):
    avg_score: float
    not_evaluated: int
    score_bands: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    worst_assets: list[DashboardExecutiveAssetOut] = Field(default_factory=list)
    trend: list[DashboardTrendPointOut] = Field(default_factory=list)


class DashboardExecutiveIncidentsOut(BaseModel):
    open_total: int
    critical_open_total: int = 0
    by_severity: list[DashboardBreakdownItemOut] = Field(default_factory=list)
    top_assets: list[DashboardExecutiveAssetOut] = Field(default_factory=list)
    recurring_assets: list[DashboardExecutiveAssetOut] = Field(default_factory=list)
    impact_assets: list[DashboardExecutiveAssetOut] = Field(default_factory=list)


class DashboardExecutiveRiskItemOut(BaseModel):
    label: str
    asset_count: int
    avg_score: float
    max_score: int
    critical_assets: int
    open_incidents: int


class DashboardExecutiveRiskOut(BaseModel):
    by_domain: list[DashboardExecutiveRiskItemOut] = Field(default_factory=list)
    by_source: list[DashboardExecutiveRiskItemOut] = Field(default_factory=list)
    by_schema: list[DashboardExecutiveRiskItemOut] = Field(default_factory=list)


class DashboardExecutiveMaturityPanelItemOut(BaseModel):
    key: str
    label: str
    asset_count: int
    owner_pct: float
    description_pct: float
    tags_pct: float
    glossary_pct: float
    pipeline_mapped_pct: float
    dq_avg_score: float
    governance_avg_score: float
    open_incidents: int
    critical_open_incidents: int
    governance_label: str
    governance_tone: str


class DashboardExecutiveMaturityPanelsOut(BaseModel):
    by_domain: list[DashboardExecutiveMaturityPanelItemOut] = Field(default_factory=list)
    by_source: list[DashboardExecutiveMaturityPanelItemOut] = Field(default_factory=list)
    by_owner: list[DashboardExecutiveMaturityPanelItemOut] = Field(default_factory=list)
    by_schema: list[DashboardExecutiveMaturityPanelItemOut] = Field(default_factory=list)


class DashboardExecutiveCampaignOut(BaseModel):
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


class DashboardExecutiveCriticalChangeOut(BaseModel):
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


class DashboardExecutiveIngestionItemOut(BaseModel):
    table_id: int | None = None
    table_name: str
    table_fqn: str
    pipeline_name: str | None = None
    dag_id: str | None = None
    task_name: str | None = None
    load_type_label: str | None = None
    latest_status_label: str | None = None
    last_success_at: datetime | None = None
    last_execution_finished_at: datetime | None = None
    watermark_value: str | None = None
    rows_processed: int | None = None
    last_error: str | None = None
    pipeline_history_href: str | None = None
    airflow_dag_href: str | None = None
    airflow_task_href: str | None = None
    target_url: str | None = None


class DashboardExecutiveIngestionOut(BaseModel):
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
    items: list[DashboardExecutiveIngestionItemOut] = Field(default_factory=list)
    high_volume_failed_items: list[DashboardExecutiveIngestionItemOut] = Field(default_factory=list)


class DashboardOperationalIntelligenceItemOut(BaseModel):
    entity_kind: str
    key: str
    label: str
    href: str
    table_id: int | None = None
    domain_name: str | None = None
    owner_name: str | None = None
    score: int = 0
    priority_score: int = 0
    risk_label: str = "Baixo"
    risk_tone: str = "neutral"
    asset_count: int = 0
    open_incidents: int = 0
    critical_open_incidents: int = 0
    recent_incidents_30d: int = 0
    recent_dq_failure_runs_30d: int = 0
    change_events_30d: int = 0
    search_clicks_30d: int = 0
    stale_hours: int | None = None
    degraded_pipelines: int = 0
    failed_pipelines: int = 0
    reasons: list[str] = Field(default_factory=list)
    suggested_actions: list[str] = Field(default_factory=list)
    suggested_incident: bool = False
    incident_hint: str | None = None


class DashboardOperationalIntelligenceAlertOut(BaseModel):
    key: str
    title: str
    description: str
    severity: str
    tone: str
    entity_kind: str
    href: str
    table_id: int | None = None
    suggested_incident: bool = False


class DashboardOperationalIntelligenceOut(BaseModel):
    generated_at: datetime
    window_days: int = 30
    evaluated_assets: int = 0
    priority_queue_size: int = 0
    high_risk_assets: int = 0
    high_risk_domains: int = 0
    high_risk_products: int = 0
    unstable_pipelines: int = 0
    deteriorating_assets: int = 0
    recurring_instability: int = 0
    suggested_incidents: int = 0
    by_asset: list[DashboardOperationalIntelligenceItemOut] = Field(default_factory=list)
    by_domain: list[DashboardOperationalIntelligenceItemOut] = Field(default_factory=list)
    by_product: list[DashboardOperationalIntelligenceItemOut] = Field(default_factory=list)
    by_pipeline: list[DashboardOperationalIntelligenceItemOut] = Field(default_factory=list)
    alerts: list[DashboardOperationalIntelligenceAlertOut] = Field(default_factory=list)
    trend: list[DashboardTrendPointOut] = Field(default_factory=list)


class DashboardExecutiveGovernanceTrendPointOut(BaseModel):
    bucket_date: datetime
    avg_score: float
    assets: int


class DashboardExecutiveGovernanceTrendOut(BaseModel):
    delta: float = 0
    direction: str = "flat"
    label: str = "Sem histórico"
    tone: str = "neutral"
    history: list[DashboardExecutiveGovernanceTrendPointOut] = Field(default_factory=list)


class DashboardStrategicMetricOut(BaseModel):
    key: str
    label: str
    current: float
    previous: float
    delta: float
    unit: str | None = None
    hint: str | None = None
    tone: str = "neutral"
    reverse_trend: bool = False


class DashboardStrategicTopUserOut(BaseModel):
    user_id: int
    label: str
    total_count: int
    usage_count: int
    search_count: int


class DashboardStrategicBenchmarkItemOut(BaseModel):
    key: str
    label: str
    href: str | None = None
    asset_count: int = 0
    quality_score: float = 0.0
    governance_score: float = 0.0
    coverage_score: float = 0.0
    reliability_score: float = 0.0
    adoption_count: float = 0.0
    adoption_score: float = 0.0
    open_incidents: int = 0
    critical_open_incidents: int = 0
    maturity_score: float = 0.0
    maturity_label: str = "Inicial"
    tone: str = "neutral"
    domain_name: str | None = None
    domain_href: str | None = None


class DashboardStrategicRoadmapStageOut(BaseModel):
    key: str
    label: str
    description: str
    criteria: list[str] = Field(default_factory=list)
    minimum_score: int = 0
    current_count: int = 0
    current_pct: float = 0.0
    tone: str = "neutral"


class DashboardStrategicAdoptionOut(BaseModel):
    active_users: int = 0
    active_domains: int = 0
    active_areas: int = 0
    active_products: int = 0
    top_users: list[DashboardStrategicTopUserOut] = Field(default_factory=list)
    top_domains: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    top_areas: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    top_products: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    low_adoption_areas: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    top_assets: list[dict[str, object]] = Field(default_factory=list)


class DashboardStrategicReportsOut(BaseModel):
    maturity_by_domain: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    reliability_by_domain: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    quality_by_domain: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    governance_by_domain: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    coverage_by_domain: list[DashboardStrategicBenchmarkItemOut] = Field(default_factory=list)
    value_trend: list[DashboardTrendPointOut] = Field(default_factory=list)
    quality_trend: list[DashboardTrendPointOut] = Field(default_factory=list)
    governance_trend: list[DashboardTrendPointOut] = Field(default_factory=list)
    adoption_trend: list[DashboardTrendPointOut] = Field(default_factory=list)


class DashboardStrategicSummaryOut(BaseModel):
    generated_at: datetime
    window_days: int = 30
    value_score: float = 0.0
    value_score_previous: float = 0.0
    value_score_delta: float = 0.0
    value_metrics: list[DashboardStrategicMetricOut] = Field(default_factory=list)
    adoption: DashboardStrategicAdoptionOut
    reports: DashboardStrategicReportsOut
    benchmark: dict[str, list[DashboardStrategicBenchmarkItemOut]] = Field(default_factory=dict)
    roadmap: list[DashboardStrategicRoadmapStageOut] = Field(default_factory=list)
    narrative: list[str] = Field(default_factory=list)


class DashboardExecutiveSummaryOut(BaseModel):
    generated_at: datetime
    available_filters: DashboardExecutiveFiltersOut
    applied_filters: DashboardExecutiveAppliedFiltersOut
    kpis: list[DashboardKpiOut] = Field(default_factory=list)
    top_critical: DashboardExecutiveTopCriticalOut
    certification: DashboardExecutiveCertificationOut
    governance_gaps: DashboardExecutiveGovernanceGapsOut
    governance_reviews: DashboardExecutiveGovernanceGapsOut
    governance_maturity: DashboardExecutiveGovernanceMaturityOut
    governance_trend: DashboardExecutiveGovernanceTrendOut = Field(default_factory=DashboardExecutiveGovernanceTrendOut)
    stewardship: DashboardExecutiveStewardshipOut
    campaigns: list[DashboardExecutiveCampaignOut] = Field(default_factory=list)
    critical_changes: list[DashboardExecutiveCriticalChangeOut] = Field(default_factory=list)
    ingestion: DashboardExecutiveIngestionOut
    dq: DashboardExecutiveDQOut
    incidents: DashboardExecutiveIncidentsOut
    risk: DashboardExecutiveRiskOut
    maturity_panels: DashboardExecutiveMaturityPanelsOut
    operational_intelligence: DashboardOperationalIntelligenceOut


class DashboardExecutiveOverviewOut(BaseModel):
    generated_at: datetime
    available_filters: DashboardExecutiveFiltersOut
    applied_filters: DashboardExecutiveAppliedFiltersOut
    kpis: list[DashboardKpiOut] = Field(default_factory=list)
    top_critical: DashboardExecutiveTopCriticalOut
    certification: DashboardExecutiveCertificationOut
    governance_gaps: DashboardExecutiveGovernanceGapsOut
    governance_reviews: DashboardExecutiveGovernanceGapsOut
    governance_maturity: DashboardExecutiveGovernanceMaturityOut
    governance_trend: DashboardExecutiveGovernanceTrendOut = Field(default_factory=DashboardExecutiveGovernanceTrendOut)


class DashboardExecutiveSecondaryOut(BaseModel):
    generated_at: datetime
    stewardship: DashboardExecutiveStewardshipOut
    campaigns: list[DashboardExecutiveCampaignOut] = Field(default_factory=list)
    critical_changes: list[DashboardExecutiveCriticalChangeOut] = Field(default_factory=list)
    ingestion: DashboardExecutiveIngestionOut
    dq: DashboardExecutiveDQOut
    incidents: DashboardExecutiveIncidentsOut
    risk: DashboardExecutiveRiskOut
    maturity_panels: DashboardExecutiveMaturityPanelsOut
    operational_intelligence: DashboardOperationalIntelligenceOut


class DashboardExecutiveAssetIncidentOut(BaseModel):
    id: int
    title: str
    severity: str
    status: str
    detected_at: datetime | None = None
    occurrences: int = 0


class DashboardExecutiveAssetDataNotesOut(BaseModel):
    domain: str
    dq_status: str
    eligibility: str


class DashboardExecutiveAssetDetailsOut(BaseModel):
    generated_at: datetime
    asset: DashboardExecutiveAssetOut
    incidents: list[DashboardExecutiveAssetIncidentOut] = Field(default_factory=list)
    next_actions: list[str] = Field(default_factory=list)
    data_notes: DashboardExecutiveAssetDataNotesOut
