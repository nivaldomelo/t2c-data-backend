from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


LineageAssetType = Literal["table", "view", "dashboard", "question", "source", "job", "incident", "certification", "dq_rule"]
LineageLayer = Literal["bronze", "silver", "gold", "mart", "dashboard", "source", "definir"]
LineageRelationType = Literal[
    "ingestion",
    "transformation",
    "load",
    "consumption",
    "extracted_from",
    "transformed_to",
    "loaded_to",
    "consumed_by",
    "validates",
    "impacts",
    "depends_on",
    "derived_from",
]
LineageOrigin = Literal["manual", "automatic", "merged"]


class LineageProcessCreate(BaseModel):
    name: str
    description: str | None = None


class LineageEdgeCreate(BaseModel):
    process_id: int
    from_entity_type: str
    from_entity_id: int
    to_entity_type: str
    to_entity_id: int


class LineageProcessOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None
    created_at: datetime


class LineageEdgeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    process_id: int
    from_entity_type: str
    from_entity_id: int
    to_entity_type: str
    to_entity_id: int
    created_at: datetime


class LineageGraphNode(BaseModel):
    key: str
    entity_type: str
    entity_id: int


class LineageGraphEdge(BaseModel):
    process_id: int
    source: str
    target: str


class LineageGraphOut(BaseModel):
    nodes: list[LineageGraphNode]
    edges: list[LineageGraphEdge]


class LineageNodeInput(BaseModel):
    kind: Literal["table", "system", "process", "dashboard"] = "table"
    label: str | None = None
    datasource_id: int | None = None
    table_id: int | None = None
    meta: dict[str, Any] | None = None


class TableLineageUpsert(BaseModel):
    upstreams: list[LineageNodeInput] = Field(default_factory=list)
    processes: list[LineageNodeInput] = Field(default_factory=list)
    downstreams: list[LineageNodeInput] = Field(default_factory=list)
    notes: str | None = None


class TableLineageNodeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    kind: str
    label: str
    datasource_id: int | None = None
    table_id: int | None = None
    meta: dict[str, Any] | None = None


class TableLineageEdgeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    from_node_id: int
    to_node_id: int
    edge_type: str
    transform: str | None = None
    notes: str | None = None


class TableLineageOut(BaseModel):
    table_id: int
    nodes: list[TableLineageNodeOut]
    edges: list[TableLineageEdgeOut]
    notes: str | None = None
    updated_at: datetime | None = None


class SourceSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str
    name: str | None = None
    datasource_id: int | None = None
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    object: str | None = None


class ProcessSpec(BaseModel):
    type: str = "manual"
    name: str
    dag_id: str | None = None
    task_id: str | None = None
    meta: dict[str, Any] | None = None


class DownstreamSpec(BaseModel):
    type: str = "dashboard"
    name: str
    url: str | None = None


class LineageSpec(BaseModel):
    table_id: int
    upstreams: list[SourceSpec] = Field(default_factory=list)
    process: ProcessSpec
    downstreams: list[DownstreamSpec] = Field(default_factory=list)
    notes: str | None = None
    updated_at: datetime | None = None


class LineageSpecIn(BaseModel):
    table_id: int | None = None
    upstreams: list[SourceSpec] = Field(default_factory=list)
    process: ProcessSpec
    downstreams: list[DownstreamSpec] = Field(default_factory=list)
    notes: str | None = None


class LineageSpecOut(BaseModel):
    table_id: int
    upstreams: list[SourceSpec] = Field(default_factory=list)
    process: ProcessSpec | None = None
    downstreams: list[DownstreamSpec] = Field(default_factory=list)
    notes: str | None = None
    updated_at: datetime | None = None


class LineageSpecLookupOut(BaseModel):
    table_id: int
    table_fqn: str
    table_name: str
    table_type: str
    schema_name: str
    database_name: str
    db_type: str
    spec: LineageSpecOut


class LineageAssetBase(BaseModel):
    asset_name: str
    asset_type: LineageAssetType
    layer: LineageLayer
    schema_name: str | None = None
    object_name: str | None = None
    system_name: str | None = None
    description: str | None = None
    datasource_id: int | None = None


class LineageAssetCreate(LineageAssetBase):
    asset_key: str | None = None
    catalog_table_id: int | None = None


class LineageAssetUpdate(BaseModel):
    asset_name: str | None = None
    asset_type: LineageAssetType | None = None
    layer: LineageLayer | None = None
    schema_name: str | None = None
    object_name: str | None = None
    system_name: str | None = None
    description: str | None = None
    datasource_id: int | None = None
    is_active: bool | None = None


class LineageAssetRefOut(BaseModel):
    id: int | None = None
    catalog_table_id: int | None = None
    datasource_id: int | None = None
    asset_key: str
    asset_name: str
    asset_type: str
    layer: str
    schema_name: str | None = None
    object_name: str | None = None
    system_name: str | None = None
    description: str | None = None
    asset_origin: str = "manual"
    external_namespace: str | None = None
    external_name: str | None = None
    external_type: str | None = None
    external_node_id: str | None = None
    is_active: bool = True


class LineageAssetOut(LineageAssetRefOut):
    created_at: datetime
    updated_at: datetime


class LineageAssetCandidateOut(LineageAssetRefOut):
    lineage_asset_id: int | None = None


class LineageAssetProcessOut(BaseModel):
    process_name: str
    process_type: str | None = None
    relation_type: str
    count: int


class LineageJobRunOut(BaseModel):
    external_run_id: str
    status: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    nominal_start_time: str | None = None


class LineageJobSummaryOut(BaseModel):
    id: int | None = None
    namespace: str | None = None
    job_name: str
    display_name: str
    job_type: str | None = None
    latest_run_id: str | None = None
    latest_run_status: str | None = None
    latest_run_at: str | None = None
    recent_runs: list[LineageJobRunOut] = Field(default_factory=list)


class LineageImpactOut(BaseModel):
    upstream_count: int
    downstream_count: int
    process_count: int
    dashboard_count: int
    direct_dependencies_count: int
    impact_level: str


class LineageGraphNodeOut(BaseModel):
    id: str
    label: str
    kind: str
    asset_id: int | None = None
    catalog_table_id: int | None = None
    node_type: str | None = None
    asset_type: str | None = None
    layer: str | None = None
    subtitle: str | None = None
    database_engine: str | None = None
    source_type: str | None = None
    process_type: str | None = None
    lineage_origin: LineageOrigin = "manual"


class LineageGraphEdgeOut(BaseModel):
    id: str
    source: str
    target: str
    relation_type: str
    confidence_score: int | None = None
    confidence_tier: str | None = None
    is_verified: bool | None = None
    version: int | None = None
    evidence: str | None = None


class LineageGraphOut(BaseModel):
    summary: LineageAssetSummaryOut
    nodes: list[LineageGraphNodeOut] = Field(default_factory=list)
    edges: list[LineageGraphEdgeOut] = Field(default_factory=list)


class LineageAssetSummaryOut(BaseModel):
    asset: LineageAssetRefOut
    upstream: list[LineageAssetRefOut] = Field(default_factory=list)
    downstream: list[LineageAssetRefOut] = Field(default_factory=list)
    related_processes: list[LineageAssetProcessOut] = Field(default_factory=list)
    related_dashboards: list[LineageAssetRefOut] = Field(default_factory=list)
    related_jobs: list[LineageJobSummaryOut] = Field(default_factory=list)
    lineage_origin: LineageOrigin = "manual"
    lineage_sources: list[str] = Field(default_factory=list)
    recent_runs: list[LineageJobRunOut] = Field(default_factory=list)
    impact: LineageImpactOut
    graph_nodes: list[LineageGraphNodeOut] = Field(default_factory=list)
    graph_edges: list[LineageGraphEdgeOut] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    graph_truncated: bool = False
    graph_limit: int | None = None


class LineageRelationAssetRefIn(BaseModel):
    asset_id: int | None = None
    catalog_table_id: int | None = None
    asset: LineageAssetCreate | None = None


class LineageRelationCreate(BaseModel):
    source: LineageRelationAssetRefIn
    target: LineageRelationAssetRefIn
    relation_type: LineageRelationType
    process_name: str | None = None
    process_type: str | None = None
    dashboard_name: str | None = None
    notes: str | None = None
    evidence: str | None = None
    discovery_method: str = "manual"
    confidence_score: int = 100
    is_verified: bool | None = None


class LineageRelationUpdate(BaseModel):
    source: LineageRelationAssetRefIn | None = None
    target: LineageRelationAssetRefIn | None = None
    relation_type: LineageRelationType | None = None
    process_name: str | None = None
    process_type: str | None = None
    dashboard_name: str | None = None
    notes: str | None = None
    evidence: str | None = None
    confidence_score: int | None = None
    is_verified: bool | None = None
    is_active: bool | None = None


class LineageRelationOut(BaseModel):
    id: int
    source_asset_id: int
    target_asset_id: int
    source_asset: LineageAssetRefOut
    target_asset: LineageAssetRefOut
    relation_type: str
    process_name: str | None = None
    process_type: str | None = None
    dashboard_name: str | None = None
    notes: str | None = None
    evidence: str | None = None
    discovery_method: str
    lineage_origin: LineageOrigin = "manual"
    lineage_source_name: str | None = None
    lineage_namespace: str | None = None
    lineage_job_name: str | None = None
    confidence_score: int
    confidence_tier: str | None = None
    is_verified: bool = False
    version: int = 1
    last_seen_at: datetime | None = None
    created_by_user_id: int | None = None
    updated_by_user_id: int | None = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class LineageOverviewOut(BaseModel):
    total_assets: int
    total_relations: int
    total_gold_tables_with_lineage: int
    total_dashboards_related: int
    automatic_relations: int = 0
    manual_relations: int = 0
    merged_assets: int = 0


class LineageRelationListOut(BaseModel):
    summary: LineageOverviewOut
    page: int = 1
    page_size: int = 200
    total: int = 0
    has_more: bool = False
    items: list[LineageRelationOut]


class LineageColumnEdgeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    lineage_source_id: int | None = None
    lineage_job_id: int | None = None
    source_asset: LineageAssetRefOut
    target_asset: LineageAssetRefOut
    source_asset_id: int
    target_asset_id: int
    relative_direction: Literal["upstream", "downstream"]
    local_asset_name: str
    related_asset_name: str
    local_asset_path: str | None = None
    related_asset_path: str | None = None
    local_column_name: str
    related_column_name: str
    source_column_name: str
    target_column_name: str
    relation_type: str
    discovery_method: str
    evidence_source: str | None = None
    evidence_label: str | None = None
    evidence: str | None = None
    confidence_score: int
    confidence_label: str | None = None
    confidence_tier: str | None = None
    is_verified: bool = False
    version: int = 1
    last_seen_at: datetime | None = None
    created_by_user_id: int | None = None
    updated_by_user_id: int | None = None
    transform_expression: str | None = None
    notes: str | None = None
    external_edge_key: str | None = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class LineageColumnEdgeCreate(BaseModel):
    lineage_source_id: int | None = None
    lineage_job_id: int | None = None
    source_asset_id: int
    target_asset_id: int
    source_column_name: str
    target_column_name: str
    relation_type: str = "transformation"
    discovery_method: str = "automatic"
    confidence_score: int = 100
    evidence_source: str | None = None
    evidence: str | None = None
    transform_expression: str | None = None
    notes: str | None = None
    external_edge_key: str | None = None
    is_verified: bool | None = None


class LineageColumnEdgeUpdate(BaseModel):
    lineage_source_id: int | None = None
    lineage_job_id: int | None = None
    source_asset_id: int | None = None
    target_asset_id: int | None = None
    source_column_name: str | None = None
    target_column_name: str | None = None
    relation_type: str | None = None
    discovery_method: str | None = None
    confidence_score: int | None = None
    evidence_source: str | None = None
    evidence: str | None = None
    transform_expression: str | None = None
    notes: str | None = None
    external_edge_key: str | None = None
    is_verified: bool | None = None
    is_active: bool | None = None


class LineageRelationVersionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    lineage_relation_id: int
    version_number: int
    source_asset_id: int
    target_asset_id: int
    relation_type: str
    process_name: str | None = None
    process_type: str | None = None
    dashboard_name: str | None = None
    notes: str | None = None
    evidence: str | None = None
    discovery_method: str
    confidence_score: int
    is_verified: bool
    last_seen_at: datetime | None = None
    external_edge_key: str | None = None
    is_active: bool
    created_by_user_id: int | None = None
    updated_by_user_id: int | None = None
    snapshot_json: str
    recorded_at: datetime
    recorded_by_user_id: int | None = None
    created_at: datetime
    updated_at: datetime


class LineageColumnEdgeVersionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    lineage_column_edge_id: int
    version_number: int
    lineage_source_id: int | None = None
    lineage_job_id: int | None = None
    source_asset_id: int
    target_asset_id: int
    source_column_name: str
    target_column_name: str
    relation_type: str
    discovery_method: str
    confidence_score: int
    evidence_source: str | None = None
    evidence: str | None = None
    transform_expression: str | None = None
    notes: str | None = None
    external_edge_key: str | None = None
    is_verified: bool
    last_seen_at: datetime | None = None
    is_active: bool
    created_by_user_id: int | None = None
    updated_by_user_id: int | None = None
    snapshot_json: str
    recorded_at: datetime
    recorded_by_user_id: int | None = None
    created_at: datetime
    updated_at: datetime


class LineageEventIn(BaseModel):
    source_id: int | None = None
    source_name: str | None = None
    payload: dict[str, Any]


class LineageEventBulkIn(BaseModel):
    events: list[LineageEventIn] = Field(default_factory=list)


class LineageEventIngestionOut(BaseModel):
    source_id: int
    source_name: str
    event_raw_id: int
    event_key: str
    event_type: str | None = None
    processed: bool = True
    jobs_synced: int = 0
    runs_synced: int = 0
    datasets_synced: int = 0
    relations_created: int = 0
    relations_updated: int = 0
    column_edges_created: int = 0
    matched_catalog_assets: int = 0
    unmatched_assets_created: int = 0
    warnings: list[str] = Field(default_factory=list)


class LineageEventsBulkOut(BaseModel):
    items: list[LineageEventIngestionOut] = Field(default_factory=list)
    processed: int = 0
    warnings: list[str] = Field(default_factory=list)


class LineageSyncCheckpointOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    lineage_source_id: int
    checkpoint_type: str
    last_event_raw_id: int | None = None
    last_processed_at: datetime | None = None
    last_status: str | None = None
    message: str | None = None
    cursor_value: str | None = None
    created_at: datetime
    updated_at: datetime


class LineageSourceConfigCreate(BaseModel):
    name: str
    source_type: Literal["openlineage"] = "openlineage"
    base_url: str
    default_namespace: str | None = None
    auth_type: Literal["none", "basic", "bearer"] = "none"
    auth_username: str | None = None
    auth_secret: str | None = None
    enabled: bool = True


class LineageSourceConfigUpdate(BaseModel):
    name: str | None = None
    base_url: str | None = None
    default_namespace: str | None = None
    auth_type: Literal["none", "basic", "bearer"] | None = None
    auth_username: str | None = None
    auth_secret: str | None = None
    enabled: bool | None = None


class LineageSourceConfigOut(BaseModel):
    id: int
    name: str
    source_type: str
    base_url: str
    default_namespace: str | None = None
    auth_type: str | None = None
    auth_username: str | None = None
    auth_secret: str | None = None
    configured_auth: bool = False
    enabled: bool
    last_sync_at: str | None = None
    last_sync_status: str | None = None
    last_sync_message: str | None = None
    created_at: datetime
    updated_at: datetime


class LineageSourceStatusOut(BaseModel):
    id: int
    name: str
    source_type: str
    enabled: bool
    last_sync_at: str | None = None
    last_sync_status: str | None = None
    last_sync_message: str | None = None
    events_processed: int = 0
    jobs_synced: int = 0
    datasets_synced: int = 0
    relations_synced: int = 0
    column_edges_synced: int = 0
    created_at: datetime
    updated_at: datetime


class LineageSourceSyncIn(BaseModel):
    namespace: str | None = None
    node_id: str | None = None
    depth: int = Field(default=1, ge=1, le=5)
    table_id: int | None = None


class LineageRebuildIn(BaseModel):
    source_id: int | None = None
    namespace: str | None = None
    node_id: str | None = None
    depth: int = Field(default=1, ge=1, le=5)
    table_id: int | None = None


class LineageSourceSyncOut(BaseModel):
    source: LineageSourceConfigOut
    namespace: str | None = None
    node_id: str | None = None
    depth: int
    datasets_synced: int
    jobs_synced: int
    runs_synced: int
    assets_created: int = 0
    assets_updated: int = 0
    relations_created: int
    relations_updated: int
    matched_catalog_assets: int = 0
    unmatched_assets_created: int = 0
    warnings: list[str] = Field(default_factory=list)


class LineageSpreadsheetIssueOut(BaseModel):
    sheet: str
    row_number: int
    message: str


class LineageImportPreviewSummaryOut(BaseModel):
    assets_found: int
    total_assets_identified: int
    assets_created: int
    total_new_assets: int
    assets_updated: int
    edges_found: int
    total_relations_identified: int
    edges_created: int
    total_new_relations: int
    edges_updated: int
    total_updated_relations: int
    ignored_rows: int
    warnings_count: int
    errors_count: int


class LineageImportPreviewOut(BaseModel):
    mode: str
    summary: LineageImportPreviewSummaryOut
    assets_preview: list[dict[str, Any]] = Field(default_factory=list)
    relations_preview: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[LineageSpreadsheetIssueOut] = Field(default_factory=list)
    errors: list[LineageSpreadsheetIssueOut] = Field(default_factory=list)


class LineageImportCommitOut(BaseModel):
    mode: str
    assets_found: int
    processed_assets: int
    assets_created: int
    created_assets: int
    assets_updated: int
    updated_assets: int
    edges_found: int
    processed_relations: int
    edges_created: int
    created_relations: int
    edges_updated: int
    updated_relations: int
    created_dashboards: int
    warnings: list[LineageSpreadsheetIssueOut] = Field(default_factory=list)
    errors: list[LineageSpreadsheetIssueOut] = Field(default_factory=list)
