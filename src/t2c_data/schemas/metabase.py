from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


MetabaseObjectType = Literal["dashboard", "question", "collection"]
MetabaseImpactAssetType = Literal["dashboard", "question", "collection", "model"]
MetabaseImpactDependencyType = Literal["direct", "sql_native", "indirect", "dashboard_card", "collection_membership", "unknown"]
MetabaseImpactRiskLevel = Literal["none", "low", "medium", "high"]
MetabaseImpactConfidenceLevel = Literal["low", "medium", "high"]


class MetabaseInstanceBase(BaseModel):
    name: str
    base_url: str
    auth_type: str | None = None
    auth_username: str | None = None
    auth_secret: str | None = None
    timeout_seconds: int = 10
    sync_dashboards: bool = True
    sync_questions: bool = True
    sync_collections: bool = True
    enabled: bool = True


class MetabaseInstanceCreate(MetabaseInstanceBase):
    pass


class MetabaseInstanceUpdate(BaseModel):
    name: str | None = None
    base_url: str | None = None
    auth_type: str | None = None
    auth_username: str | None = None
    auth_secret: str | None = None
    timeout_seconds: int | None = None
    sync_dashboards: bool | None = None
    sync_questions: bool | None = None
    sync_collections: bool | None = None
    enabled: bool | None = None


class MetabaseInstanceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    base_url: str
    auth_type: str | None = None
    auth_username: str | None = None
    auth_secret_configured: bool = False
    timeout_seconds: int
    sync_dashboards: bool
    sync_questions: bool
    sync_collections: bool
    enabled: bool
    last_sync_at: datetime | None = None
    last_sync_status: str | None = None
    last_sync_message: str | None = None
    last_sync_dashboards: int = 0
    last_sync_questions: int = 0
    last_sync_collections: int = 0
    last_sync_links: int = 0
    last_sync_unresolved: int = 0
    last_sync_warnings: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


class MetabaseSyncRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    instance_id: int
    instance_name: str | None = None
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: int | None = None
    dashboards_count: int = 0
    questions_count: int = 0
    collections_count: int = 0
    links_count: int = 0
    artifacts_processed: int = 0
    links_created: int = 0
    unresolved_count: int = 0
    warnings_count: int = 0
    error_message: str | None = None
    error_type: str | None = None
    summary: dict | list | None = Field(default=None, alias="summary_json")
    created_at: datetime | None = None
    updated_at: datetime | None = None


class MetabaseConsumptionItemOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    object_id: int
    external_id: str
    object_type: MetabaseObjectType
    title: str
    description: str | None = None
    url: str | None = None
    collection_name: str | None = None
    collection_external_id: str | None = None
    confidence_level: str
    confidence_reason: str | None = None
    match_method: str
    match_state: str | None = None
    link_count: int = 1
    source_table_name: str | None = None
    source_schema_name: str | None = None
    source_database_name: str | None = None
    source_column_name: str | None = None


class MetabaseConsumptionSummaryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    table_id: int
    table_fqn: str
    available: bool
    configured: bool = False
    enabled: bool = True
    instance_id: int | None = None
    instance_name: str | None = None
    instance_base_url: str | None = None
    message: str | None = None
    last_sync_at: datetime | None = None
    last_sync_status: str | None = None
    last_sync_message: str | None = None
    dashboards_count: int = 0
    questions_count: int = 0
    collections_count: int = 0
    confirmed_count: int = 0
    inferred_count: int = 0
    partial_count: int = 0
    direct_count: int = 0
    indirect_count: int = 0
    match_state: str | None = None
    unresolved_count: int = 0
    dashboards: list[MetabaseConsumptionItemOut] = Field(default_factory=list)
    questions: list[MetabaseConsumptionItemOut] = Field(default_factory=list)
    collections: list[MetabaseConsumptionItemOut] = Field(default_factory=list)


class MetabaseImpactDependencyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    metabase_asset_id: int
    metabase_id: str
    asset_type: MetabaseImpactAssetType
    name: str
    collection_name: str | None = None
    url: str | None = None
    dependency_type: MetabaseImpactDependencyType
    confidence_level: MetabaseImpactConfidenceLevel
    break_risk_on_drop: MetabaseImpactRiskLevel
    break_risk_on_change: MetabaseImpactRiskLevel
    last_verified_at: datetime | None = None
    details_json: dict | list | None = Field(default=None)


class MetabaseImpactSummaryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    table_id: int
    table_fqn: str
    available: bool
    configured: bool = False
    enabled: bool = True
    instance_id: int | None = None
    instance_name: str | None = None
    instance_base_url: str | None = None
    message: str | None = None
    last_verified_at: datetime | None = None
    dashboard_count: int = 0
    question_count: int = 0
    model_count: int = 0
    asset_count: int = 0
    break_risk_on_drop: MetabaseImpactRiskLevel = "none"
    break_risk_on_change: MetabaseImpactRiskLevel = "none"
    dependencies: list[MetabaseImpactDependencyOut] = Field(default_factory=list)
