from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class DataOwnerBase(BaseModel):
    name: str
    email: EmailStr
    area: str | None = None
    description: str | None = None
    is_active: bool = True


class DataOwnerCreate(DataOwnerBase):
    pass


class DataOwnerUpdate(BaseModel):
    name: str | None = None
    email: EmailStr | None = None
    area: str | None = None
    description: str | None = None
    is_active: bool | None = None


class DataOwnerRefOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    email: str
    area: str | None
    is_active: bool


class DataOwnerTablePreviewOut(BaseModel):
    id: int
    name: str
    schema_name: str
    database_name: str
    datasource_name: str
    description: str | None


class DataOwnerOut(DataOwnerRefOut):
    description: str | None
    created_at: datetime
    updated_at: datetime


class DataOwnerListItemOut(DataOwnerOut):
    tables_count: int
    tables_preview: list[DataOwnerTablePreviewOut]


class DataOwnerDetailOut(DataOwnerOut):
    tables_count: int
    tables: list[DataOwnerTablePreviewOut]


class OwnershipTotalsOut(BaseModel):
    owners: int = 0
    active_owners: int = 0
    inactive_owners: int = 0
    owners_with_assets: int = 0
    owners_without_assets: int = 0
    assets_with_owner: int = 0
    assets_without_owner: int = 0
    critical_assets_without_owner: int = 0
    personal_data_assets_without_owner: int = 0
    certification_pending_assets: int = 0
    privacy_pending_assets: int = 0
    dq_unmonitored_assets: int = 0
    assets_with_open_incidents: int = 0


class OwnershipOwnerSummaryOut(BaseModel):
    id: int
    name: str
    email: str
    area: str | None = None
    status: str
    updated_at: datetime
    asset_count: int = 0
    certified_assets: int = 0
    certification_pending_assets: int = 0
    eligible_assets: int = 0
    not_eligible_assets: int = 0
    in_review_assets: int = 0
    rejected_assets: int = 0
    revalidation_pending_assets: int = 0
    dq_monitored_assets: int = 0
    dq_unmonitored_assets: int = 0
    open_incidents: int = 0
    critical_incidents: int = 0
    assets_with_open_incidents: int = 0
    privacy_pending_assets: int = 0
    personal_data_assets: int = 0
    sensitive_data_assets: int = 0
    restricted_assets: int = 0
    possible_personal_data_assets: int = 0
    assets_without_legal_basis: int = 0
    assets_without_privacy_review: int = 0
    assets_without_description: int = 0
    assets_without_tags: int = 0
    assets_without_terms: int = 0
    assets_without_sla: int = 0
    average_quality_score: float | None = None
    average_governance_score: float | None = None
    average_readiness_score: float | None = None
    risk_level: str = "low"
    main_blocker: str | None = None
    recommended_action: str | None = None


class OwnershipUnownedAssetOut(BaseModel):
    id: int
    name: str
    database_name: str
    schema_name: str
    connection_name: str
    criticality: str | None = None
    certification_status: str
    privacy_signal: str | None = None
    open_incidents: int = 0
    dq_score: float | None = None
    updated_at: datetime | None = None
    recommended_action: str = "Atribuir owner"


class OwnershipPriorityOut(BaseModel):
    type: str
    severity: str
    title: str
    description: str
    owner_id: int | None = None
    asset_id: int | None = None
    recommended_action: str


class OwnershipAreaDistributionOut(BaseModel):
    area: str
    owners: int = 0
    active_owners: int = 0
    assets: int = 0


class OwnershipAssetDistributionOut(BaseModel):
    database_name: str | None = None
    schema_name: str | None = None
    total_assets: int = 0
    assets_with_owner: int = 0
    assets_without_owner: int = 0
    privacy_pending_assets: int = 0
    certification_pending_assets: int = 0


class OwnershipDistributionOut(BaseModel):
    by_area: list[OwnershipAreaDistributionOut]
    by_schema: list[OwnershipAssetDistributionOut]
    by_database: list[OwnershipAssetDistributionOut]


class OwnershipRankingItemOut(BaseModel):
    owner_id: int
    name: str
    area: str | None = None
    status: str
    metric_value: int
    risk_level: str


class OwnershipRankingsOut(BaseModel):
    most_assets: list[OwnershipRankingItemOut]
    most_certification_pending: list[OwnershipRankingItemOut]
    most_privacy_pending: list[OwnershipRankingItemOut]
    most_incidents: list[OwnershipRankingItemOut]
    most_dq_unmonitored: list[OwnershipRankingItemOut]
    inactive_with_assets: list[OwnershipRankingItemOut]


class OwnershipSummaryOut(BaseModel):
    totals: OwnershipTotalsOut
    owners_total: int
    page: int
    page_size: int
    total_pages: int
    owners: list[OwnershipOwnerSummaryOut]
    unowned_assets: list[OwnershipUnownedAssetOut]
    priorities: list[OwnershipPriorityOut]
    distribution: OwnershipDistributionOut
    rankings: OwnershipRankingsOut


class OwnershipDeleteImpactOwnerOut(BaseModel):
    id: int
    name: str
    email: str
    area: str | None = None


class OwnershipDeleteImpactMetricsOut(BaseModel):
    asset_count: int = 0
    certified_assets: int = 0
    critical_assets: int = 0
    personal_data_assets: int = 0
    sensitive_data_assets: int = 0
    restricted_assets: int = 0
    open_incidents: int = 0
    certification_pending_assets: int = 0
    privacy_pending_assets: int = 0
    dq_unmonitored_assets: int = 0


class OwnershipDeleteImpactAssetOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    id: int
    name: str
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    risk: str | None = None
    reason: str


class OwnershipDeleteImpactOut(BaseModel):
    owner: OwnershipDeleteImpactOwnerOut
    impact: OwnershipDeleteImpactMetricsOut
    sample_assets: list[OwnershipDeleteImpactAssetOut] = Field(default_factory=list)
    can_delete_without_force: bool
    warning_message: str


class OwnershipReassignOwnerOut(BaseModel):
    id: int
    name: str
    email: str
    area: str | None = None


class OwnershipReassignAssetOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    id: int
    name: str
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    criticality: str | None = None
    certification_status: str
    privacy_signal: str | None = None
    has_personal_data: bool = False
    has_sensitive_personal_data: bool = False
    dq_monitored: bool = False
    privacy_pending: bool = False
    open_incidents: int = 0
    recommended_action: str


class OwnershipReassignImpactOut(BaseModel):
    asset_count: int = 0
    certified_assets: int = 0
    critical_assets: int = 0
    personal_data_assets: int = 0
    sensitive_data_assets: int = 0
    open_incidents: int = 0
    certification_pending_assets: int = 0
    privacy_pending_assets: int = 0
    dq_unmonitored_assets: int = 0


class OwnershipReassignPreviewOut(BaseModel):
    source_owner: OwnershipReassignOwnerOut
    target_owner: OwnershipReassignOwnerOut | None = None
    impact: OwnershipReassignImpactOut
    assets: list[OwnershipReassignAssetOut] = Field(default_factory=list)
    page: int = 1
    page_size: int = 100
    total_assets: int = 0


class OwnershipReassignRequestIn(BaseModel):
    target_owner_id: int = Field(ge=1)
    asset_ids: list[int] = Field(default_factory=list)
    mode: Literal["selected", "all"] = "selected"
    note: str | None = None


class OwnershipReassignResultOut(BaseModel):
    reassigned_count: int = 0
    source_owner_id: int
    target_owner_id: int
    assets: list[OwnershipReassignAssetOut] = Field(default_factory=list)
    note: str | None = None
