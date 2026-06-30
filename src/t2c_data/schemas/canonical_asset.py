from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from t2c_data.schemas.asset_context import AssetLinksOut
from t2c_data.schemas.glossary import GlossaryTermOut
from t2c_data.schemas.ingestion import (
    IngestionPipelineRefOut,
    IngestionStabilityHistoryPointOut,
    IngestionStabilitySummaryOut,
)
from t2c_data.schemas.lineage import LineageAssetSummaryOut
from t2c_data.schemas.tag import TagOut


class CanonicalAssetSourceOut(BaseModel):
    datasource_id: int
    datasource_name: str
    database_id: int | None = None
    database_name: str | None = None
    schema_id: int
    schema_name: str
    table_type: str
    engine: str


class CanonicalAssetOwnerOut(BaseModel):
    data_owner_id: int | None = None
    owner_name: str | None = None
    owner_email: str | None = None
    owner_defined: bool = False


class CanonicalAssetClassificationOut(BaseModel):
    certification_status: str
    certification_status_label: str
    certification_criticality: str | None = None
    certification_badges: list[str] = Field(default_factory=list)
    sensitivity_level: str | None = None
    sensitivity_label: str
    classification_defined: bool = False
    has_personal_data: bool = False
    has_sensitive_personal_data: bool = False
    tags_count: int = 0
    terms_count: int = 0
    readiness_score: int = 0
    governance_score: int | None = None
    governance_label: str | None = None
    governance_tone: str | None = None
    trust_score: int | None = None
    trust_label: str | None = None
    trust_tone: str | None = None
    total_columns: int = 0
    classified_columns: int = 0
    personal_classified_columns: int = 0
    sensitive_classified_columns: int = 0
    financial_classified_columns: int = 0
    operational_classified_columns: int = 0
    classification_coverage_pct: float = 0.0
    column_classification_reviewed_at: datetime | None = None


class CanonicalAssetEvidenceOut(BaseModel):
    description_complete: bool = False
    dictionary_complete: bool = False
    dq_score: float | None = None
    completeness_pct_avg: float | None = None
    freshness_seconds: int | None = None
    open_incidents: int = 0
    critical_open_incidents: int = 0
    active_dq_violation: bool = False
    active_dq_rule_names: list[str] = Field(default_factory=list)
    last_review_at: datetime | None = None
    last_sync_at: datetime | None = None
    last_updated_at: datetime | None = None
    trust_summary: str | None = None


class CanonicalGovernanceEventOut(BaseModel):
    id: str
    event_type: str
    category: str
    label: str
    detail: str | None = None
    source: str
    actor_name: str | None = None
    actor_email: str | None = None
    created_at: datetime


class CanonicalAssetColumnPreviewOut(BaseModel):
    id: int
    name: str
    data_type: str
    ordinal_position: int
    is_nullable: bool
    is_primary_key: bool
    description_complete: bool
    classification_taxonomy_key: str | None = None
    classification_taxonomy_label: str | None = None
    classification_taxonomy_group: str | None = None
    classification_review_status: str | None = None
    classification_confidence_score: int | None = None
    classification_is_personal_data: bool = False
    classification_is_sensitive_data: bool = False
    classification_is_financial_data: bool = False
    classification_is_operational_data: bool = False
    tags: list[TagOut] = Field(default_factory=list)


class CanonicalPipelineOut(BaseModel):
    linked: bool
    state: str
    message: str | None = None
    table_schema: str
    table_name: str
    pipeline_count: int = 0
    primary_pipeline: IngestionPipelineRefOut | None = None
    pipelines: list[IngestionPipelineRefOut] = Field(default_factory=list)
    stability: IngestionStabilitySummaryOut | None = None
    history: list[IngestionStabilityHistoryPointOut] = Field(default_factory=list)


class CanonicalAssetOut(BaseModel):
    entity_kind: Literal["table", "column"]
    table_id: int
    table_name: str
    table_fqn: str
    table_type: str
    column_id: int | None = None
    column_name: str | None = None
    column_data_type: str | None = None
    column_ordinal_position: int | None = None
    asset_key: str
    display_name: str
    source: CanonicalAssetSourceOut
    owner: CanonicalAssetOwnerOut
    classification: CanonicalAssetClassificationOut
    evidence: CanonicalAssetEvidenceOut
    lineage: LineageAssetSummaryOut | None = None
    tags: list[TagOut] = Field(default_factory=list)
    terms: list[GlossaryTermOut] = Field(default_factory=list)
    columns: list[CanonicalAssetColumnPreviewOut] = Field(default_factory=list)
    recent_events: list[CanonicalGovernanceEventOut] = Field(default_factory=list)
    pipeline: CanonicalPipelineOut | None = None
    links: AssetLinksOut
    generated_at: datetime
