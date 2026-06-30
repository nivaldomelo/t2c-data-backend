from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from t2c_data.schemas.data_owner import DataOwnerRefOut


class PrivacyAccessOptionsOut(BaseModel):
    sensitivity_levels: list[dict[str, str]]
    legal_basis_options: list[dict[str, str]]
    access_scopes: list[dict[str, str]]
    access_roles: list[dict[str, str]]


class PrivacySummaryOut(BaseModel):
    sensitivity_level: str | None = None
    sensitivity_label: str
    has_personal_data: bool
    has_sensitive_personal_data: bool
    legal_basis: str | None = None
    legal_basis_label: str | None = None
    privacy_purpose: str | None = None
    retention_policy: str | None = None
    is_masked: bool
    external_sharing: bool
    access_scope: str
    access_scope_label: str
    access_roles: list[str] = Field(default_factory=list)
    access_role_labels: list[str] = Field(default_factory=list)
    privacy_notes: str | None = None
    privacy_reviewed_by_user_id: int | None = None
    privacy_reviewed_by_user_name: str | None = None
    privacy_reviewed_by_user_email: str | None = None
    privacy_reviewed_at: datetime | None = None
    possible_personal_data: bool = False


class PrivacyTableListItemOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    table_type: str
    schema_name: str
    database_name: str
    datasource_name: str
    engine: str
    owner: str | None = None
    owner_email: str | None = None
    data_owner: DataOwnerRefOut | None = None
    privacy: PrivacySummaryOut
    updated_at: datetime


class PrivacyTableDetailOut(PrivacyTableListItemOut):
    description_manual: str | None = None
    description_source: str | None = None
    lifecycle_status: str | None = None
    certification_status: str
    certification_criticality: str | None = None
    certification_badges: list[str] | None = None
    suspected_columns: list["PrivacySuspectedColumnOut"] = Field(default_factory=list)


class PrivacySuspectedColumnOut(BaseModel):
    column_name: str
    data_type: str
    signal: str
    reason: str
    suggested_classification: str
    confidence: str


class PrivacySummaryTotalsOut(BaseModel):
    visible_assets: int = 0
    classified_assets: int = 0
    unclassified_assets: int = 0
    confirmed_personal_data: int = 0
    confirmed_sensitive_data: int = 0
    restricted_assets: int = 0
    possible_personal_data: int = 0
    without_legal_basis: int = 0
    wide_access_with_suspicion: int = 0
    without_owner: int = 0
    without_review: int = 0


class PrivacyRiskBucketsOut(BaseModel):
    critical: int = 0
    high: int = 0
    medium: int = 0
    low: int = 0


class PrivacyTopBlockerOut(BaseModel):
    key: str
    label: str
    count: int
    percent: float
    description: str
    action: str


class PrivacyBySchemaOut(BaseModel):
    database: str
    schema_name: str
    total: int
    unclassified: int
    possible_personal_data: int
    confirmed_personal_data: int
    sensitive_data: int
    restricted: int
    wide_access_with_suspicion: int
    without_legal_basis: int
    risk_score: int


class PrivacyPriorityOut(BaseModel):
    asset_id: int
    asset_name: str
    database_name: str
    schema_name: str
    risk_level: str
    reason: str
    recommended_action: str


class PrivacySummaryOutPage(BaseModel):
    totals: PrivacySummaryTotalsOut
    risk: PrivacyRiskBucketsOut
    top_blockers: list[PrivacyTopBlockerOut] = Field(default_factory=list)
    by_schema: list[PrivacyBySchemaOut] = Field(default_factory=list)
    priorities: list[PrivacyPriorityOut] = Field(default_factory=list)


class PrivacyAccessPatch(BaseModel):
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


class PrivacyReviewChangedFieldOut(BaseModel):
    field: str
    previous: object | None = None
    new: object | None = None


class PrivacyReviewEventOut(BaseModel):
    id: int
    table_id: int
    table_name: str
    schema_name: str
    database_name: str
    review_type: str
    review_source: str
    reviewer_user_id: int | None = None
    reviewer_name: str | None = None
    reviewer_email: str | None = None
    notes: str | None = None
    risk_before: str | None = None
    risk_after: str | None = None
    next_review_at: datetime | None = None
    created_at: datetime
    changed_fields: list[PrivacyReviewChangedFieldOut] = Field(default_factory=list)


class PrivacyReviewEventPageOut(BaseModel):
    items: list[PrivacyReviewEventOut] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 25


class PrivacyReviewDueOut(BaseModel):
    overdue: int = 0
    due_30_days: int = 0
    due_60_days: int = 0
    without_next_review: int = 0
    sensitive_without_next_review: int = 0


class PrivacyReviewEventSummaryOut(BaseModel):
    total_events: int = 0
    by_type: dict[str, int] = Field(default_factory=dict)
    by_reviewer: dict[str, int] = Field(default_factory=dict)
    by_schema: dict[str, int] = Field(default_factory=dict)
    increased_risk: int = 0
    reduced_risk: int = 0
    unchanged_risk: int = 0
    periodic_reviews: int = 0
    access_changes: int = 0
    legal_basis_changes: int = 0
    purpose_changes: int = 0
    assets_with_review_due: int = 0
    upcoming_review_due: int = 0
    due_60_days: int = 0
    without_next_review: int = 0
    sensitive_without_next_review: int = 0
    current_risk_critical: int = 0
    current_risk_high: int = 0
    review_due: PrivacyReviewDueOut = Field(default_factory=PrivacyReviewDueOut)
    recent_events: list[PrivacyReviewEventOut] = Field(default_factory=list)


class PrivacyPeriodicReviewIn(BaseModel):
    notes: str | None = None
    next_review_at: datetime | None = None
    confirmed: bool = False
