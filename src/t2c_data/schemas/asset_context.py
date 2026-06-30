from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class AssetLinksOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    explorer: str
    change_management: str
    lineage: str
    data_quality: str
    incidents: str
    audit: str
    certification: str
    owners: str
    privacy: str
    datasource: str
    database: str
    schema_name: str = Field(alias="schema")
    metabase_consumption: str


class AssetContextualActionOut(BaseModel):
    key: str
    label: str
    description: str
    href: str
    category: str
    tone: str = "neutral"


class AssetOperationalContextOut(BaseModel):
    table_id: int
    table_name: str
    table_fqn: str
    datasource_id: int
    datasource_name: str
    database_id: int | None = None
    database_name: str
    schema_id: int
    schema_name: str
    owner_name: str
    owner_defined: bool
    data_owner_id: int | None = None
    criticality_score: int
    criticality_label: str
    criticality_tone: str
    dq_score: float | None = None
    dq_status_label: str
    certification_status: str
    certification_status_label: str
    dictionary_complete: bool
    description_complete: bool
    tags_count: int
    terms_count: int
    open_incidents: int
    critical_open_incidents: int
    eligible_for_certification: bool
    sensitivity_level: str | None = None
    sensitivity_label: str
    owner_review_due: bool = False
    owner_review_next_at: datetime | None = None
    privacy_review_due: bool = False
    privacy_review_next_at: datetime | None = None
    certification_review_due: bool = False
    certification_next_review_at: datetime | None = None
    review_due_label: str | None = None
    last_review_at: datetime | None = None
    last_updated_at: datetime | None = None
    last_sync_at: datetime | None = None
    recommended_actions: list[str] = Field(default_factory=list)
    actions: list[AssetContextualActionOut] = Field(default_factory=list)
    links: AssetLinksOut


class DQIncidentSuggestionOut(BaseModel):
    key: str
    mode: str
    title: str
    detail: str
    severity: str
    severity_label: str
    trigger_code: str
    existing_incident_id: int | None = None
    source_type: str = "dq_profile"


class DQIncidentSignalsOut(BaseModel):
    table_id: int
    generated_incident_id: int | None = None
    generated_mode: str | None = None
    open_incidents: int = 0
    suggestions: list[DQIncidentSuggestionOut] = Field(default_factory=list)
    links: AssetLinksOut
