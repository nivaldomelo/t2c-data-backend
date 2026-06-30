from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TagBase(BaseModel):
    external_id: str | None = Field(default=None, max_length=40)
    slug: str = Field(min_length=2, max_length=160)
    name: str = Field(min_length=2, max_length=120)
    color: str | None = Field(default=None, max_length=20)
    description: str | None = None
    group_name: str | None = Field(default=None, max_length=120)
    subgroup_name: str | None = Field(default=None, max_length=120)
    example_of_use: str | None = None
    tag_type: str | None = Field(default=None, max_length=120)
    suggested_scope: str | None = Field(default=None, max_length=160)
    status: str = Field(default="active", max_length=30)
    synonyms: str | None = None
    notes: str | None = None


class TagCreate(TagBase):
    pass


class TagUpdate(BaseModel):
    external_id: str | None = Field(default=None, max_length=40)
    slug: str | None = Field(default=None, min_length=2, max_length=160)
    name: str | None = Field(default=None, min_length=2, max_length=120)
    color: str | None = Field(default=None, max_length=20)
    description: str | None = None
    group_name: str | None = Field(default=None, max_length=120)
    subgroup_name: str | None = Field(default=None, max_length=120)
    example_of_use: str | None = None
    tag_type: str | None = Field(default=None, max_length=120)
    suggested_scope: str | None = Field(default=None, max_length=160)
    status: str | None = Field(default=None, max_length=30)
    synonyms: str | None = None
    notes: str | None = None


class TagLinkedTablePreview(BaseModel):
    id: int
    name: str
    schema_name: str
    database_name: str
    datasource_name: str
    description: str | None = None


class TagOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    external_id: str | None
    slug: str
    name: str
    color: str | None
    description: str | None
    group_name: str | None
    subgroup_name: str | None
    example_of_use: str | None
    tag_type: str | None
    suggested_scope: str | None
    status: str
    synonyms: str | None
    notes: str | None
    tables_count: int = 0
    columns_count: int = 0
    confidence_score: int | None = None
    inference_source: str | None = None
    inference_reason: str | None = None
    evidence: dict | None = None
    applied_automatically: bool | None = None
    review_status: str | None = None
    rule_key: str | None = None
    rule_label: str | None = None
    assignment_id: int | None = None
    assigned_entity_type: str | None = None
    assigned_entity_id: int | None = None
    assigned_scope: str | None = None
    reviewed_by_user_id: int | None = None
    reviewed_at: datetime | None = None
    linked_tables_preview: list[TagLinkedTablePreview] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class TagDetailOut(TagOut):
    linked_tables: list[TagLinkedTablePreview] = Field(default_factory=list)


class TagListFiltersOut(BaseModel):
    groups: list[str] = Field(default_factory=list)
    subgroups: list[str] = Field(default_factory=list)
    statuses: list[str] = Field(default_factory=list)
    tag_types: list[str] = Field(default_factory=list)


class TagSummaryOut(BaseModel):
    total: int
    active: int
    in_use: int
    groups: int


class TagAssignRequest(BaseModel):
    tag_id: int
    entity_type: str
    entity_id: int


class TagAssignmentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: int
    tag_id: int
    entity_type: str
    entity_id: int
    confidence_score: int = 100
    inference_source: str | None = None
    inference_reason: str | None = None
    evidence: dict | None = Field(default=None, alias="evidence_json")
    applied_automatically: bool = False
    review_status: str = "manual_applied"
    rule_key: str | None = None
    rule_label: str | None = None
    reviewed_by_user_id: int | None = None
    reviewed_at: datetime | None = None
    created_at: datetime


class TagIntelligenceReprocessOut(BaseModel):
    table_id: int
    current_columns: int
    column_tags_applied: int
    table_tags_applied: int
    suggestions_created: int
    assignments_updated: int
    assignments_removed: int
    blocked_assignments: int
    manual_assignments_preserved: int


class TagIntelligenceReprocessByFqnIn(BaseModel):
    datasource_name: str | None = None
    database_name: str
    schema_name: str
    table_name: str


class TagIntelligenceReprocessBatchIn(BaseModel):
    datasource_id: int | None = None
    database_name: str | None = None
    schema_name: str | None = None
    limit: int = 200


class TagIntelligenceReprocessBatchOut(BaseModel):
    total: int
    processed: int
    table_ids: list[int] = Field(default_factory=list)


class TagAutomationRuleBase(BaseModel):
    tag_id: int
    name: str
    scope: str = "column"
    status: str = "active"
    action: str = "apply"
    category: str | None = None
    priority: int = 100
    match_fields: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    regex_pattern: str | None = None
    min_confidence: int = 90
    notes: str | None = None


class TagAutomationRuleCreate(TagAutomationRuleBase):
    pass


class TagAutomationRuleUpdate(BaseModel):
    name: str | None = None
    status: str | None = None
    action: str | None = None
    category: str | None = None
    priority: int | None = None
    match_fields: list[str] | None = None
    keywords: list[str] | None = None
    aliases: list[str] | None = None
    regex_pattern: str | None = None
    min_confidence: int | None = None
    notes: str | None = None


class TagAutomationRuleOut(TagAutomationRuleBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    tag_name: str | None = None
    tag_slug: str | None = None
    created_at: datetime
    updated_at: datetime


class TagIntelligenceEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    tag_id: int
    tag_name: str
    tag_slug: str
    entity_type: str
    entity_id: int
    datasource_id: int | None = None
    datasource_name: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    table_id: int | None = None
    table_name: str | None = None
    column_id: int | None = None
    column_name: str | None = None
    table_fqn: str | None = None
    rule_key: str | None = None
    rule_label: str | None = None
    inference_source: str | None = None
    inference_reason: str | None = None
    confidence_score: int = 100
    applied_automatically: bool = False
    review_status: str = "suggested"
    evidence: dict | None = None
    explorer_url: str | None = None
    created_by_user_id: int | None = None
    reviewed_by_user_id: int | None = None
    reviewed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class TagSpreadsheetImportError(BaseModel):
    row_number: int
    slug: str | None = None
    message: str


class TagSpreadsheetImportResult(BaseModel):
    processed: int
    imported: int
    updated: int
    rejected: int
    errors: list[TagSpreadsheetImportError] = Field(default_factory=list)


class TagResetOut(BaseModel):
    deleted_tags: int
    deleted_assignments: int
    deleted_overrides: int = 0
    deleted_events: int = 0


class TagIntelligenceBatchActionIn(BaseModel):
    event_ids: list[int] = Field(default_factory=list, min_length=1, max_length=200)


class TagIntelligenceBatchActionError(BaseModel):
    event_id: int
    message: str


class TagIntelligenceBatchActionOut(BaseModel):
    requested: int
    succeeded: int
    failed: int
    applied_ids: list[int] = Field(default_factory=list)
    failed_items: list[TagIntelligenceBatchActionError] = Field(default_factory=list)
