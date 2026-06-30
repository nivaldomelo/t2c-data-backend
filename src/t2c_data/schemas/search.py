from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class SearchOption(BaseModel):
    value: str
    label: str


class SearchBadge(BaseModel):
    label: str
    tone: str


class SearchResultMetadata(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    source: str | None = None
    datasource_id: int | None = None
    database: str | None = None
    database_id: int | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    schema_id: int | None = None
    owner: str | None = None
    domain: str | None = None
    classification: str | None = None
    table_name: str | None = None
    table_id: int | None = None
    table_fqn: str | None = None
    data_type: str | None = None
    category: str | None = None
    status: str | None = None
    group_name: str | None = None
    tag_type: str | None = None
    assignments: int | None = None
    area: str | None = None
    assets_count: int | None = None
    db_type: str | None = None
    incidents_target_url: str | None = None
    dq_target_url: str | None = None
    alias_count: int | None = None
    popularity_count: int | None = None
    governance_score: int | None = None
    governance_label: str | None = None
    governance_tone: str | None = None
    certification_status: str | None = None
    readiness_score: int | None = None
    active_dq_violation: bool | None = None
    owner_defined: bool | None = None


class SearchResultItem(BaseModel):
    entity_type: str
    entity_id: int
    category: str
    title: str
    subtitle: str | None = None
    description: str | None = None
    context_path: str | None = None
    match_reason: str
    relevance_score: int
    target_url: str
    badges: list[SearchBadge] = Field(default_factory=list)
    metadata: SearchResultMetadata = Field(default_factory=SearchResultMetadata)


class SearchGroup(BaseModel):
    key: str
    label: str
    total: int
    items: list[SearchResultItem] = Field(default_factory=list)


class SearchAvailableFilters(BaseModel):
    types: list[SearchOption] = Field(default_factory=list)
    sources: list[SearchOption] = Field(default_factory=list)
    databases: list[SearchOption] = Field(default_factory=list)
    schemas: list[SearchOption] = Field(default_factory=list)
    domains: list[SearchOption] = Field(default_factory=list)
    owners: list[SearchOption] = Field(default_factory=list)
    classifications: list[SearchOption] = Field(default_factory=list)
    certification: list[SearchOption] = Field(default_factory=list)
    incidents: list[SearchOption] = Field(default_factory=list)
    governance_maturity: list[SearchOption] = Field(default_factory=list)


class SearchAppliedFilters(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    result_type: str | None = None
    source: str | None = None
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    domain: str | None = None
    owner: str | None = None
    classification: str | None = None
    certification: str | None = None
    incidents: str | None = None
    governance_maturity: str | None = None


class SearchResultsResponse(BaseModel):
    query: str
    total: int
    groups: list[SearchGroup] = Field(default_factory=list)
    items: list[SearchResultItem] = Field(default_factory=list)
    available_filters: SearchAvailableFilters = Field(default_factory=SearchAvailableFilters)
    applied_filters: SearchAppliedFilters = Field(default_factory=SearchAppliedFilters)
    took_ms: int = 0
    min_query_length: int = 2


class SearchSuggestionsResponse(BaseModel):
    query: str
    groups: list[SearchGroup] = Field(default_factory=list)
    took_ms: int = 0
    min_query_length: int = 2


class SearchCollectionItem(BaseModel):
    label: str
    target_url: str | None = None
    entity_type: str | None = None
    entity_id: int | None = None
    category: str | None = None
    subtitle: str | None = None
    context_path: str | None = None
    description: str | None = None
    count: int | None = None


class SearchCollectionResponse(BaseModel):
    enabled: bool
    items: list[SearchCollectionItem] = Field(default_factory=list)


class SearchFavoriteAssetIn(BaseModel):
    entity_type: str = Field(min_length=1, max_length=40)
    entity_id: int
    label: str = Field(min_length=1, max_length=255)
    target_url: str | None = Field(default=None, max_length=500)
    category: str | None = Field(default=None, max_length=80)
    subtitle: str | None = Field(default=None, max_length=255)
    context_path: str | None = Field(default=None, max_length=500)
    metadata: dict | list | None = None


class SearchFavoriteStatusOut(BaseModel):
    favorite: bool


class SearchTrackQueryIn(BaseModel):
    query: str


class SearchTrackClickIn(BaseModel):
    entity_type: str
    entity_id: int
    query: str | None = None
    target_url: str | None = None


class SearchTrackOut(BaseModel):
    ok: bool = True


class SearchAliasFiltersOut(BaseModel):
    datasources: list[SearchOption] = Field(default_factory=list)
    databases: list[SearchOption] = Field(default_factory=list)
    schemas: list[SearchOption] = Field(default_factory=list)
    tables: list[SearchOption] = Field(default_factory=list)
    columns: list[SearchOption] = Field(default_factory=list)
    label_kinds: list[SearchOption] = Field(default_factory=list)
    entity_types: list[SearchOption] = Field(default_factory=list)


class SearchAliasItemOut(BaseModel):
    id: int
    entity_type: str
    label_kind: str
    label: str
    normalized_label: str
    datasource_id: int | None = None
    datasource_name: str | None = None
    database_id: int | None = None
    database_name: str | None = None
    schema_id: int | None = None
    schema_name: str | None = None
    table_id: int | None = None
    table_name: str | None = None
    column_id: int | None = None
    column_name: str | None = None


class SearchAliasListOut(BaseModel):
    total: int
    items: list[SearchAliasItemOut] = Field(default_factory=list)


class SearchAliasCreateIn(BaseModel):
    entity_type: str
    label_kind: str
    label: str
    table_id: int | None = None
    column_id: int | None = None


class SearchAliasUpdateIn(BaseModel):
    label_kind: str
    label: str


class SearchHit(BaseModel):
    entity_type: str
    entity_id: int
    name: str
    description: str | None = None


class SearchResponse(BaseModel):
    query: str
    total: int
    hits: list[SearchHit] = Field(default_factory=list)
