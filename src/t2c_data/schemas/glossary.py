from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class GlossaryTermBase(BaseModel):
    external_id: str | None = Field(default=None, max_length=40)
    slug: str = Field(min_length=2, max_length=160)
    name: str = Field(min_length=2, max_length=200)
    definition: str
    description: str | None = None
    steward: str | None = Field(default=None, max_length=120)
    category: str | None = Field(default=None, max_length=120)
    subcategory: str | None = Field(default=None, max_length=120)
    example_of_use: str | None = None
    synonyms: str | None = None
    suggested_priority: str | None = Field(default=None, max_length=40)
    status: str = Field(default="active", max_length=30)
    tag_labels: str | None = None
    notes: str | None = None


class GlossaryTermCreate(GlossaryTermBase):
    pass


class GlossaryTermUpdate(BaseModel):
    external_id: str | None = Field(default=None, max_length=40)
    slug: str | None = Field(default=None, min_length=2, max_length=160)
    name: str | None = Field(default=None, min_length=2, max_length=200)
    definition: str | None = None
    description: str | None = None
    steward: str | None = Field(default=None, max_length=120)
    category: str | None = Field(default=None, max_length=120)
    subcategory: str | None = Field(default=None, max_length=120)
    example_of_use: str | None = None
    synonyms: str | None = None
    suggested_priority: str | None = Field(default=None, max_length=40)
    status: str | None = Field(default=None, max_length=30)
    tag_labels: str | None = None
    notes: str | None = None


class GlossaryLinkedTablePreview(BaseModel):
    id: int
    name: str
    schema_name: str
    database_name: str
    datasource_name: str
    description: str | None = None


class GlossaryTermOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    external_id: str | None
    slug: str
    name: str
    definition: str
    description: str | None
    steward: str | None
    category: str | None
    subcategory: str | None
    example_of_use: str | None
    synonyms: str | None
    suggested_priority: str | None
    status: str
    tag_labels: str | None
    notes: str | None
    tables_count: int = 0
    linked_tables_preview: list[GlossaryLinkedTablePreview] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class GlossaryTermDetailOut(GlossaryTermOut):
    linked_tables: list[GlossaryLinkedTablePreview] = Field(default_factory=list)


class GlossaryTermFiltersOut(BaseModel):
    categories: list[str] = Field(default_factory=list)
    subcategories: list[str] = Field(default_factory=list)
    statuses: list[str] = Field(default_factory=list)
    priorities: list[str] = Field(default_factory=list)


class GlossarySummaryOut(BaseModel):
    total: int
    active: int
    in_use: int
    categories: int


class GlossaryAssignRequest(BaseModel):
    term_id: int
    entity_type: str
    entity_id: int


class GlossaryAssignmentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    term_id: int
    entity_type: str
    entity_id: int
    created_at: datetime


class GlossarySpreadsheetImportError(BaseModel):
    row_number: int
    slug: str | None = None
    message: str


class GlossarySpreadsheetImportResult(BaseModel):
    processed: int
    imported: int
    updated: int
    rejected: int
    errors: list[GlossarySpreadsheetImportError] = Field(default_factory=list)


class GlossaryResetOut(BaseModel):
    deleted_terms: int
    deleted_assignments: int
