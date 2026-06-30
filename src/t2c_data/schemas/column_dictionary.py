from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from t2c_data.schemas.catalog import ColumnDictionaryImportError, ColumnDictionaryImportResult
from t2c_data.schemas.tag import TagOut


class ColumnDictionaryFilterOptionsOut(BaseModel):
    datasources: list[str] = Field(default_factory=list)
    schemas: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)
    data_types: list[str] = Field(default_factory=list)


class ColumnDictionaryGapTableOut(BaseModel):
    schema_name: str
    table_name: str
    total_columns: int
    documented_columns: int
    pending_columns: int
    documented_pct: int


class ColumnDictionarySummaryOut(BaseModel):
    total_columns: int
    total_tables: int
    total_schemas: int
    documented_columns: int
    documented_pct: int
    comment_columns: int
    comment_pct: int
    existing_comment_columns: int
    existing_comment_pct: int
    pending_columns: int
    top_gap_tables: list[ColumnDictionaryGapTableOut] = Field(default_factory=list)


class ColumnDictionaryItemOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    external_id: str | None = None
    slug: str | None = None
    datasource_name: str
    schema_name: str
    table_name: str
    table_id: int
    ordinal_position: int
    name: str
    data_type: str
    udt_name: str | None = None
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    is_nullable: bool
    column_default: str | None = None
    existing_comment: str | None = None
    is_primary_key: bool
    description_source: str | None = None
    description_manual: str | None = None
    dictionary_description: str | None = None
    dictionary_comment: str | None = None
    documentation_status: str
    documentation_status_label: str
    documentation_pct: int
    has_description: bool
    has_comment: bool
    has_existing_comment: bool
    tags: list[TagOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class ColumnDictionaryDetailOut(ColumnDictionaryItemOut):
    database_name: str
    schema_description_source: str | None = None
    schema_description_manual: str | None = None
    table_description_source: str | None = None
    table_description_manual: str | None = None
    table_owner: str | None = None
    table_lifecycle_status: str | None = None


class ColumnDictionaryPageOut(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[ColumnDictionaryItemOut] = Field(default_factory=list)
    filters: ColumnDictionaryFilterOptionsOut = Field(default_factory=ColumnDictionaryFilterOptionsOut)


class ColumnDictionaryUpdateIn(BaseModel):
    dictionary_description: str | None = None
    dictionary_comment: str | None = None
    existing_comment: str | None = None


class ColumnDictionaryBulkUpdateIn(ColumnDictionaryUpdateIn):
    column_ids: list[int] = Field(default_factory=list)


class ColumnDictionaryBulkUpdateOut(BaseModel):
    matched: int
    updated: int
    not_found: list[int] = Field(default_factory=list)


class ColumnDictionaryResetOut(BaseModel):
    deleted_columns: int


class ColumnDictionaryImportPreviewRowOut(BaseModel):
    row_number: int
    status: str
    schema_name: str
    table_name: str
    column_name: str
    slug: str | None = None
    match_source: str | None = None
    message: str | None = None


class ColumnDictionaryCatalogGapTableOut(BaseModel):
    schema_name: str
    table_name: str
    rows_count: int


class ColumnDictionaryImportPreviewOut(BaseModel):
    processed: int
    matched: int
    inserted: int
    updated: int
    ignored: int
    rejected: int
    duplicate_rows: int = 0
    missing_catalog_rows: int = 0
    catalog_sync_required: bool = False
    missing_catalog_schemas: list[str] = Field(default_factory=list)
    missing_catalog_tables: list[ColumnDictionaryCatalogGapTableOut] = Field(default_factory=list)
    rows: list[ColumnDictionaryImportPreviewRowOut] = Field(default_factory=list)
    errors: list[ColumnDictionaryImportError] = Field(default_factory=list)


__all__ = [
    "ColumnDictionaryBulkUpdateIn",
    "ColumnDictionaryBulkUpdateOut",
    "ColumnDictionaryDetailOut",
    "ColumnDictionaryFilterOptionsOut",
    "ColumnDictionaryGapTableOut",
    "ColumnDictionaryImportPreviewOut",
    "ColumnDictionaryImportPreviewRowOut",
    "ColumnDictionaryCatalogGapTableOut",
    "ColumnDictionaryItemOut",
    "ColumnDictionaryPageOut",
    "ColumnDictionaryResetOut",
    "ColumnDictionarySummaryOut",
    "ColumnDictionaryUpdateIn",
    "ColumnDictionaryImportError",
    "ColumnDictionaryImportResult",
]
