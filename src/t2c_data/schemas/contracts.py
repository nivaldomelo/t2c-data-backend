from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class DataContractColumnIn(BaseModel):
    column_name: str
    data_type: str | None = None
    is_nullable: bool | None = None
    is_primary_key: bool | None = None
    is_required: bool | None = None
    ordinal_position: int | None = None
    notes: str | None = None


class DataContractColumnOut(DataContractColumnIn):
    model_config = ConfigDict(from_attributes=True)

    id: int


class DataContractIn(BaseModel):
    status: str = Field(default="draft")
    description: str | None = None
    notes: str | None = None
    owner_user_id: int | None = None
    steward_user_id: int | None = None
    freshness_hours: int | None = None
    min_row_count: int | None = None
    max_row_count: int | None = None
    compatibility_rules_json: dict | None = None
    columns: list[DataContractColumnIn] = Field(default_factory=list)


class DataContractOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    table_id: int
    version: int
    status: str
    description: str | None = None
    notes: str | None = None
    owner_user_id: int | None = None
    steward_user_id: int | None = None
    published_at: datetime | None = None
    freshness_hours: int | None = None
    min_row_count: int | None = None
    max_row_count: int | None = None
    compatibility_rules_json: dict | None = None
    last_validation_status: str | None = None
    last_validation_at: datetime | None = None
    last_validation_issues: int | None = None
    columns: list[DataContractColumnOut] = Field(default_factory=list)


class DataContractSummaryOut(BaseModel):
    contract_id: int | None = None
    version: int | None = None
    status: str | None = None
    published_at: datetime | None = None
    last_validation_status: str | None = None
    last_validation_at: datetime | None = None
    last_validation_issues: int | None = None


class DataContractValidationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    contract_id: int
    table_id: int
    status: str
    checked_at: datetime
    duration_ms: int | None = None
    issues_json: list | dict | None = None
    summary_json: dict | None = None


class DataContractValidationResultOut(BaseModel):
    validation: DataContractValidationOut
    summary: dict[str, object]


class DataContractSchemaChangeOut(BaseModel):
    column_name: str | None = None
    kind: str
    breaking: bool = False
    detail: str | None = None


class DataContractImpactLineageOut(BaseModel):
    upstream_count: int
    downstream_count: int
    process_count: int
    dashboard_count: int
    direct_dependencies_count: int
    impact_level: str


class DataContractImpactOut(BaseModel):
    table_id: int
    table_fqn: str
    contract_id: int | None = None
    contract_version: int | None = None
    contract_status: str | None = None
    contract_validation_status: str | None = None
    schema_state: str
    schema_label: str
    expected_columns: int = 0
    actual_columns: int = 0
    breaking_changes_count: int = 0
    warning_changes_count: int = 0
    changes: list[DataContractSchemaChangeOut] = Field(default_factory=list)
    lineage: DataContractImpactLineageOut
    recommendation: str
