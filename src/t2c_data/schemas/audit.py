from datetime import datetime

from pydantic import BaseModel, ConfigDict
from pydantic import Field


class AuditFilterOptionOut(BaseModel):
    value: str
    label: str


class AuditLogPageOut(BaseModel):
    items: list["AuditLogOut"]
    total: int
    page: int
    page_size: int


class AuditLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: int | None
    actor_name: str | None = None
    user_email: str | None
    ip: str | None
    user_agent: str | None
    action: str
    entity_type: str | None
    entity_id: str | None
    parent_entity_type: str | None = None
    parent_entity_id: str | None = None
    change_set_id: str | None = None
    change_type: str | None = None
    field_name: str | None = None
    source_module: str | None = None
    is_sensitive_change: bool = False
    sensitive_category: str | None = None
    route: str | None
    method: str | None
    status_code: int | None
    request_id: str | None
    before_json: object | None = Field(default=None)
    after_json: object | None = Field(default=None)
    metadata_json: object | None = Field(default=None)
    created_at: datetime


class AuditHistoryFilterOptionsOut(BaseModel):
    entity_types: list[AuditFilterOptionOut] = Field(default_factory=list)
    change_types: list[AuditFilterOptionOut] = Field(default_factory=list)
    field_names: list[AuditFilterOptionOut] = Field(default_factory=list)
    source_modules: list[AuditFilterOptionOut] = Field(default_factory=list)
    users: list[AuditFilterOptionOut] = Field(default_factory=list)


class AuditHistoryEventOut(BaseModel):
    id: int
    change_set_id: str | None = None
    changed_at: datetime
    actor_user_id: int | None = None
    actor_name: str | None = None
    actor_email: str | None = None
    action: str
    change_type: str | None = None
    field_name: str | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    parent_entity_type: str | None = None
    parent_entity_id: str | None = None
    source_module: str | None = None
    is_sensitive_change: bool = False
    sensitive_category: str | None = None
    before_value: object | None = None
    after_value: object | None = None
    metadata_json: object | None = None
    route: str | None = None
    method: str | None = None
    status_code: int | None = None
    table_id: int | None = None
    table_name: str | None = None
    schema_name: str | None = None
    database_name: str | None = None
    datasource_name: str | None = None


class AuditHistoryPageOut(BaseModel):
    items: list[AuditHistoryEventOut]
    total: int
    page: int
    page_size: int


class AuditHistoryExportRowOut(BaseModel):
    changed_at: datetime
    actor_name: str | None = None
    actor_email: str | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    table_name: str | None = None
    schema_name: str | None = None
    database_name: str | None = None
    datasource_name: str | None = None
    field_name: str | None = None
    change_type: str | None = None
    source_module: str | None = None
    change_set_id: str | None = None
    is_sensitive_change: bool = False
    sensitive_category: str | None = None
    before_value: object | None = None
    after_value: object | None = None
    metadata_json: object | None = None


AuditLogPageOut.model_rebuild()
