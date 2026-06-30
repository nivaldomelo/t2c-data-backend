from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class UserActivityPageViewIn(BaseModel):
    route_path: str
    page_key: str | None = None
    event_type: str = "page_view"
    action: str = "view"
    resource_type: str | None = None
    resource_id: str | int | None = None
    resource_fqn: str | None = None
    datasource_id: int | None = None
    schema_name: str | None = None
    table_id: int | None = None
    table_name: str | None = None
    column_id: int | None = None
    column_name: str | None = None
    sensitivity_level: str | None = None
    has_personal_data: bool = False
    has_sensitive_data: bool = False
    privacy_classification: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class UserActivityHeartbeatOut(BaseModel):
    ok: bool = True
    updated: bool = False


class UserAuditSummaryCountOut(BaseModel):
    label: str
    value: int


class UserAuditSummaryOut(BaseModel):
    generated_at: datetime
    period_days: int
    users_active_today: int
    logins_last_24h: int
    open_sessions: int
    avg_session_seconds: int | None = None
    page_views_last_24h: int
    asset_views_last_24h: int
    changes_last_24h: int
    exports_last_24h: int
    sensitive_access_last_24h: int
    denied_requests_last_24h: int
    top_pages: list[UserAuditSummaryCountOut] = Field(default_factory=list)
    top_assets: list[UserAuditSummaryCountOut] = Field(default_factory=list)
    top_users: list[UserAuditSummaryCountOut] = Field(default_factory=list)


class UserAuditSessionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: int
    user_name: str | None = None
    user_email: str | None = None
    session_jti: str
    started_at: datetime
    last_seen_at: datetime
    ended_at: datetime | None = None
    duration_seconds: int | None = None
    end_reason: str | None = None
    status: str
    ip_address: str | None = None
    user_agent: str | None = None
    device_type: str | None = None
    browser: str | None = None
    os: str | None = None
    country: str | None = None
    city: str | None = None
    auth_method: str | None = None
    mfa_used: bool = False
    success: bool = True
    failure_reason: str | None = None


class UserAuditAccessEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    user_id: int | None = None
    user_name: str | None = None
    user_email: str | None = None
    session_id: int | None = None
    session_jti: str | None = None
    event_type: str
    page_key: str | None = None
    route_path: str | None = None
    http_method: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None
    resource_fqn: str | None = None
    datasource_id: int | None = None
    schema_name: str | None = None
    table_id: int | None = None
    table_name: str | None = None
    column_id: int | None = None
    column_name: str | None = None
    action: str | None = None
    sensitivity_level: str | None = None
    has_personal_data: bool = False
    has_sensitive_data: bool = False
    privacy_classification: str | None = None
    metadata_json: dict[str, object] | list[object] | None = None
    ip_address: str | None = None
    user_agent: str | None = None
    request_id: str | None = None
    correlation_id: str | None = None


class UserAuditChangeEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    user_id: int | None = None
    actor_name: str | None = None
    user_email: str | None = None
    action: str
    entity_type: str | None = None
    entity_id: str | None = None
    parent_entity_type: str | None = None
    parent_entity_id: str | None = None
    change_set_id: str | None = None
    change_type: str | None = None
    field_name: str | None = None
    source_module: str | None = None
    is_sensitive_change: bool = False
    sensitive_category: str | None = None
    route: str | None = None
    method: str | None = None
    status_code: int | None = None
    request_id: str | None = None
    before_json: dict[str, object] | list[object] | None = None
    after_json: dict[str, object] | list[object] | None = None
    metadata_json: dict[str, object] | list[object] | None = None

