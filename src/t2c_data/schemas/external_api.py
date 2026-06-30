from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ExternalApiScopeActionOut(BaseModel):
    key: str
    action: Literal["read", "create", "update", "delete"]
    label: str
    description: str
    available: bool = True
    destructive: bool = False
    requires_read: bool = False
    methods: list[str] = Field(default_factory=list)
    endpoints: list[str] = Field(default_factory=list)


class ExternalApiScopeOut(BaseModel):
    key: str
    label: str
    description: str
    actions: list[ExternalApiScopeActionOut] = Field(default_factory=list)


class ExternalApiPermissionSummaryOut(BaseModel):
    read: int = 0
    create: int = 0
    update: int = 0
    delete: int = 0
    total: int = 0
    risk_level: Literal["low", "medium", "high"] = "low"


class ExternalApiKeyOut(BaseModel):
    id: int
    public_id: str
    name: str
    description: str | None = None
    status: str
    effective_status: str
    scopes: list[str] = Field(default_factory=list)
    permission_summary: ExternalApiPermissionSummaryOut = Field(default_factory=ExternalApiPermissionSummaryOut)
    environment: str = "shared"
    allowed_ips: list[str] = Field(default_factory=list)
    token_prefix: str
    expires_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_used_at: datetime | None = None
    last_used_ip: str | None = None
    last_used_user_agent: str | None = None
    usage_count: int = 0
    created_by_user_id: int | None = None
    created_by_user_email: str | None = None
    created_by_user_name: str | None = None


class ExternalApiKeyCreateIn(BaseModel):
    name: str
    description: str | None = None
    scopes: list[str] = Field(default_factory=list)
    environment: str = "shared"
    allowed_ips: list[str] = Field(default_factory=list)
    status: str = "active"
    expires_at: datetime | None = None
    expires_in_days: int | None = Field(default=None, ge=1)


class ExternalApiKeyUpdateIn(BaseModel):
    name: str | None = None
    description: str | None = None
    scopes: list[str] | None = None
    environment: str | None = None
    allowed_ips: list[str] | None = None
    status: str | None = None
    expires_at: datetime | None = None
    expires_in_days: int | None = Field(default=None, ge=1)


class ExternalApiKeyCreatedOut(BaseModel):
    key: ExternalApiKeyOut
    token: str
    token_preview: str


class ExternalApiKeyRotateOut(BaseModel):
    key: ExternalApiKeyOut
    token: str
    token_preview: str
