from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class PermissionBase(BaseModel):
    name: str
    description: str | None = None


class PermissionCreate(PermissionBase):
    pass


class PermissionUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


class PermissionOut(PermissionBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime


class RoleCreate(BaseModel):
    name: str
    description: str | None = None
    permission_ids: list[int] = Field(default_factory=list)


class RoleUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    permission_ids: list[int] | None = None


class RoleOut(BaseModel):
    id: int
    name: str
    description: str | None
    permissions: list[PermissionOut]
    created_at: datetime
    updated_at: datetime


class DataScopeGrantBase(BaseModel):
    effect: Literal["allow", "deny"] = "allow"
    datasource_id: int | None = None
    schema_id: int | None = None
    table_id: int | None = None
    note: str | None = None


class DataScopeGrantCreate(DataScopeGrantBase):
    pass


class DataScopeGrantUpdate(BaseModel):
    effect: Literal["allow", "deny"] | None = None
    datasource_id: int | None = None
    schema_id: int | None = None
    table_id: int | None = None
    note: str | None = None


class DataScopeGrantOut(DataScopeGrantBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    scope_kind: str
    datasource_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    datasource_fqn: str | None = None
    table_fqn: str | None = None
    created_at: datetime
    updated_at: datetime


class AccessGroupMemberOut(BaseModel):
    id: int
    email: str
    name: str | None
    full_name: str | None
    is_active: bool


class AccessGroupBase(BaseModel):
    name: str
    description: str | None = None
    is_active: bool = True
    member_user_ids: list[int] = Field(default_factory=list)
    grants: list[DataScopeGrantBase] = Field(default_factory=list)


class AccessGroupCreate(AccessGroupBase):
    pass


class AccessGroupUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    is_active: bool | None = None
    member_user_ids: list[int] | None = None
    grants: list[DataScopeGrantBase] | None = None


class AccessGroupOut(BaseModel):
    id: int
    name: str
    description: str | None
    is_active: bool
    members: list[AccessGroupMemberOut] = Field(default_factory=list)
    grants: list[DataScopeGrantOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class AccessDatasourceOptionOut(BaseModel):
    id: int
    name: str
    db_type: str
    database: str


class AccessSchemaOptionOut(BaseModel):
    id: int
    datasource_id: int
    database_id: int
    name: str


class AccessTableOptionOut(BaseModel):
    id: int
    datasource_id: int
    database_id: int
    schema_id: int
    name: str
    table_type: str
    table_fqn: str


class AccessTargetOptionsOut(BaseModel):
    datasources: list[AccessDatasourceOptionOut] = Field(default_factory=list)
    schemas: list[AccessSchemaOptionOut] = Field(default_factory=list)
    tables: list[AccessTableOptionOut] = Field(default_factory=list)


class UserCreate(BaseModel):
    email: str
    name: str | None = None
    full_name: str | None = None
    password: str
    is_active: bool = True
    role_ids: list[int] = Field(default_factory=list)
    access_group_ids: list[int] = Field(default_factory=list)
    data_scope_grants: list[DataScopeGrantBase] = Field(default_factory=list)


class UserUpdate(BaseModel):
    email: str | None = None
    name: str | None = None
    full_name: str | None = None
    password: str | None = None
    is_active: bool | None = None
    role_ids: list[int] | None = None
    access_group_ids: list[int] | None = None
    data_scope_grants: list[DataScopeGrantBase] | None = None


class UserOut(BaseModel):
    id: int
    email: str
    name: str | None
    full_name: str | None
    is_active: bool
    mfa_enabled: bool = False
    mfa_locked: bool = False
    mfa_grace_logins_used: int = 0
    password_expires_at: datetime | None = None
    password_expired: bool = False
    roles: list[RoleOut]
    access_group_ids: list[int] = Field(default_factory=list)
    access_groups: list[AccessGroupOut] = Field(default_factory=list)
    data_scope_grants: list[DataScopeGrantOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
