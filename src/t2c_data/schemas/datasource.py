from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr


DataSourceEngine = Literal[
    "postgres",
    "mysql",
    "sqlserver",
    "oracle",
    "mongodb",
    "snowflake",
    "bigquery",
    "redshift",
    "databricks",
    "mariadb",
    "sqlite",
    "other",
]


class ConnectorCapabilitiesOut(BaseModel):
    test_connection: bool
    list_schemas: bool
    list_tables: bool
    get_database_info: bool


class ConnectorDefinitionOut(BaseModel):
    id: DataSourceEngine
    label: str
    group: str
    description: str
    required_fields: list[str]
    optional_fields: list[str]
    secret_fields: list[str]
    capabilities: ConnectorCapabilitiesOut
    enabled: bool
    order: int


class DataSourceCreate(BaseModel):
    name: str
    db_type: DataSourceEngine
    connection: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, SecretStr] = Field(default_factory=dict)
    detected_schemas: list[str] | None = None
    include_schemas: list[str] | None = None
    exclude_schemas: list[str] | None = None
    is_active: bool = True


class DataSourceUpdate(BaseModel):
    name: str | None = None
    db_type: DataSourceEngine | None = None
    connection: dict[str, Any] | None = None
    secrets: dict[str, SecretStr] | None = None
    detected_schemas: list[str] | None = None
    include_schemas: list[str] | None = None
    exclude_schemas: list[str] | None = None
    is_active: bool | None = None


class DataSourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    db_type: DataSourceEngine
    host: str
    port: int
    database: str
    username: str
    is_active: bool
    detected_schemas: list[str] | None
    include_schemas: list[str] | None
    exclude_schemas: list[str] | None
    created_at: datetime
    updated_at: datetime
    capabilities: ConnectorCapabilitiesOut


class DataSourceDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    db_type: DataSourceEngine
    host: str
    port: int
    database: str
    username: str
    connection: dict[str, Any]
    configured_secrets: list[str]
    is_active: bool
    detected_schemas: list[str] | None
    include_schemas: list[str] | None
    exclude_schemas: list[str] | None
    created_at: datetime
    updated_at: datetime
    capabilities: ConnectorCapabilitiesOut


class DataSourceTestRequest(BaseModel):
    db_type: DataSourceEngine
    connection: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, SecretStr] = Field(default_factory=dict)


class DataSourceConnectionTestOut(BaseModel):
    success: bool
    message: str
    engine: DataSourceEngine
    host: str | None = None
    port: int | None = None
    database: str | None = None
    default_schema: str | None = None
    latency_ms: int | None = None
    details: dict[str, Any] | None = None
    capabilities: ConnectorCapabilitiesOut
    schemas: list[str] | None = None
    warning: str | None = None


class DataSourceSchemaListOut(BaseModel):
    engine: DataSourceEngine
    schemas: list[str]
    capabilities: ConnectorCapabilitiesOut


class DataSourceTableListOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    engine: DataSourceEngine
    schema_name: str | None = Field(default=None, alias="schema")
    tables: list[str]
    capabilities: ConnectorCapabilitiesOut
