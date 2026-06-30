from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class QueryAssistantGenerateIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    question: str = Field(min_length=3, max_length=4000)
    schema_name: str | None = Field(default=None, alias="schema")
    table_fqn: str | None = None
    model: str | None = None
    dialect: Literal["postgres"] = "postgres"


class QueryAssistantUsedObjectsOut(BaseModel):
    tables: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)


class QueryAssistantGenerateOut(BaseModel):
    model_used: str
    sql: str
    explanation: list[str] = Field(default_factory=list)
    used_objects: QueryAssistantUsedObjectsOut
    assumptions: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class QueryAssistantHealthOut(BaseModel):
    ok: bool
    model: str | None = None
    configured_model: str | None = None
    models: list[str] = Field(default_factory=list)
    model_available: bool = False
    hint: str | None = None
    details: str | None = None
