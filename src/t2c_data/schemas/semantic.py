from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class SemanticLinkCreate(BaseModel):
    relation_kind: str = Field(min_length=1, max_length=60)
    entity_kind: str = Field(min_length=1, max_length=60)
    entity_id: int | None = Field(default=None, ge=1)
    entity_label: str = Field(min_length=1, max_length=255)
    entity_href: str | None = Field(default=None, max_length=500)
    notes: str | None = None
    is_primary: bool = False


class SemanticLinkOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    domain_id: int | None = None
    product_id: int | None = None
    relation_kind: str
    entity_kind: str
    entity_id: int | None = None
    entity_label: str
    entity_href: str | None = None
    notes: str | None = None
    is_primary: bool = False
    created_at: datetime
    updated_at: datetime


class SemanticDomainBase(BaseModel):
    slug: str = Field(min_length=2, max_length=160)
    name: str = Field(min_length=2, max_length=200)
    description: str | None = None
    owner: str | None = Field(default=None, max_length=160)
    steward: str | None = Field(default=None, max_length=160)
    criticality: str | None = Field(default=None, max_length=30)
    maturity_status: str = Field(default="emerging", max_length=40)
    quality_score: int | None = Field(default=None, ge=0, le=100)
    governance_score: int | None = Field(default=None, ge=0, le=100)
    notes: str | None = None
    is_active: bool = True


class SemanticDomainCreate(SemanticDomainBase):
    pass


class SemanticDomainUpdate(BaseModel):
    slug: str | None = Field(default=None, min_length=2, max_length=160)
    name: str | None = Field(default=None, min_length=2, max_length=200)
    description: str | None = None
    owner: str | None = Field(default=None, max_length=160)
    steward: str | None = Field(default=None, max_length=160)
    criticality: str | None = Field(default=None, max_length=30)
    maturity_status: str | None = Field(default=None, max_length=40)
    quality_score: int | None = Field(default=None, ge=0, le=100)
    governance_score: int | None = Field(default=None, ge=0, le=100)
    notes: str | None = None
    is_active: bool | None = None


class SemanticProductBase(BaseModel):
    slug: str = Field(min_length=2, max_length=160)
    name: str = Field(min_length=2, max_length=200)
    description: str | None = None
    owner: str | None = Field(default=None, max_length=160)
    steward: str | None = Field(default=None, max_length=160)
    consumers: list[str] = Field(default_factory=list)
    sla_text: str | None = None
    contract_text: str | None = None
    maturity_status: str = Field(default="emerging", max_length=40)
    quality_score: int | None = Field(default=None, ge=0, le=100)
    governance_score: int | None = Field(default=None, ge=0, le=100)
    notes: str | None = None
    is_active: bool = True


class SemanticProductCreate(SemanticProductBase):
    domain_slug: str = Field(min_length=2, max_length=160)


class SemanticProductUpdate(BaseModel):
    slug: str | None = Field(default=None, min_length=2, max_length=160)
    name: str | None = Field(default=None, min_length=2, max_length=200)
    description: str | None = None
    owner: str | None = Field(default=None, max_length=160)
    steward: str | None = Field(default=None, max_length=160)
    consumers: list[str] | None = None
    sla_text: str | None = None
    contract_text: str | None = None
    maturity_status: str | None = Field(default=None, max_length=40)
    quality_score: int | None = Field(default=None, ge=0, le=100)
    governance_score: int | None = Field(default=None, ge=0, le=100)
    notes: str | None = None
    is_active: bool | None = None
    domain_slug: str | None = Field(default=None, min_length=2, max_length=160)


class SemanticAssetOut(BaseModel):
    entity_kind: Literal["table"] = "table"
    entity_id: int
    label: str
    href: str
    table_fqn: str
    domain_name: str | None = None
    datasource_name: str
    database_name: str
    schema_name: str
    owner_name: str | None = None
    dq_score: float | None = None
    trust_score: int | None = None
    readiness_score: int | None = None
    documentation_score: int | None = None
    open_incidents: int = 0
    critical_open_incidents: int = 0


class SemanticDomainSuggestionOut(BaseModel):
    slug: str
    name: str
    criticality: str | None = None
    assets_count: int
    quality_score: int
    governance_score: int
    maturity_score: int
    maturity_status: str
    open_incidents: int
    critical_open_incidents: int


class SemanticDomainOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    slug: str
    name: str
    description: str | None = None
    owner: str | None = None
    steward: str | None = None
    criticality: str | None = None
    maturity_status: str
    quality_score: int | None = None
    governance_score: int | None = None
    notes: str | None = None
    is_active: bool = True
    products_count: int = 0
    assets_count: int = 0
    pipelines_count: int = 0
    rules_count: int = 0
    incidents_count: int = 0
    dashboards_count: int = 0
    contracts_count: int = 0
    maturity_score: int = 0
    maturity_label: str = "Em evolução"
    created_at: datetime
    updated_at: datetime


class SemanticDomainDetailOut(SemanticDomainOut):
    products: list["SemanticProductOut"] = Field(default_factory=list)
    links: list[SemanticLinkOut] = Field(default_factory=list)
    assets: list[SemanticAssetOut] = Field(default_factory=list)


class SemanticProductOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    domain_id: int
    domain_slug: str | None = None
    domain_name: str | None = None
    slug: str
    name: str
    description: str | None = None
    owner: str | None = None
    steward: str | None = None
    consumers: list[str] = Field(default_factory=list)
    sla_text: str | None = None
    contract_text: str | None = None
    maturity_status: str
    quality_score: int | None = None
    governance_score: int | None = None
    notes: str | None = None
    is_active: bool = True
    assets_count: int = 0
    pipelines_count: int = 0
    rules_count: int = 0
    incidents_count: int = 0
    dashboards_count: int = 0
    contracts_count: int = 0
    maturity_score: int = 0
    maturity_label: str = "Em evolução"
    created_at: datetime
    updated_at: datetime


class SemanticProductDetailOut(SemanticProductOut):
    links: list[SemanticLinkOut] = Field(default_factory=list)
    assets: list[SemanticAssetOut] = Field(default_factory=list)


class SemanticDomainPageOut(BaseModel):
    page: int
    page_size: int
    total: int
    has_more: bool = False
    items: list[SemanticDomainOut] = Field(default_factory=list)
    suggestions: list[SemanticDomainSuggestionOut] = Field(default_factory=list)


class SemanticProductPageOut(BaseModel):
    page: int
    page_size: int
    total: int
    has_more: bool = False
    items: list[SemanticProductOut] = Field(default_factory=list)
