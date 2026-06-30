from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class AssistantExplainProblemOut(BaseModel):
    key: str
    label: str
    severity: str
    detail: str
    evidence: dict[str, object] = Field(default_factory=dict)
    action_hint: str | None = None
    href: str | None = None


class AssistantExplainImpactOut(BaseModel):
    key: str
    label: str
    tone: str = "neutral"
    detail: str
    evidence: dict[str, object] = Field(default_factory=dict)


class AssistantRecommendationOut(BaseModel):
    key: str
    label: str
    detail: str
    action_key: str
    action_label: str
    tone: str = "neutral"
    destructive: bool = False
    confirmation_required: bool = False
    confirmation_hint: str | None = None
    can_execute: bool = True
    href: str | None = None


class AssistantActionOptionOut(BaseModel):
    key: str
    label: str
    description: str
    tone: str = "neutral"
    destructive: bool = False
    confirmation_required: bool = False
    confirmation_hint: str | None = None
    can_execute: bool = True
    requires_owner_id: bool = False
    recommended: bool = False
    href: str | None = None
    disabled_reason: str | None = None


class AssistantExplainOut(BaseModel):
    generated_at: datetime
    asset_ref: str
    asset_type: str
    asset_id: int
    entity_kind: str
    asset_name: str
    asset_fqn: str
    table_id: int
    column_id: int | None = None
    asset_owner_id: int | None = None
    asset_owner_name: str | None = None
    asset_owner_email: str | None = None
    asset_owner_defined: bool = False
    sla_defined: bool = False
    sla_hours: int | None = None
    summary: str
    problems: list[AssistantExplainProblemOut] = Field(default_factory=list)
    impact: list[AssistantExplainImpactOut] = Field(default_factory=list)
    recommendation: AssistantRecommendationOut
    actions: list[AssistantActionOptionOut] = Field(default_factory=list)
    context: dict[str, object] = Field(default_factory=dict)


class AssistantActionIn(BaseModel):
    action_key: str = Field(min_length=1)
    confirm: bool = False
    data_owner_id: int | None = Field(default=None, ge=1)
    resolution_note: str | None = None


class AssistantActionOut(BaseModel):
    ok: bool = True
    asset_ref: str
    asset_type: str
    asset_id: int
    action_key: str
    executed: bool
    message: str
    result: dict[str, object] = Field(default_factory=dict)
    follow_up_href: str | None = None

