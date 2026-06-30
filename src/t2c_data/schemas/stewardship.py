from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator

from t2c_data.schemas.governance import GovernanceScoreOut


class StewardshipFilterOptionOut(BaseModel):
    value: str
    label: str


class StewardshipRequestTypeOptionOut(BaseModel):
    value: str
    label: str
    description: str


class StewardshipUserRefOut(BaseModel):
    id: int | None = None
    name: str | None = None
    email: str | None = None


class StewardshipRequestEventOut(BaseModel):
    id: int
    event_type: str
    event_type_label: str
    actor: StewardshipUserRefOut
    comment: str | None = None
    payload_json: dict[str, Any] | None = None
    created_at: datetime


class StewardshipRequestLinksOut(BaseModel):
    explorer: str
    pending_center: str


class StewardshipRequestContextOut(BaseModel):
    table_id: int
    request_type: str
    request_type_label: str
    suggested_approver: StewardshipUserRefOut
    approver_source: str
    approver_source_label: str
    assignment_rule: str
    assignment_rule_label: str
    sla_days: int
    due_at: datetime
    sla_status: str
    sla_status_label: str
    hint: str


class StewardshipRequestOut(BaseModel):
    id: int
    table_id: int | None = None
    table_name: str
    table_fqn: str
    datasource_name: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    data_owner_id: int | None = None
    owner_name: str | None = None
    request_type: str
    request_type_label: str
    request_type_description: str
    status: str
    status_label: str
    request_origin: str
    request_origin_label: str
    requester_comment: str | None = None
    decision_comment: str | None = None
    current_value_json: dict[str, Any] | None = None
    proposed_value_json: dict[str, Any] | None = None
    context_json: dict[str, Any] | None = None
    requested_by: StewardshipUserRefOut
    approver: StewardshipUserRefOut
    suggested_approver: StewardshipUserRefOut
    approver_source: str
    approver_source_label: str
    assignment_rule: str
    assignment_rule_label: str
    decided_by: StewardshipUserRefOut
    governance_score: GovernanceScoreOut | None = None
    aging_days: int = 0
    sla_days: int
    due_at: datetime
    sla_status: str
    sla_status_label: str
    created_at: datetime
    updated_at: datetime
    decided_at: datetime | None = None
    links: StewardshipRequestLinksOut
    events: list[StewardshipRequestEventOut] = Field(default_factory=list)


class StewardshipRequestSummaryItemOut(BaseModel):
    key: str
    label: str
    count: int


class StewardshipRequestFiltersOut(BaseModel):
    statuses: list[StewardshipFilterOptionOut] = Field(default_factory=list)
    request_types: list[StewardshipRequestTypeOptionOut] = Field(default_factory=list)
    owners: list[StewardshipFilterOptionOut] = Field(default_factory=list)
    approvers: list[StewardshipFilterOptionOut] = Field(default_factory=list)
    sla_statuses: list[StewardshipFilterOptionOut] = Field(default_factory=list)


class StewardshipInboxGroupOut(BaseModel):
    key: str
    label: str
    count: int
    href: str


class StewardshipInboxSummaryOut(BaseModel):
    pending_total: int = 0
    awaiting_assignment: int = 0
    review_pending: int = 0
    certification_pending: int = 0
    my_approvals_pending: int = 0
    my_owner_queue: int = 0
    by_owner: list[StewardshipInboxGroupOut] = Field(default_factory=list)
    by_approver: list[StewardshipInboxGroupOut] = Field(default_factory=list)


class StewardshipRequestListOut(BaseModel):
    generated_at: str
    total: int
    page: int = 1
    page_size: int = 20
    total_pages: int = 1
    summary: list[StewardshipRequestSummaryItemOut] = Field(default_factory=list)
    filters: StewardshipRequestFiltersOut
    inbox: StewardshipInboxSummaryOut = Field(default_factory=StewardshipInboxSummaryOut)
    items: list[StewardshipRequestOut] = Field(default_factory=list)


class StewardshipRequestCreateIn(BaseModel):
    table_id: int = Field(ge=1)
    request_type: str
    description_manual: str | None = None
    data_owner_id: int | None = Field(default=None, ge=1)
    term_ids: list[int] | None = None
    requester_comment: str | None = None
    approver_user_id: int | None = Field(default=None, ge=1)
    request_origin: str | None = None

    @model_validator(mode="after")
    def validate_payload(self) -> "StewardshipRequestCreateIn":
        request_type = (self.request_type or "").strip()
        if request_type == "table_description" and not (self.description_manual or "").strip():
            raise ValueError("description_manual is required for table_description requests")
        if request_type == "owner_assignment" and self.data_owner_id is None:
            raise ValueError("data_owner_id is required for owner_assignment requests")
        if request_type == "glossary_terms" and self.term_ids is None:
            raise ValueError("term_ids is required for glossary_terms requests")
        return self


class StewardshipDecisionIn(BaseModel):
    decision_comment: str | None = None


class StewardshipCancelIn(BaseModel):
    decision_comment: str | None = None
