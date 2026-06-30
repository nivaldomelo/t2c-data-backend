from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from t2c_data.schemas.asset_context import AssetContextualActionOut, AssetLinksOut

IncidentEntityType = Literal["table", "airflow_dag"]
IncidentStatus = Literal["open", "investigating", "mitigated", "resolved", "closed", "reopened", "recurring"]
IncidentSeverity = Literal["sev1", "sev2", "sev3", "sev4"]


class IncidentUserRefOut(BaseModel):
    id: int
    name: str | None
    email: str


class IncidentAssetContextOut(BaseModel):
    table_id: int | None = None
    table_name: str | None = None
    table_fqn: str | None = None
    datasource_name: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    domain_name: str | None = None
    owner_name: str | None = None
    owner_defined: bool = False
    data_owner_id: int | None = None
    criticality_score: int | None = None
    criticality_label: str | None = None
    sensitivity_level: str | None = None
    sensitivity_label: str | None = None
    dq_score: float | None = None
    certification_status: str | None = None
    open_incidents: int = 0
    critical_open_incidents: int = 0
    links: AssetLinksOut | None = None
    actions: list[AssetContextualActionOut] = Field(default_factory=list)


class IncidentOriginOut(BaseModel):
    kind: str
    label: str
    mode: str
    dq_run_id: int | None = None
    dq_rule_id: int | None = None
    dq_rule_run_id: int | None = None
    dag_id: str | None = None
    task_id: str | None = None
    integration_name: str | None = None
    source_module: str | None = None
    source_ref_id: str | int | None = None


class IncidentImpactOut(BaseModel):
    summary: str
    operational: str | None = None
    governance: str | None = None


class IncidentEventUserRefOut(BaseModel):
    id: int
    name: str | None
    email: str


class IncidentEventOut(BaseModel):
    id: int
    incident_id: int
    event_type: str
    title: str
    detail: str | None = None
    status_from: str | None = None
    status_to: str | None = None
    evidence_json: dict | list | None = None
    actor_user_id: int | None = None
    actor_user: IncidentEventUserRefOut | None = None
    actor_name: str | None = None
    actor_email: str | None = None
    created_at: datetime
    updated_at: datetime


class IncidentOperationalSLAOut(BaseModel):
    issue_type: str
    issue_label: str
    detected_at: datetime
    due_at: datetime | None = None
    aging_hours: int = 0
    sla_hours: int | None = None
    status: str = "within_sla"
    status_label: str = "Dentro do SLA"
    recurrent: bool = False


class IncidentBase(BaseModel):
    title: str = Field(min_length=3, max_length=255)
    description: str | None = None
    entity_type: IncidentEntityType
    table_fqn: str | None = None
    airflow_dag_id: str | None = None
    detected_at: datetime
    last_seen_at: datetime | None = None
    acknowledged_at: datetime | None = None
    triaged_at: datetime | None = None
    mitigated_at: datetime | None = None
    resolved_at: datetime | None = None
    closed_at: datetime | None = None
    reopened_at: datetime | None = None
    sla_due_at: datetime | None = None
    status: IncidentStatus = "open"
    severity: IncidentSeverity = "sev3"
    owner_user_id: int | None = None
    reporter_user_id: int | None = None
    tags: list[str] | None = None
    source_type: str | None = None
    source_ref_id: int | None = None
    evidence_json: dict | None = None
    technical_origin_json: dict | None = None
    related_links_json: dict | list | None = None
    impact_json: dict | list | None = None
    mitigation_json: dict | list | None = None
    postmortem_json: dict | list | None = None
    root_cause: str | None = None
    impact_summary: str | None = None
    mitigation_summary: str | None = None
    postmortem_summary: str | None = None
    domain_name: str | None = None
    owner_team: str | None = None
    squad_name: str | None = None
    recurrence_count: int | None = None
    occurrences: int | None = None


class IncidentCreate(IncidentBase):
    pass


class IncidentUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=3, max_length=255)
    description: str | None = None
    entity_type: IncidentEntityType | None = None
    table_fqn: str | None = None
    airflow_dag_id: str | None = None
    detected_at: datetime | None = None
    last_seen_at: datetime | None = None
    acknowledged_at: datetime | None = None
    triaged_at: datetime | None = None
    mitigated_at: datetime | None = None
    resolved_at: datetime | None = None
    closed_at: datetime | None = None
    reopened_at: datetime | None = None
    sla_due_at: datetime | None = None
    status: IncidentStatus | None = None
    severity: IncidentSeverity | None = None
    owner_user_id: int | None = None
    reporter_user_id: int | None = None
    tags: list[str] | None = None
    source_type: str | None = None
    source_ref_id: int | None = None
    evidence_json: dict | None = None
    technical_origin_json: dict | None = None
    related_links_json: dict | list | None = None
    impact_json: dict | list | None = None
    mitigation_json: dict | list | None = None
    postmortem_json: dict | list | None = None
    root_cause: str | None = None
    impact_summary: str | None = None
    mitigation_summary: str | None = None
    postmortem_summary: str | None = None
    domain_name: str | None = None
    owner_team: str | None = None
    squad_name: str | None = None
    recurrence_count: int | None = None
    occurrences: int | None = None


class IncidentOut(BaseModel):
    id: int
    title: str
    description: str | None
    entity_type: IncidentEntityType
    table_fqn: str | None
    airflow_dag_id: str | None
    detected_at: datetime
    last_seen_at: datetime | None
    acknowledged_at: datetime | None = None
    triaged_at: datetime | None = None
    mitigated_at: datetime | None = None
    resolved_at: datetime | None = None
    closed_at: datetime | None = None
    reopened_at: datetime | None = None
    sla_due_at: datetime | None = None
    status: IncidentStatus
    severity: IncidentSeverity
    severity_label: str
    owner_user_id: int | None
    reporter_user_id: int | None
    owner_user: IncidentUserRefOut | None = None
    reporter_user: IncidentUserRefOut | None = None
    tags: list[str] | None
    source_type: str | None = None
    source_ref_id: int | None = None
    evidence_json: dict | None = None
    technical_origin_json: dict | None = None
    related_links_json: dict | list | None = None
    impact_json: dict | list | None = None
    mitigation_json: dict | list | None = None
    postmortem_json: dict | list | None = None
    root_cause: str | None = None
    impact_summary: str | None = None
    mitigation_summary: str | None = None
    postmortem_summary: str | None = None
    domain_name: str | None = None
    owner_team: str | None = None
    squad_name: str | None = None
    recurrence_count: int = 0
    occurrences: int = 1
    asset_context: IncidentAssetContextOut | None = None
    origin: IncidentOriginOut | None = None
    impact: IncidentImpactOut | None = None
    operational_sla: IncidentOperationalSLAOut | None = None
    timeline: list[IncidentEventOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class IncidentSummaryOut(BaseModel):
    total: int
    open: int
    resolved: int
    critical: int
    by_status: dict[str, int]
    by_severity: dict[str, int]
    counts_by_status: dict[str, int]
    counts_by_severity: dict[str, int]
    counts_by_entity_type: dict[str, int]
    detected_per_day: list[dict[str, int | str]]
    total_last_7_days: int


class IncidentCenterQueueOut(BaseModel):
    key: str
    label: str
    count: int
    tone: str = "neutral"
    href: str | None = None
    description: str | None = None


class IncidentCenterMetricOut(BaseModel):
    key: str
    label: str
    value: float
    unit: str | None = None
    tone: str = "neutral"
    detail: str | None = None


class IncidentCenterKpiOut(BaseModel):
    key: str
    label: str
    value: float
    unit: str | None = None
    tone: str = "neutral"
    detail: str | None = None


class IncidentCenterAssetOut(BaseModel):
    key: str
    label: str
    table_id: int | None = None
    table_fqn: str | None = None
    domain_name: str | None = None
    owner_name: str | None = None
    open_count: int = 0
    critical_count: int = 0
    overdue_count: int = 0
    last_detected_at: datetime | None = None
    href: str | None = None
    signals: list[str] = Field(default_factory=list)


class IncidentCenterSummaryOut(BaseModel):
    generated_at: datetime
    window_days: int
    metrics: list[IncidentCenterKpiOut] = Field(default_factory=list)
    by_status: list[IncidentCenterQueueOut] = Field(default_factory=list)
    by_severity: list[IncidentCenterQueueOut] = Field(default_factory=list)
    by_domain: list[IncidentCenterQueueOut] = Field(default_factory=list)
    by_owner: list[IncidentCenterQueueOut] = Field(default_factory=list)
    by_sla: list[IncidentCenterQueueOut] = Field(default_factory=list)
    top_assets: list[IncidentCenterAssetOut] = Field(default_factory=list)
    recent_incidents: list[IncidentOut] = Field(default_factory=list)


class IncidentEventCreate(BaseModel):
    event_type: str = Field(min_length=1, max_length=60)
    title: str = Field(min_length=3, max_length=255)
    detail: str | None = None
    status_from: str | None = None
    status_to: str | None = None
    evidence_json: dict | list | None = None
    root_cause: str | None = None
    impact_summary: str | None = None
    mitigation_summary: str | None = None
    postmortem_summary: str | None = None
