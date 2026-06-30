from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


TimelineCategory = Literal["governance", "operation", "quality", "incident", "audit"]
TimelineMode = Literal["manual", "automatic", "unknown"]
TimelineSeverity = Literal["low", "medium", "high", "critical"]


class TimelineEventOut(BaseModel):
    id: str
    occurred_at: datetime
    category: TimelineCategory
    event_type: str
    title: str
    detail: str | None = None
    source_module: str | None = None
    source_label: str | None = None
    actor_name: str | None = None
    actor_email: str | None = None
    mode: TimelineMode = "unknown"
    severity: TimelineSeverity = "medium"
    priority: int = 0
    entity_type: str | None = None
    entity_id: str | None = None
    table_id: int | None = None
    column_id: int | None = None
    table_name: str | None = None
    column_name: str | None = None
    schema_name: str | None = None
    database_name: str | None = None
    datasource_name: str | None = None
    table_fqn: str | None = None
    owner_name: str | None = None
    certification_status: str | None = None
    certification_status_label: str | None = None
    readiness_score: int | None = None
    trust_score: int | None = None
    trust_label: str | None = None
    trust_tone: str | None = None
    trust_delta: int | None = None
    trust_summary: str | None = None
    active_dq_violation: bool = False
    active_dq_rule_names: list[str] = Field(default_factory=list)
    href: str | None = None
    metadata_json: dict[str, Any] | None = None


class TimelineEpisodeMemberOut(BaseModel):
    id: str
    occurred_at: datetime
    title: str
    detail: str | None = None
    category: TimelineCategory
    event_type: str
    mode: TimelineMode = "unknown"
    severity: TimelineSeverity = "medium"
    priority: int = 0
    table_id: int | None = None
    column_id: int | None = None
    table_name: str | None = None
    column_name: str | None = None
    table_fqn: str | None = None
    owner_name: str | None = None
    trust_score: int | None = None
    trust_delta: int | None = None
    active_dq_violation: bool = False
    href: str | None = None
    metadata_json: dict[str, Any] | None = None


class TimelineEpisodeOut(BaseModel):
    episode_key: str
    id: str
    episode_type: str
    title: str
    summary: str
    impact_summary: str
    why_it_matters: str
    next_action: str
    status: Literal["open", "watching", "acknowledged", "silenced", "resolved"] = "open"
    category: TimelineCategory
    source_module: str | None = None
    source_label: str | None = None
    mode: TimelineMode = "unknown"
    severity: TimelineSeverity = "medium"
    priority: int = 0
    importance_score: int = 0
    occurred_at: datetime
    updated_at: datetime
    window_start: datetime
    window_end: datetime
    event_count: int = 0
    affected_assets_count: int = 0
    affected_columns_count: int = 0
    impacted_table_ids: list[int] = Field(default_factory=list)
    impacted_table_fqns: list[str] = Field(default_factory=list)
    impacted_owner_names: list[str] = Field(default_factory=list)
    related_labels: list[str] = Field(default_factory=list)
    child_events: list[TimelineEpisodeMemberOut] = Field(default_factory=list)
    action_count: int = 0
    acknowledged_at: datetime | None = None
    acknowledged_by_name: str | None = None
    silenced_until: datetime | None = None
    silence_reason: str | None = None
    last_action_type: str | None = None
    href: str | None = None
    metadata_json: dict[str, Any] | None = None
    correlation_label: str | None = None
    correlation_chain: list[str] = Field(default_factory=list)


class TimelineSummaryOut(BaseModel):
    total: int = 0
    governance: int = 0
    operation: int = 0
    quality: int = 0
    incident: int = 0
    audit: int = 0
    manual: int = 0
    automatic: int = 0
    critical: int = 0


class TimelineAnalyticsBucketOut(BaseModel):
    label: str
    count: int


class TimelineAnalyticsOut(BaseModel):
    total_episodes: int = 0
    open_episodes: int = 0
    acknowledged_episodes: int = 0
    silenced_episodes: int = 0
    resolved_episodes: int = 0
    critical_episodes: int = 0
    recurrent_episodes: int = 0
    impacted_assets: int = 0
    impacted_columns: int = 0
    average_importance_score: float = 0
    average_event_count: float = 0
    top_episode_types: list[TimelineAnalyticsBucketOut] = Field(default_factory=list)
    top_sources: list[TimelineAnalyticsBucketOut] = Field(default_factory=list)
    top_statuses: list[TimelineAnalyticsBucketOut] = Field(default_factory=list)


class TimelineEpisodeActionIn(BaseModel):
    episode_key: str = Field(min_length=1)
    action_type: Literal["acknowledge", "silence"]
    table_id: int | None = None
    column_id: int | None = None
    reason: str | None = None
    silent_until: datetime | None = None


class TimelineEpisodeActionOut(BaseModel):
    id: int
    episode_key: str
    table_id: int | None = None
    column_id: int | None = None
    action_type: str
    status: str
    reason: str | None = None
    silent_until: datetime | None = None
    actor_user_id: int | None = None
    actor_name: str | None = None
    actor_email: str | None = None
    metadata_json: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TimelinePageOut(BaseModel):
    generated_at: datetime
    scope: Literal["global", "asset"] = "global"
    table_id: int | None = None
    column_id: int | None = None
    table_fqn: str | None = None
    page: int
    page_size: int
    total: int
    summary: TimelineSummaryOut = Field(default_factory=TimelineSummaryOut)
    items: list[TimelineEventOut] = Field(default_factory=list)
    episode_page: int = 1
    episode_page_size: int = 0
    episode_total: int = 0
    episodes: list[TimelineEpisodeOut] = Field(default_factory=list)
    analytics: TimelineAnalyticsOut = Field(default_factory=TimelineAnalyticsOut)
