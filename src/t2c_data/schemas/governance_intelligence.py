from __future__ import annotations

from pydantic import BaseModel


class IntelligenceKpiOut(BaseModel):
    key: str
    label: str
    value: float | int
    hint: str | None = None
    tone: str | None = None
    unit: str | None = None


class IntelligenceAttentionItemOut(BaseModel):
    table_id: int | None = None
    signal: str | None = None
    priority_score: int = 0
    tone: str | None = None
    metabase_dashboards: int = 0
    cause: str | None = None
    causes: list[str] = []
    impact: str | None = None
    action: str | None = None
    href: str | None = None


class IntelligenceAssetRiskItemOut(BaseModel):
    table_id: int | None = None
    label: str | None = None
    href: str | None = None
    domain_name: str | None = None
    owner_name: str | None = None
    risk_score: int = 0
    priority_score: int = 0
    risk_label: str | None = None
    risk_tone: str | None = None
    reasons: list[str] = []
    suggested_actions: list[str] = []
    next_action: str | None = None
    metabase_dashboards: int = 0
    stale_hours: int | None = None
    open_incidents: int = 0
    critical_open_incidents: int = 0
    suggested_incident: bool = False


class IntelligenceDomainRiskItemOut(BaseModel):
    domain: str
    asset_count: int = 0
    risk_score: float = 0.0
    max_score: int = 0
    critical_assets: int = 0
    open_incidents: int = 0
    tone: str | None = None


class IntelligenceTrackItemOut(BaseModel):
    key: str
    label: str
    count: int = 0


class IntelligenceActionTrackOut(BaseModel):
    key: str
    label: str
    description: str | None = None
    total: int = 0
    href: str | None = None
    items: list[IntelligenceTrackItemOut] = []


class IntelligenceNextBestActionOut(BaseModel):
    order: int
    action: str
    count: int = 0
    tone: str | None = None


class IntelligenceTimelineStepOut(BaseModel):
    occurred_at: str
    title: str
    severity: str | None = None
    event_type: str | None = None


class IntelligenceTimelineEpisodeOut(BaseModel):
    episode_key: str
    title: str
    summary: str | None = None
    impact_summary: str | None = None
    why_it_matters: str | None = None
    next_action: str | None = None
    status: str = "open"
    severity: str | None = None
    tone: str | None = None
    importance_score: int = 0
    occurred_at: str
    correlation_label: str | None = None
    correlation_chain: list[str] = []
    affected_assets_count: int = 0
    impacted_table_ids: list[int] = []
    steps: list[IntelligenceTimelineStepOut] = []


class GovernanceIntelligenceTimelineOut(BaseModel):
    generated_at: str
    episodes: list[IntelligenceTimelineEpisodeOut] = []


class GovernanceIntelligenceFeedOut(BaseModel):
    generated_at: str
    total_assets: int = 0
    metabase_priority_count: int = 0
    kpis: list[IntelligenceKpiOut] = []
    attention_now: list[IntelligenceAttentionItemOut] = []
    asset_risk: list[IntelligenceAssetRiskItemOut] = []
    by_domain: list[IntelligenceDomainRiskItemOut] = []
    tracks: list[IntelligenceActionTrackOut] = []
    next_best_actions: list[IntelligenceNextBestActionOut] = []
