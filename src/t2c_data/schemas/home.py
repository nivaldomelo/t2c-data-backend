from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class HomeSummaryCountsOut(BaseModel):
    active_datasources: int
    datasources: int
    schemas: int
    tables: int
    monitored_tables: int
    columns: int
    tags: int
    glossary_terms: int


class HomeSummaryHistoryPointOut(BaseModel):
    run_id: int
    run_at: datetime
    dq_score: float
    completeness_pct_avg: float
    row_count: int
    freshness_seconds: int


class HomeIssueTableOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    table_id: int
    datasource: str
    schema_name: str = Field(alias="schema")
    table: str
    table_fqn: str
    dq_score: float
    completeness_pct_avg: float
    row_count: int
    freshness_seconds: int
    run_at: datetime
    sensitivity_level: str | None = None
    has_personal_data: bool = False
    access_scope: str | None = None


class HomeSummaryOut(BaseModel):
    counts: HomeSummaryCountsOut
    dq_avg_score: float
    completeness_avg: float
    freshness_sla_pct: float
    freshness_sla_seconds: int
    last_scan_at: datetime | None = None
    top_critical_tables: list[HomeIssueTableOut] = Field(default_factory=list)
    top_stale_tables: list[HomeIssueTableOut] = Field(default_factory=list)
    history: list[HomeSummaryHistoryPointOut] = Field(default_factory=list)
