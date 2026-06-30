from __future__ import annotations

from pydantic import BaseModel, Field


class AssetSignalOut(BaseModel):
    type: str
    severity: str


class AssetImpactOut(BaseModel):
    dashboards: int = 0
    users: int = 0


class AssetIntelligenceOut(BaseModel):
    risk_score: int = Field(ge=0, le=100)
    priority_score: int = Field(ge=0, le=100)
    trust_score: int = Field(ge=0, le=100)
    signals: list[AssetSignalOut] = Field(default_factory=list)
    impact: AssetImpactOut = Field(default_factory=AssetImpactOut)
    recommended_actions: list[str] = Field(default_factory=list)
