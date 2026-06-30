from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

_SUPPORTED_ENGINES: list[tuple[str, str]] = [
    ("postgres", "PostgreSQL"),
    ("mysql", "MySQL"),
    ("sqlserver", "SQL Server"),
    ("oracle", "Oracle"),
    ("mongodb", "MongoDB"),
    ("snowflake", "Snowflake"),
    ("bigquery", "BigQuery"),
    ("redshift", "Redshift"),
    ("databricks", "Databricks"),
    ("mariadb", "MariaDB"),
    ("sqlite", "SQLite"),
    ("other", "Outros"),
]

_CERTIFICATION_STATUS_LABELS = {
    "not_assessed": "Não elegível",
    "not_eligible": "Não elegível",
    "eligible": "Elegível",
    "in_review": "Em revisão",
    "certified": "Certificada",
    "rejected": "Recusada",
    "expired": "Vencida",
    "revalidation_pending": "Pendente de revalidação",
}

_CRITICALITY_LABELS = {
    "low": "Baixa",
    "medium": "Média",
    "high": "Alta",
    "critical": "Crítica",
}

_BADGE_LABELS = {
    "internal_use": "Uso interno",
    "official_use": "Uso regulatório/oficial",
    "restricted_sensitive": "Restrito/sensível",
}

_INCIDENT_STATUS_LABELS = {
    "open": "Abertos",
    "investigating": "Investigando",
    "mitigated": "Mitigados",
    "resolved": "Resolvidos",
    "closed": "Fechados",
}

_INCIDENT_SEVERITY_LABELS = {
    "sev1": "Crítico",
    "sev2": "Alto",
    "sev3": "Médio",
    "sev4": "Baixo",
}


@dataclass
class TableProfile:
    table_id: int
    datasource_id: int
    database_id: int | None
    schema_id: int
    table_name: str
    table_type: str
    schema_name: str
    database_name: str
    datasource_name: str
    engine: str
    owner_defined: bool
    description_complete: bool
    dictionary_complete: bool
    classification_defined: bool
    tags_count: int
    terms_count: int
    total_columns: int
    documented_columns: int
    certification_status: str
    certification_criticality: str | None
    certification_badges: list[str]
    certification_decided_at: datetime | None
    certification_review_at: datetime | None
    certification_expires_at: datetime | None
    review_recent: bool
    dq_score: float | None
    completeness_pct_avg: float | None
    freshness_seconds: int | None
    open_incidents: int
    critical_open_incidents: int
    active_dq_violation: bool = False
    active_dq_violation_count: int = 0
    active_dq_rule_names: list[str] | None = None
    owner_name: str | None = None
    data_owner_id: int | None = None
    data_owner_is_active: bool | None = None
    domain_name: str | None = None
    sensitivity_level: str | None = None
    has_personal_data: bool = False
    has_sensitive_personal_data: bool = False
    owner_reviewed_at: datetime | None = None
    privacy_reviewed_at: datetime | None = None
    last_review_at: datetime | None = None
    last_sync_at: datetime | None = None
    last_updated_at: datetime | None = None
    search_clicks_30d: int = 0
    active_dq_rules_count: int = 0
    recent_dq_failure_runs_30d: int = 0
    sla_defined: bool = False
    sla_hours: int | None = None
    trust_score: int = 0
    trust_label: str = "Sem leitura"
    trust_tone: str = "neutral"
    classified_columns: int = 0
    personal_classified_columns: int = 0
    sensitive_classified_columns: int = 0
    financial_classified_columns: int = 0
    operational_classified_columns: int = 0
    classification_coverage_pct: float = 0.0
    column_classification_reviewed_at: datetime | None = None

    @property
    def table_fqn(self) -> str:
        return f"{self.datasource_name}.{self.schema_name}.{self.table_name}"

    @property
    def incident_lookup_key(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    @property
    def documentation_score(self) -> int:
        checks = [
            self.description_complete,
            self.dictionary_complete,
            self.tags_count > 0,
            self.terms_count > 0,
        ]
        return int(round((sum(1 for item in checks if item) / len(checks)) * 100))

    @property
    def readiness_score(self) -> int:
        checks = [
            self.owner_defined,
            self.description_complete,
            self.total_columns > 0 and int(round((self.documented_columns / self.total_columns) * 100)) >= 80,
            self.tags_count > 0,
            self.terms_count > 0,
            self.dq_score is not None and self.dq_score >= 90,
            self.critical_open_incidents == 0,
            self.review_recent,
        ]
        return int(round((sum(1 for item in checks if item) / len(checks)) * 100))

    @property
    def eligible_for_certification(self) -> bool:
        return self.readiness_score >= 50

    def to_summary(self) -> dict:
        return {
            "table_id": self.table_id,
            "table_name": self.table_name,
            "table_fqn": self.table_fqn,
            "datasource_name": self.datasource_name,
            "database_name": self.database_name,
            "schema_name": self.schema_name,
            "engine": self.engine,
            "table_type": self.table_type,
            "dq_score": round(self.dq_score, 1) if self.dq_score is not None else None,
            "completeness_pct_avg": round(self.completeness_pct_avg, 1)
            if self.completeness_pct_avg is not None
            else None,
            "freshness_seconds": self.freshness_seconds,
            "open_incidents": self.open_incidents,
            "critical_open_incidents": self.critical_open_incidents,
            "certification_status": self.certification_status,
            "certification_criticality": self.certification_criticality,
            "certification_badges": self.certification_badges,
            "certification_decided_at": self.certification_decided_at.isoformat() if self.certification_decided_at else None,
            "certification_review_at": self.certification_review_at.isoformat() if self.certification_review_at else None,
            "certification_expires_at": self.certification_expires_at.isoformat() if self.certification_expires_at else None,
            "owner_defined": self.owner_defined,
            "owner_name": self.owner_name,
            "dictionary_complete": self.dictionary_complete,
            "description_complete": self.description_complete,
            "tags_count": self.tags_count,
            "terms_count": self.terms_count,
            "total_columns": self.total_columns,
            "documented_columns": self.documented_columns,
            "classified_columns": self.classified_columns,
            "personal_classified_columns": self.personal_classified_columns,
            "sensitive_classified_columns": self.sensitive_classified_columns,
            "financial_classified_columns": self.financial_classified_columns,
            "operational_classified_columns": self.operational_classified_columns,
            "classification_coverage_pct": self.classification_coverage_pct,
            "column_classification_reviewed_at": self.column_classification_reviewed_at.isoformat()
            if self.column_classification_reviewed_at
            else None,
            "search_clicks_30d": self.search_clicks_30d,
            "active_dq_rules_count": self.active_dq_rules_count,
            "recent_dq_failure_runs_30d": self.recent_dq_failure_runs_30d,
            "sla_defined": self.sla_defined,
            "sla_hours": self.sla_hours,
            "documented_columns_pct": int(round((self.documented_columns / self.total_columns) * 100)) if self.total_columns else 0,
            "readiness_score": self.readiness_score,
            "documentation_score": self.documentation_score,
            "trust_score": self.trust_score,
            "trust_label": self.trust_label,
            "trust_tone": self.trust_tone,
            "domain_name": self.domain_name,
            "sensitivity_level": self.sensitivity_level,
            "data_owner_is_active": self.data_owner_is_active,
            "has_personal_data": self.has_personal_data,
            "has_sensitive_personal_data": self.has_sensitive_personal_data,
            "active_dq_violation": self.active_dq_violation,
            "active_dq_violation_count": self.active_dq_violation_count,
            "active_dq_rule_names": self.active_dq_rule_names or [],
            "owner_reviewed_at": self.owner_reviewed_at.isoformat() if self.owner_reviewed_at else None,
            "privacy_reviewed_at": self.privacy_reviewed_at.isoformat() if self.privacy_reviewed_at else None,
            "last_review_at": self.last_review_at.isoformat() if self.last_review_at else None,
            "last_sync_at": self.last_sync_at.isoformat() if self.last_sync_at else None,
            "last_updated_at": self.last_updated_at.isoformat() if self.last_updated_at else None,
        }


def round_pct(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((count / total) * 100.0, 1)


def engine_label(key: str) -> str:
    for engine_key, label in _SUPPORTED_ENGINES:
        if engine_key == key:
            return label
    return key.upper()


def normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
