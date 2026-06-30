from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.features.governance.settings import get_governance_settings_snapshot


def _positive(value: int | None, fallback: int) -> int:
    if value is None:
        return max(int(fallback), 1)
    return max(int(value), 1)


@dataclass(frozen=True, slots=True)
class RetentionPolicySnapshot:
    audit_log_retention_days: int
    audit_log_archive_retention_days: int
    access_log_retention_days: int
    access_log_archive_retention_days: int
    platform_usage_event_retention_days: int
    search_result_click_retention_days: int
    user_session_retention_days: int
    user_access_event_retention_days: int
    audit_event_retention_days: int
    export_file_ttl_hours: int
    dq_sample_retention_days: int
    profiling_sample_retention_days: int
    incident_evidence_retention_days: int
    temp_file_ttl_hours: int
    row_count_snapshot_retention_days: int
    certification_history_retention_days: int
    privacy_review_event_retention_days: int
    system_log_retention_days: int


def get_retention_policy_snapshot(session: Session) -> RetentionPolicySnapshot:
    governance = get_governance_settings_snapshot(session)
    audit_event_retention_days = _positive(getattr(settings, "audit_event_retention_days", None), governance.audit_log_retention_days)
    return RetentionPolicySnapshot(
        audit_log_retention_days=audit_event_retention_days,
        audit_log_archive_retention_days=governance.audit_log_archive_retention_days,
        access_log_retention_days=governance.access_log_retention_days,
        access_log_archive_retention_days=governance.access_log_archive_retention_days,
        platform_usage_event_retention_days=governance.platform_usage_event_retention_days,
        search_result_click_retention_days=governance.search_result_click_retention_days,
        user_session_retention_days=_positive(getattr(settings, "user_session_retention_days", None), 365),
        user_access_event_retention_days=_positive(getattr(settings, "user_access_event_retention_days", None), 365),
        audit_event_retention_days=audit_event_retention_days,
        export_file_ttl_hours=_positive(getattr(settings, "export_file_ttl_hours", None), 1),
        dq_sample_retention_days=_positive(getattr(settings, "dq_sample_retention_days", None), 90),
        profiling_sample_retention_days=_positive(getattr(settings, "profiling_sample_retention_days", None), 90),
        incident_evidence_retention_days=_positive(getattr(settings, "incident_evidence_retention_days", None), 365),
        temp_file_ttl_hours=_positive(getattr(settings, "temp_file_ttl_hours", None), 24),
        row_count_snapshot_retention_days=_positive(getattr(settings, "row_count_snapshot_retention_days", None), 180),
        certification_history_retention_days=_positive(getattr(settings, "certification_history_retention_days", None), 365),
        privacy_review_event_retention_days=_positive(getattr(settings, "privacy_review_event_retention_days", None), 365),
        system_log_retention_days=_positive(getattr(settings, "system_log_retention_days", None), 365),
    )


__all__ = ["RetentionPolicySnapshot", "get_retention_policy_snapshot"]
