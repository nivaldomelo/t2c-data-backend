from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timezone

from sqlalchemy.orm import Session

from t2c_data.core.legacy_api_surface import LEGACY_API_MANAGED_MODULES
from t2c_data.features.governance.score_config import (
    DEFAULT_GOVERNANCE_SCORE_WEIGHTS,
    normalize_governance_score_weights,
    normalize_governance_policy_rules,
    normalize_trust_score_adjustments,
)
from t2c_data.models.governance import GovernanceSettings

LEGACY_API_AUTO_CUTOFF_MODULES: tuple[str, ...] = ("datasources", "scan-runs")


@dataclass(frozen=True)
class GovernanceSettingsSnapshot:
    owner_review_interval_days: int = 90
    privacy_review_interval_days: int = 180
    sensitive_privacy_review_interval_days: int = 90
    certification_review_interval_days: int = 180
    certification_review_sla_days: int = 7
    certification_revalidation_window_days: int = 30
    audit_log_retention_days: int = 730
    audit_log_archive_retention_days: int = 2555
    access_log_retention_days: int = 30
    access_log_archive_retention_days: int = 365
    platform_usage_event_retention_days: int = 180
    search_result_click_retention_days: int = 180
    legacy_api_cutoff_window_days: int = 30
    legacy_api_disabled_modules: tuple[str, ...] = ()
    legacy_api_force_enabled_modules: tuple[str, ...] = ()
    stewardship_assignment_rules: tuple[dict[str, object], ...] = ()
    governance_policy_rules: tuple[dict[str, object], ...] = ()
    governance_score_weights: dict[str, int] | None = None
    trust_score_domain_adjustments: dict[str, int] | None = None
    trust_score_criticality_adjustments: dict[str, int] | None = None
    governance_notifications_enabled: bool = True
    governance_notification_repeat_days: int = 7
    governance_notification_critical_repeat_hours: int = 24
    pipeline_failure_owner_sla_hours: int = 24
    platform_job_running_attention_minutes: int = 120
    platform_job_running_critical_hours: int = 24
    platform_job_next_expected_delay_minutes: int = 60
    platform_recent_success_window_hours: int = 72
    operational_high_volume_threshold_rows: int = 100000
    governance_high_usage_click_threshold: int = 20
    dq_operational_failure_penalty_points: int = 15
    dq_operational_stale_penalty_points: int = 8
    dq_operational_recurrent_penalty_points: int = 5
    airflow_ui_base_url: str | None = None


def _coerce_module_tokens(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None or raw_value == "":
        return ()
    parts = [part.strip().lower() for part in raw_value.split(",")]
    return tuple(part for part in parts if part)


def _coerce_positive(value: int | None, fallback: int) -> int:
    if value is None:
        return fallback
    return max(int(value), 1)


def _coerce_non_negative(value: int | None, fallback: int) -> int:
    if value is None:
        return fallback
    return max(int(value), 0)


def _coerce_governance_score_weights(raw_value: str | None) -> dict[str, int]:
    if raw_value is None or raw_value == "":
        return dict(DEFAULT_GOVERNANCE_SCORE_WEIGHTS)
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return dict(DEFAULT_GOVERNANCE_SCORE_WEIGHTS)
    if not isinstance(parsed, dict):
        return dict(DEFAULT_GOVERNANCE_SCORE_WEIGHTS)
    return normalize_governance_score_weights(parsed)


def _coerce_trust_score_adjustments(raw_value: str | None) -> dict[str, int]:
    if raw_value is None or raw_value == "":
        return {}
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return normalize_trust_score_adjustments(parsed)


def _coerce_stewardship_assignment_rules(raw_value: str | None) -> tuple[dict[str, object], ...]:
    if raw_value is None or raw_value == "":
        return ()
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return ()
    if not isinstance(parsed, list):
        return ()
    normalized: list[dict[str, object]] = []
    for index, item in enumerate(parsed, start=1):
        if not isinstance(item, dict):
            continue
        request_type = str(item.get("request_type") or "any").strip().lower() or "any"
        domain_name = str(item.get("domain_name") or "").strip() or None
        owner_area = str(item.get("owner_area") or "").strip() or None
        approver_user_id_raw = item.get("approver_user_id")
        if approver_user_id_raw in {None, ""}:
            continue
        try:
            approver_user_id = max(int(approver_user_id_raw), 1)
        except Exception:
            continue
        priority_raw = item.get("priority", 100)
        try:
            priority = max(int(priority_raw), 1)
        except Exception:
            priority = 100
        if request_type == "any" and domain_name is None and owner_area is None:
            continue
        normalized.append(
            {
                "key": str(item.get("key") or f"rule-{index}"),
                "request_type": request_type,
                "domain_name": domain_name,
                "owner_area": owner_area,
                "approver_user_id": approver_user_id,
                "priority": priority,
                "is_active": bool(item.get("is_active", True)),
            }
        )
    return tuple(
        sorted(
            normalized,
            key=lambda item: (
                int(item["priority"]),
                0 if item["request_type"] != "any" else 1,
                0 if item["domain_name"] else 1,
                0 if item["owner_area"] else 1,
                str(item["key"]),
            ),
        )
    )


def _normalize_dt(value):
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_or_create_governance_settings(session: Session) -> GovernanceSettings:
    settings = session.get(GovernanceSettings, 1)
    if settings is None:
        settings = GovernanceSettings(id=1)
        session.add(settings)
        session.flush()
    return settings


def get_governance_settings_snapshot(session: Session) -> GovernanceSettingsSnapshot:
    settings = get_or_create_governance_settings(session)
    return GovernanceSettingsSnapshot(
        owner_review_interval_days=_coerce_positive(settings.owner_review_interval_days, 90),
        privacy_review_interval_days=_coerce_positive(settings.privacy_review_interval_days, 180),
        sensitive_privacy_review_interval_days=_coerce_positive(settings.sensitive_privacy_review_interval_days, 90),
        certification_review_interval_days=_coerce_positive(settings.certification_review_interval_days, 180),
        certification_review_sla_days=_coerce_positive(settings.certification_review_sla_days, 7),
        certification_revalidation_window_days=_coerce_positive(settings.certification_revalidation_window_days, 30),
        audit_log_retention_days=_coerce_positive(getattr(settings, "audit_log_retention_days", None), 730),
        audit_log_archive_retention_days=_coerce_positive(getattr(settings, "audit_log_archive_retention_days", None), 2555),
        access_log_retention_days=_coerce_positive(getattr(settings, "access_log_retention_days", None), 30),
        access_log_archive_retention_days=_coerce_positive(getattr(settings, "access_log_archive_retention_days", None), 365),
        platform_usage_event_retention_days=_coerce_positive(getattr(settings, "platform_usage_event_retention_days", None), 180),
        search_result_click_retention_days=_coerce_positive(getattr(settings, "search_result_click_retention_days", None), 180),
        legacy_api_cutoff_window_days=_coerce_positive(getattr(settings, "legacy_api_cutoff_window_days", None), 30),
        legacy_api_disabled_modules=_coerce_module_tokens(getattr(settings, "legacy_api_disabled_modules", None)),
        legacy_api_force_enabled_modules=_coerce_module_tokens(getattr(settings, "legacy_api_force_enabled_modules", None)),
        stewardship_assignment_rules=_coerce_stewardship_assignment_rules(getattr(settings, "stewardship_assignment_rules", None)),
        governance_policy_rules=tuple(
            normalize_governance_policy_rules(getattr(settings, "governance_policy_rules", None))
        ),
        governance_score_weights=_coerce_governance_score_weights(getattr(settings, "governance_score_weights", None)),
        trust_score_domain_adjustments=_coerce_trust_score_adjustments(
            getattr(settings, "trust_score_domain_adjustments", None)
        ),
        trust_score_criticality_adjustments=_coerce_trust_score_adjustments(
            getattr(settings, "trust_score_criticality_adjustments", None)
        ),
        governance_notifications_enabled=bool(getattr(settings, "governance_notifications_enabled", True)),
        governance_notification_repeat_days=_coerce_positive(
            getattr(settings, "governance_notification_repeat_days", None),
            7,
        ),
        governance_notification_critical_repeat_hours=_coerce_positive(
            getattr(settings, "governance_notification_critical_repeat_hours", None),
            24,
        ),
        pipeline_failure_owner_sla_hours=_coerce_positive(
            getattr(settings, "pipeline_failure_owner_sla_hours", None),
            24,
        ),
        platform_job_running_attention_minutes=_coerce_positive(
            getattr(settings, "platform_job_running_attention_minutes", None),
            120,
        ),
        platform_job_running_critical_hours=_coerce_positive(
            getattr(settings, "platform_job_running_critical_hours", None),
            24,
        ),
        platform_job_next_expected_delay_minutes=_coerce_positive(
            getattr(settings, "platform_job_next_expected_delay_minutes", None),
            60,
        ),
        platform_recent_success_window_hours=_coerce_positive(
            getattr(settings, "platform_recent_success_window_hours", None),
            72,
        ),
        operational_high_volume_threshold_rows=_coerce_positive(
            getattr(settings, "operational_high_volume_threshold_rows", None),
            100000,
        ),
        governance_high_usage_click_threshold=_coerce_positive(
            getattr(settings, "governance_high_usage_click_threshold", None),
            20,
        ),
        dq_operational_failure_penalty_points=_coerce_non_negative(
            getattr(settings, "dq_operational_failure_penalty_points", None),
            15,
        ),
        dq_operational_stale_penalty_points=_coerce_non_negative(
            getattr(settings, "dq_operational_stale_penalty_points", None),
            8,
        ),
        dq_operational_recurrent_penalty_points=_coerce_non_negative(
            getattr(settings, "dq_operational_recurrent_penalty_points", None),
            5,
        ),
        airflow_ui_base_url=(getattr(settings, "airflow_ui_base_url", None) or None),
    )


def get_effective_legacy_api_disabled_modules(session: Session) -> tuple[str, ...]:
    from datetime import datetime, timedelta, timezone

    from t2c_data.features.platform.analytics import legacy_api_usage_stats_by_module

    snapshot = get_governance_settings_snapshot(session)
    since = datetime.now(timezone.utc) - timedelta(days=max(snapshot.legacy_api_cutoff_window_days, 1))
    usage_by_module = legacy_api_usage_stats_by_module(session, days=snapshot.legacy_api_cutoff_window_days)
    auto_disabled = {
        module
        for module in LEGACY_API_AUTO_CUTOFF_MODULES
        if (
            int(usage_by_module.get(module, {}).get("hits_in_window", 0) or 0) <= 0
            and module not in snapshot.legacy_api_force_enabled_modules
        )
    }
    auto_disabled.update(
        module
        for module in LEGACY_API_MANAGED_MODULES
        if (
            module not in LEGACY_API_AUTO_CUTOFF_MODULES
            and _normalize_dt(usage_by_module.get(module, {}).get("last_hit_at")) is not None
            and _normalize_dt(usage_by_module[module]["last_hit_at"]) < since
            and module not in snapshot.legacy_api_force_enabled_modules
        )
    )
    explicit_disabled = set(snapshot.legacy_api_disabled_modules)
    return tuple(sorted(explicit_disabled.union(auto_disabled)))
