from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from t2c_data.features.governance.settings import GovernanceSettingsSnapshot

OWNER_REVIEW_INTERVAL_DAYS = 90
PRIVACY_REVIEW_INTERVAL_DAYS = 180
SENSITIVE_PRIVACY_REVIEW_INTERVAL_DAYS = 90
CERTIFICATION_REVIEW_INTERVAL_DAYS = 180
CRITICAL_CHANGE_LOOKBACK_DAYS = 14


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _bool(value: Any) -> bool:
    return bool(value)


def owner_review_due(
    entity: Any,
    *,
    now: datetime | None = None,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> bool:
    now = now or datetime.now(timezone.utc)
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    if not _bool(getattr(entity, "owner_defined", False) or getattr(entity, "data_owner_id", None) or getattr(entity, "owner", None)):
        return False
    reviewed_at = _aware(getattr(entity, "owner_reviewed_at", None))
    if reviewed_at is None:
        return True
    return reviewed_at < now - timedelta(days=settings_snapshot.owner_review_interval_days)


def owner_review_next_at(
    entity: Any,
    *,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> datetime | None:
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    reviewed_at = _aware(getattr(entity, "owner_reviewed_at", None))
    if reviewed_at is None:
        return None
    return reviewed_at + timedelta(days=settings_snapshot.owner_review_interval_days)


def privacy_review_due(
    entity: Any,
    *,
    now: datetime | None = None,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> bool:
    now = now or datetime.now(timezone.utc)
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    has_sensitive_context = _bool(getattr(entity, "sensitivity_level", None)) or _bool(
        getattr(entity, "has_personal_data", False) or getattr(entity, "has_sensitive_personal_data", False)
    )
    certified = (getattr(entity, "certification_status", None) or "") == "certified"
    if not has_sensitive_context and not certified:
        return False
    reviewed_at = _aware(getattr(entity, "privacy_reviewed_at", None))
    interval = (
        settings_snapshot.sensitive_privacy_review_interval_days
        if has_sensitive_context
        else settings_snapshot.privacy_review_interval_days
    )
    if reviewed_at is None:
        return True
    return reviewed_at < now - timedelta(days=interval)


def privacy_review_next_at(
    entity: Any,
    *,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> datetime | None:
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    reviewed_at = _aware(getattr(entity, "privacy_reviewed_at", None))
    if reviewed_at is None:
        return None
    has_sensitive_context = _bool(getattr(entity, "sensitivity_level", None)) or _bool(
        getattr(entity, "has_personal_data", False) or getattr(entity, "has_sensitive_personal_data", False)
    )
    interval = (
        settings_snapshot.sensitive_privacy_review_interval_days
        if has_sensitive_context
        else settings_snapshot.privacy_review_interval_days
    )
    return reviewed_at + timedelta(days=interval)


def certification_review_due(
    entity: Any,
    *,
    now: datetime | None = None,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> bool:
    now = now or datetime.now(timezone.utc)
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    status = (getattr(entity, "certification_status", None) or "").strip().lower()
    if status not in {"certified", "revalidation_pending", "expired"}:
        return False
    expires_at = _aware(getattr(entity, "certification_expires_at", None))
    if expires_at is not None and expires_at <= now:
        return True
    review_at = _aware(getattr(entity, "certification_review_at", None))
    if review_at is not None and review_at <= now:
        return True
    decided_at = _aware(getattr(entity, "certification_decided_at", None))
    if decided_at is None:
        return True
    return decided_at < now - timedelta(days=settings_snapshot.certification_review_interval_days)


def certification_next_review_at(entity: Any) -> datetime | None:
    expires_at = _aware(getattr(entity, "certification_expires_at", None))
    if expires_at is not None:
        return expires_at
    return _aware(getattr(entity, "certification_review_at", None))


def review_due_label(*, owner_due: bool, privacy_due: bool, certification_due: bool) -> str | None:
    labels: list[str] = []
    if owner_due:
        labels.append("owner")
    if privacy_due:
        labels.append("privacidade")
    if certification_due:
        labels.append("certificação")
    if not labels:
        return None
    return ", ".join(labels)
