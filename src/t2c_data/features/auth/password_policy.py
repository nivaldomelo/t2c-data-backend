"""Password expiration policy helpers (rotate every N days, warn, then block)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil

from t2c_data.core.config import settings
from t2c_data.models.auth import User


@dataclass(slots=True)
class PasswordExpiryStatus:
    changed_at: datetime
    expires_at: datetime
    days_remaining: int  # may be negative when already expired
    expired: bool
    warning: bool  # within the warning threshold and not expired


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def password_expiry_status(user: User, *, now: datetime | None = None) -> PasswordExpiryStatus:
    now = now or datetime.now(timezone.utc)
    max_age = int(getattr(settings, "password_max_age_days", 90) or 90)
    warn_days = int(getattr(settings, "password_expiry_warning_days", 10) or 10)
    changed_at = _aware(getattr(user, "password_changed_at", None) or getattr(user, "created_at", None) or now)
    expires_at = changed_at + timedelta(days=max_age)
    remaining_seconds = (expires_at - now).total_seconds()
    days_remaining = int(ceil(remaining_seconds / 86400)) if remaining_seconds > 0 else int(remaining_seconds // 86400)
    expired = now >= expires_at
    return PasswordExpiryStatus(
        changed_at=changed_at,
        expires_at=expires_at,
        days_remaining=days_remaining,
        expired=expired,
        warning=(not expired) and days_remaining <= warn_days,
    )
