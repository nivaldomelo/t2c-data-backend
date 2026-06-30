from __future__ import annotations

import os
from datetime import datetime, date, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.data_quality.schedule_utils import compute_next_run_at, describe_schedule, infer_schedule_mode, validate_schedule_payload


def test_schedule_utils_infer_legacy_interval_mode() -> None:
    rule = SimpleNamespace(schedule_mode="manual", schedule_enabled=True, schedule_every_minutes=30)
    assert infer_schedule_mode(
        schedule_mode=rule.schedule_mode,
        schedule_enabled=rule.schedule_enabled,
        schedule_every_minutes=rule.schedule_every_minutes,
    ) == "interval"


def test_schedule_utils_compute_daily_weekly_biweekly_monthly() -> None:
    reference = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)

    daily = SimpleNamespace(
        schedule_mode="daily",
        schedule_enabled=True,
        schedule_every_minutes=None,
        schedule_time="08:00",
        schedule_day_of_week=None,
        schedule_day_of_month=None,
        schedule_anchor_date=None,
        schedule_last_run_at=None,
        created_at=reference,
    )
    weekly = SimpleNamespace(
        schedule_mode="weekly",
        schedule_enabled=True,
        schedule_every_minutes=None,
        schedule_time="09:00",
        schedule_day_of_week=0,
        schedule_day_of_month=None,
        schedule_anchor_date=None,
        schedule_last_run_at=None,
        created_at=reference,
    )
    biweekly = SimpleNamespace(
        schedule_mode="biweekly",
        schedule_enabled=True,
        schedule_every_minutes=None,
        schedule_time="07:00",
        schedule_day_of_week=None,
        schedule_day_of_month=None,
        schedule_anchor_date=date(2026, 4, 1),
        schedule_last_run_at=None,
        created_at=reference,
    )
    monthly = SimpleNamespace(
        schedule_mode="monthly",
        schedule_enabled=True,
        schedule_every_minutes=None,
        schedule_time="06:00",
        schedule_day_of_week=None,
        schedule_day_of_month=1,
        schedule_anchor_date=None,
        schedule_last_run_at=None,
        created_at=reference,
    )

    assert compute_next_run_at(daily, reference=reference) == datetime(2026, 4, 7, 8, 0, tzinfo=timezone.utc)
    assert compute_next_run_at(weekly, reference=reference) == datetime(2026, 4, 13, 9, 0, tzinfo=timezone.utc)
    assert compute_next_run_at(biweekly, reference=reference) == datetime(2026, 4, 15, 7, 0, tzinfo=timezone.utc)
    assert compute_next_run_at(monthly, reference=reference) == datetime(2026, 5, 1, 6, 0, tzinfo=timezone.utc)


def test_schedule_utils_describe_schedule_formats_labels() -> None:
    rule = SimpleNamespace(
        schedule_mode="monthly",
        schedule_enabled=True,
        schedule_every_minutes=None,
        schedule_time="06:00",
        schedule_day_of_week=None,
        schedule_day_of_month=1,
        schedule_anchor_date=None,
        schedule_last_run_at=None,
        created_at=datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc),
    )
    assert describe_schedule(rule) == "Mensal no dia 1 às 06:00"


def test_schedule_utils_preserves_mode_when_disabled() -> None:
    normalized = validate_schedule_payload(
        {
            "schedule_mode": "weekly",
            "schedule_enabled": False,
            "schedule_day_of_week": 1,
            "schedule_time": "09:00",
        }
    )
    assert normalized["schedule_mode"] == "weekly"
    assert normalized["schedule_enabled"] is False
