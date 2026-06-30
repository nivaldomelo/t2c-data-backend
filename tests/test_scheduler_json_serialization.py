from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.platform import scheduler


def test_scheduler_summary_normalizes_datetime() -> None:
    payload = {
        "refreshed_at": datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
        "metrics": {"duration": Decimal("12.5")},
    }
    normalized = scheduler._normalize_summary_payload(payload)

    assert isinstance(normalized, dict)
    assert normalized["refreshed_at"].startswith("2026-04-13")
    assert normalized["metrics"]["duration"] == 12.5


if __name__ == "__main__":
    test_scheduler_summary_normalizes_datetime()
    print("scheduler json serialization tests: OK")
