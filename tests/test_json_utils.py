from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.core.json_utils import make_json_safe


def test_make_json_safe_handles_nested_datetime_and_decimal_values() -> None:
    payload = {
        "created_at": datetime(2026, 4, 14, 10, 30, tzinfo=timezone.utc),
        "effective_date": date(2026, 4, 14),
        "amount": Decimal("12.50"),
        "nested": [
            {"updated_at": datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc)},
            (Decimal("1.25"), {"ns": SimpleNamespace(completed_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc))}),
        ],
    }

    normalized = make_json_safe(payload)

    assert normalized["created_at"] == "2026-04-14T10:30:00+00:00"
    assert normalized["effective_date"] == "2026-04-14"
    assert normalized["amount"] == 12.5
    assert normalized["nested"][0]["updated_at"] == "2026-04-14T11:00:00+00:00"
    assert normalized["nested"][1][0] == 1.25
    assert normalized["nested"][1][1]["ns"]["completed_at"] == "2026-04-14T12:00:00+00:00"


if __name__ == "__main__":
    test_make_json_safe_handles_nested_datetime_and_decimal_values()
    print("json utils tests: OK")
