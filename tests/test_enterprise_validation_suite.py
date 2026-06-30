from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException
import pytest
from pydantic import ValidationError

from t2c_data.features.export_security import enforce_export_limit
from t2c_data.api.exports import export_job_status
from t2c_data.schemas.dq_rules import DQRuleCreate


def test_large_exports_are_truncated_before_delivery() -> None:
    rows = list(range(6000))
    bounded, truncated = enforce_export_limit(rows, limit=1000)

    assert truncated is True
    assert len(bounded) == 1000
    assert bounded[0] == 0
    assert bounded[-1] == 999


def test_export_job_status_requires_owner_or_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    job = SimpleNamespace(
        id=1,
        artifact_public_id="exp-1",
        artifact_storage_path="/tmp/exp-1.csv",
        artifact_filename="exp-1.csv",
        artifact_content_type="text/csv",
        artifact_expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
        requested_by_user_id=99,
        status="success",
        source="export",
        job_type="privacy_access.csv",
        context_json={},
        payload_json={},
        result_summary_json={"export_format": "csv"},
    )

    monkeypatch.setattr("t2c_data.api.exports.load_export_job_by_public_id", lambda db, public_id: job)

    with pytest.raises(HTTPException) as excinfo:
        export_job_status(
            "exp-1",
            db=SimpleNamespace(),
            current_user=SimpleNamespace(id=7, roles=[SimpleNamespace(name="viewer", permissions=[])]),
        )

    assert excinfo.value.status_code == 403


@pytest.mark.parametrize("legacy_field", ["sql_text", "custom_sql"])
def test_dq_visual_payload_rejects_legacy_sql_fields(legacy_field: str) -> None:
    payload = {
        "name": "Legacy SQL",
        "description": "não deve aceitar SQL livre",
        "table_id": 42,
        "table_fqn": "warehouse.sales.orders",
        "notification_recipient_user_ids": [1],
        "rule_type": "nullability",
        "severity": "high",
        "logic": "AND",
        "conditions": [{"column": "status", "operator": "not_null"}],
        "is_active": True,
        legacy_field: "SELECT 1",
    }

    with pytest.raises(ValidationError):
        DQRuleCreate.model_validate(payload)
