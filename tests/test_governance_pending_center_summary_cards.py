from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.governance.queries import _pending_summary_cards


def test_pending_summary_cards_are_derived_from_pending_items() -> None:
    items = [
        {"key": "no_owner", "origin": "governance"},
        {"key": "no_description", "origin": "metadata"},
        {"key": "no_classification", "origin": "governance"},
        {"key": "no_sla", "origin": "governance"},
        {"key": "owner_review_due", "origin": "governance"},
        {"key": "no_certification", "origin": "certification"},
        {"key": "critical_incident", "origin": "operations"},
    ]
    summary = _pending_summary_cards(
        items,
        stewardship_summary={"my_approvals_pending": 4, "my_owner_queue": 7},
        notification_summary={
            "active_total": 9,
            "due_now_total": 2,
            "critical_total": 1,
            "operational_total": 3,
            "quality_total": 5,
            "incident_total": 6,
        },
    )

    assert summary["stewardship_pending"] == 6
    assert summary["without_approver"] == 1
    assert summary["reviews"] == 1
    assert summary["certification"] == 1
    assert summary["my_approval"] == 4
    assert summary["my_queue"] == 7
    assert summary["active_notifications"] == 9
    assert summary["ready_to_resend"] == 2
    assert summary["critical"] == 1
    assert summary["operation"] == 3
    assert summary["quality_incidents"] == 11


if __name__ == "__main__":
    test_pending_summary_cards_are_derived_from_pending_items()
    print("governance pending center summary cards tests: OK")
