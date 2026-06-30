from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi.testclient import TestClient

from t2c_data.features.platform.event_catalog import list_supported_platform_events, supported_platform_event_keys
from t2c_data.features.notifications.templates import build_slack_payload, build_teams_payload
from t2c_data.main import app


def test_supported_platform_events_exclude_webhook_keys() -> None:
    keys = supported_platform_event_keys()
    response = list_supported_platform_events()

    assert "platform.webhook_subscription.create" not in keys
    assert "platform.webhook_subscription.test" not in keys
    assert "platform.webhook_delivery.retry" not in keys
    assert all("webhook" not in item["event_key"] for item in response["items"])


def test_notification_templates_target_inbox_for_test_actions() -> None:
    notification = type(
        "Notification",
        (),
        {
            "title": "Teste",
            "message": "Mensagem",
            "severity": "info",
            "category": "platform",
            "href": "/inbox",
            "source_entity_type": "platform",
            "context_json": {},
            "created_at": None,
        },
    )()

    slack_payload = build_slack_payload(notification, is_test=True)
    teams_payload = build_teams_payload(notification, is_test=True)

    slack_action = next(block for block in slack_payload["blocks"] if block["type"] == "actions")
    teams_action = teams_payload["attachments"][0]["content"].get("actions", [])

    assert slack_action["elements"][0]["url"].endswith("/inbox")
    assert not teams_action or teams_action[0]["url"].endswith("/inbox")


def test_removed_webhook_routes_return_not_found() -> None:
    client = TestClient(app)

    assert client.get("/v1/platform/webhooks/subscriptions").status_code == 404
    assert client.post("/v1/platform/webhooks/deliveries/dispatch").status_code == 404
