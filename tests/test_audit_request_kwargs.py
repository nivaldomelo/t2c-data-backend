from __future__ import annotations

from types import SimpleNamespace

from t2c_data.services.audit import request_audit_kwargs, request_audit_kwargs_without_user_email


def test_request_audit_kwargs_without_user_email_strips_duplicate_key() -> None:
    request = SimpleNamespace(
        headers={"user-agent": "pytest"},
        url=SimpleNamespace(path="/api/v1/auth/login"),
        method="POST",
        state=SimpleNamespace(request_id="req-123"),
    )
    user = SimpleNamespace(id=7, name="Admin", full_name="Admin User", email="admin@local")

    payload = request_audit_kwargs_without_user_email(request, user)

    assert payload["user_id"] == 7
    assert payload["actor_name"] == "Admin"
    assert payload["ip"] is None
    assert payload["user_agent"] == "pytest"
    assert payload["route"] == "/api/v1/auth/login"
    assert payload["method"] == "POST"
    assert payload["request_id"] == "req-123"
    assert "user_email" not in payload


def test_request_audit_kwargs_still_includes_user_email_for_existing_callers() -> None:
    request = SimpleNamespace(
        headers={"user-agent": "pytest"},
        url=SimpleNamespace(path="/api/v1/auth/login"),
        method="POST",
        state=SimpleNamespace(request_id="req-123"),
    )
    user = SimpleNamespace(id=7, name="Admin", full_name="Admin User", email="admin@local")

    payload = request_audit_kwargs(request, user)

    assert payload["user_email"] == "admin@local"
