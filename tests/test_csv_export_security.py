from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from t2c_data.features.export_security import (
    audit_export_event,
    classify_export_sensitivity,
    enforce_export_limit,
    enforce_export_permission,
    redact_export_row,
    redact_export_value,
    resolve_export_limit,
)


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.committed = False

    def add(self, obj: object) -> None:
        self.added.append(obj)

    def commit(self) -> None:
        self.committed = True


def _fake_request():
    return SimpleNamespace(
        url=SimpleNamespace(path="/api/v1/privacy-access/export.csv"),
        method="GET",
        headers={"user-agent": "pytest"},
        state=SimpleNamespace(request_id="req-export-1"),
        client=SimpleNamespace(host="127.0.0.1"),
    )


def _fake_user():
    return SimpleNamespace(id=7, email="editor@example.com", name="Editor", full_name="Editor User")


def _fake_user_with_permissions(*permissions: str, is_admin: bool = False):
    role_name = "admin" if is_admin else "editor"
    permission_objs = [SimpleNamespace(name=permission) for permission in permissions]
    role = SimpleNamespace(name=role_name, permissions=permission_objs)
    return SimpleNamespace(
        id=7,
        email="editor@example.com",
        name="Editor",
        full_name="Editor User",
        roles=[role],
    )


def test_redact_export_value_masks_sensitive_text_and_uris() -> None:
    assert redact_export_value("postgresql://user:secret@db.local/catalog", field_name="connection_uri") == "[redacted]"
    assert redact_export_value("super-secret", field_name="password") == "[redacted]"
    assert redact_export_value({"api_key": "abc123"}, field_name="api_key") == '{"api_key": "[redacted]"}'
    assert redact_export_value({"webhook_url": "https://hooks.example.com/secret"}, field_name="webhook_url") == '{"webhook_url": "[redacted]"}'


def test_redact_export_row_masks_known_sensitive_fields() -> None:
    row = redact_export_row(
        {
            "name": "Fonte Gold",
            "password": "super-secret",
            "connection_uri": "postgresql://user:secret@db.local/catalog",
        }
    )

    assert row["name"] == "Fonte Gold"
    assert row["password"] == "[redacted]"
    assert row["connection_uri"] == "[redacted]"


def test_enforce_export_limit_truncates_large_exports() -> None:
    rows, truncated = enforce_export_limit(list(range(6000)), limit=5000)
    assert truncated is True
    assert len(rows) == 5000
    assert rows[0] == 0
    assert rows[-1] == 4999


@pytest.mark.parametrize(
    ("source_module", "entity_type", "expected"),
    [
        ("privacy_access", "privacy_asset", "regulatory"),
        ("audit", "audit_entry", "regulatory"),
        ("certification", "certification_queue", "high"),
        ("governance", "ownership_export", "operational_critical"),
        ("dashboard", "dashboard_campaign", "operational_critical"),
        ("platform", "platform_cockpit", "operational_critical"),
        ("io", "bundle", "operational_critical"),
        ("catalog", "column_dictionary", "medium"),
        ("lineage", "lineage_relation", "medium"),
        ("unknown", "privacy_asset", "regulatory"),
        ("unknown", "other", "low"),
    ],
)
def test_classify_export_sensitivity(source_module: str, entity_type: str, expected: str) -> None:
    assert classify_export_sensitivity(source_module=source_module, entity_type=entity_type) == expected


@pytest.mark.parametrize(
    ("source_module", "entity_type", "expected"),
    [
        ("audit", "audit_history", 1000),
        ("privacy_access", "privacy_asset", 1000),
        ("certification", "certification_queue", 2000),
        ("governance", "ownership_export", 2000),
        ("dashboard", "dashboard_campaign", 2000),
        ("platform", "platform_cockpit", 2000),
        ("io", "bundle", 2000),
        ("catalog", "column_dictionary", 2500),
        ("unknown", "other", 5000),
    ],
)
def test_resolve_export_limit_matches_sensitivity(source_module: str, entity_type: str, expected: int) -> None:
    assert resolve_export_limit(source_module=source_module, entity_type=entity_type) == expected


def test_enforce_export_permission_requires_explicit_scope() -> None:
    user = _fake_user_with_permissions("privacy_access:read")

    with pytest.raises(HTTPException) as excinfo:
        enforce_export_permission(user, "privacy_access:export")

    assert excinfo.value.status_code == 403


def test_enforce_export_permission_accepts_admin_and_specific_scope() -> None:
    scoped_user = _fake_user_with_permissions("privacy_access:export")
    dotted_user = _fake_user_with_permissions("owners.export")
    admin_user = _fake_user_with_permissions(is_admin=True)

    assert enforce_export_permission(scoped_user, "privacy_access:export") is scoped_user
    assert enforce_export_permission(dotted_user, "owners.export") is dotted_user
    assert enforce_export_permission(admin_user, "privacy_access:export") is admin_user


def test_audit_export_event_records_sensitive_audit_entry() -> None:
    session = _FakeSession()

    audit_export_event(
        session,
        request=_fake_request(),
        current_user=_fake_user(),
        action="privacy_access.export_csv",
        entity_type="privacy_asset",
        source_module="privacy_access",
        row_count=120,
        filters={"password": "secret", "q": "clientes"},
        limit=5000,
        truncated=False,
    )

    assert session.committed is True
    assert len(session.added) == 1
    entry = session.added[0]
    assert getattr(entry, "action") == "privacy_access.export_csv"
    assert getattr(entry, "is_sensitive_change") is True
    assert getattr(entry, "sensitive_category") == "export"
    assert getattr(entry, "metadata_json")["filters"]["password"] == "[redacted]"
    assert getattr(entry, "metadata_json")["row_count"] == 120
    assert getattr(entry, "metadata_json")["endpoint"] == "/api/v1/privacy-access/export.csv"
    assert getattr(entry, "metadata_json")["http_method"] == "GET"
    assert getattr(entry, "metadata_json")["export_format"] == "unknown"
    assert getattr(entry, "metadata_json")["classification"] == "regulatory"
    assert getattr(entry, "metadata_json")["is_large_export"] is False
