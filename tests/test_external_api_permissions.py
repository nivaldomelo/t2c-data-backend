from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

import pytest
from fastapi import HTTPException

from t2c_data.features.platform.api_keys import (
    is_api_key_ip_allowed,
    list_external_api_scopes,
    validate_expiration_policy,
    validate_scope_keys,
)
from t2c_data.models.platform import PlatformApiKey


def test_external_scope_catalog_includes_actions() -> None:
    groups = list_external_api_scopes()
    tags = next(item for item in groups if item["key"] == "tags")
    glossary = next(item for item in groups if item["key"] == "glossary")

    assert any(action["key"] == "tags.read" and action["available"] for action in tags["actions"])
    assert any(action["key"] == "tags.delete" and action["available"] for action in tags["actions"])
    assert any(action["key"] == "glossary.create" and action["available"] for action in glossary["actions"])


def test_write_scopes_auto_include_read() -> None:
    scopes = validate_scope_keys(["tags.create", "glossary.update"])

    assert scopes == ["tags.create", "tags.read", "glossary.update", "glossary.read"]


def test_unsupported_scopes_are_rejected() -> None:
    with pytest.raises(HTTPException) as exc_info:
        validate_scope_keys(["catalog.create"])

    assert "indisponíveis" in str(exc_info.value)


def test_write_keys_default_to_short_expiration() -> None:
    expires_at, expires_in_days = validate_expiration_policy(
        scopes=["tags.create"],
        expires_at=None,
        expires_in_days=None,
    )

    assert expires_at is None
    assert expires_in_days == 30


def test_delete_keys_have_tighter_expiration_limit() -> None:
    with pytest.raises(HTTPException):
        validate_expiration_policy(
            scopes=["tags.delete"],
            expires_at=datetime.now(timezone.utc) + timedelta(days=15),
            expires_in_days=15,
        )


def test_api_key_ip_allowlist_matches_exact_and_cidr() -> None:
    key = PlatformApiKey(
        public_id="ext-key",
        name="External Integration",
        status="active",
        scopes_json=["tags.read"],
        environment="shared",
        allowed_ips_json=["10.0.0.10", "10.0.1.0/24"],
        token_hash="hash",
        token_prefix="hash",
    )

    assert is_api_key_ip_allowed(key, "10.0.0.10") is True
    assert is_api_key_ip_allowed(key, "10.0.1.42") is True
    assert is_api_key_ip_allowed(key, "10.0.2.1") is False
