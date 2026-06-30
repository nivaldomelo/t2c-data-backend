from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from t2c_data.api.auth import _check_login_rate_limit
from t2c_data.core.config import settings


class _FakeSession:
    def __init__(self, count: int) -> None:
        self.count = count
        self.queries = []

    def scalar(self, statement):  # noqa: ANN001
        self.queries.append(statement)
        return self.count


def test_check_login_rate_limit_allows_within_threshold(monkeypatch) -> None:
    monkeypatch.setattr(settings, "auth_rate_limit_attempts", 3)
    monkeypatch.setattr(settings, "auth_rate_limit_window_seconds", 300)
    session = _FakeSession(count=2)
    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))

    _check_login_rate_limit(session, request, "admin@andromeda.com")

    assert len(session.queries) == 1


def test_check_login_rate_limit_blocks_when_threshold_reached(monkeypatch) -> None:
    monkeypatch.setattr(settings, "auth_rate_limit_attempts", 2)
    monkeypatch.setattr(settings, "auth_rate_limit_window_seconds", 300)
    session = _FakeSession(count=2)
    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))

    with pytest.raises(HTTPException) as exc_info:
        _check_login_rate_limit(session, request, "admin@andromeda.com")

    assert exc_info.value.status_code == 429
    assert len(session.queries) == 1


def test_check_login_rate_limit_uses_socket_peer(monkeypatch) -> None:
    monkeypatch.setattr(settings, "auth_rate_limit_attempts", 2)
    monkeypatch.setattr(settings, "auth_rate_limit_window_seconds", 300)
    session = _FakeSession(count=1)

    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))

    _check_login_rate_limit(session, request, "admin@andromeda.com")

    assert len(session.queries) == 1
