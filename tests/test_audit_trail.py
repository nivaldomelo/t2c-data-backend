from __future__ import annotations

from contextlib import AbstractContextManager
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from t2c_data.main import AuditRequestMiddleware, _sanitize_query_param_value
from t2c_data.core.network import get_request_client_ip
from t2c_data.services.audit import write_audit_log_sync


class _FakeSession:
    def __init__(self) -> None:
        self.added = []
        self.committed = False

    def add(self, obj):  # noqa: ANN001
        self.added.append(obj)

    def commit(self) -> None:
        self.committed = True


class _FakeSessionCtx(AbstractContextManager):
    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    def __enter__(self) -> _FakeSession:
        return self._session

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


def test_audit_request_middleware_writes_http_request(monkeypatch):
    app = FastAPI()
    app.add_middleware(AuditRequestMiddleware)

    @app.get("/api/test")
    def _test():
        return {"ok": True}

    calls = []
    fake_session = _FakeSession()

    def fake_session_local():
        return _FakeSessionCtx(fake_session)

    def fake_write(session, **kwargs):  # noqa: ANN001
        calls.append((session, kwargs))

    monkeypatch.setattr("t2c_data.main.SessionLocal", fake_session_local)
    monkeypatch.setattr("t2c_data.main.commit_access_log_with_repair", fake_write)

    client = TestClient(app)
    response = client.get("/api/test?foo=bar", headers={"User-Agent": "pytest"})

    assert response.status_code == 200
    assert "X-Request-Id" in response.headers
    assert calls, "expected middleware to write audit log"
    _, payload = calls[0]
    assert payload["route"] == "/api/test"
    assert payload["method"] == "GET"
    assert payload["status_code"] == 200
    assert payload["metadata"]["query_params"]["foo"] == "bar"
    assert "duration_ms" in payload["metadata"]


def test_write_audit_log_redacts_and_truncates_sensitive_fields():
    session = _FakeSession()
    large_text = "x" * 60000
    write_audit_log_sync(
        session,
        action="test",
        after={
            "password": "secret123",
            "nested": {"token": "abc", "connection_uri": "postgresql://user:pass@host/db"},
            "payload": large_text,
        },
        metadata={"api_key": "k", "dsn": "postgresql://a:b@localhost/db"},
    )

    assert len(session.added) == 1
    entry = session.added[0]
    # after_json should be truncated summary because payload exceeds limit
    assert entry.after_json["truncated"] is True
    # metadata stays small, should be redacted, not truncated
    assert entry.metadata_json["api_key"] == "***"
    assert entry.metadata_json["dsn"] == "***"

    # Validate redaction on non-truncated payload path via second write
    session2 = _FakeSession()
    write_audit_log_sync(
        session2,
        action="test",
        after={"password": "p", "token": "t", "uri": "postgresql://user:pass@host/db"},
    )
    entry2 = session2.added[0]
    assert entry2.after_json["password"] == "***"
    assert entry2.after_json["token"] == "***"
    assert entry2.after_json["uri"] == "postgresql://user:********@host/db"


def test_sanitize_query_param_value_redacts_sensitive_and_truncates_long_values():
    assert _sanitize_query_param_value("password", "super-secret") == "[redacted]"
    assert _sanitize_query_param_value("search", "x" * 200).endswith("[truncated]")
    assert _sanitize_query_param_value("tags", ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]) == [
        "alpha",
        "beta",
        "gamma",
        "delta",
        "epsilon",
        "...(+1 more)",
    ]


def test_get_request_client_ip_prefers_socket_peer():
    request = SimpleNamespace(
        headers={"x-forwarded-for": "203.0.113.10, 10.0.0.2", "x-real-ip": "198.51.100.2"},
        client=SimpleNamespace(host="127.0.0.1"),
    )

    assert get_request_client_ip(request) == "127.0.0.1"
