from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data import main


def test_log_metabase_startup_health_warns_when_unavailable(monkeypatch, caplog) -> None:
    health = SimpleNamespace(
        available=False,
        status="DOWN",
        integration_status="unavailable",
        configured=True,
        enabled=True,
        instance_base_url="http://metabase-metabase-1:3000",
        message="Metabase indisponível no startup.",
    )

    def fake_load_metabase_integration_health(session):  # noqa: ANN001
        return health

    monkeypatch.setattr(main, "load_metabase_integration_health", fake_load_metabase_integration_health)

    with caplog.at_level("WARNING"):
        main._log_metabase_startup_health(object())

    assert "metabase startup health unavailable" in caplog.text
    assert "metabase-metabase-1:3000" in caplog.text


def test_log_metabase_startup_health_logs_failures_without_raising(monkeypatch, caplog) -> None:
    def fake_load_metabase_integration_health(session):  # noqa: ANN001
        raise RuntimeError("boom")

    monkeypatch.setattr(main, "load_metabase_integration_health", fake_load_metabase_integration_health)

    with caplog.at_level("WARNING"):
        main._log_metabase_startup_health(object())

    assert "metabase startup health check failed" in caplog.text
