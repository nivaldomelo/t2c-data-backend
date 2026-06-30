from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

import pytest

from t2c_data.core.config import Settings, embedded_scheduler_allowed, normalize_scheduler_mode, settings
from t2c_data.features.data_quality import profiling_scheduler
from t2c_data.features.data_quality import scheduler as dq_scheduler
from t2c_data.features.platform import scheduler as platform_scheduler


def _secure_prod_settings(**overrides):
    payload = {
        "_env_file": None,
        "database_url": "sqlite+pysqlite:///:memory:",
        "env": "prod",
        "jwt_secret_key": "prod-jwt-secret",
        "datasource_secret_key": "prod-datasource-secret",
        "admin_password": "strong-admin-password",
        "viewer_password": "strong-viewer-password",
    }
    payload.update(overrides)
    return Settings(**payload)


def test_worker_scheduler_mode_is_default_operational_mode() -> None:
    assert normalize_scheduler_mode(None) == "worker"
    assert normalize_scheduler_mode("dedicated") == "worker"


def test_dev_allows_embedded_dev_only_scheduler_mode() -> None:
    config = Settings(
        _env_file=None,
        database_url="sqlite+pysqlite:///:memory:",
        env="test",
        dq_scheduler_mode="embedded_dev_only",
    )

    assert config.dq_scheduler_mode == "embedded_dev_only"
    assert embedded_scheduler_allowed(config.dq_scheduler_mode, config.env)


@pytest.mark.parametrize(
    "field",
    [
        "dq_scheduler_mode",
        "dq_profiling_scheduler_mode",
        "datasource_scan_scheduler_mode",
        "data_lake_scan_scheduler_mode",
        "platform_scheduler_mode",
    ],
)
def test_prod_rejects_embedded_scheduler_modes(field: str) -> None:
    with pytest.raises(ValueError, match="Embedded schedulers are not allowed"):
        _secure_prod_settings(**{field: "embedded"})


def test_scheduler_cycles_fail_closed_when_embedded_is_forced_outside_dev(monkeypatch) -> None:
    monkeypatch.setattr(settings, "env", "prod")

    dq_summary = dq_scheduler.run_dq_scheduler_cycle(trigger="manual", scheduler_mode="embedded")
    profiling_summary = profiling_scheduler.run_dq_profiling_scheduler_cycle(trigger="manual", scheduler_mode="embedded")
    platform_summary = platform_scheduler.run_platform_maintenance_cycle(trigger="manual", scheduler_mode="embedded")

    assert dq_summary["skipped"] == "embedded_not_allowed"
    assert profiling_summary["skipped"] == "embedded_not_allowed"
    assert platform_summary["skipped"] == "embedded_not_allowed"
