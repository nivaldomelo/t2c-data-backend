from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException, status

from t2c_data.features.data_quality import scheduler as dq_scheduler
from t2c_data.features.data_quality import profiling_scheduler


class _FakeSession:
    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def close(self) -> None:
        return None


def test_dq_profiling_scheduler_skips_running_job_conflict(monkeypatch) -> None:
    fake_session = _FakeSession()

    monkeypatch.setattr(profiling_scheduler, "SessionLocal", lambda: fake_session)
    monkeypatch.setattr(
        profiling_scheduler,
        "maybe_start_integration_job",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Já existe uma execução em andamento para este job.",
            )
        ),
    )
    monkeypatch.setattr(profiling_scheduler, "_try_update_scheduler_status_in_session", lambda *args, **kwargs: True)
    monkeypatch.setattr(profiling_scheduler, "_release_scheduler_lock", lambda *args, **kwargs: None)

    summary = profiling_scheduler.run_dq_profiling_scheduler_cycle(trigger="scheduled", scheduler_mode="embedded")

    assert summary["skipped"] == "job_already_running"
    assert summary["failed"] == 0


def test_dq_scheduler_skips_running_job_conflict(monkeypatch) -> None:
    fake_session = _FakeSession()

    monkeypatch.setattr(dq_scheduler, "SessionLocal", lambda: fake_session)
    monkeypatch.setattr(
        dq_scheduler,
        "maybe_start_integration_job",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Já existe uma execução em andamento para este job.",
            )
        ),
    )
    monkeypatch.setattr(dq_scheduler, "_try_update_scheduler_status_in_session", lambda *args, **kwargs: True)
    monkeypatch.setattr(dq_scheduler, "_release_scheduler_lock", lambda *args, **kwargs: None)

    summary = dq_scheduler.run_dq_scheduler_cycle(trigger="scheduled", scheduler_mode="embedded")

    assert summary["skipped"] == "job_already_running"
    assert summary["failed_rules"] == []
