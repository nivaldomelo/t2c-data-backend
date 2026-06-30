from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException, status

from t2c_data.features.datasource import scheduler


class _FakeSession:
    def __init__(self) -> None:
        self.datasource = SimpleNamespace(id=11, name="warehouse", db_type="postgres")
        self.scalar_value = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def get(self, model, identity):  # noqa: ANN001
        if identity == self.datasource.id:
            return self.datasource
        return None

    def scalar(self, _statement):  # noqa: ANN001
        return self.scalar_value

    def add(self, _obj) -> None:
        return None

    def flush(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_datasource_scan_scheduler_skips_conflict_without_failing_cycle(monkeypatch) -> None:
    fake_session = _FakeSession()
    schedule = SimpleNamespace(id=7, datasource_id=fake_session.datasource.id)
    calls: list[tuple[str, dict[str, object]]] = []
    exception_logs: list[tuple[tuple[object, ...], dict[str, object]]] = []

    monkeypatch.setattr(scheduler, "SessionLocal", lambda: fake_session)
    monkeypatch.setattr(scheduler, "_schedule_support_ready", lambda _session: True)
    monkeypatch.setattr(scheduler, "_advisory_lock", lambda _session: True)
    monkeypatch.setattr(scheduler, "_select_due_schedules", lambda _session: [schedule])
    monkeypatch.setattr(scheduler, "_try_update_scheduler_status_in_session", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler, "_release_advisory_lock", lambda _session: None)
    monkeypatch.setattr(
        scheduler,
        "mark_scan_schedule_queued",
        lambda _session, schedule_id, queued_at=None: calls.append(("queued", {"schedule_id": schedule_id, "queued_at": queued_at})),
    )
    monkeypatch.setattr(
        scheduler,
        "update_scan_schedule_run_state",
        lambda _session, **kwargs: calls.append(("state", kwargs)),
    )
    monkeypatch.setattr(
        scheduler,
        "enqueue_datasource_scan",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma execução ativa ou enfileirada para este job.")
        ),
    )
    monkeypatch.setattr(
        scheduler.logger,
        "exception",
        lambda *args, **kwargs: exception_logs.append((args, kwargs)),
    )

    summary = scheduler.run_datasource_scan_scheduler_cycle(force=True)

    assert summary["due_count"] == 1
    assert summary["success_count"] == 0
    assert summary["failed_count"] == 0
    assert summary["skipped_count"] == 1
    assert summary["next_expected_run_at"] == fake_session.scalar_value.isoformat()
    assert any(call[0] == "queued" for call in calls)
    assert any(call[0] == "state" and call[1]["status"] == "skipped" for call in calls)
    assert not exception_logs
