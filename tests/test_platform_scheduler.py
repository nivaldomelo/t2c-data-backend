from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://user:pass@localhost/testdb")

from t2c_data.features.platform import scheduler as platform_scheduler


class PlatformSchedulerStatusTests(unittest.TestCase):
    def test_scheduler_status_snapshot_marks_stale_dedicated_worker(self) -> None:
        original_get_or_create = platform_scheduler._get_or_create_scheduler_status
        original_exists = platform_scheduler._scheduler_status_table_exists
        original_grace = platform_scheduler.settings.platform_scheduler_heartbeat_grace_minutes
        try:
            platform_scheduler._scheduler_status_table_exists = lambda session: True
            platform_scheduler._get_or_create_scheduler_status = lambda session: SimpleNamespace(
                scheduler_name="platform_maintenance",
                mode="dedicated",
                is_enabled=True,
                last_started_at="2026-03-28T00:00:00+00:00",
                last_heartbeat_at="2026-03-27T00:00:00+00:00",
                last_success_at="2026-03-27T00:00:00+00:00",
                last_failure_at=None,
                last_error=None,
                last_run_summary_json={"maintenance": {"audit_archived": 5}},
            )
            platform_scheduler.settings.platform_scheduler_heartbeat_grace_minutes = 1
            snapshot = platform_scheduler.scheduler_status_snapshot(session=object())
        finally:
            platform_scheduler._get_or_create_scheduler_status = original_get_or_create
            platform_scheduler._scheduler_status_table_exists = original_exists
            platform_scheduler.settings.platform_scheduler_heartbeat_grace_minutes = original_grace

        self.assertEqual(snapshot["health"], "stale")
        self.assertEqual(snapshot["mode"], "dedicated")
        self.assertTrue(snapshot["applicable"])
        self.assertEqual(snapshot["last_run_summary"]["maintenance"]["audit_archived"], 5)

    def test_scheduler_status_snapshot_falls_back_to_runtime_when_status_table_is_unavailable(self) -> None:
        original_exists = platform_scheduler._scheduler_status_table_exists
        original_runtime = platform_scheduler._runtime_state
        try:
            platform_scheduler._scheduler_status_table_exists = lambda session: False
            platform_scheduler._runtime_state = platform_scheduler.SchedulerRuntimeState(
                phase="bootstrap_failed",
                mode="embedded",
                is_enabled=True,
                bootstrap_attempts=3,
                last_error="status table unavailable",
                last_error_at="2026-03-28T12:00:00+00:00",
            )
            snapshot = platform_scheduler.scheduler_status_snapshot(session=object())
        finally:
            platform_scheduler._scheduler_status_table_exists = original_exists
            platform_scheduler._runtime_state = original_runtime

        self.assertEqual(snapshot["health"], "unavailable")
        self.assertEqual(snapshot["mode"], "embedded")
        self.assertTrue(snapshot["applicable"])
        self.assertEqual(snapshot["last_error"], "status table unavailable")

    def test_start_platform_scheduler_does_not_raise_without_running_loop(self) -> None:
        original_get_running_loop = platform_scheduler.asyncio.get_running_loop
        original_runtime = platform_scheduler._runtime_state
        try:
            def _raise_runtime_error():
                raise RuntimeError("loop missing")

            platform_scheduler.asyncio.get_running_loop = _raise_runtime_error
            platform_scheduler._runtime_state = platform_scheduler.SchedulerRuntimeState()
            platform_scheduler.start_platform_scheduler()
        finally:
            platform_scheduler.asyncio.get_running_loop = original_get_running_loop
            runtime_state = platform_scheduler._runtime_state
            platform_scheduler._runtime_state = original_runtime

        self.assertEqual(runtime_state.phase, "bootstrap_failed")
        self.assertEqual(runtime_state.last_error, "no running event loop available during startup")


if __name__ == "__main__":
    unittest.main()
