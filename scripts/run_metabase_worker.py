"""Metabase worker: schedules automatic syncs AND executes queued sync jobs.

Runs as a single dedicated long-running process (stable in dev and prod, unlike the
backend's --reload). Each tick it:
  1) runs the schedule cycle (enqueues a sync when a due slot is reached — guarded so it
     enqueues at most once per slot), and
  2) drains queued metabase sync jobs (executes the actual sync).
"""

from __future__ import annotations

import logging
import time

from t2c_data.features.metabase.scheduler import run_metabase_sync_scheduler_cycle
from t2c_data.features.platform.job_worker import process_next_integration_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("metabase_worker")

POLL_SECONDS = 30
MAX_JOBS_PER_TICK = 50


def main() -> None:
    logger.info("metabase worker started (scheduler + sync consumer), poll=%ss", POLL_SECONDS)
    while True:
        try:
            result = run_metabase_sync_scheduler_cycle()
            if result.get("enqueued"):
                logger.info("scheduler enqueued %s sync(s)", result.get("enqueued"))
        except Exception:  # noqa: BLE001
            logger.exception("metabase scheduler cycle failed")

        try:
            processed = 0
            while process_next_integration_job(source="metabase", job_type="sync") is not None:
                processed += 1
                if processed >= MAX_JOBS_PER_TICK:
                    break
            if processed:
                logger.info("processed %s metabase sync job(s)", processed)
        except Exception:  # noqa: BLE001
            logger.exception("metabase job processing failed")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
