from __future__ import annotations

import asyncio
import logging
from time import monotonic

from t2c_data.core.config import settings
from t2c_data.core.logging import setup_logging
from t2c_data.features.platform.job_worker import process_next_integration_job
from t2c_data.features.platform.scheduler import run_platform_maintenance_cycle

setup_logging()
logger = logging.getLogger(__name__)


async def _main() -> None:
    poll_interval_seconds = max(int(settings.platform_read_model_refresh_interval_minutes or 30), 1) * 60
    heartbeat_interval_seconds = max(
        min(float(settings.platform_worker_heartbeat_grace_seconds or 90) / 3.0, 15.0),
        1.0,
    )
    next_maintenance_run_at = 0.0
    logger.info(
        "starting dedicated platform maintenance worker poll_interval_seconds=%s heartbeat_interval_seconds=%s",
        poll_interval_seconds,
        heartbeat_interval_seconds,
    )
    while True:
        now = monotonic()
        if now >= next_maintenance_run_at:
            run_platform_maintenance_cycle(trigger="scheduled", scheduler_mode="dedicated")
            next_maintenance_run_at = now + poll_interval_seconds
        processed = process_next_integration_job(source="platform", job_type="maintenance")
        if processed is None:
            await asyncio.sleep(heartbeat_interval_seconds)
        else:
            await asyncio.sleep(0)


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
