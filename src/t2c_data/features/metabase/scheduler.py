"""Lightweight embedded scheduler for automatic Metabase syncs.

Default cadence: every 2h between 08:00 and 18:00, Monday–Friday (configurable via
METABASE_SYNC_* settings). It only ENQUEUES a sync job per due slot — the
metabase-sync-worker (run_platform_job_worker --source metabase) executes it.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException, status
from sqlalchemy import select

from t2c_data.core.config import embedded_scheduler_allowed, normalize_scheduler_mode, settings
from t2c_data.core.db import SessionLocal
from t2c_data.models.metabase import MetabaseInstance, MetabaseSyncRun

logger = logging.getLogger(__name__)

_scheduler_task: asyncio.Task[None] | None = None
_stop_event: asyncio.Event | None = None


def resolve_timezone(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError):
        logger.warning("metabase scheduler: unknown timezone %s, falling back to UTC", name)
        return ZoneInfo("UTC")


def target_sync_hours(start_hour: int, end_hour: int, interval_hours: int) -> list[int]:
    """The hours of day at which a sync should fire (e.g. [8,10,12,14,16,18])."""
    if interval_hours <= 0 or end_hour < start_hour:
        return []
    return list(range(start_hour, end_hour + 1, interval_hours))


def is_metabase_sync_due(
    now_local: datetime,
    last_started_local: datetime | None,
    *,
    target_hours: set[int],
    weekdays_only: bool,
) -> bool:
    """Whether a sync is due for the current slot (one enqueue per target hour)."""
    if weekdays_only and now_local.weekday() >= 5:
        return False
    if now_local.hour not in target_hours:
        return False
    slot_start = now_local.replace(minute=0, second=0, microsecond=0)
    if last_started_local is not None and last_started_local >= slot_start:
        return False
    return True


def run_metabase_sync_scheduler_cycle() -> dict[str, object]:
    if not settings.metabase_sync_scheduler_enabled:
        return {"enabled": False, "enqueued": 0, "skipped": 0}

    tz = resolve_timezone(settings.metabase_sync_timezone)
    now_local = datetime.now(tz)
    hours = set(
        target_sync_hours(
            int(settings.metabase_sync_window_start_hour),
            int(settings.metabase_sync_window_end_hour),
            int(settings.metabase_sync_interval_hours),
        )
    )
    enqueued = 0
    skipped = 0
    with SessionLocal() as session:
        instances = session.scalars(select(MetabaseInstance).where(MetabaseInstance.enabled.is_(True))).all()
        for instance in instances:
            last_started = session.scalar(
                select(MetabaseSyncRun.started_at)
                .where(MetabaseSyncRun.instance_id == instance.id)
                .order_by(MetabaseSyncRun.started_at.desc())
                .limit(1)
            )
            last_started_local: datetime | None = None
            if last_started is not None:
                if last_started.tzinfo is None:
                    last_started = last_started.replace(tzinfo=timezone.utc)
                last_started_local = last_started.astimezone(tz)
            if not is_metabase_sync_due(now_local, last_started_local, target_hours=hours, weekdays_only=bool(settings.metabase_sync_weekdays_only)):
                continue
            try:
                from t2c_data.features.metabase.service import enqueue_metabase_instance_sync

                enqueue_metabase_instance_sync(session, instance.id, current_user=None, reason="scheduled")
                enqueued += 1
                logger.info("metabase scheduler enqueued sync instance_id=%s slot_hour=%s", instance.id, now_local.hour)
            except HTTPException as exc:
                if exc.status_code == status.HTTP_409_CONFLICT:
                    skipped += 1
                    continue
                logger.exception("metabase scheduler enqueue failed instance_id=%s", instance.id)
            except Exception:  # noqa: BLE001
                logger.exception("metabase scheduler enqueue error instance_id=%s", instance.id)
    return {"enabled": True, "enqueued": enqueued, "skipped": skipped}


async def run_metabase_sync_scheduler_forever() -> None:
    global _stop_event
    _stop_event = asyncio.Event()
    poll_interval = max(1, int(settings.metabase_sync_scheduler_poll_interval_minutes))
    logger.info(
        "metabase sync scheduler started interval_hours=%s window=%s-%s weekdays_only=%s tz=%s poll_minutes=%s",
        settings.metabase_sync_interval_hours,
        settings.metabase_sync_window_start_hour,
        settings.metabase_sync_window_end_hour,
        settings.metabase_sync_weekdays_only,
        settings.metabase_sync_timezone,
        poll_interval,
    )
    while not _stop_event.is_set():
        try:
            run_metabase_sync_scheduler_cycle()
        except Exception as exc:  # noqa: BLE001
            logger.exception("metabase sync scheduler cycle failed error=%s", exc)
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=poll_interval * 60)
        except TimeoutError:
            continue


def start_metabase_sync_scheduler() -> None:
    global _scheduler_task
    configured_mode = normalize_scheduler_mode(settings.metabase_sync_scheduler_mode)
    if not settings.metabase_sync_scheduler_enabled:
        logger.info("metabase sync scheduler disabled")
        return
    if not embedded_scheduler_allowed(configured_mode, settings.env):
        logger.info("metabase sync embedded scheduler skipped mode=%s env=%s", configured_mode, settings.env)
        return
    if _scheduler_task is not None and not _scheduler_task.done():
        return
    loop = asyncio.get_running_loop()
    _scheduler_task = loop.create_task(run_metabase_sync_scheduler_forever())


async def stop_metabase_sync_scheduler() -> None:
    global _scheduler_task, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    if _scheduler_task is not None:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    _scheduler_task = None


__all__ = [
    "is_metabase_sync_due",
    "target_sync_hours",
    "run_metabase_sync_scheduler_cycle",
    "run_metabase_sync_scheduler_forever",
    "start_metabase_sync_scheduler",
    "stop_metabase_sync_scheduler",
]
