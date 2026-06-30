"""Internal Spark cluster health monitor.

Runs inside the backend process on a fixed interval (default hourly), probes the Spark
master's TCP endpoint, records the result and raises an operational alert (inbox + email to
admins) when the cluster is unreachable. It does NOT restart containers itself — recovery is
handled by Docker `restart: unless-stopped` (crashes) and the host watchdog
(`scripts/cluster-watchdog.sh`). Keeping recovery outside the app avoids granting the backend
control over the Docker daemon.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from t2c_data.core.config import settings
from t2c_data.core.db import SessionLocal
from t2c_data.features.notifications import resolve_inbox_notification_recipients
from t2c_data.features.platform.alerting import _emit_alert

logger = logging.getLogger(__name__)

_DEFAULT_MASTER_URL = "spark://spark-master:7077"

_stop_event: asyncio.Event | None = None
_monitor_task: asyncio.Task[None] | None = None

# In-process state: detect outage transitions and throttle alerts (one alert per outage).
_consecutive_failures = 0
_alerted_for_current_outage = False
_last_status: str | None = None
_last_checked_at: str | None = None
_last_error: str | None = None


def _parse_endpoint(raw: str) -> tuple[str, int] | None:
    candidate = (raw or "").strip()
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"spark://{candidate}"
    parsed = urlparse(candidate)
    host = parsed.hostname
    if not host:
        return None
    return host, int(parsed.port or 7077)


def _spark_master_endpoints() -> list[tuple[str, int]]:
    raw = os.getenv("SPARK_MASTER_URL") or _DEFAULT_MASTER_URL
    endpoints: list[tuple[str, int]] = []
    for part in raw.split(","):
        endpoint = _parse_endpoint(part)
        if endpoint and endpoint not in endpoints:
            endpoints.append(endpoint)
    return endpoints or [(_parse_endpoint(_DEFAULT_MASTER_URL))]  # type: ignore[list-item]


def check_spark_cluster_health(*, timeout: float = 5.0) -> dict:
    """TCP-probe the Spark master endpoint(s). Healthy if any endpoint accepts a connection."""
    endpoints = _spark_master_endpoints()
    errors: list[str] = []
    for host, port in endpoints:
        started = time.perf_counter()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                latency_ms = round((time.perf_counter() - started) * 1000, 1)
                return {
                    "healthy": True,
                    "host": host,
                    "port": port,
                    "latency_ms": latency_ms,
                    "checked_endpoints": [f"{h}:{p}" for h, p in endpoints],
                }
        except OSError as exc:
            errors.append(f"{host}:{port} -> {exc}")
    return {
        "healthy": False,
        "host": endpoints[0][0] if endpoints else None,
        "port": endpoints[0][1] if endpoints else None,
        "error": "; ".join(errors) or "no spark master endpoint configured",
        "checked_endpoints": [f"{h}:{p}" for h, p in endpoints],
    }


def _emit_cluster_down_alert(health: dict, failures: int) -> None:
    host = health.get("host")
    port = health.get("port")
    try:
        with SessionLocal() as session:
            recipients = resolve_inbox_notification_recipients(session, include_admins=True)
            _emit_alert(
                session,
                event_key="platform.alert.spark_cluster",
                module_name="spark_cluster",
                title="Cluster Spark indisponível",
                severity="critical",
                summary=(
                    f"O master do Spark não respondeu em {host}:{port} "
                    f"após {failures} verificação(ões) consecutiva(s)."
                ),
                probable_cause="Container spark-master fora do ar, rede indisponível ou porta bloqueada.",
                evidence=health.get("error"),
                impact="Scans de fontes de dados e jobs de Data Quality não serão executados até o cluster voltar.",
                recommended_action=(
                    "Verifique os containers spark-master/spark-worker e a rede. "
                    "O restart automático do Docker e o watchdog devem reerguer o cluster; "
                    "se persistir, suba manualmente com 'docker compose up -d'."
                ),
                runbook_url=None,
                correlation_id="spark-cluster",
                payload={"failures": failures, **health},
                recipient_users=recipients,
                entity_type="spark_cluster",
                entity_id=None,
                source_action="platform.alert.spark_cluster",
                category="operations",
            )
            session.commit()
    except Exception:  # noqa: BLE001
        logger.exception("failed to emit spark cluster down alert")


def run_spark_cluster_health_cycle(*, trigger: str = "scheduled") -> dict:
    global _consecutive_failures, _alerted_for_current_outage, _last_status, _last_checked_at, _last_error
    timeout = max(1, int(settings.spark_cluster_monitor_connect_timeout_seconds))
    health = check_spark_cluster_health(timeout=timeout)
    health["trigger"] = trigger
    _last_checked_at = datetime.now(timezone.utc).isoformat()
    health["checked_at"] = _last_checked_at

    if health["healthy"]:
        if _last_status == "down":
            logger.info(
                "spark cluster recovered host=%s port=%s latency_ms=%s",
                health.get("host"),
                health.get("port"),
                health.get("latency_ms"),
            )
        _consecutive_failures = 0
        _alerted_for_current_outage = False
        _last_status = "up"
        _last_error = None
        return health

    _consecutive_failures += 1
    _last_status = "down"
    _last_error = health.get("error")
    logger.warning(
        "spark cluster health check failed (#%s) host=%s port=%s error=%s",
        _consecutive_failures,
        health.get("host"),
        health.get("port"),
        health.get("error"),
    )
    alert_after = max(1, int(settings.spark_cluster_monitor_alert_after_failures))
    # One alert per outage: fire when failures first cross the threshold; reset on recovery.
    if _consecutive_failures >= alert_after and not _alerted_for_current_outage:
        _emit_cluster_down_alert(health, _consecutive_failures)
        _alerted_for_current_outage = True
    return health


def spark_cluster_monitor_status() -> dict:
    return {
        "enabled": bool(settings.spark_cluster_monitor_enabled),
        "interval_minutes": int(settings.spark_cluster_monitor_interval_minutes),
        "last_status": _last_status,
        "last_checked_at": _last_checked_at,
        "consecutive_failures": _consecutive_failures,
        "last_error": _last_error,
        "endpoints": [f"{h}:{p}" for h, p in _spark_master_endpoints()],
    }


async def run_spark_cluster_monitor_forever() -> None:
    global _stop_event
    _stop_event = asyncio.Event()
    interval_minutes = max(1, int(settings.spark_cluster_monitor_interval_minutes))
    logger.info("spark cluster monitor started interval_minutes=%s", interval_minutes)
    while not _stop_event.is_set():
        try:
            await asyncio.to_thread(run_spark_cluster_health_cycle, trigger="scheduled")
        except Exception as exc:  # noqa: BLE001
            logger.exception("spark cluster monitor cycle failed error=%s", exc)
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=interval_minutes * 60)
        except (asyncio.TimeoutError, TimeoutError):
            continue


def start_spark_cluster_monitor() -> None:
    global _monitor_task
    if not settings.spark_cluster_monitor_enabled:
        logger.info("spark cluster monitor disabled by configuration")
        return
    if _monitor_task is not None and not _monitor_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning("spark cluster monitor not started: no running event loop")
        return
    _monitor_task = loop.create_task(run_spark_cluster_monitor_forever(), name="spark-cluster-monitor")


async def stop_spark_cluster_monitor() -> None:
    global _monitor_task, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    if _monitor_task is not None:
        _monitor_task.cancel()
        try:
            await _monitor_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    _monitor_task = None


__all__ = [
    "check_spark_cluster_health",
    "run_spark_cluster_health_cycle",
    "spark_cluster_monitor_status",
    "start_spark_cluster_monitor",
    "stop_spark_cluster_monitor",
]
