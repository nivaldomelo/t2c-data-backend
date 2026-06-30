from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import inspect, select, text
from sqlalchemy.orm import Session

from t2c_data.core.config import embedded_scheduler_allowed, is_dev_environment, normalize_scheduler_mode, settings
from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.models.auth import User
from t2c_data.core.secret_audit import audit_plaintext_secrets
from t2c_data.core.secret_store import decrypt_secret_mapping, encrypt_secret_mapping
from t2c_data.features.governance.settings import (
    LEGACY_API_AUTO_CUTOFF_MODULES,
    get_effective_legacy_api_disabled_modules,
    get_governance_settings_snapshot,
)
from t2c_data.features.data_quality.profiling_scheduler import scheduler_status_snapshot as dq_profiling_scheduler_status_snapshot
from t2c_data.features.data_quality.scheduler import scheduler_status_snapshot as dq_scheduler_status_snapshot
from t2c_data.features.datasource.scheduler import scheduler_status_snapshot as datasource_scheduler_status_snapshot
from t2c_data.features.platform.analytics import legacy_api_usage_stats_by_module
from t2c_data.features.platform.jobs import integration_jobs_status_snapshot
from t2c_data.features.platform.scheduler import scheduler_status_snapshot as platform_scheduler_status_snapshot
from t2c_data.features.platform.worker_health import worker_health_snapshot
from t2c_data.models.dq import DQJobRun, DQRun
from t2c_data.models.scan import ScanRun


router = APIRouter(tags=["system"])


@router.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/ready")
def readiness(response: Response, db: Session = Depends(get_db)) -> dict[str, object]:
    payload = _build_readiness_payload(db, detailed=False)
    if response is not None and payload["status"] != "ready":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return payload


@router.get("/ready/detailed")
def readiness_detailed(
    response: Response,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> dict[str, object]:
    # Detailed readiness exposes internal config/secret-store state -> admin only.
    payload = _build_readiness_payload(db, detailed=True)
    if response is not None and payload["status"] != "ready":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return payload


def _build_readiness_payload(db: Session, *, detailed: bool) -> dict[str, object]:
    started = perf_counter()
    checks: list[dict[str, object]] = []
    has_error = False
    warnings = 0
    schema = settings.db_schema

    try:
        db.execute(text("SELECT 1"))
        checks.append({"name": "database", "status": "ok"})
    except Exception:  # noqa: BLE001
        has_error = True
        checks.append({"name": "database", "status": "error", "detail": "database unavailable"})

    try:
        inspector = inspect(db.get_bind())
        required_tables = ["users", "tables", "schemas", "data_sources"]
        missing = [name for name in required_tables if not inspector.has_table(name, schema=schema)]
        if missing:
            has_error = True
            checks.append({"name": "schema", "status": "error", "missing_tables": missing})
        else:
            checks.append({"name": "schema", "status": "ok"})
    except Exception:  # noqa: BLE001
        has_error = True
        checks.append({"name": "schema", "status": "error", "detail": "schema inspection unavailable"})

    try:
        inspector = inspect(db.get_bind())
        has_alembic_table = inspector.has_table("alembic_version", schema=schema)
        alembic_version = None
        if has_alembic_table:
            alembic_version = db.execute(text(f"SELECT version_num FROM {schema}.alembic_version LIMIT 1")).scalar()
        if not has_alembic_table or not alembic_version:
            has_error = True
            checks.append({"name": "migrations", "status": "error", "detail": "alembic version unavailable"})
        else:
            checks.append({"name": "migrations", "status": "ok", "version": str(alembic_version)})
    except Exception:  # noqa: BLE001
        has_error = True
        checks.append({"name": "migrations", "status": "error", "detail": "migration state unavailable"})

    try:
        probe = {"probe": "ready"}
        encrypted = encrypt_secret_mapping(probe)
        decrypted = decrypt_secret_mapping(encrypted)
        if decrypted != probe:
            has_error = True
            checks.append({"name": "secret_store", "status": "error", "detail": "secret store roundtrip mismatch"})
        else:
            checks.append({"name": "secret_store", "status": "ok"})
    except Exception:  # noqa: BLE001
        has_error = True
        checks.append({"name": "secret_store", "status": "error", "detail": "secret store unavailable"})

    config_status = "ok"
    scheduler_modes = {
        "platform_scheduler": normalize_scheduler_mode(settings.platform_scheduler_mode),
        "dq_scheduler": normalize_scheduler_mode(settings.dq_scheduler_mode),
        "dq_profiling_scheduler": normalize_scheduler_mode(settings.dq_profiling_scheduler_mode),
        "datasource_scan_scheduler": normalize_scheduler_mode(settings.datasource_scan_scheduler_mode),
        "data_lake_scan_scheduler": normalize_scheduler_mode(settings.data_lake_scan_scheduler_mode),
    }
    embedded_modes = [
        name
        for name, mode in scheduler_modes.items()
        if mode == "embedded_dev_only" and not embedded_scheduler_allowed(mode, settings.env)
    ]
    config_detail = {
        "env": settings.env,
        "dq_execution_engine": settings.dq_execution_engine,
        "dq_execution_mode": settings.dq_execution_mode,
        "scheduler_modes": scheduler_modes,
        "embedded_allowed": is_dev_environment(settings.env),
    }
    if (settings.dq_execution_engine or "").strip().lower() != "spark":
        has_error = True
        config_status = "error"
        config_detail["detail"] = "dq_execution_engine must be spark"
    elif (settings.dq_execution_mode or "").strip().lower() not in {"spark_only", "local_disabled"}:
        has_error = True
        config_status = "error"
        config_detail["detail"] = "dq_execution_mode must disable local execution"
    elif embedded_modes:
        has_error = True
        config_status = "error"
        config_detail["detail"] = "Embedded schedulers are not allowed outside dev/test. Use worker mode."
        config_detail["embedded_modes"] = embedded_modes
    checks.append({"name": "config", "status": config_status, **config_detail})

    if detailed:
        # Informational only: a down Spark cluster does not make the API "not ready"
        # (browsing/catalog still work). The internal monitor alerts admins separately.
        try:
            from t2c_data.features.platform.spark_cluster_monitor import (
                check_spark_cluster_health,
                spark_cluster_monitor_status,
            )

            live = check_spark_cluster_health(timeout=3.0)
            checks.append(
                {
                    "name": "spark_cluster",
                    "status": "ok" if live.get("healthy") else "degraded",
                    "monitor": spark_cluster_monitor_status(),
                    "live_probe": live,
                }
            )
        except Exception:  # noqa: BLE001
            checks.append({"name": "spark_cluster", "status": "unknown", "detail": "cluster probe unavailable"})

    try:
        secret_audit = audit_plaintext_secrets(db, fix=False)
        plaintext_total = sum(int(item.get("detected") or 0) for item in secret_audit)
        if plaintext_total:
            if is_dev_environment(settings.env) and settings.allow_plaintext_secrets:
                warnings += 1
                checks.append(
                    {
                        "name": "plaintext_secrets",
                        "status": "warning",
                        "detected": plaintext_total,
                        "items": secret_audit,
                    }
                )
            else:
                has_error = True
                checks.append(
                    {
                        "name": "plaintext_secrets",
                        "status": "error",
                        "detected": plaintext_total,
                        "detail": "Plaintext secret is not allowed. Rotate or migrate this credential.",
                        "items": secret_audit,
                    }
                )
        else:
            checks.append({"name": "plaintext_secrets", "status": "ok", "detected": 0})
    except Exception:  # noqa: BLE001
        has_error = True
        checks.append({"name": "plaintext_secrets", "status": "error", "detail": "plaintext secret audit unavailable"})

    try:
        scheduler_snapshots = [
            ("platform_scheduler", platform_scheduler_status_snapshot(db)),
            ("dq_scheduler", dq_scheduler_status_snapshot(db)),
            ("dq_profiling_scheduler", dq_profiling_scheduler_status_snapshot(db)),
            ("datasource_scan_scheduler", datasource_scheduler_status_snapshot(db)),
        ]
        degraded_health = {"unavailable", "degraded", "stale"}
        for name, snapshot in scheduler_snapshots:
            health = str(snapshot.get("health") or "unknown")
            if health in degraded_health:
                has_error = True
                checks.append({"name": name, "status": "error", "health": health, "snapshot": snapshot})
            else:
                checks.append({"name": name, "status": "ok", "health": health, "snapshot": snapshot})
    except Exception:  # noqa: BLE001
        has_error = True
        checks.append({"name": "schedulers", "status": "error", "detail": "scheduler diagnostics unavailable"})

    try:
        worker_snapshot = worker_health_snapshot(db)
        worker_status = str(worker_snapshot.get("status") or "unknown").strip().lower()
        if worker_status == "error":
            has_error = True
        elif worker_status == "warning":
            warnings += 1
        checks.append(
            {
                "name": "worker_health",
                "status": worker_status if worker_status in {"ok", "warning", "error"} else "error",
                **worker_snapshot,
            }
        )
    except Exception:  # noqa: BLE001
        has_error = True
        checks.append({"name": "worker_health", "status": "error", "detail": "worker health diagnostics unavailable"})

    if detailed:
        try:
            job_snapshot = integration_jobs_status_snapshot(db, limit=15)
            latest_running = int(job_snapshot.get("running") or 0)
            latest_failed = int(job_snapshot.get("failed") or 0)
            latest_partial = int(job_snapshot.get("partial_success") or 0)
            status_name = "ok"
            if latest_running > 0:
                status_name = "warning"
            if latest_failed > 0:
                status_name = "warning"
            if status_name == "warning":
                warnings += 1
            checks.append(
                {
                    "name": "job_queue",
                    "status": status_name,
                    "running": latest_running,
                    "failed": latest_failed,
                    "partial_success": latest_partial,
                    "snapshot": job_snapshot,
                }
            )
        except Exception:  # noqa: BLE001
            has_error = True
            checks.append({"name": "job_queue", "status": "error", "detail": "job queue diagnostics unavailable"})

        try:
            inspector = inspect(db.get_bind())
            scanner_tables = ["scan_runs", "scan_snapshots", "scan_diffs"]
            missing = [name for name in scanner_tables if not inspector.has_table(name, schema=schema)]
            latest_scan = db.scalar(select(ScanRun.status).order_by(ScanRun.id.desc()).limit(1)) if not missing else None
            if missing:
                has_error = True
                checks.append({"name": "datasource_scanner", "status": "error", "missing_tables": missing})
            else:
                scanner_status = "ok"
                if latest_scan in {"failed", "partial_success"}:
                    scanner_status = "warning"
                if scanner_status == "warning":
                    warnings += 1
                checks.append(
                    {
                        "name": "datasource_scanner",
                        "status": scanner_status,
                        "latest_scan_status": latest_scan,
                        "scheduler_enabled": settings.datasource_scan_scheduler_enabled,
                    }
                )
        except Exception:  # noqa: BLE001
            has_error = True
            checks.append({"name": "datasource_scanner", "status": "error", "detail": "scanner diagnostics unavailable"})

        try:
            inspector = inspect(db.get_bind())
            dq_tables = ["dq_runs", "dq_job_runs", "dq_rules"]
            missing = [name for name in dq_tables if not inspector.has_table(name, schema=schema)]
            latest_dq_run = db.scalar(select(DQRun.status).order_by(DQRun.id.desc()).limit(1)) if not missing else None
            latest_dq_job = db.scalar(select(DQJobRun.status).order_by(DQJobRun.id.desc()).limit(1)) if not missing else None
            if missing:
                has_error = True
                checks.append({"name": "dq_engine", "status": "error", "missing_tables": missing})
            else:
                dq_status = "ok"
                if latest_dq_run in {"failed", "timeout"} or latest_dq_job in {"failed", "timeout"}:
                    dq_status = "warning"
                if dq_status == "warning":
                    warnings += 1
                checks.append(
                    {
                        "name": "dq_engine",
                        "status": dq_status,
                        "execution_engine": settings.dq_execution_engine,
                        "execution_mode": settings.dq_execution_mode,
                        "latest_run_status": latest_dq_run,
                        "latest_job_status": latest_dq_job,
                    }
                )
        except Exception:  # noqa: BLE001
            has_error = True
            checks.append({"name": "dq_engine", "status": "error", "detail": "dq diagnostics unavailable"})

        try:
            governance_snapshot = get_governance_settings_snapshot(db)
            legacy_stats = legacy_api_usage_stats_by_module(
                db,
                days=max(governance_snapshot.legacy_api_cutoff_window_days, 1),
            )
            disabled_modules = set(get_effective_legacy_api_disabled_modules(db))
            protected_modules = []
            for module in LEGACY_API_AUTO_CUTOFF_MODULES:
                payload = legacy_stats.get(module, {})
                protected_modules.append(
                    {
                        "module": module,
                        "hits_in_window": int(payload.get("hits_in_window", 0) or 0),
                        "hits_total": int(payload.get("hits_total", 0) or 0),
                        "last_hit_at": payload.get("last_hit_at").isoformat() if payload.get("last_hit_at") else None,
                        "disabled": module in disabled_modules,
                    }
                )
            active_modules = [item["module"] for item in protected_modules if not item["disabled"] and int(item["hits_in_window"]) > 0]
            cutoff_ready = [item["module"] for item in protected_modules if item["disabled"]]
            legacy_status = "ok"
            if active_modules:
                legacy_status = "warning"
                warnings += 1
            checks.append(
                {
                    "name": "legacy_api_surface",
                    "status": legacy_status,
                    "window_days": governance_snapshot.legacy_api_cutoff_window_days,
                    "auto_cutoff_modules": list(LEGACY_API_AUTO_CUTOFF_MODULES),
                    "active_modules": active_modules,
                    "cutoff_ready_modules": cutoff_ready,
                    "items": protected_modules,
                }
            )
        except Exception:  # noqa: BLE001
            has_error = True
            checks.append({"name": "legacy_api_surface", "status": "error", "detail": "legacy api diagnostics unavailable"})

        try:
            metabase = settings.metabase_config
            integrations_payload = {
                "metabase": {
                    "enabled": bool(metabase.enabled),
                    "configured": bool(metabase.normalized_base_url()),
                    "base_url": metabase.normalized_base_url(),
                },
                "operational_ingestion": {
                    "configured": bool(settings.operational_ingestion_configured),
                    "schema": settings.operational_db_schema,
                },
                "airflow": {
                    "contract_version": settings.airflow_contract_version,
                    "source_schema": settings.airflow_source_schema,
                },
            }
            integrations_status = "ok"
            if metabase.enabled and not metabase.normalized_base_url():
                integrations_status = "warning"
            elif not settings.operational_ingestion_configured:
                integrations_status = "warning"
            if integrations_status == "warning":
                warnings += 1
            checks.append(
                {
                    "name": "critical_integrations",
                    "status": integrations_status,
                    "integrations": integrations_payload,
                }
            )
        except Exception:  # noqa: BLE001
            has_error = True
            checks.append({"name": "critical_integrations", "status": "error", "detail": "integration configuration diagnostics unavailable"})

    duration_ms = round((perf_counter() - started) * 1000, 2)
    overall = "not_ready" if has_error else "ready"
    return {
        "status": overall,
        "mode": "detailed" if detailed else "standard",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "duration_ms": duration_ms,
        "summary": {
            "checks_total": len(checks),
            "warnings": warnings,
            "errors": sum(1 for check in checks if check.get("status") == "error"),
        },
        "checks": checks,
    }
