from __future__ import annotations

import logging
import socket
from time import monotonic
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Request
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from t2c_data.core.config import OperationalIngestionDatabaseConfig, settings
from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.platform_settings.resolvers import (
    resolve_control_db_url,
    resolve_metabase_config,
    resolve_spark_config,
)
from t2c_data.features.platform_settings.store import (
    NONSECRET_KEYS,
    get_settings_row,
    read_settings_dict,
    write_settings_dict,
)
from t2c_data.models.auth import User
from t2c_data.schemas.platform_settings import (
    PlatformConfigTestResult,
    PlatformSettingsEffective,
    PlatformSettingsOut,
    PlatformSettingsUpdate,
)
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync

logger = logging.getLogger(__name__)
router = APIRouter()


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    trimmed = str(value).strip()
    return trimmed or None


def _effective(db: Session, d: dict[str, Any]) -> PlatformSettingsEffective:
    spark = resolve_spark_config(db)
    mb = resolve_metabase_config(db)
    env_db = OperationalIngestionDatabaseConfig()

    def _pick(key: str, env_value):
        v = d.get(key)
        return v if v not in (None, "") else env_value

    env_sslmode = None
    if env_db.database_url and "sslmode=" in env_db.database_url:
        env_sslmode = urlparse(env_db.database_url).query.split("sslmode=")[-1].split("&")[0] or None

    return PlatformSettingsEffective(
        spark_master_url=spark.master_url,
        spark_results_dir=spark.results_dir,
        spark_jobs_dir=spark.jobs_dir,
        spark_local_jars_dir=spark.local_jars_dir,
        spark_driver_host=spark.driver_host,
        spark_driver_memory=spark.driver_memory,
        spark_executor_memory=spark.executor_memory,
        spark_submit_timeout_seconds=spark.timeout_seconds,
        spark_packages_enabled=spark.packages_enabled,
        spark_packages=spark.packages,
        metabase_enabled=bool(mb.enabled),
        metabase_base_url=mb.normalized_base_url(),
        metabase_auth_type=mb.auth_type,
        metabase_auth_username=mb.auth_username,
        metabase_timeout_seconds=mb.timeout_seconds,
        metabase_sync_dashboards=mb.sync_dashboards,
        metabase_sync_questions=mb.sync_questions,
        metabase_sync_collections=mb.sync_collections,
        control_db_host=_pick("control_db_host", env_db.host),
        control_db_port=_pick("control_db_port", env_db.port),
        control_db_name=_pick("control_db_name", env_db.database),
        control_db_user=_pick("control_db_user", env_db.user),
        control_db_schema=_pick("control_db_schema", env_db.schema_name),
        control_db_sslmode=_pick("control_db_sslmode", env_sslmode),
        dq_execution_engine=_pick("dq_execution_engine", settings.dq_execution_engine),
    )


def _build_out(db: Session) -> PlatformSettingsOut:
    d = read_settings_dict(db)
    row = get_settings_row(db)
    fields = {key: d.get(key) for key in NONSECRET_KEYS}
    return PlatformSettingsOut(
        **fields,
        metabase_auth_secret_set=bool(_clean(d.get("metabase_auth_secret"))),
        control_db_password_set=bool(_clean(d.get("control_db_password"))),
        effective=_effective(db, d),
        updated_at=row.updated_at if row else None,
        updated_by_user_id=row.updated_by_user_id if row else None,
    )


def _audit_snapshot(d: dict[str, Any]) -> dict[str, Any]:
    """Audit-safe projection: non-secret values + booleans for whether secrets are set."""
    snapshot: dict[str, Any] = {key: d.get(key) for key in NONSECRET_KEYS}
    snapshot["metabase_auth_secret_set"] = bool(_clean(d.get("metabase_auth_secret")))
    snapshot["control_db_password_set"] = bool(_clean(d.get("control_db_password")))
    return snapshot


@router.get("/platform-settings", response_model=PlatformSettingsOut)
def get_platform_settings(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> PlatformSettingsOut:
    return _build_out(db)


@router.put("/platform-settings", response_model=PlatformSettingsOut)
def update_platform_settings(
    payload: PlatformSettingsUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> PlatformSettingsOut:
    d = read_settings_dict(db)
    before = _audit_snapshot(d)

    for key, value in payload.model_dump(exclude_unset=True).items():
        if isinstance(value, str):
            value = value.strip()
        if value is None or value == "":
            d.pop(key, None)  # cleared → inherit from env/default
        else:
            d[key] = value

    write_settings_dict(db, d, user_id=current_user.id)
    db.commit()

    write_audit_log_sync(
        db,
        action="admin.platform_settings.update",
        entity_type="platform_settings",
        entity_id=1,
        before=before,
        after=_audit_snapshot(read_settings_dict(db)),
        metadata={"message": "Platform settings updated"},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return _build_out(db)


@router.post("/platform-settings/test/spark", response_model=PlatformConfigTestResult)
def test_spark_connection(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> PlatformConfigTestResult:
    master = resolve_spark_config(db).master_url
    parsed = urlparse(master)
    host = parsed.hostname
    port = parsed.port or 7077
    if not host:
        return PlatformConfigTestResult(ok=False, target=master, detail="URL do master Spark inválida.")
    started = monotonic()
    try:
        with socket.create_connection((host, port), timeout=4):
            pass
        return PlatformConfigTestResult(
            ok=True,
            target=f"{host}:{port}",
            detail="Master Spark acessível (TCP).",
            latency_ms=int((monotonic() - started) * 1000),
        )
    except Exception as exc:  # noqa: BLE001
        return PlatformConfigTestResult(ok=False, target=f"{host}:{port}", detail=f"Falha ao conectar: {exc}")


@router.post("/platform-settings/test/metabase", response_model=PlatformConfigTestResult)
def test_metabase_connection(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> PlatformConfigTestResult:
    mb = resolve_metabase_config(db)
    base = mb.normalized_base_url()
    if not base:
        return PlatformConfigTestResult(ok=False, target="metabase", detail="base_url do Metabase não configurada.")
    started = monotonic()
    try:
        import httpx

        with httpx.Client(timeout=mb.timeout_seconds or 10) as client:
            resp = client.get(f"{base}/api/health")
        ok = resp.status_code == 200
        return PlatformConfigTestResult(
            ok=ok,
            target=base,
            detail="Metabase respondeu /api/health." if ok else f"HTTP {resp.status_code} em /api/health.",
            latency_ms=int((monotonic() - started) * 1000),
        )
    except Exception as exc:  # noqa: BLE001
        return PlatformConfigTestResult(ok=False, target=base, detail=f"Falha ao conectar: {exc}")


@router.post("/platform-settings/test/db", response_model=PlatformConfigTestResult)
def test_control_db_connection(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> PlatformConfigTestResult:
    url = resolve_control_db_url(db)
    if not url:
        return PlatformConfigTestResult(ok=False, target="control-db", detail="Banco de controle não configurado.")
    parsed = urlparse(url.replace("postgresql+psycopg://", "postgresql://"))
    target = f"{parsed.hostname or '?'}/{(parsed.path or '').lstrip('/') or '?'}"
    started = monotonic()
    engine = None
    try:
        engine = create_engine(url, connect_args={"connect_timeout": 5}, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return PlatformConfigTestResult(
            ok=True,
            target=target,
            detail="Conexão OK (SELECT 1).",
            latency_ms=int((monotonic() - started) * 1000),
        )
    except Exception as exc:  # noqa: BLE001
        return PlatformConfigTestResult(ok=False, target=target, detail=f"Falha ao conectar: {exc}")
    finally:
        if engine is not None:
            engine.dispose()
