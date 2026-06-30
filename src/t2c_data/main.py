import asyncio
import logging
import time
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from t2c_data.api.router import api_v1_router
from t2c_data.core.bootstrap import dq_rules_table_exists
from t2c_data.core.config import normalize_scheduler_mode, settings
from t2c_data.core.db import SessionLocal, engine
from t2c_data.core.legacy_api_surface import legacy_surface_route_match
from t2c_data.core.network import get_request_client_ip
from t2c_data.core.logging import setup_logging
from t2c_data.core.request_context import clear_request_context, set_request_context
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.integrations import load_metabase_integration_health
from t2c_data.features.metabase import ensure_metabase_instance_from_settings
from t2c_data.features.metabase.service import enqueue_metabase_instance_sync
from t2c_data.features.metabase.bootstrap import snapshot_metabase_instance
from t2c_data.features.platform.scheduler import run_platform_maintenance_cycle, start_platform_scheduler, stop_platform_scheduler
from t2c_data.features.data_quality.scheduler import start_dq_scheduler, stop_dq_scheduler
from t2c_data.features.data_quality.profiling_scheduler import start_dq_profiling_scheduler, stop_dq_profiling_scheduler
from t2c_data.features.datasource.scheduler import start_datasource_scan_scheduler, stop_datasource_scan_scheduler
from t2c_data.features.metabase.scheduler import start_metabase_sync_scheduler, stop_metabase_sync_scheduler
from t2c_data.features.platform.spark_cluster_monitor import start_spark_cluster_monitor, stop_spark_cluster_monitor
from t2c_data.services.audit import commit_access_log_with_repair
from t2c_data.services.user_activity_tracker import record_session_heartbeat
from t2c_data.seed import run_startup_seed_if_enabled

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title=settings.app_name)
app.include_router(api_v1_router, prefix="/api/v1")

# Root-level probes (Turn2C standard): /liveness, /readiness, /health alias — no /api/v1 prefix, no auth.
from t2c_data.api.observability import router as observability_router  # noqa: E402

app.include_router(observability_router)

# Prometheus /metrics (no auth). Guarded import so the app keeps working until the
# dependency is installed in the image (activates after the next build).
try:
    from prometheus_fastapi_instrumentator import Instrumentator

    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
except Exception:  # noqa: BLE001
    logger.warning("prometheus-fastapi-instrumentator indisponível; /metrics desabilitado até o build incluir a dependência")


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    # Guarantees unhandled errors never leak stack traces / internal details to clients.
    # HTTPException keeps its own handler; this only catches truly unexpected failures.
    request_id = getattr(request.state, "request_id", None)
    logger.exception("unhandled exception path=%s method=%s", request.url.path, request.method)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Erro interno do servidor.", "request_id": request_id},
    )

_SENSITIVE_QUERY_PARAM_KEYS = (
    "password",
    "passwd",
    "token",
    "secret",
    "key",
    "authorization",
    "auth",
    "session",
    "cookie",
    "credential",
)
_MAX_QUERY_PARAM_STRING_LENGTH = 128
_MAX_QUERY_PARAM_LIST_ITEMS = 5


def _looks_sensitive_query_param(key: str) -> bool:
    lowered = key.lower()
    return any(keyword in lowered for keyword in _SENSITIVE_QUERY_PARAM_KEYS)


def _truncate_query_param_string(value: str) -> str:
    normalized = value.strip()
    if len(normalized) <= _MAX_QUERY_PARAM_STRING_LENGTH:
        return normalized
    return f"{normalized[:_MAX_QUERY_PARAM_STRING_LENGTH]}...[truncated]"


def _sanitize_query_param_value(key: str, value):
    if _looks_sensitive_query_param(key):
        return "[redacted]"
    if isinstance(value, str):
        return _truncate_query_param_string(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        sanitized_values = [_sanitize_query_param_value(key, item) for item in value[:_MAX_QUERY_PARAM_LIST_ITEMS]]
        if len(value) > _MAX_QUERY_PARAM_LIST_ITEMS:
            sanitized_values.append(f"...(+{len(value) - _MAX_QUERY_PARAM_LIST_ITEMS} more)")
        return sanitized_values
    return _truncate_query_param_string(str(value))


def _enqueue_metabase_startup_sync(session, instance) -> None:
    startup_sync_mode = getattr(settings, "metabase_startup_sync_mode", "disabled")
    if startup_sync_mode != "enqueue":
        logger.info("metabase startup sync skipped mode=%s", startup_sync_mode)
        return
    try:
        logger.info(
            "metabase startup sync enqueue requested instance_id=%s base_url=%s mode=%s",
            instance.id,
            instance.base_url,
            startup_sync_mode,
        )
        enqueue_metabase_instance_sync(session, instance.id, current_user=None, reason="startup")
    except HTTPException as exc:
        if exc.status_code == status.HTTP_409_CONFLICT:
            logger.info("metabase startup sync skipped: %s", exc.detail)
            return
        logger.exception("metabase startup sync enqueue failed: %s", exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("metabase startup sync enqueue failed: %s", exc)


def _log_metabase_startup_health(session) -> None:
    try:
        health = load_metabase_integration_health(session)
    except Exception as exc:  # noqa: BLE001
        logger.warning("metabase startup health check failed: %s", exc)
        return

    if health.available:
        logger.info(
            "metabase startup health ok status=%s integration_status=%s configured=%s enabled=%s base_url=%s message=%s",
            health.status,
            health.integration_status,
            health.configured,
            health.enabled,
            health.instance_base_url,
            health.message,
        )
        return

    logger.warning(
        "metabase startup health unavailable; verify metabase-integration network and base_url=%s status=%s integration_status=%s configured=%s enabled=%s message=%s",
        health.instance_base_url,
        health.status,
        health.integration_status,
        health.configured,
        health.enabled,
        health.message,
    )


def _api_version_from_path(path: str) -> str:
    if path.startswith("/api/v1/") or path == "/api/v1":
        return "v1"
    if path.startswith("/api/"):
        return "legacy"
    return "external"


def _module_name_from_path(path: str) -> str:
    normalized = path[1:] if path.startswith("/") else path
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return "root"
    if parts[0] == "api":
        if len(parts) > 2 and parts[1] == "v1":
            return parts[2]
        if len(parts) > 1:
            return parts[1]
        return "api"
    return parts[0]


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        path = request.url.path
        if (path == "/api" or path.startswith("/api/")) and not path.startswith("/api/v1/"):
            _, canonical_path = legacy_surface_route_match(path)
            detail = f"Esta rota legada foi removida. Use {canonical_path}."
            response = Response(
                content=f'{{"detail":"{detail}","canonical_path":"{canonical_path}"}}',
                status_code=status.HTTP_410_GONE,
                media_type="application/json",
            )
            response.headers.setdefault("Deprecation", "true")
            response.headers.setdefault("Sunset", "Wed, 30 Sep 2026 23:59:59 GMT")
            response.headers.setdefault("Link", f'<{canonical_path}>; rel="successor-version"')
            response.headers.setdefault("X-API-Canonical-Path", canonical_path)
            response.headers.setdefault("Cache-Control", "no-store")
            return response
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        # API responses are JSON; a tight CSP still hardens any accidental HTML/redoc surface.
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'",
        )
        forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
        if request.url.scheme == "https" or forwarded_proto == "https":
            response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        return response


class AuditRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        request_id = request.headers.get("X-Request-Id") or str(uuid4())
        correlation_id = request.headers.get("X-Correlation-Id") or request_id
        request.state.request_id = request_id
        request.state.correlation_id = correlation_id
        request_context_tokens = set_request_context(
            request_id=request_id,
            correlation_id=correlation_id,
            path=request.url.path,
            method=request.method,
        )
        runtime_metrics.request_started(method=request.method)
        started = time.perf_counter()
        response = None
        status_code = None
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            route = request.scope.get("route")
            route_path = getattr(route, "path", None) or request.url.path
            runtime_metrics.request_finished(
                status_code=status_code,
                duration_ms=duration_ms,
                method=request.method,
                route=route_path,
            )
            if response is not None:
                response.headers.setdefault("X-Request-Id", request_id)
                response.headers.setdefault("X-Correlation-Id", correlation_id)
            path = request.url.path
            logger.info(
                "http request completed status=%s duration_ms=%.2f",
                status_code,
                duration_ms,
                extra={"correlation_id": correlation_id},
            )
            if path.startswith("/api") and path not in {"/api/docs", "/api/redoc", "/api/openapi.json"}:
                user_id = getattr(request.state, "current_user_id", None)
                actor_name = getattr(request.state, "current_user_name", None)
                user_email = getattr(request.state, "current_user_email", None)
                session_jti = getattr(request.state, "current_user_session_jti", None)
                api_key = getattr(request.state, "current_api_key", None)
                query_params = {}
                for key in request.query_params.keys():
                    values = request.query_params.getlist(key)
                    raw_value = values if len(values) > 1 else (values[0] if values else None)
                    query_params[key] = _sanitize_query_param_value(key, raw_value)
                metadata = {"query_params": query_params, "duration_ms": duration_ms}
                api_key_snapshot = getattr(request.state, "current_api_key_data", None)
                if isinstance(api_key_snapshot, dict):
                    metadata.update(
                        {
                            "api_key_id": api_key_snapshot.get("id"),
                            "api_key_public_id": api_key_snapshot.get("public_id"),
                            "api_key_name": api_key_snapshot.get("name"),
                            "api_key_token_prefix": api_key_snapshot.get("token_prefix"),
                            "api_key_status": api_key_snapshot.get("status"),
                            "api_key_environment": api_key_snapshot.get("environment"),
                            "api_key_usage_count": api_key_snapshot.get("usage_count"),
                            "api_key_scope_count": api_key_snapshot.get("scope_count"),
                            "api_key_allowed_ips_count": api_key_snapshot.get("allowed_ips_count"),
                        }
                    )
                elif api_key is not None:
                    metadata["api_key_id"] = getattr(api_key, "id", None)
                    metadata["api_key_public_id"] = getattr(api_key, "public_id", None)
                    metadata["api_key_name"] = getattr(api_key, "name", None)
                    metadata["api_key_token_prefix"] = getattr(api_key, "token_prefix", None)
                    metadata["api_key_status"] = getattr(api_key, "status", None)
                    metadata["api_key_environment"] = getattr(api_key, "environment", None)
                    metadata["api_key_usage_count"] = int(getattr(api_key, "usage_count", 0) or 0)
                    metadata["api_key_scope_count"] = len(getattr(api_key, "scopes_json", []) or [])
                    metadata["api_key_allowed_ips_count"] = len(getattr(api_key, "allowed_ips_json", []) or [])
                try:
                    with SessionLocal() as session:
                        commit_access_log_with_repair(
                            session,
                            user_id=user_id,
                            actor_name=actor_name,
                            user_email=user_email,
                            ip=get_request_client_ip(request),
                            user_agent=request.headers.get("user-agent"),
                            route=path,
                            method=request.method,
                            status_code=status_code,
                            request_id=request_id,
                            api_version=_api_version_from_path(path),
                            module_name=_module_name_from_path(path),
                            duration_ms=duration_ms,
                            metadata=metadata,
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("access middleware failed route=%s method=%s error=%s", path, request.method, exc)
                if user_id is not None and session_jti:
                    try:
                        with SessionLocal() as session:
                            record_session_heartbeat(
                                session,
                                user_id=user_id,
                                session_jti=session_jti,
                                user_agent=request.headers.get("user-agent"),
                                ip_address=get_request_client_ip(request),
                            )
                            session.commit()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("session heartbeat failed route=%s method=%s error=%s", path, request.method, exc)
            clear_request_context(request_context_tokens)


dev_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:13000",
    "http://127.0.0.1:13000",
]
is_dev = settings.env.lower() in {"dev", "development", "local", "test"}
configured_cors_origins = settings.cors_origins_list
cors_origins = configured_cors_origins or (dev_origins if is_dev else [])
cors_origin_regex = (
    r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$"
    if is_dev and not configured_cors_origins
    else None
)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(AuditRequestMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=cors_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_seed() -> None:
    metabase_bootstrap = None
    with SessionLocal() as session:
        if not dq_rules_table_exists(session):
            logger.warning("DQ rules table not found. Rodar alembic upgrade head")
        run_startup_seed_if_enabled(session)
        instance = ensure_metabase_instance_from_settings(session)
        metabase_bootstrap = snapshot_metabase_instance(instance)
        _log_metabase_startup_health(session)
    operational_configured = settings.operational_ingestion_configured
    logger.info(
        "operational ingestion source bootstrap configured=%s schema=%s",
        operational_configured,
        settings.operational_ingestion_config.schema_name,
    )
    if not operational_configured:
        logger.warning("operational ingestion source is not configured; cockpit operational will show a friendly empty state")
    logger.info(
        "platform maintenance bootstrap auto_refresh=%s scheduler_mode=%s interval_minutes=%s",
        settings.platform_read_model_auto_refresh_enabled,
        settings.platform_scheduler_mode,
        settings.platform_read_model_refresh_interval_minutes,
    )
    if metabase_bootstrap is not None:
        logger.info(
            "metabase bootstrap configured name=%s base_url=%s configured=%s credentials_state=%s sync_state=%s startup_sync_mode=%s",
            metabase_bootstrap["name"],
            metabase_bootstrap["base_url"],
            metabase_bootstrap["configured"],
            metabase_bootstrap["credentials_state"],
            metabase_bootstrap["sync_state"],
            getattr(settings, "metabase_startup_sync_mode", "disabled"),
        )
        _enqueue_metabase_startup_sync(session, instance)
    else:
        logger.info(
            "metabase bootstrap skipped: no enabled instance configured in environment configured=%s credentials_state=%s sync_state=%s",
            False,
            "missing",
            "never_synced",
        )
    try:
        start_platform_scheduler()
    except Exception:  # noqa: BLE001
        logger.exception("platform maintenance scheduler scheduling failed during app startup")
    if settings.platform_read_model_auto_refresh_enabled:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                asyncio.to_thread(
                    run_platform_maintenance_cycle,
                    trigger="startup",
                    scheduler_mode=normalize_scheduler_mode(settings.platform_scheduler_mode),
                ),
                name="platform-maintenance-startup-refresh",
            )
        except RuntimeError:
            logger.warning("platform maintenance startup refresh skipped: no running event loop")
    try:
        start_dq_scheduler()
    except Exception:  # noqa: BLE001
        logger.exception("dq scheduler scheduling failed during app startup")
    try:
        start_dq_profiling_scheduler()
    except Exception:  # noqa: BLE001
        logger.exception("dq profiling scheduler scheduling failed during app startup")
    try:
        start_datasource_scan_scheduler()
    except Exception:  # noqa: BLE001
        logger.exception("datasource scan scheduler scheduling failed during app startup")
    try:
        start_spark_cluster_monitor()
    except Exception:  # noqa: BLE001
        logger.exception("spark cluster monitor scheduling failed during app startup")
    try:
        start_metabase_sync_scheduler()
    except Exception:  # noqa: BLE001
        logger.exception("metabase sync scheduler scheduling failed during app startup")


@app.on_event("shutdown")
async def shutdown_tasks() -> None:
    await stop_platform_scheduler()
    await stop_dq_scheduler()
    await stop_dq_profiling_scheduler()
    await stop_datasource_scan_scheduler()
    await stop_spark_cluster_monitor()
    await stop_metabase_sync_scheduler()
