from __future__ import annotations

import logging
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from math import ceil
from typing import Any

from sqlalchemy import case, func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import settings
from t2c_data.core.sql_utils import safe_relation
from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.pagination import paginate_items
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.integrations.airflow_read_models import (
    AIRFLOW_CATALOG_SCHEMA,
    AIRFLOW_DAG_RUNS_VIEW,
    AIRFLOW_DAGS_VIEW,
    AIRFLOW_FAILURES_VIEW,
    AIRFLOW_OPERATIONAL_VIEW,
    AirflowReadModelContractSnapshot,
    inspect_airflow_operational_contract,
)
from t2c_data.features.lineage.sql_lineage import extract_sql_table_lineage
from t2c_data.features.integrations.health import (
    DEFAULT_BREAKER_OPEN_SECONDS,
    DEFAULT_BREAKER_THRESHOLD,
    IntegrationHealthSnapshot,
    build_retryable_predicate,
    classify_integration_issue,
    close_breaker,
    get_integration_health,
    get_integration_health_details,
    is_breaker_open,
    now_utc,
    open_breaker,
    retry_with_backoff,
    upsert_integration_health,
)
from t2c_data.features.ingestion.runtime import operational_session
from t2c_data.features.metabase.client import MetabaseClient, MetabaseClientConfig, MetabaseClientError
from t2c_data.features.metabase.bootstrap import ensure_metabase_instance_from_settings
from t2c_data.services.integrations.status_resolver import (
    dimension_available,
    dimension_degraded,
    dimension_delayed,
    dimension_down,
    dimension_failed,
    dimension_healthy,
    dimension_idle,
    dimension_partial,
    dimension_running,
    dimension_unavailable,
    dimension_unknown,
    resolve_status_contract,
)
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.integrations import IntegrationHealth
from t2c_data.models.metabase import MetabaseInstance, MetabaseObject, MetabaseObjectLink, MetabaseSyncRun
from t2c_data.schemas.integrations import (
    AirflowIntegrationHealthOut,
    AirflowIntegrationDagRunOut,
    AirflowIntegrationDagSummaryOut,
    AirflowIntegrationFailuresOut,
    AirflowIntegrationPipelinesOut,
    AirflowIntegrationSummaryOut,
    AirflowIntegrationTaskFailureOut,
    MetabaseArtifactCardOut,
    MetabaseArtifactDetailOut,
    MetabaseArtifactLinkSummaryOut,
    MetabaseArtifactLinkedTableOut,
    MetabaseArtifactReferencedTableOut,
    MetabaseIntegrationHealthOut,
    MetabaseIntegrationArtifactOut,
    MetabaseIntegrationRecommendationOut,
    MetabaseIntegrationSummaryOut,
    MetabaseIntegrationTopTableOut,
)
from t2c_data.schemas.metabase import MetabaseSyncRunOut
from t2c_data.schemas.pagination import PageOut


DIRECT_LINK_METHODS = {"confirmed", "direct", "sql", "inferred"}
INDIRECT_LINK_METHODS = {"indirect_view", "indirect_lineage", "lineage_indirect"}
logger = logging.getLogger(__name__)
_AIRFLOW_READ_MODELS_CACHE_LOCK = threading.Lock()
_AIRFLOW_READ_MODELS_CACHE_SUCCESS_TTL_SECONDS = 120.0
_AIRFLOW_READ_MODELS_CACHE_FAILURE_TTL_SECONDS = 20.0
_AIRFLOW_READ_MODELS_CACHE = {
    "last_result": None,
    "last_success_at": 0.0,
    "last_failure_at": 0.0,
    "last_snapshot": None,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _integration_health_fields(health: IntegrationHealth | None) -> dict[str, object]:
    if health is None:
        return {
            "integration_status": "unavailable",
            "status_message": None,
            "reason_code": None,
            "health_category": None,
            "checked_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "consecutive_failures": 0,
            "failure_count": 0,
            "latency_ms": None,
            "error_type": None,
            "error_summary": None,
            "breaker_state": None,
            "breaker_open_until_at": None,
        }
    return {
        "integration_status": health.status,
        "status_message": health.status_message,
        "reason_code": health.reason_code,
        "health_category": health.category,
        "checked_at": health.checked_at,
        "last_success_at": health.last_success_at,
        "last_failure_at": health.last_failure_at,
        "consecutive_failures": health.consecutive_failures,
        "failure_count": health.failure_count,
        "latency_ms": health.latency_ms,
        "error_type": health.error_type,
        "error_summary": health.error_summary,
        "breaker_state": health.breaker_state,
        "breaker_open_until_at": health.breaker_open_until_at,
    }


def _base_health_snapshot(
    integration_name: str,
    *,
    base_url: str | None,
    status: str,
    status_message: str | None,
    category: str | None,
    checked_at: datetime | None = None,
    last_success_at: datetime | None = None,
    last_failure_at: datetime | None = None,
    consecutive_failures: int = 0,
    failure_count: int = 0,
    latency_ms: int | None = None,
    error_type: str | None = None,
    error_summary: str | None = None,
    details_json: dict | list | None = None,
    breaker_state: str = "closed",
    breaker_open_until_at: datetime | None = None,
    reason_code: str | None = None,
) -> IntegrationHealthSnapshot:
    return IntegrationHealthSnapshot(
        integration_name=integration_name,
        status=status,
        status_message=status_message,
        category=category,
        base_url=base_url,
        checked_at=checked_at or _now(),
        reason_code=reason_code or error_type or category,
        last_success_at=last_success_at,
        last_failure_at=last_failure_at,
        consecutive_failures=consecutive_failures,
        failure_count=failure_count,
        latency_ms=latency_ms,
        error_type=error_type,
        error_summary=error_summary,
        details_json=details_json,
        breaker_state=breaker_state,
        breaker_open_until_at=breaker_open_until_at,
    )


def _airflow_relation_name(relation: str) -> str:
    return safe_relation(AIRFLOW_CATALOG_SCHEMA, relation, label="relation")


def _airflow_relation_exists(session: Session, relation: str) -> bool:
    return bool(
        session.execute(text("SELECT to_regclass(:relation_name)"), {"relation_name": _airflow_relation_name(relation)}).scalar_one()
    )


def _airflow_safe_rollback(session: Session, *, context: str) -> None:
    try:
        session.rollback()
    except Exception:  # noqa: BLE001
        logger.debug("airflow rollback failed context=%s", context, exc_info=True)


def _validate_airflow_operational_contract_cached(session: Session) -> AirflowReadModelContractSnapshot:
    now = time.monotonic()
    with _AIRFLOW_READ_MODELS_CACHE_LOCK:
        last_result = _AIRFLOW_READ_MODELS_CACHE["last_result"]
        last_success_at = float(_AIRFLOW_READ_MODELS_CACHE["last_success_at"] or 0.0)
        last_failure_at = float(_AIRFLOW_READ_MODELS_CACHE["last_failure_at"] or 0.0)
        snapshot = _AIRFLOW_READ_MODELS_CACHE["last_snapshot"]
        if last_result is True and snapshot is not None and (now - last_success_at) < _AIRFLOW_READ_MODELS_CACHE_SUCCESS_TTL_SECONDS:
            return snapshot
        if last_result is False and snapshot is not None and (now - last_failure_at) < _AIRFLOW_READ_MODELS_CACHE_FAILURE_TTL_SECONDS:
            return snapshot
    snapshot = inspect_airflow_operational_contract(session)
    with _AIRFLOW_READ_MODELS_CACHE_LOCK:
        _AIRFLOW_READ_MODELS_CACHE["last_result"] = snapshot.ready
        _AIRFLOW_READ_MODELS_CACHE["last_snapshot"] = snapshot
        if snapshot.ready:
            _AIRFLOW_READ_MODELS_CACHE["last_success_at"] = now
        else:
            _AIRFLOW_READ_MODELS_CACHE["last_failure_at"] = now
    return snapshot


def ensure_airflow_operational_read_models(session: Session) -> bool:
    """Compatibility shim for tests and older call sites.

    Runtime code should validate the Airflow contract via
    _validate_airflow_operational_contract_cached() instead of recreating views.
    """

    return _validate_airflow_operational_contract_cached(session).ready


def _airflow_fetch_one(session: Session, relation: str) -> dict[str, object] | None:
    if not _airflow_relation_exists(session, relation):
        return None
    return session.execute(text(f"SELECT * FROM {_airflow_relation_name(relation)} LIMIT 1")).mappings().first()


def _airflow_fetch_many(session: Session, relation: str, *, limit: int, order_by: str) -> list[dict[str, object]]:
    if not _airflow_relation_exists(session, relation):
        return []
    rows = session.execute(
        text(f"SELECT * FROM {_airflow_relation_name(relation)} ORDER BY {order_by} LIMIT :limit"),
        {"limit": limit},
    )
    return list(rows.mappings().all())


def _airflow_dag_filter_clause(*, search: str | None, status: str | None) -> tuple[str, dict[str, object]]:
    """Build a safe WHERE clause (bound params only) for the DAG summary view."""
    clauses: list[str] = []
    params: dict[str, object] = {}
    normalized_search = (search or "").strip()
    if normalized_search:
        params["q"] = f"%{normalized_search}%"
        clauses.append(
            "(dag_id ILIKE :q OR COALESCE(dag_display_name, '') ILIKE :q "
            "OR COALESCE(description, '') ILIKE :q OR COALESCE(owner, '') ILIKE :q)"
        )
    normalized_status = (status or "all").strip().lower()
    if normalized_status == "active":
        clauses.append("is_active = TRUE AND is_paused = FALSE")
    elif normalized_status == "paused":
        clauses.append("is_paused = TRUE")
    elif normalized_status == "failing":
        clauses.append("COALESCE(recent_failures_count_24h, 0) > 0")
    where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def _airflow_count(session: Session, relation: str, *, where_sql: str, params: dict[str, object]) -> int:
    if not _airflow_relation_exists(session, relation):
        return 0
    result = session.execute(
        text(f"SELECT COUNT(*) FROM {_airflow_relation_name(relation)}{where_sql}"),
        params,
    )
    return int(result.scalar() or 0)


def _airflow_fetch_page(
    session: Session,
    relation: str,
    *,
    limit: int,
    offset: int,
    order_by: str,
    where_sql: str = "",
    params: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    if not _airflow_relation_exists(session, relation):
        return []
    query_params: dict[str, object] = {**(params or {}), "limit": limit, "offset": offset}
    rows = session.execute(
        text(
            f"SELECT * FROM {_airflow_relation_name(relation)}{where_sql} "
            f"ORDER BY {order_by} LIMIT :limit OFFSET :offset"
        ),
        query_params,
    )
    return list(rows.mappings().all())


def _airflow_integration_status(
    *,
    configured: bool,
    view_available: bool,
    summary_row: dict[str, object] | None,
) -> tuple[bool, bool, bool, str, str, bool]:
    if not configured:
        return False, False, False, "inactive", "unavailable", False
    if not view_available or summary_row is None:
        return True, True, False, "degraded", "unavailable", False

    total_dags = int(summary_row.get("total_dags") or 0)
    active_dags = int(summary_row.get("active_dags") or 0)
    paused_dags = int(summary_row.get("paused_dags") or 0)
    success_runs_24h = int(summary_row.get("success_runs_24h") or 0)
    failed_runs_24h = int(summary_row.get("failed_runs_24h") or 0)
    task_failures_24h = int(summary_row.get("task_failures_24h") or 0)
    has_runs = bool(
        summary_row.get("latest_execution_at")
        or success_runs_24h > 0
        or failed_runs_24h > 0
        or task_failures_24h > 0
    )
    if total_dags == 0:
        return True, True, True, "active", "connected_empty", True
    if not has_runs:
        return True, True, True, "active", "connected_no_runs", True
    if failed_runs_24h > 0 or task_failures_24h > 0:
        return True, True, True, "degraded", "connected_active", True
    if success_runs_24h > 0:
        return True, True, True, "active", "connected_active", True
    return True, True, True, "active", "connected_active", True


def _airflow_summary_latest_label(summary_row: dict[str, object] | None) -> datetime | None:
    if summary_row is None:
        return None
    return (
        summary_row.get("latest_execution_at")
        or summary_row.get("last_execution_at")
        or summary_row.get("latest_failure_at")
        or summary_row.get("latest_log_at")
    )


def _normalize_airflow_dag_summary_row(row: dict[str, object]) -> dict[str, object]:
    normalized = dict(row)
    if "latest_execution_at" not in normalized and normalized.get("last_execution_at") is not None:
        normalized["latest_execution_at"] = normalized.get("last_execution_at")
    if "last_execution_at" not in normalized and normalized.get("latest_execution_at") is not None:
        normalized["last_execution_at"] = normalized.get("latest_execution_at")
    return normalized


def _airflow_operational_status_from_summary(summary_row: dict[str, object] | None) -> str:
    if summary_row is None:
        return "unavailable"
    total_dags = int(summary_row.get("total_dags") or 0)
    success_runs_24h = int(summary_row.get("success_runs_24h") or 0)
    failed_runs_24h = int(summary_row.get("failed_runs_24h") or 0)
    task_failures_24h = int(summary_row.get("task_failures_24h") or 0)
    has_runs = bool(
        summary_row.get("latest_execution_at")
        or success_runs_24h > 0
        or failed_runs_24h > 0
        or task_failures_24h > 0
    )
    if total_dags == 0:
        return "connected_empty"
    if not has_runs:
        return "connected_no_runs"
    return "connected_active"


def _airflow_health_status_from_summary(summary_row: dict[str, object] | None) -> str:
    if summary_row is None:
        return "unavailable"
    total_dags = int(summary_row.get("total_dags") or 0)
    success_runs_24h = int(summary_row.get("success_runs_24h") or 0)
    failed_runs_24h = int(summary_row.get("failed_runs_24h") or 0)
    task_failures_24h = int(summary_row.get("task_failures_24h") or 0)
    has_runs = bool(
        summary_row.get("latest_execution_at")
        or success_runs_24h > 0
        or failed_runs_24h > 0
        or task_failures_24h > 0
    )
    if total_dags == 0 or not has_runs:
        return "empty"
    if failed_runs_24h > 0 or task_failures_24h > 0:
        return "degraded"
    return "healthy"


def _airflow_status_contract(
    *,
    configured: bool,
    contract: AirflowReadModelContractSnapshot | None,
    health_row: IntegrationHealth | None,
    summary_row: dict[str, object] | None,
    recent_runs: list[dict[str, object]] | None = None,
    recent_failures: list[dict[str, object]] | None = None,
) -> IntegrationStatusContractOut:
    checked_at = None
    if health_row is not None:
        checked_at = health_row.checked_at
    elif summary_row is not None:
        checked_at = summary_row.get("updated_at")
    checked_at = checked_at or _now()

    contract_version = contract.contract_version if contract is not None else settings.airflow_contract_version
    if not configured:
        return resolve_status_contract(
            source_name="airflow",
            connectivity=dimension_down(
                message="A integração do Airflow não está configurada neste ambiente.",
                reason_code="not_configured",
                checked_at=checked_at,
            ),
            operation=dimension_unknown(
                message="A integração do Airflow não está configurada neste ambiente.",
                reason_code="not_configured",
                checked_at=checked_at,
            ),
            consumption=dimension_unavailable(
                message="Sem contrato de leitura do Airflow.",
                reason_code="not_configured",
                checked_at=checked_at,
            ),
            checked_at=checked_at,
            contract_version=contract_version,
        )

    if contract is not None and not contract.ready:
        details = {
            "source_schema": contract.source_schema,
            "missing_tables": contract.missing_tables,
            "missing_views": contract.missing_views,
            "contract_version": contract.contract_version,
        }
        return resolve_status_contract(
            source_name="airflow",
            connectivity=dimension_down(
                message=f"Contrato do Airflow incompatível com o schema {contract.source_schema}.",
                reason_code="schema_contract_mismatch",
                checked_at=checked_at,
                details=details,
            ),
            operation=dimension_unavailable(
                message="Contrato do Airflow incompatível.",
                reason_code="schema_contract_mismatch",
                checked_at=checked_at,
                details=details,
            ),
            consumption=dimension_unavailable(
                message="Contrato do Airflow incompatível.",
                reason_code="schema_contract_mismatch",
                checked_at=checked_at,
                details=details,
            ),
            checked_at=checked_at,
            contract_version=contract_version,
        )

    if health_row is None:
        connectivity = dimension_unknown(
            message="Saúde do Airflow ainda não foi materializada.",
            reason_code="health_unknown",
            checked_at=checked_at,
        )
    elif health_row.status in {"unavailable", "misconfigured"}:
        connectivity = dimension_down(
            message=health_row.status_message or "A integração do Airflow não está disponível no momento.",
            reason_code=health_row.error_type or health_row.category or "integration_unavailable",
            checked_at=health_row.checked_at,
            details={"breaker_state": health_row.breaker_state, "category": health_row.category},
        )
    elif health_row.status == "degraded":
        connectivity = dimension_degraded(
            message=health_row.status_message or "A integração do Airflow apresenta degradação.",
            reason_code=health_row.error_type or health_row.category or "integration_degraded",
            checked_at=health_row.checked_at,
            details={"breaker_state": health_row.breaker_state, "category": health_row.category},
        )
    else:
        connectivity = dimension_healthy(
            message=health_row.status_message or "A integração do Airflow está saudável.",
            reason_code=health_row.category or "integration_healthy",
            checked_at=health_row.checked_at,
            details={"breaker_state": health_row.breaker_state, "category": health_row.category},
        )

    if summary_row is None:
        operation = dimension_unknown(
            message="Resumo operacional do Airflow indisponível.",
            reason_code="summary_unavailable",
            checked_at=checked_at,
        )
        consumption = dimension_unavailable(
            message="Resumo operacional do Airflow indisponível.",
            reason_code="summary_unavailable",
            checked_at=checked_at,
        )
    else:
        total_dags = int(summary_row.get("total_dags") or 0)
        active_dags = int(summary_row.get("active_dags") or 0)
        paused_dags = int(summary_row.get("paused_dags") or 0)
        success_runs_24h = int(summary_row.get("success_runs_24h") or 0)
        failed_runs_24h = int(summary_row.get("failed_runs_24h") or 0)
        task_failures_24h = int(summary_row.get("task_failures_24h") or 0)
        total_recent_runs = len(recent_runs or [])
        total_recent_failures = len(recent_failures or [])
        summary_details = {
            "total_dags": total_dags,
            "active_dags": active_dags,
            "paused_dags": paused_dags,
            "success_runs_24h": success_runs_24h,
            "failed_runs_24h": failed_runs_24h,
            "task_failures_24h": task_failures_24h,
            "recent_runs_count": total_recent_runs,
            "recent_failures_count": total_recent_failures,
        }
        if total_dags == 0:
            operation = dimension_idle(
                message="Airflow conectado, sem DAGs cadastradas.",
                reason_code="empty_catalog",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )
            consumption = dimension_available(
                message="Camada de consumo do Airflow ainda vazia.",
                reason_code="empty_catalog",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )
        elif failed_runs_24h > 0 or task_failures_24h > 0:
            operation = dimension_failed(
                message="Falhas recentes detectadas na orquestração do Airflow.",
                reason_code="recent_failures",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )
            consumption = dimension_available(
                message="DAGs e execuções materializadas no Airflow.",
                reason_code="materialized",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )
        elif success_runs_24h > 0:
            operation = dimension_running(
                message="Airflow conectado e com execuções disponíveis.",
                reason_code="recent_runs",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )
            consumption = dimension_available(
                message="DAGs e execuções materializadas no Airflow.",
                reason_code="materialized",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )
        else:
            operation = dimension_idle(
                message="Airflow conectado, sem atividade recente.",
                reason_code="idle",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )
            consumption = dimension_available(
                message="DAGs materializadas no Airflow.",
                reason_code="materialized",
                checked_at=summary_row.get("updated_at") or checked_at,
                details=summary_details,
            )

    return resolve_status_contract(
        source_name="airflow",
        connectivity=connectivity,
        operation=operation,
        consumption=consumption,
        checked_at=checked_at,
        contract_version=contract_version,
    )


def _airflow_contract_snapshot_details(contract: AirflowReadModelContractSnapshot | None) -> dict[str, object] | None:
    if contract is None:
        return None
    return {
        "source_schema": contract.source_schema,
        "schema_exists": contract.schema_exists,
        "dag_runs_table_exists": contract.dag_runs_table_exists,
        "dag_table_exists": contract.dag_table_exists,
        "task_instance_table_exists": contract.task_instance_table_exists,
        "dag_tag_table_exists": contract.dag_tag_table_exists,
        "task_fail_table_exists": contract.task_fail_table_exists,
        "log_table_exists": contract.log_table_exists,
        "dag_runs_view_exists": contract.dag_runs_view_exists,
        "dags_view_exists": contract.dags_view_exists,
        "failures_view_exists": contract.failures_view_exists,
        "operational_view_exists": contract.operational_view_exists,
        "ready": contract.ready,
        "missing_tables": contract.missing_tables,
        "missing_views": contract.missing_views,
        "contract_version": contract.contract_version,
    }


def _airflow_summary_message(
    *,
    health_status: str,
    operational_status: str,
    summary_row: dict[str, object] | None,
    fallback_message: str | None = None,
) -> str | None:
    if health_status == "unavailable":
        return fallback_message or "A integração do Airflow não está disponível no momento."
    if health_status == "misconfigured":
        return fallback_message or "A integração do Airflow está mal configurada."
    if operational_status == "connected_empty":
        return "Airflow conectado, sem DAGs cadastradas."
    if operational_status == "connected_no_runs":
        return "Existem DAGs cadastradas, mas ainda sem execuções."
    if summary_row is not None and int(summary_row.get("failed_runs_24h") or 0) > 0:
        return "Falhas recentes detectadas na orquestração do Airflow."
    if summary_row is not None and int(summary_row.get("success_runs_24h") or 0) > 0:
        return "Airflow conectado e com execuções disponíveis."
    return fallback_message or "Airflow conectado e pronto para acompanhar a orquestração."


def _airflow_summary_details(
    summary_row: dict[str, object] | None,
    recent_runs: list[dict[str, object]] | None = None,
    recent_failures: list[dict[str, object]] | None = None,
) -> dict[str, object] | None:
    if summary_row is None:
        return None
    payload: dict[str, object] = {
        "total_dags": int(summary_row.get("total_dags") or 0),
        "active_dags": int(summary_row.get("active_dags") or 0),
        "paused_dags": int(summary_row.get("paused_dags") or 0),
        "success_runs_24h": int(summary_row.get("success_runs_24h") or 0),
        "failed_runs_24h": int(summary_row.get("failed_runs_24h") or 0),
        "task_failures_24h": int(summary_row.get("task_failures_24h") or 0),
        "latest_execution_at": summary_row.get("latest_execution_at"),
        "latest_failure_at": summary_row.get("latest_failure_at"),
        "latest_log_at": summary_row.get("latest_log_at"),
        "updated_at": summary_row.get("updated_at"),
    }
    if recent_runs is not None:
        payload["recent_runs"] = recent_runs
    if recent_failures is not None:
        payload["recent_failures"] = recent_failures
    return payload


def _restore_airflow_runs(payload: dict[str, object], key: str) -> list[AirflowIntegrationDagRunOut]:
    items = payload.get(key)
    if not isinstance(items, list):
        return []
    restored: list[AirflowIntegrationDagRunOut] = []
    for item in items:
        if isinstance(item, dict):
            restored.append(AirflowIntegrationDagRunOut.model_validate(item))
    return restored


def _restore_airflow_failures(payload: dict[str, object], key: str) -> list[AirflowIntegrationTaskFailureOut]:
    items = payload.get(key)
    if not isinstance(items, list):
        return []
    restored: list[AirflowIntegrationTaskFailureOut] = []
    for item in items:
        if isinstance(item, dict):
            restored.append(AirflowIntegrationTaskFailureOut.model_validate(item))
    return restored


def load_airflow_integration_summary(session: Session) -> AirflowIntegrationSummaryOut:
    airflow_ui_base_url = None
    try:
        settings_snapshot = get_governance_settings_snapshot(session)
        airflow_ui_base_url = settings_snapshot.airflow_ui_base_url
    except Exception:  # noqa: BLE001
        pass
    configured = bool(settings.operational_ingestion_configured)
    health_row = get_integration_health(session, "airflow")
    persisted_details = get_integration_health_details(health_row)
    if not configured:
        snapshot = _base_health_snapshot(
            "airflow",
            base_url=airflow_ui_base_url,
            status="misconfigured",
            status_message="A integração do Airflow não está configurada neste ambiente.",
            category="configuration",
            last_failure_at=_now(),
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            consecutive_failures=(health_row.consecutive_failures if health_row is not None else 0) + 1,
            details_json=persisted_details or None,
            breaker_state="closed",
        )
        health_row = upsert_integration_health(session, snapshot)
        details = get_integration_health_details(health_row)
        return AirflowIntegrationSummaryOut(
            configured=configured,
            enabled=False,
            available=bool(details),
            integration_status=health_row.status,
            health_category=health_row.category,
            status_message=health_row.status_message,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            operational_status=details.get("operational_status") if details else "unavailable",
            message=health_row.status_message,
            generated_at=health_row.checked_at,
            airflow_ui_base_url=airflow_ui_base_url,
            total_dags=int(details.get("total_dags") or 0) if details else 0,
            active_dags=int(details.get("active_dags") or 0) if details else 0,
            paused_dags=int(details.get("paused_dags") or 0) if details else 0,
            success_runs_24h=int(details.get("success_runs_24h") or 0) if details else 0,
            failed_runs_24h=int(details.get("failed_runs_24h") or 0) if details else 0,
            task_failures_24h=int(details.get("task_failures_24h") or 0) if details else 0,
            latest_execution_at=details.get("latest_execution_at") if details else None,
            latest_failure_at=details.get("latest_failure_at") if details else None,
            latest_log_at=details.get("latest_log_at") if details else None,
            updated_at=details.get("updated_at") if details else None,
            recent_runs=_restore_airflow_runs(details, "recent_runs") if details else [],
            recent_failures=_restore_airflow_failures(details, "recent_failures") if details else [],
        )

    status_contract = _airflow_status_contract(
        configured=configured,
        contract=None,
        health_row=health_row,
        summary_row=None,
    )
    if not configured:
        snapshot = _base_health_snapshot(
            "airflow",
            base_url=airflow_ui_base_url,
            status="misconfigured",
            status_message=status_contract.overall_message,
            category="configuration",
            last_failure_at=_now(),
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            consecutive_failures=(health_row.consecutive_failures if health_row is not None else 0) + 1,
            details_json={"status_contract": status_contract.model_dump(mode="json")},
            breaker_state="closed",
        )
        health_row = upsert_integration_health(session, snapshot)
        return AirflowIntegrationSummaryOut(
            configured=configured,
            enabled=False,
            available=False,
            integration_status=health_row.status,
            health_category=health_row.category,
            status_message=health_row.status_message,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            operational_status="unavailable",
            message=health_row.status_message,
            generated_at=health_row.checked_at,
            airflow_ui_base_url=airflow_ui_base_url,
            total_dags=0,
            active_dags=0,
            paused_dags=0,
            success_runs_24h=0,
            failed_runs_24h=0,
            task_failures_24h=0,
            latest_execution_at=None,
            latest_failure_at=None,
            latest_log_at=None,
            updated_at=None,
            recent_runs=[],
            recent_failures=[],
            status_contract=status_contract,
        )

    summary_row: dict[str, object] | None = None
    recent_runs: list[AirflowIntegrationDagRunOut] = []
    recent_failures: list[AirflowIntegrationTaskFailureOut] = []
    contract_snapshot: AirflowReadModelContractSnapshot | None = None
    try:
        with operational_session() as airflow_session:
            contract_snapshot = _validate_airflow_operational_contract_cached(airflow_session)
            if contract_snapshot.ready:
                summary_row = _airflow_fetch_one(airflow_session, AIRFLOW_OPERATIONAL_VIEW)
                if summary_row is not None:
                    summary_row = _normalize_airflow_dag_summary_row(summary_row)
                recent_runs = [
                    AirflowIntegrationDagRunOut(**row)
                    for row in _airflow_fetch_many(
                        airflow_session,
                        AIRFLOW_DAG_RUNS_VIEW,
                        limit=5,
                        order_by="end_date DESC NULLS LAST, start_date DESC NULLS LAST, dag_id ASC, run_id DESC",
                    )
                ]
                recent_failures = [
                    AirflowIntegrationTaskFailureOut(**row)
                    for row in _airflow_fetch_many(
                        airflow_session,
                        AIRFLOW_FAILURES_VIEW,
                        limit=5,
                        order_by="end_date DESC NULLS LAST, start_date DESC NULLS LAST, dag_id ASC, task_id ASC, run_id DESC",
                    )
                ]
    except SQLAlchemyError as exc:
        logger.warning("airflow operational read unavailable error=%s", exc)
        classification = classify_integration_issue(exc, integration_name="airflow", phase="summary")
        current_time = _now()
        current_failures = (health_row.consecutive_failures if health_row is not None else 0) + 1
        status_contract = _airflow_status_contract(
            configured=configured,
            contract=contract_snapshot,
            health_row=health_row,
            summary_row=summary_row,
            recent_runs=[item.model_dump() for item in recent_runs],
            recent_failures=[item.model_dump() for item in recent_failures],
        )
        snapshot = _base_health_snapshot(
            "airflow",
            base_url=airflow_ui_base_url,
            status=classification["status"],
            status_message=classification["message"],
            category=classification["category"],
            last_success_at=health_row.last_success_at if health_row is not None else None,
            last_failure_at=current_time,
            consecutive_failures=current_failures,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type=classification["error_type"],
            error_summary=classification["message"],
            details_json={
                "status_contract": status_contract.model_dump(mode="json"),
                "contract": _airflow_contract_snapshot_details(contract_snapshot),
                "persisted_details": persisted_details or None,
            },
            breaker_state="open" if classification["retryable"] and current_failures >= DEFAULT_BREAKER_THRESHOLD else "closed",
        )
        if classification["retryable"]:
            snapshot = open_breaker(snapshot, threshold=DEFAULT_BREAKER_THRESHOLD, open_seconds=DEFAULT_BREAKER_OPEN_SECONDS)
        health_row = upsert_integration_health(session, snapshot)
        return AirflowIntegrationSummaryOut(
            configured=configured,
            enabled=True,
            available=bool(summary_row),
            integration_status=health_row.status,
            health_category=health_row.category,
            status_message=health_row.status_message,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            operational_status="unavailable",
            message=health_row.status_message,
            generated_at=health_row.checked_at,
            airflow_ui_base_url=airflow_ui_base_url,
            total_dags=0,
            active_dags=0,
            paused_dags=0,
            success_runs_24h=0,
            failed_runs_24h=0,
            task_failures_24h=0,
            latest_execution_at=None,
            latest_failure_at=None,
            latest_log_at=None,
            updated_at=None,
            recent_runs=[],
            recent_failures=[],
            status_contract=status_contract,
        )

    if contract_snapshot is not None and not contract_snapshot.ready:
        status_contract = _airflow_status_contract(
            configured=configured,
            contract=contract_snapshot,
            health_row=health_row,
            summary_row=summary_row,
            recent_runs=[item.model_dump() for item in recent_runs],
            recent_failures=[item.model_dump() for item in recent_failures],
        )
        snapshot = _base_health_snapshot(
            "airflow",
            base_url=airflow_ui_base_url,
            status="unavailable",
            status_message=status_contract.overall_message,
            category="configuration",
            last_success_at=health_row.last_success_at if health_row is not None else None,
            last_failure_at=_now(),
            consecutive_failures=(health_row.consecutive_failures if health_row is not None else 0) + 1,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type="schema_contract_mismatch",
            error_summary=status_contract.overall_message,
            details_json={
                "status_contract": status_contract.model_dump(mode="json"),
                "contract": _airflow_contract_snapshot_details(contract_snapshot),
            },
            breaker_state="closed",
        )
        health_row = upsert_integration_health(session, snapshot)
        return AirflowIntegrationSummaryOut(
            configured=configured,
            enabled=True,
            available=False,
            integration_status=health_row.status,
            health_category=health_row.category,
            status_message=health_row.status_message,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            operational_status="unavailable",
            message=health_row.status_message,
            generated_at=health_row.checked_at,
            airflow_ui_base_url=airflow_ui_base_url,
            total_dags=0,
            active_dags=0,
            paused_dags=0,
            success_runs_24h=0,
            failed_runs_24h=0,
            task_failures_24h=0,
            latest_execution_at=None,
            latest_failure_at=None,
            latest_log_at=None,
            updated_at=None,
            recent_runs=[],
            recent_failures=[],
            status_contract=status_contract,
        )

    operational_status = _airflow_operational_status_from_summary(summary_row)
    health_status = _airflow_health_status_from_summary(summary_row)
    status_contract = _airflow_status_contract(
        configured=configured,
        contract=contract_snapshot,
        health_row=health_row,
        summary_row=summary_row,
        recent_runs=[item.model_dump() for item in recent_runs],
        recent_failures=[item.model_dump() for item in recent_failures],
    )
    details = _airflow_summary_details(
        summary_row,
        recent_runs=[item.model_dump() for item in recent_runs],
        recent_failures=[item.model_dump() for item in recent_failures],
    )
    if details is not None:
        details["operational_status"] = operational_status
        details["integration_status"] = health_status
        details["status_contract"] = status_contract.model_dump(mode="json")
    message = _airflow_summary_message(
        health_status=health_status,
        operational_status=operational_status,
        summary_row=summary_row,
        fallback_message=None,
    )
    snapshot = _base_health_snapshot(
        "airflow",
        base_url=airflow_ui_base_url,
        status=health_status,
        status_message=message,
        category="consumption" if health_status == "empty" else "operation",
        last_success_at=_now(),
        last_failure_at=health_row.last_failure_at if health_row is not None else None,
        consecutive_failures=0,
        failure_count=health_row.failure_count if health_row is not None else 0,
        details_json=details,
        breaker_state="closed",
    )
    health_row = upsert_integration_health(session, snapshot)
    latest_execution_at = _airflow_summary_latest_label(summary_row)
    return AirflowIntegrationSummaryOut(
        configured=configured,
        enabled=True,
        available=summary_row is not None,
        integration_status=health_row.status,
        health_category=health_row.category,
        status_message=health_row.status_message,
        checked_at=health_row.checked_at,
        last_success_at=health_row.last_success_at,
        last_failure_at=health_row.last_failure_at,
        consecutive_failures=health_row.consecutive_failures,
        failure_count=health_row.failure_count,
        latency_ms=health_row.latency_ms,
        error_type=health_row.error_type,
        error_summary=health_row.error_summary,
        breaker_state=health_row.breaker_state,
        breaker_open_until_at=health_row.breaker_open_until_at,
        operational_status=operational_status,
        message=message,
        generated_at=(summary_row.get("updated_at") if summary_row is not None else _now()),
        airflow_ui_base_url=airflow_ui_base_url,
        total_dags=int(summary_row.get("total_dags") or 0) if summary_row is not None else 0,
        active_dags=int(summary_row.get("active_dags") or 0) if summary_row is not None else 0,
        paused_dags=int(summary_row.get("paused_dags") or 0) if summary_row is not None else 0,
        success_runs_24h=int(summary_row.get("success_runs_24h") or 0) if summary_row is not None else 0,
        failed_runs_24h=int(summary_row.get("failed_runs_24h") or 0) if summary_row is not None else 0,
        task_failures_24h=int(summary_row.get("task_failures_24h") or 0) if summary_row is not None else 0,
        latest_execution_at=latest_execution_at,
        latest_failure_at=(summary_row.get("latest_failure_at") if summary_row is not None else None),
        latest_log_at=(summary_row.get("latest_log_at") if summary_row is not None else None),
        updated_at=(summary_row.get("updated_at") if summary_row is not None else None),
        recent_runs=recent_runs,
        recent_failures=recent_failures,
        status_contract=status_contract,
    )


def load_airflow_integration_health(session: Session) -> AirflowIntegrationHealthOut:
    summary = load_airflow_integration_summary(session)
    status = "DOWN" if summary.integration_status in {"unavailable", "misconfigured"} else "UP"
    return AirflowIntegrationHealthOut(
        status=status,
        configured=summary.configured,
        enabled=summary.enabled,
        available=summary.available,
        integration_status=summary.integration_status,
        status_message=summary.status_message,
        health_category=summary.health_category,
        checked_at=summary.checked_at,
        last_success_at=summary.last_success_at,
        last_failure_at=summary.last_failure_at,
        consecutive_failures=summary.consecutive_failures,
        failure_count=summary.failure_count,
        latency_ms=summary.latency_ms,
        error_type=summary.error_type,
        error_summary=summary.error_summary,
        breaker_state=summary.breaker_state,
        breaker_open_until_at=summary.breaker_open_until_at,
        message=summary.message,
        airflow_ui_base_url=summary.airflow_ui_base_url,
        status_contract=summary.status_contract,
    )


def load_airflow_integration_pipelines(
    session: Session,
    *,
    page: int = 1,
    page_size: int = 10,
    search: str | None = None,
    status: str | None = None,
) -> AirflowIntegrationPipelinesOut:
    page = max(int(page or 1), 1)
    page_size = min(max(int(page_size or 10), 1), 100)
    offset = (page - 1) * page_size
    where_sql, filter_params = _airflow_dag_filter_clause(search=search, status=status)
    airflow_ui_base_url = None
    try:
        settings_snapshot = get_governance_settings_snapshot(session)
        airflow_ui_base_url = settings_snapshot.airflow_ui_base_url
    except Exception:  # noqa: BLE001
        pass
    configured = bool(settings.operational_ingestion_configured)
    health_row = get_integration_health(session, "airflow")
    total = 0
    if not configured:
        rows: list[dict[str, object]] = []
        view_available = False
        contract_snapshot = None
    else:
        try:
            with operational_session() as airflow_session:
                contract_snapshot = _validate_airflow_operational_contract_cached(airflow_session)
                view_available = contract_snapshot.ready and _airflow_relation_exists(airflow_session, AIRFLOW_DAGS_VIEW)
                if view_available:
                    total = _airflow_count(airflow_session, AIRFLOW_DAGS_VIEW, where_sql=where_sql, params=filter_params)
                    try:
                        rows = [
                            _normalize_airflow_dag_summary_row(row)
                            for row in _airflow_fetch_page(
                                airflow_session,
                                AIRFLOW_DAGS_VIEW,
                                limit=page_size,
                                offset=offset,
                                where_sql=where_sql,
                                params=filter_params,
                                order_by="last_execution_at DESC NULLS LAST, recent_failures_count_24h DESC, dag_id ASC",
                            )
                        ]
                    except SQLAlchemyError:
                        _airflow_safe_rollback(airflow_session, context="load_airflow_integration_pipelines:last_execution_at_fallback")
                        rows = [
                            _normalize_airflow_dag_summary_row(row)
                            for row in _airflow_fetch_page(
                                airflow_session,
                                AIRFLOW_DAGS_VIEW,
                                limit=page_size,
                                offset=offset,
                                where_sql=where_sql,
                                params=filter_params,
                                order_by="latest_execution_at DESC NULLS LAST, recent_failures_count_24h DESC, dag_id ASC",
                            )
                        ]
                else:
                    rows = []
        except SQLAlchemyError as exc:
            logger.warning("airflow pipelines read unavailable error=%s", exc)
            view_available = False
            contract_snapshot = None
            rows = []
    total_pages = (total + page_size - 1) // page_size if page_size else 0
    items = [AirflowIntegrationDagSummaryOut(**row) for row in rows]
    available = bool(rows)
    status_contract = _airflow_status_contract(
        configured=configured,
        contract=contract_snapshot,
        health_row=health_row,
        summary_row=None if not rows else {
            "total_dags": len(rows),
            "active_dags": sum(1 for row in rows if row.get("is_active")),
            "paused_dags": sum(1 for row in rows if row.get("is_paused")),
            "success_runs_24h": sum(int(row.get("recent_runs_count_24h") or 0) for row in rows),
            "failed_runs_24h": sum(int(row.get("recent_failures_count_24h") or 0) for row in rows),
            "task_failures_24h": 0,
            "updated_at": _now(),
        },
    )
    if contract_snapshot is not None and not contract_snapshot.ready:
        snapshot = _base_health_snapshot(
            "airflow",
            base_url=airflow_ui_base_url,
            status="unavailable",
            status_message=status_contract.overall_message,
            category="configuration",
            last_success_at=health_row.last_success_at if health_row is not None else None,
            last_failure_at=_now(),
            consecutive_failures=(health_row.consecutive_failures if health_row is not None else 0) + 1,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type="schema_contract_mismatch",
            error_summary=status_contract.overall_message,
            details_json={
                "status_contract": status_contract.model_dump(mode="json"),
                "contract": _airflow_contract_snapshot_details(contract_snapshot),
            },
            breaker_state="closed",
        )
        health_row = upsert_integration_health(session, snapshot)
        return AirflowIntegrationPipelinesOut(
            configured=configured,
            enabled=configured,
            available=False,
            integration_status=health_row.status,
            status_message=health_row.status_message,
            health_category=health_row.category,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            operational_status="unavailable",
            message=health_row.status_message,
            generated_at=health_row.checked_at,
            airflow_ui_base_url=airflow_ui_base_url,
            items=[],
            status_contract=status_contract,
            total=0,
            page=page,
            page_size=page_size,
            total_pages=0,
        )
    integration_status = health_row.status if health_row is not None else ("inactive" if not configured else ("degraded" if configured and not view_available else "healthy"))
    operational_status = "empty" if not rows else ("error" if any(item.recent_failures_count_24h > 0 for item in items) else "healthy")
    message = health_row.status_message if health_row is not None else None
    if not configured:
        message = "A integração do Airflow não está configurada neste ambiente."
    elif not view_available:
        message = f"A visão operacional do Airflow não está disponível no schema {AIRFLOW_CATALOG_SCHEMA}."
    elif not rows:
        message = "Ainda não há DAGs resumidas na camada modelada do Airflow."
    return AirflowIntegrationPipelinesOut(
        configured=configured,
        enabled=configured,
        available=available,
        integration_status=integration_status,
        status_message=message,
        health_category=health_row.category if health_row is not None else None,
        checked_at=health_row.checked_at if health_row is not None else None,
        last_success_at=health_row.last_success_at if health_row is not None else None,
        last_failure_at=health_row.last_failure_at if health_row is not None else None,
        consecutive_failures=health_row.consecutive_failures if health_row is not None else 0,
        failure_count=health_row.failure_count if health_row is not None else 0,
        latency_ms=health_row.latency_ms if health_row is not None else None,
        error_type=health_row.error_type if health_row is not None else None,
        error_summary=health_row.error_summary if health_row is not None else None,
        breaker_state=health_row.breaker_state if health_row is not None else None,
        breaker_open_until_at=health_row.breaker_open_until_at if health_row is not None else None,
        operational_status=operational_status,
        message=message,
        generated_at=(_now()),
        airflow_ui_base_url=airflow_ui_base_url,
        items=items,
        status_contract=status_contract,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


def load_airflow_integration_failures(session: Session, *, limit: int = 20) -> AirflowIntegrationFailuresOut:
    airflow_ui_base_url = None
    try:
        settings_snapshot = get_governance_settings_snapshot(session)
        airflow_ui_base_url = settings_snapshot.airflow_ui_base_url
    except Exception:  # noqa: BLE001
        pass
    configured = bool(settings.operational_ingestion_configured)
    health_row = get_integration_health(session, "airflow")
    if not configured:
        rows = []
        view_available = False
        contract_snapshot = None
    else:
        try:
            with operational_session() as airflow_session:
                contract_snapshot = _validate_airflow_operational_contract_cached(airflow_session)
                view_available = contract_snapshot.ready and _airflow_relation_exists(airflow_session, AIRFLOW_FAILURES_VIEW)
                if view_available:
                    rows = _airflow_fetch_many(
                        airflow_session,
                        AIRFLOW_FAILURES_VIEW,
                        limit=limit,
                        order_by="end_date DESC NULLS LAST, start_date DESC NULLS LAST, dag_id ASC, task_id ASC, run_id DESC",
                    )
                else:
                    rows = []
        except SQLAlchemyError as exc:
            logger.warning("airflow failures read unavailable error=%s", exc)
            view_available = False
            contract_snapshot = None
            rows = []
    items = [AirflowIntegrationTaskFailureOut(**row) for row in rows]
    available = bool(rows)
    status_contract = _airflow_status_contract(
        configured=configured,
        contract=contract_snapshot,
        health_row=health_row,
        summary_row=None if not rows else {
            "total_dags": len(rows),
            "active_dags": 0,
            "paused_dags": 0,
            "success_runs_24h": 0,
            "failed_runs_24h": len(rows),
            "task_failures_24h": len(rows),
            "updated_at": _now(),
        },
    )
    if contract_snapshot is not None and not contract_snapshot.ready:
        snapshot = _base_health_snapshot(
            "airflow",
            base_url=airflow_ui_base_url,
            status="unavailable",
            status_message=status_contract.overall_message,
            category="configuration",
            last_success_at=health_row.last_success_at if health_row is not None else None,
            last_failure_at=_now(),
            consecutive_failures=(health_row.consecutive_failures if health_row is not None else 0) + 1,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type="schema_contract_mismatch",
            error_summary=status_contract.overall_message,
            details_json={
                "status_contract": status_contract.model_dump(mode="json"),
                "contract": _airflow_contract_snapshot_details(contract_snapshot),
            },
            breaker_state="closed",
        )
        health_row = upsert_integration_health(session, snapshot)
        return AirflowIntegrationFailuresOut(
            configured=configured,
            enabled=configured,
            available=False,
            integration_status=health_row.status,
            status_message=health_row.status_message,
            health_category=health_row.category,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            operational_status="unavailable",
            message=health_row.status_message,
            generated_at=_now(),
            airflow_ui_base_url=airflow_ui_base_url,
            items=[],
            status_contract=status_contract,
        )
    integration_status = health_row.status if health_row is not None else ("inactive" if not configured else ("degraded" if configured and not view_available else "healthy"))
    operational_status = "empty" if not rows else "error"
    message = health_row.status_message if health_row is not None else None
    if not configured:
        message = "A integração do Airflow não está configurada neste ambiente."
    elif not view_available:
        message = f"A visão de falhas do Airflow não está disponível no schema {AIRFLOW_CATALOG_SCHEMA}."
    elif not rows:
        message = "Ainda não há falhas recentes materializadas no Airflow."
    return AirflowIntegrationFailuresOut(
        configured=configured,
        enabled=configured,
        available=available,
        integration_status=integration_status,
        status_message=message,
        health_category=health_row.category if health_row is not None else None,
        checked_at=health_row.checked_at if health_row is not None else None,
        last_success_at=health_row.last_success_at if health_row is not None else None,
        last_failure_at=health_row.last_failure_at if health_row is not None else None,
        consecutive_failures=health_row.consecutive_failures if health_row is not None else 0,
        failure_count=health_row.failure_count if health_row is not None else 0,
        latency_ms=health_row.latency_ms if health_row is not None else None,
        error_type=health_row.error_type if health_row is not None else None,
        error_summary=health_row.error_summary if health_row is not None else None,
        breaker_state=health_row.breaker_state if health_row is not None else None,
        breaker_open_until_at=health_row.breaker_open_until_at if health_row is not None else None,
        operational_status=operational_status,
        message=message,
        generated_at=_now(),
        airflow_ui_base_url=airflow_ui_base_url,
        items=items,
        status_contract=status_contract,
    )


def _metabase_instance_or_none(session: Session) -> MetabaseInstance | None:
    return session.scalar(
        select(MetabaseInstance).where(MetabaseInstance.enabled.is_(True)).order_by(MetabaseInstance.updated_at.desc(), MetabaseInstance.id.desc())
    )


def _metabase_any_instance(session: Session) -> MetabaseInstance | None:
    return session.scalar(select(MetabaseInstance).order_by(MetabaseInstance.updated_at.desc(), MetabaseInstance.id.desc()))


def _metabase_health_details(health_row: IntegrationHealth | None) -> dict[str, object]:
    return get_integration_health_details(health_row)


def _metabase_health_status(instance: MetabaseInstance | None, latest_sync: MetabaseSyncRun | None, health_row: IntegrationHealth | None) -> tuple[bool, bool, bool, str, str | None]:
    if instance is None:
        return False, False, False, "misconfigured", "Nenhuma instância do Metabase está configurada."
    enabled = bool(instance.enabled)
    available = bool(enabled and instance.base_url)
    if not enabled:
        return True, False, False, "misconfigured", instance.last_sync_message or "Integração desativada."
    if not instance.base_url:
        return True, True, False, "misconfigured", "A URL da instância não está configurada."
    if latest_sync is None:
        return True, True, True, "empty", "Integração configurada, aguardando primeira sync."
    if (latest_sync.status or "").lower() == "failed":
        if health_row is not None and health_row.status == "unavailable":
            return True, True, True, "unavailable", health_row.status_message or latest_sync.error_message or instance.last_sync_message
        return True, True, True, "degraded", latest_sync.error_message or instance.last_sync_message
    if int(instance.last_sync_dashboards or 0) == 0 and int(instance.last_sync_questions or 0) == 0 and int(instance.last_sync_collections or 0) == 0:
        return True, True, True, "empty", "A integração está conectada, mas ainda sem artefatos sincronizados."
    if int(instance.last_sync_unresolved or 0) > 0 or int(instance.last_sync_warnings or 0) > 0:
        return True, True, True, "degraded", latest_sync.error_message or instance.last_sync_message or "Sincronização parcial detectada."
    return True, True, True, "healthy", latest_sync.error_message or instance.last_sync_message


def _metabase_health_category_from_status(status: str) -> str:
    if status == "misconfigured":
        return "configuration"
    if status == "empty":
        return "consumption"
    return "operation"


def _metabase_status_contract(
    *,
    instance: MetabaseInstance | None,
    health_row: IntegrationHealth | None,
    latest_sync: MetabaseSyncRun | None,
    summary_counts: dict[str, int] | None = None,
    checked_at: datetime | None = None,
) -> IntegrationStatusContractOut:
    checked_at = checked_at or (health_row.checked_at if health_row is not None else (_now()))
    contract_version = "v1"
    if instance is None:
        return resolve_status_contract(
            source_name="metabase",
            connectivity=dimension_down(
                message="Nenhuma instância do Metabase está configurada.",
                reason_code="not_configured",
                checked_at=checked_at,
            ),
            operation=dimension_unknown(
                message="Nenhuma instância do Metabase está configurada.",
                reason_code="not_configured",
                checked_at=checked_at,
            ),
            consumption=dimension_unavailable(
                message="Nenhuma instância do Metabase está configurada.",
                reason_code="not_configured",
                checked_at=checked_at,
            ),
            checked_at=checked_at,
            contract_version=contract_version,
        )

    summary_counts = summary_counts or {}
    dashboards_count = int(summary_counts.get("dashboards_count") or 0)
    questions_count = int(summary_counts.get("questions_count") or 0)
    collections_count = int(summary_counts.get("collections_count") or 0)
    direct_links_count = int(summary_counts.get("direct_links_count") or 0)
    indirect_links_count = int(summary_counts.get("indirect_links_count") or 0)
    total_links_count = int(summary_counts.get("total_links_count") or 0)
    tables_with_consumption_count = int(summary_counts.get("tables_with_consumption_count") or 0)
    unresolved_count = int(summary_counts.get("unresolved_count") or 0)
    warnings_count = int(summary_counts.get("warnings_count") or 0)

    if not instance.base_url:
        connectivity = dimension_down(
            message="A URL da instância do Metabase não está configurada.",
            reason_code="missing_base_url",
            checked_at=checked_at,
        )
    elif health_row is not None and health_row.status == "unavailable":
        connectivity = dimension_down(
            message=health_row.status_message or "A instância do Metabase não está disponível.",
            reason_code=health_row.error_type or health_row.category or "source_unreachable",
            checked_at=health_row.checked_at,
            details={"breaker_state": health_row.breaker_state},
        )
    elif health_row is not None and health_row.status == "degraded":
        connectivity = dimension_degraded(
            message=health_row.status_message or "A instância do Metabase apresenta degradação.",
            reason_code=health_row.error_type or health_row.category or "integration_degraded",
            checked_at=health_row.checked_at,
            details={"breaker_state": health_row.breaker_state},
        )
    elif health_row is not None and health_row.status == "misconfigured":
        connectivity = dimension_down(
            message=health_row.status_message or "A integração do Metabase está mal configurada.",
            reason_code=health_row.error_type or health_row.category or "misconfigured",
            checked_at=health_row.checked_at,
            details={"breaker_state": health_row.breaker_state},
        )
    else:
        connectivity = dimension_healthy(
            message=health_row.status_message if health_row is not None else "A integração do Metabase está saudável.",
            reason_code="health_ok" if health_row is not None else "health_unknown",
            checked_at=health_row.checked_at if health_row is not None else checked_at,
            details={"breaker_state": health_row.breaker_state if health_row is not None else "closed"},
        )

    if latest_sync is None:
        operation = dimension_idle(
            message="Integração configurada, aguardando primeira sync.",
            reason_code="never_synced",
            checked_at=checked_at,
            details={"last_sync_status": "never_synced"},
        )
    else:
        latest_sync_status = (latest_sync.status or "").strip().lower()
        if latest_sync_status == "running":
            operation = dimension_running(
                message="Sincronização do Metabase em andamento.",
                reason_code="sync_running",
                checked_at=latest_sync.started_at or checked_at,
                details={"last_sync_status": latest_sync_status},
            )
        elif latest_sync_status == "failed":
            operation = dimension_failed(
                message=latest_sync.error_message or instance.last_sync_message or "Sincronização do Metabase falhou.",
                reason_code="sync_failed",
                checked_at=latest_sync.started_at or checked_at,
                details={"last_sync_status": latest_sync_status},
            )
        elif unresolved_count > 0 or warnings_count > 0:
            operation = dimension_delayed(
                message=latest_sync.error_message or instance.last_sync_message or "Sincronização parcial detectada.",
                reason_code="sync_partial",
                checked_at=latest_sync.started_at or checked_at,
                details={"last_sync_status": latest_sync_status},
            )
        else:
            operation = dimension_idle(
                message=latest_sync.error_message or instance.last_sync_message or "Sincronização concluída.",
                reason_code="sync_ok",
                checked_at=latest_sync.started_at or checked_at,
                details={"last_sync_status": latest_sync_status},
            )

    if dashboards_count == 0 and questions_count == 0 and collections_count == 0 and total_links_count == 0:
        consumption = dimension_available(
            message="A integração do Metabase está conectada, mas ainda sem artefatos sincronizados.",
            reason_code="empty_consumption",
            checked_at=checked_at,
            details=summary_counts,
        )
    elif unresolved_count > 0 or warnings_count > 0:
        consumption = dimension_partial(
            message="Há artefatos sincronizados, mas com pendências ou inconsistências.",
            reason_code="partial_consumption",
            checked_at=checked_at,
            details=summary_counts,
        )
    else:
        consumption = dimension_available(
            message="Artefatos do Metabase disponíveis para consumo analítico.",
            reason_code="consumption_available",
            checked_at=checked_at,
            details=summary_counts,
        )

    return resolve_status_contract(
        source_name="metabase",
        connectivity=connectivity,
        operation=operation,
        consumption=consumption,
        checked_at=checked_at,
        contract_version=contract_version,
    )


def _metabase_health_snapshot(
    *,
    instance: MetabaseInstance,
    health_row: IntegrationHealth | None,
    status: str,
    status_message: str | None,
    category: str | None,
    checked_at: datetime,
    last_success_at: datetime | None = None,
    last_failure_at: datetime | None = None,
    consecutive_failures: int | None = None,
    failure_count: int | None = None,
    latency_ms: int | None = None,
    error_type: str | None = None,
    error_summary: str | None = None,
    details_json: dict[str, object] | None = None,
    breaker_state: str = "closed",
    breaker_open_until_at: datetime | None = None,
) -> IntegrationHealthSnapshot:
    return _base_health_snapshot(
        "metabase",
        base_url=instance.base_url,
        status=status,
        status_message=status_message,
        category=category,
        checked_at=checked_at,
        last_success_at=last_success_at if last_success_at is not None else (health_row.last_success_at if health_row is not None else None),
        last_failure_at=last_failure_at if last_failure_at is not None else (health_row.last_failure_at if health_row is not None else None),
        consecutive_failures=consecutive_failures if consecutive_failures is not None else (health_row.consecutive_failures if health_row is not None else 0),
        failure_count=failure_count if failure_count is not None else (health_row.failure_count if health_row is not None else 0),
        latency_ms=latency_ms,
        error_type=error_type,
        error_summary=error_summary,
        details_json=details_json,
        breaker_state=breaker_state,
        breaker_open_until_at=breaker_open_until_at,
    )


def load_metabase_integration_health(session: Session) -> MetabaseIntegrationHealthOut:
    latest_instance = _metabase_any_instance(session)
    if latest_instance is None:
        ensure_metabase_instance_from_settings(session)
        latest_instance = _metabase_any_instance(session)
    if latest_instance is None:
        return MetabaseIntegrationHealthOut(
            status="DOWN",
            configured=False,
            enabled=False,
            available=False,
            message="Nenhuma instância do Metabase está configurada.",
            checked_at=_now(),
            status_contract=_metabase_status_contract(
                instance=None,
                health_row=None,
                latest_sync=None,
                summary_counts=None,
                checked_at=_now(),
            ),
        )

    instance = _metabase_instance_or_none(session) or latest_instance
    health_row = get_integration_health(session, "metabase")
    latest_sync = session.scalar(
        select(MetabaseSyncRun)
        .where(MetabaseSyncRun.instance_id == instance.id)
        .order_by(MetabaseSyncRun.started_at.desc(), MetabaseSyncRun.id.desc())
        .limit(1)
    )
    if health_row is not None and is_breaker_open(health_row):
        return MetabaseIntegrationHealthOut(
            status="DOWN",
            configured=bool(instance.base_url),
            enabled=bool(instance.enabled and instance.base_url),
            available=False,
            integration_status=health_row.status,
            status_message=health_row.status_message,
            health_category=health_row.category,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            instance_id=instance.id,
            instance_name=instance.name,
            instance_base_url=instance.base_url,
            message=health_row.status_message,
            status_contract=_metabase_status_contract(
                instance=instance,
                health_row=health_row,
                latest_sync=latest_sync,
                summary_counts={
                    "dashboards_count": int(instance.last_sync_dashboards or 0),
                    "questions_count": int(instance.last_sync_questions or 0),
                    "collections_count": int(instance.last_sync_collections or 0),
                    "direct_links_count": int(instance.last_sync_links or 0),
                    "indirect_links_count": 0,
                    "total_links_count": int(instance.last_sync_links or 0),
                    "tables_with_consumption_count": 0,
                    "unresolved_count": int(instance.last_sync_unresolved or 0),
                    "warnings_count": int(instance.last_sync_warnings or 0),
                },
                checked_at=health_row.checked_at,
            ),
        )

    configured = bool(instance.base_url)
    enabled = bool(instance.enabled and configured)
    if not configured:
        snapshot = _metabase_health_snapshot(
            instance=instance,
            health_row=health_row,
            status="misconfigured",
            status_message="A URL da instância do Metabase não está configurada.",
            category="configuration",
            checked_at=_now(),
            error_type="configuration_error",
            error_summary="A URL da instância do Metabase não está configurada.",
        )
        health_row = upsert_integration_health(session, snapshot)
        return MetabaseIntegrationHealthOut(
            status="DOWN",
            configured=False,
            enabled=enabled,
            available=False,
            integration_status=health_row.status,
            status_message=health_row.status_message,
            health_category=health_row.category,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            instance_id=instance.id,
            instance_name=instance.name,
            instance_base_url=instance.base_url,
            message=health_row.status_message,
            status_contract=_metabase_status_contract(
                instance=instance,
                health_row=health_row,
                latest_sync=latest_sync,
                summary_counts={
                    "dashboards_count": int(instance.last_sync_dashboards or 0),
                    "questions_count": int(instance.last_sync_questions or 0),
                    "collections_count": int(instance.last_sync_collections or 0),
                    "direct_links_count": int(instance.last_sync_links or 0),
                    "indirect_links_count": 0,
                    "total_links_count": int(instance.last_sync_links or 0),
                    "tables_with_consumption_count": 0,
                    "unresolved_count": int(instance.last_sync_unresolved or 0),
                    "warnings_count": int(instance.last_sync_warnings or 0),
                },
                checked_at=health_row.checked_at,
            ),
        )
    if not instance.enabled:
        snapshot = _metabase_health_snapshot(
            instance=instance,
            health_row=health_row,
            status="misconfigured",
            status_message="A integração do Metabase está desativada.",
            category="configuration",
            checked_at=_now(),
            error_type="configuration_error",
            error_summary="A integração do Metabase está desativada.",
        )
        health_row = upsert_integration_health(session, snapshot)
        return MetabaseIntegrationHealthOut(
            status="DOWN",
            configured=True,
            enabled=False,
            available=False,
            integration_status=health_row.status,
            status_message=health_row.status_message,
            health_category=health_row.category,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            instance_id=instance.id,
            instance_name=instance.name,
            instance_base_url=instance.base_url,
            message=health_row.status_message,
            status_contract=_metabase_status_contract(
                instance=instance,
                health_row=health_row,
                latest_sync=latest_sync,
                summary_counts={
                    "dashboards_count": int(instance.last_sync_dashboards or 0),
                    "questions_count": int(instance.last_sync_questions or 0),
                    "collections_count": int(instance.last_sync_collections or 0),
                    "direct_links_count": int(instance.last_sync_links or 0),
                    "indirect_links_count": 0,
                    "total_links_count": int(instance.last_sync_links or 0),
                    "tables_with_consumption_count": 0,
                    "unresolved_count": int(instance.last_sync_unresolved or 0),
                    "warnings_count": int(instance.last_sync_warnings or 0),
                },
                checked_at=health_row.checked_at,
            ),
        )

    try:
        start = _now()
        with MetabaseClient(
            MetabaseClientConfig(
                base_url=instance.base_url,
                auth_type=instance.auth_type,
                auth_username=instance.auth_username,
                auth_secret=instance.auth_secret,
                timeout_seconds=instance.timeout_seconds,
            )
        ) as client:
            retryable_predicate = build_retryable_predicate(source="integrations.metabase.health")
            if (instance.auth_type or "").strip().lower() == "session":
                retry_with_backoff(lambda: client.authenticate(), retryable=retryable_predicate)
            probe_payload = retry_with_backoff(lambda: client.probe_health(), retryable=retryable_predicate)
        semantic_status, status_message = _metabase_health_status(instance, latest_sync, health_row)[3:5]
        if latest_sync is None:
            semantic_status = "empty"
            status_message = "Integração configurada, aguardando primeira sync."
        details = {
            "sync_status": latest_sync.status if latest_sync is not None else "never_synced",
            "last_sync_at": instance.last_sync_at,
            "last_sync_status": instance.last_sync_status,
            "last_sync_message": instance.last_sync_message,
            "dashboards_count": int(instance.last_sync_dashboards or 0),
            "questions_count": int(instance.last_sync_questions or 0),
            "collections_count": int(instance.last_sync_collections or 0),
            "direct_links_count": int(instance.last_sync_links or 0),
            "total_links_count": int(instance.last_sync_links or 0),
            "unresolved_count": int(instance.last_sync_unresolved or 0),
            "warnings_count": int(instance.last_sync_warnings or 0),
            "transport_health": probe_payload,
        }
        if latest_sync is not None and (latest_sync.status or "").lower() == "failed":
            semantic_status = "degraded"
            status_message = latest_sync.error_message or instance.last_sync_message or status_message
        if int(instance.last_sync_dashboards or 0) == 0 and int(instance.last_sync_questions or 0) == 0 and int(instance.last_sync_collections or 0) == 0:
            semantic_status = "empty"
            status_message = status_message or "A integração está conectada, mas ainda sem artefatos sincronizados."
        elif int(instance.last_sync_unresolved or 0) > 0 or int(instance.last_sync_warnings or 0) > 0:
            semantic_status = "degraded"
            status_message = status_message or "Sincronização parcial detectada."
        snapshot = _metabase_health_snapshot(
            instance=instance,
            health_row=health_row,
            status=semantic_status,
            status_message=status_message,
            category="consumption" if semantic_status == "empty" else "operation",
            checked_at=start,
            latency_ms=int(((_now() - start).total_seconds()) * 1000),
            details_json=details,
        )
        snapshot = close_breaker(snapshot)
        health_row = upsert_integration_health(session, snapshot)
        return MetabaseIntegrationHealthOut(
            status="UP" if semantic_status in {"healthy", "degraded", "empty"} else "DOWN",
            configured=True,
            enabled=True,
            available=semantic_status in {"healthy", "degraded", "empty"},
            integration_status=health_row.status,
            status_message=health_row.status_message,
            health_category=health_row.category,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            instance_id=instance.id,
            instance_name=instance.name,
            instance_base_url=instance.base_url,
            message=health_row.status_message,
            status_contract=_metabase_status_contract(
                instance=instance,
                health_row=health_row,
                latest_sync=latest_sync,
                summary_counts={
                    "dashboards_count": int(instance.last_sync_dashboards or 0),
                    "questions_count": int(instance.last_sync_questions or 0),
                    "collections_count": int(instance.last_sync_collections or 0),
                    "direct_links_count": int(instance.last_sync_links or 0),
                    "indirect_links_count": 0,
                    "total_links_count": int(instance.last_sync_links or 0),
                    "tables_with_consumption_count": 0,
                    "unresolved_count": int(instance.last_sync_unresolved or 0),
                    "warnings_count": int(instance.last_sync_warnings or 0),
                },
                checked_at=health_row.checked_at,
            ),
        )
    except MetabaseClientError as exc:
        classification = classify_integration_issue(exc, integration_name="metabase", phase="health")
        logger.warning("metabase health check failed instance_id=%s base_url=%s error=%s", instance.id, instance.base_url, exc)
        current_failures = (health_row.consecutive_failures if health_row is not None else 0) + 1
        snapshot = _metabase_health_snapshot(
            instance=instance,
            health_row=health_row,
            status=classification["status"],
            status_message=classification["message"],
            category=classification["category"],
            checked_at=_now(),
            last_failure_at=_now(),
            consecutive_failures=current_failures,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type=classification["error_type"],
            error_summary=classification["message"],
            details_json=_metabase_health_details(health_row) or None,
        )
        if classification["retryable"]:
            snapshot = open_breaker(snapshot, threshold=DEFAULT_BREAKER_THRESHOLD, open_seconds=DEFAULT_BREAKER_OPEN_SECONDS)
        health_row = upsert_integration_health(session, snapshot)
        return MetabaseIntegrationHealthOut(
            status="DOWN",
            configured=True,
            enabled=True,
            available=False,
            integration_status=health_row.status,
            status_message=health_row.status_message,
            health_category=health_row.category,
            checked_at=health_row.checked_at,
            last_success_at=health_row.last_success_at,
            last_failure_at=health_row.last_failure_at,
            consecutive_failures=health_row.consecutive_failures,
            failure_count=health_row.failure_count,
            latency_ms=health_row.latency_ms,
            error_type=health_row.error_type,
            error_summary=health_row.error_summary,
            breaker_state=health_row.breaker_state,
            breaker_open_until_at=health_row.breaker_open_until_at,
            instance_id=instance.id,
            instance_name=instance.name,
            instance_base_url=instance.base_url,
            message=health_row.status_message,
            status_contract=_metabase_status_contract(
                instance=instance,
                health_row=health_row,
                latest_sync=latest_sync,
                summary_counts={
                    "dashboards_count": int(instance.last_sync_dashboards or 0),
                    "questions_count": int(instance.last_sync_questions or 0),
                    "collections_count": int(instance.last_sync_collections or 0),
                    "direct_links_count": int(instance.last_sync_links or 0),
                    "indirect_links_count": 0,
                    "total_links_count": int(instance.last_sync_links or 0),
                    "tables_with_consumption_count": 0,
                    "unresolved_count": int(instance.last_sync_unresolved or 0),
                    "warnings_count": int(instance.last_sync_warnings or 0),
                },
                checked_at=health_row.checked_at,
            ),
        )


def _count_links(session: Session, instance_id: int) -> tuple[int, int, int]:
    filters = [MetabaseObjectLink.instance_id == instance_id, MetabaseObjectLink.is_active.is_(True)]
    total_links = session.scalar(select(func.count(MetabaseObjectLink.id)).where(*filters)) or 0
    direct_links = session.scalar(
        select(func.count(MetabaseObjectLink.id)).where(
            *filters,
            MetabaseObjectLink.match_method.in_(DIRECT_LINK_METHODS),
        )
    ) or 0
    indirect_links = session.scalar(
        select(func.count(MetabaseObjectLink.id)).where(
            *filters,
            MetabaseObjectLink.match_method.in_(INDIRECT_LINK_METHODS),
        )
    ) or 0
    return int(direct_links), int(indirect_links), int(total_links)


def _privacy_signals_for_profile(profile) -> list[str]:
    signals: list[str] = []
    if bool(getattr(profile, "has_sensitive_personal_data", False)):
        signals.append("dados pessoais sensíveis")
    elif bool(getattr(profile, "has_personal_data", False)):
        signals.append("dados pessoais")
    sensitivity_level = getattr(profile, "sensitivity_level", None)
    if sensitivity_level:
        signals.append(f"sensibilidade {sensitivity_level}")
    return signals


def _privacy_status_for_profile(profile) -> str | None:
    if bool(getattr(profile, "has_sensitive_personal_data", False)):
        return "sensitive"
    if bool(getattr(profile, "has_personal_data", False)):
        return "personal"
    sensitivity_level = getattr(profile, "sensitivity_level", None)
    if sensitivity_level:
        return f"classified:{sensitivity_level}"
    return None


def _linked_table_payload(table: TableEntity) -> MetabaseArtifactLinkedTableOut:
    return MetabaseArtifactLinkedTableOut(
        table_id=table.id,
        full_name=f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}",
        connection=table.schema.database.datasource.name,
        database=table.schema.database.name,
        schema_name=table.schema.name,
        table=table.name,
    )


def _build_referenced_tables(
    referenced_tables_json: Any,
    linked_tables: list[MetabaseArtifactLinkedTableOut],
) -> list[MetabaseArtifactReferencedTableOut]:
    """Serialize stored referenced tables, cross-linking to catalog tables when matched."""
    if not isinstance(referenced_tables_json, list):
        return []
    # Index catalog matches by the candidate names a referenced entry might carry.
    catalog_index: dict[str, MetabaseArtifactLinkedTableOut] = {}
    for linked in linked_tables:
        for candidate in (
            linked.full_name.lower(),
            f"{linked.schema_name}.{linked.table}".lower(),
            linked.table.lower(),
        ):
            catalog_index.setdefault(candidate, linked)

    out: list[MetabaseArtifactReferencedTableOut] = []
    for entry in referenced_tables_json:
        if not isinstance(entry, dict):
            continue
        full_name = str(entry.get("full_name") or entry.get("name") or "").strip()
        name = str(entry.get("name") or full_name).strip()
        if not full_name or not name:
            continue
        schema_name = entry.get("schema")
        schema_name = str(schema_name).strip() if isinstance(schema_name, str) and schema_name.strip() else None
        match = (
            catalog_index.get(full_name.lower())
            or catalog_index.get(f"{schema_name}.{name}".lower() if schema_name else "")
            or catalog_index.get(name.lower())
        )
        out.append(
            MetabaseArtifactReferencedTableOut(
                full_name=full_name,
                name=name,
                schema_name=schema_name,
                metabase_table_id=(str(entry["metabase_table_id"]) if entry.get("metabase_table_id") is not None else None),
                source="mbql" if str(entry.get("source")) == "mbql" else "sql",
                resolved=bool(entry.get("resolved", True)),
                table_id=match.table_id if match else None,
                catalog_full_name=match.full_name if match else None,
            )
        )
    return out


def _metabase_extract_cards_from_dashboard(dashboard: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    ordered = dashboard.get("ordered_cards")
    if isinstance(ordered, list):
        for item in ordered:
            data = item if isinstance(item, dict) else {}
            card_id = data.get("card_id") or data.get("cardId") or data.get("id")
            if card_id is not None:
                ids.append(str(card_id))
    dashcards = dashboard.get("dashcards")
    if isinstance(dashcards, list):
        for item in dashcards:
            data = item if isinstance(item, dict) else {}
            card = data.get("card") if isinstance(data.get("card"), dict) else {}
            card_id = data.get("card_id") or data.get("cardId") or card.get("id")
            if card_id is not None:
                ids.append(str(card_id))
    return list(dict.fromkeys(ids))


def _metabase_parse_dataset_query(dataset_query: dict[str, Any] | None) -> tuple[list[str], str | None]:
    if not isinstance(dataset_query, dict):
        return [], None

    tables: list[str] = []
    sql: str | None = None

    def walk(value: Any) -> None:
        nonlocal sql
        if isinstance(value, dict):
            native_value = value.get("native")
            if isinstance(native_value, str) and native_value.strip():
                if sql is None:
                    sql = native_value.strip()
                tables.extend(extract_sql_table_lineage(native_value))
            elif isinstance(native_value, dict):
                native_sql = native_value.get("query")
                if isinstance(native_sql, str) and native_sql.strip():
                    if sql is None:
                        sql = native_sql.strip()
                    tables.extend(extract_sql_table_lineage(native_sql))

            source_table = value.get("source-table")
            if source_table is not None:
                tables.append(str(source_table))

            for nested in value.values():
                walk(nested)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(dataset_query)
    normalized_tables = [table for table in tables if str(table).strip()]
    return list(dict.fromkeys(normalized_tables)), sql


def _metabase_sync_error_type(error_message: str | None) -> str | None:
    if not error_message:
        return None
    classification = classify_integration_issue(Exception(error_message), integration_name="metabase", phase="sync")
    error_type = str(classification.get("error_type") or "").strip()
    if error_type and error_type != "unknown_error":
        return error_type
    return "unclassified"


def _sync_run_duration_seconds(run: MetabaseSyncRun) -> int | None:
    if run.finished_at is None or run.started_at is None:
        return None
    return max(int((run.finished_at - run.started_at).total_seconds()), 0)


def _serialize_metabase_sync_run(run: MetabaseSyncRun, *, instance_name: str | None = None) -> MetabaseSyncRunOut:
    payload = {
        "id": run.id,
        "instance_id": run.instance_id,
        "instance_name": instance_name,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_seconds": _sync_run_duration_seconds(run),
        "dashboards_count": int(run.dashboards_count or 0),
        "questions_count": int(run.questions_count or 0),
        "collections_count": int(run.collections_count or 0),
        "links_count": int(run.links_count or 0),
        "artifacts_processed": int(run.dashboards_count or 0) + int(run.questions_count or 0) + int(run.collections_count or 0),
        "links_created": int(run.links_count or 0),
        "unresolved_count": int(run.unresolved_count or 0),
        "warnings_count": int(run.warnings_count or 0),
        "error_message": run.error_message,
        "error_type": _metabase_sync_error_type(run.error_message),
        "summary_json": run.summary_json,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    }
    return MetabaseSyncRunOut.model_validate(payload)


def _metabase_top_table_link_counts(session: Session, instance_id: int, table_ids: list[int]) -> dict[int, dict[str, int]]:
    if not table_ids:
        return {}
    rows = session.execute(
        select(
            MetabaseObjectLink.table_id.label("table_id"),
            MetabaseObject.object_type.label("object_type"),
            func.count(func.distinct(MetabaseObject.id)).label("artifact_count"),
        )
        .join(MetabaseObject, MetabaseObjectLink.metabase_object_id == MetabaseObject.id)
        .where(
            MetabaseObjectLink.instance_id == instance_id,
            MetabaseObjectLink.is_active.is_(True),
            MetabaseObjectLink.table_id.in_(table_ids),
            MetabaseObject.object_type.in_(("dashboard", "question", "collection")),
        )
        .group_by(MetabaseObjectLink.table_id, MetabaseObject.object_type)
    ).all()
    counts: dict[int, dict[str, int]] = defaultdict(lambda: {"dashboard": 0, "question": 0, "collection": 0, "total": 0})
    for row in rows:
        table_id = int(row.table_id)
        object_type = str(row.object_type or "").strip().lower()
        count = int(row.artifact_count or 0)
        if object_type in {"dashboard", "question", "collection"}:
            counts[table_id][object_type] = count
            counts[table_id]["total"] += count
    return counts


def _metabase_artifact_detail_rows(session: Session, instance: MetabaseInstance) -> list[MetabaseIntegrationArtifactOut]:
    objects = session.scalars(
        select(MetabaseObject)
        .where(MetabaseObject.instance_id == instance.id, MetabaseObject.archived.is_(False))
        .order_by(MetabaseObject.last_seen_at.desc().nullslast(), MetabaseObject.updated_at.desc(), MetabaseObject.id.desc())
    ).all()
    if not objects:
        return []

    object_map = {obj.id: obj for obj in objects}
    link_rows = session.execute(
        select(MetabaseObjectLink, MetabaseObject)
        .join(MetabaseObject, MetabaseObjectLink.metabase_object_id == MetabaseObject.id)
        .where(MetabaseObjectLink.instance_id == instance.id, MetabaseObjectLink.is_active.is_(True))
        .order_by(MetabaseObject.updated_at.desc(), MetabaseObject.id.desc(), MetabaseObjectLink.id.asc())
    ).all()
    links_by_object: dict[int, list[MetabaseObjectLink]] = defaultdict(list)
    table_ids: set[int] = set()
    for link, metabase_object in link_rows:
        links_by_object[int(link.metabase_object_id)].append(link)
        table_ids.add(int(link.table_id))
    table_rows = session.execute(
        select(
            TableEntity.id.label("table_id"),
            TableEntity.name.label("table_name"),
            Schema.name.label("schema_name"),
            Database.name.label("database_name"),
            DataSource.name.label("datasource_name"),
        )
        .join(Schema, Schema.id == TableEntity.schema_id)
        .join(Database, Database.id == Schema.database_id)
        .join(DataSource, DataSource.id == Database.datasource_id)
        .where(TableEntity.id.in_(table_ids))
    ).all()
    linked_tables_by_id = {
        int(row.table_id): MetabaseArtifactLinkedTableOut(
            table_id=int(row.table_id),
            full_name=f"{row.datasource_name}.{row.database_name}.{row.schema_name}.{row.table_name}",
            connection=str(row.datasource_name),
            database=str(row.database_name),
            schema_name=str(row.schema_name),
            table=str(row.table_name),
        )
        for row in table_rows
    }

    artifact_rows: list[MetabaseIntegrationArtifactOut] = []
    for obj in objects:
        links = links_by_object.get(obj.id, [])
        direct_links = [link for link in links if link.match_method in DIRECT_LINK_METHODS]
        indirect_links = [link for link in links if link.match_method not in DIRECT_LINK_METHODS]
        linked_tables = [linked_tables_by_id[table_id] for table_id in dict.fromkeys(link.table_id for link in links) if table_id in linked_tables_by_id]
        unresolved_references: list[str] = []
        if obj.object_type == "question":
            dataset_query = obj.dataset_query_json if isinstance(obj.dataset_query_json, dict) else None
            raw_refs, _sql = _metabase_parse_dataset_query(dataset_query)
            if raw_refs:
                linked_candidates = {
                    candidate
                    for table in linked_tables
                    for candidate in {
                        table.full_name.lower(),
                        f"{table.schema}.{table.table}".lower(),
                        table.table.lower(),
                    }
                }
                unresolved_references = [ref for ref in raw_refs if ref.lower() not in linked_candidates]
        elif obj.object_type == "dashboard":
            raw_dashboard = obj.raw_json if isinstance(obj.raw_json, dict) else {}
            card_ids = _metabase_extract_cards_from_dashboard(raw_dashboard) if raw_dashboard else []
            if card_ids and not links:
                unresolved_references = [f"card:{card_id}" for card_id in card_ids]
        status = "unknown"
        resolved_links = len(direct_links) + len(indirect_links)
        if resolved_links > 0 and unresolved_references:
            status = "partially_linked"
        elif resolved_links > 0:
            status = "linked"
        elif unresolved_references:
            status = "unlinked"
        elif obj.object_type in {"dashboard", "question"} and (obj.dataset_query_json is not None or linked_tables):
            status = "unlinked"
        raw_obj = obj.raw_json if isinstance(obj.raw_json, dict) else {}
        artifact_rows.append(
            MetabaseIntegrationArtifactOut(
                object_id=obj.id,
                object_type=obj.object_type,  # type: ignore[arg-type]
                metabase_id=obj.external_id,
                title=obj.title,
                description=obj.description,
                collection_name=obj.collection_name,
                collection_external_id=obj.collection_external_id,
                url=obj.url,
                archived=bool(obj.archived),
                creator_name=_metabase_creator_name(raw_obj),
                view_count=raw_obj.get("view_count") if isinstance(raw_obj.get("view_count"), int) else None,
                linked_status=status,  # type: ignore[arg-type]
                direct_links=len(direct_links),
                indirect_links=len(indirect_links),
                linked_tables=linked_tables,
                referenced_tables=_build_referenced_tables(obj.referenced_tables_json, linked_tables),
                unresolved_references=unresolved_references,
                remote_updated_at=obj.remote_updated_at,
                last_synced_at=obj.last_seen_at,
                last_seen_at=obj.last_seen_at,
                created_at=obj.created_at,
                updated_at=obj.updated_at,
            )
        )
    return artifact_rows


def _metabase_creator_name(raw: dict | list | None) -> str | None:
    if not isinstance(raw, dict):
        return None
    for key in ("creator", "last-edit-info", "last_edit_info"):
        info = raw.get(key)
        if isinstance(info, dict):
            name = (
                info.get("common_name")
                or " ".join(part for part in [info.get("first_name"), info.get("last_name")] if part)
                or info.get("email")
            )
            if name and str(name).strip():
                return str(name).strip()
    return None


def build_metabase_artifact_detail(session: Session, *, object_id: int) -> MetabaseArtifactDetailOut | None:
    obj = session.get(MetabaseObject, object_id)
    if obj is None:
        return None

    links = session.scalars(
        select(MetabaseObjectLink).where(
            MetabaseObjectLink.metabase_object_id == obj.id,
            MetabaseObjectLink.is_active.is_(True),
        )
    ).all()
    direct_links = [link for link in links if link.match_method in DIRECT_LINK_METHODS]
    indirect_links = [link for link in links if link.match_method not in DIRECT_LINK_METHODS]

    table_ids = list(dict.fromkeys(int(link.table_id) for link in links))
    linked_tables: list[MetabaseArtifactLinkedTableOut] = []
    if table_ids:
        tables = session.scalars(
            select(TableEntity)
            .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
            .where(TableEntity.id.in_(table_ids))
        ).all()
        tables_by_id = {table.id: table for table in tables}
        linked_tables = [_linked_table_payload(tables_by_id[tid]) for tid in table_ids if tid in tables_by_id]

    raw = obj.raw_json if isinstance(obj.raw_json, dict) else {}
    unresolved_references: list[str] = []
    query_type: str | None = None
    sql: str | None = None
    viz_type: str | None = None
    cards: list[MetabaseArtifactCardOut] = []

    if obj.object_type == "question":
        dataset_query = obj.dataset_query_json if isinstance(obj.dataset_query_json, dict) else None
        raw_refs, sql = _metabase_parse_dataset_query(dataset_query)
        query_type = (dataset_query or {}).get("type") if isinstance(dataset_query, dict) else None
        viz_type = raw.get("display") if isinstance(raw.get("display"), str) else None
        if raw_refs:
            candidates = {
                candidate
                for table in linked_tables
                for candidate in {table.full_name.lower(), f"{table.schema_name}.{table.table}".lower(), table.table.lower()}
            }
            unresolved_references = [ref for ref in raw_refs if ref.lower() not in candidates]
    elif obj.object_type == "dashboard":
        card_ids = _metabase_extract_cards_from_dashboard(raw) if raw else []
        if card_ids:
            card_objects = session.scalars(
                select(MetabaseObject).where(
                    MetabaseObject.instance_id == obj.instance_id,
                    MetabaseObject.object_type == "question",
                    MetabaseObject.external_id.in_(card_ids),
                )
            ).all()
            by_external = {card.external_id: card for card in card_objects}
            for card_id in card_ids:
                card = by_external.get(card_id)
                if card is not None:
                    card_raw = card.raw_json if isinstance(card.raw_json, dict) else {}
                    cards.append(
                        MetabaseArtifactCardOut(
                            object_id=card.id,
                            metabase_id=card.external_id,
                            title=card.title,
                            url=card.url,
                            viz_type=card_raw.get("display") if isinstance(card_raw.get("display"), str) else None,
                        )
                    )
                else:
                    cards.append(MetabaseArtifactCardOut(object_id=None, metabase_id=card_id, title=f"Card {card_id}"))
            missing = [card_id for card_id in card_ids if card_id not in by_external]
            if missing and not links:
                unresolved_references = [f"card:{card_id}" for card_id in missing]

    resolved_links = len(direct_links) + len(indirect_links)
    status = "unknown"
    if resolved_links > 0 and unresolved_references:
        status = "partially_linked"
    elif resolved_links > 0:
        status = "linked"
    elif unresolved_references:
        status = "unlinked"
    elif obj.object_type in {"dashboard", "question"} and (obj.dataset_query_json is not None or linked_tables):
        status = "unlinked"

    view_count = raw.get("view_count") if isinstance(raw.get("view_count"), int) else None

    return MetabaseArtifactDetailOut(
        object_id=obj.id,
        object_type=obj.object_type,  # type: ignore[arg-type]
        metabase_id=obj.external_id,
        title=obj.title,
        description=obj.description,
        collection_name=obj.collection_name,
        collection_external_id=obj.collection_external_id,
        url=obj.url,
        archived=bool(obj.archived),
        linked_status=status,  # type: ignore[arg-type]
        direct_links=len(direct_links),
        indirect_links=len(indirect_links),
        linked_tables=linked_tables,
        referenced_tables=_build_referenced_tables(obj.referenced_tables_json, linked_tables),
        unresolved_references=unresolved_references,
        remote_updated_at=obj.remote_updated_at,
        last_synced_at=obj.last_seen_at,
        last_seen_at=obj.last_seen_at,
        created_at=obj.created_at,
        updated_at=obj.updated_at,
        creator_name=_metabase_creator_name(raw),
        view_count=view_count,
        query_type=query_type,
        sql=sql,
        viz_type=viz_type,
        database_id=obj.database_id,
        cards=cards,
    )


def _metabase_top_tables(session: Session, instance_id: int) -> list[MetabaseIntegrationTopTableOut]:
    rows = session.execute(
        select(
            TableEntity.id.label("table_id"),
            TableEntity.name.label("table_name"),
            TableEntity.owner.label("owner"),
            TableEntity.owner_email.label("owner_email"),
            Schema.name.label("schema_name"),
            DataSource.name.label("datasource_name"),
            Database.name.label("database_name"),
            func.count(MetabaseObjectLink.id).label("total_links_count"),
            func.coalesce(
                func.sum(
                    case(
                        (MetabaseObjectLink.match_method.in_(DIRECT_LINK_METHODS), 1),
                        else_=0,
                    )
                ),
                0,
            ).label("direct_links_count"),
            func.coalesce(
                func.sum(
                    case(
                        (MetabaseObjectLink.match_method.in_(INDIRECT_LINK_METHODS), 1),
                        else_=0,
                    )
                ),
                0,
            ).label("indirect_links_count"),
        )
        .join(TableEntity, TableEntity.id == MetabaseObjectLink.table_id)
        .join(Schema, Schema.id == TableEntity.schema_id)
        .join(Database, Database.id == Schema.database_id)
        .join(DataSource, DataSource.id == Database.datasource_id)
        .where(MetabaseObjectLink.instance_id == instance_id, MetabaseObjectLink.is_active.is_(True))
        .group_by(TableEntity.id, TableEntity.name, TableEntity.owner, TableEntity.owner_email, Schema.name, DataSource.name, Database.name)
        .order_by(func.count(MetabaseObjectLink.id).desc(), TableEntity.name.asc())
        .limit(5)
    ).all()

    table_ids = [int(row.table_id) for row in rows]
    try:
        profile_map = {profile.table_id: profile for profile in load_table_profiles(session, _now(), table_ids=table_ids)}
    except SQLAlchemyError:
        profile_map = {}
    link_count_map = _metabase_top_table_link_counts(session, instance_id, table_ids)

    items: list[MetabaseIntegrationTopTableOut] = []
    for row in rows:
        profile = profile_map.get(int(row.table_id))
        link_counts = link_count_map.get(int(row.table_id), {"dashboard": 0, "question": 0, "collection": 0, "total": 0})
        table_fqn = f"{row.datasource_name}.{row.database_name}.{row.schema_name}.{row.table_name}"
        certification_status = resolve_certification_status_for_profile(profile, now=_now()) if profile is not None else None
        items.append(
            MetabaseIntegrationTopTableOut(
                table_id=int(row.table_id),
                table_fqn=table_fqn,
                table_name=str(row.table_name),
                schema_name=str(row.schema_name),
                datasource_name=str(row.datasource_name),
                direct_links_count=int(row.direct_links_count or 0),
                indirect_links_count=int(row.indirect_links_count or 0),
                total_links_count=int(row.total_links_count or 0),
                owner=(getattr(profile, "owner_name", None) or row.owner or None) if profile is not None or row.owner else None,
                owner_email=(row.owner_email or None),
                certification_status=certification_status,
                certification_readiness=int(getattr(profile, "readiness_score", 0)) if profile is not None else None,
                dq_score=float(profile.dq_score) if profile is not None and profile.dq_score is not None else None,
                privacy_status=_privacy_status_for_profile(profile) if profile is not None else None,
                privacy_signals=_privacy_signals_for_profile(profile) if profile is not None else [],
                incident_count=int(profile.open_incidents) if profile is not None else None,
                linked_dashboards=int(link_counts.get("dashboard", 0)),
                linked_questions=int(link_counts.get("question", 0)),
                linked_artifacts_total=int(link_counts.get("total", 0)),
            )
        )
    return items


def _recent_artifacts(session: Session, instance_id: int) -> list[MetabaseIntegrationArtifactOut]:
    instance = session.get(MetabaseInstance, instance_id)
    if instance is None:
        return []
    items = _metabase_artifact_detail_rows(session, instance=instance)
    return items[:6]


def _metabase_instance_for_requests(session: Session, instance_id: int | None = None) -> MetabaseInstance | None:
    if instance_id is not None:
        return session.get(MetabaseInstance, instance_id)
    return _metabase_instance_or_none(session) or _metabase_any_instance(session)


def _metabase_artifact_link_coverage(artifact_rows: list[MetabaseIntegrationArtifactOut]) -> MetabaseArtifactLinkSummaryOut:
    total = len(artifact_rows)
    linked = sum(1 for item in artifact_rows if item.linked_status == "linked")
    partially_linked = sum(1 for item in artifact_rows if item.linked_status == "partially_linked")
    unlinked = sum(1 for item in artifact_rows if item.linked_status == "unlinked")
    unknown = sum(1 for item in artifact_rows if item.linked_status == "unknown" or item.linked_status is None)
    coverage_percent = int(round(((linked + partially_linked) / total) * 100)) if total > 0 else 0
    return MetabaseArtifactLinkSummaryOut(
        object_type="all",
        total_artifacts=total,
        linked_artifacts=linked,
        partially_linked_artifacts=partially_linked,
        unlinked_artifacts=unlinked,
        unknown_artifacts=unknown,
        coverage_percent=coverage_percent,
    )


def _metabase_artifact_link_summary(artifact_rows: list[MetabaseIntegrationArtifactOut]) -> list[MetabaseArtifactLinkSummaryOut]:
    summary: list[MetabaseArtifactLinkSummaryOut] = []
    for object_type in ("dashboard", "question", "collection"):
        typed_rows = [item for item in artifact_rows if item.object_type == object_type]
        total = len(typed_rows)
        linked = sum(1 for item in typed_rows if item.linked_status == "linked")
        partially_linked = sum(1 for item in typed_rows if item.linked_status == "partially_linked")
        unlinked = sum(1 for item in typed_rows if item.linked_status == "unlinked")
        unknown = sum(1 for item in typed_rows if item.linked_status == "unknown" or item.linked_status is None)
        coverage_percent = int(round(((linked + partially_linked) / total) * 100)) if total > 0 else 0
        summary.append(
            MetabaseArtifactLinkSummaryOut(
                object_type=object_type,  # type: ignore[arg-type]
                total_artifacts=total,
                linked_artifacts=linked,
                partially_linked_artifacts=partially_linked,
                unlinked_artifacts=unlinked,
                unknown_artifacts=unknown,
                coverage_percent=coverage_percent,
            )
        )
    return summary


def _metabase_sync_health_notes(
    *,
    instance: MetabaseInstance | None,
    latest_sync: MetabaseSyncRun | None,
    health_row: IntegrationHealth | None,
) -> list[str]:
    notes: list[str] = []
    if instance is None:
        return ["Nenhuma instância do Metabase está configurada."]
    if health_row is not None and health_row.consecutive_failures > 0:
        notes.append(f"{health_row.consecutive_failures} falha(s) consecutiva(s) registrada(s).")
    if latest_sync is None:
        notes.append("Ainda não há histórico de sync disponível.")
    elif (latest_sync.status or "").lower() == "failed":
        notes.append("A última sync terminou com falha.")
    elif (latest_sync.status or "").lower() == "running":
        notes.append("A última sync ainda está em execução.")
    if int(instance.last_sync_warnings or 0) > 0:
        notes.append(f"{int(instance.last_sync_warnings or 0)} alerta(s) materializado(s) na última sync.")
    if int(instance.last_sync_unresolved or 0) > 0:
        notes.append(f"{int(instance.last_sync_unresolved or 0)} referência(s) sem resolução confirmada.")
    if not notes:
        notes.append("Sincronização com leitura estável no recorte atual.")
    return notes


def _metabase_recommendations(
    *,
    instance: MetabaseInstance | None,
    health_row: IntegrationHealth | None,
    latest_sync: MetabaseSyncRun | None,
    artifact_rows: list[MetabaseIntegrationArtifactOut],
    top_tables: list[MetabaseIntegrationTopTableOut],
    link_coverage: MetabaseArtifactLinkSummaryOut | None,
) -> list[MetabaseIntegrationRecommendationOut]:
    recommendations: list[MetabaseIntegrationRecommendationOut] = []
    total_artifacts = len(artifact_rows)
    partially_linked = sum(1 for item in artifact_rows if item.linked_status == "partially_linked")
    unlinked = sum(1 for item in artifact_rows if item.linked_status == "unlinked")
    linked = sum(1 for item in artifact_rows if item.linked_status == "linked")
    has_success = latest_sync is not None and (latest_sync.status or "").lower() == "success"

    if health_row is not None and health_row.consecutive_failures > 0:
        recommendations.append(
            MetabaseIntegrationRecommendationOut(
                severity="critical",
                title="Investigar falhas consecutivas",
                description="A sincronização registra falhas seguidas e pode estar bloqueada ou instável.",
                reason="Falhas consecutivas acima de zero.",
                action_label="Ver histórico de sync",
                action_target="sync-runs",
                context={"consecutive_failures": int(health_row.consecutive_failures)},
            )
        )

    if instance is not None and not has_success and (instance.base_url or health_row is not None):
        recommendations.append(
            MetabaseIntegrationRecommendationOut(
                severity="warning",
                title="Validar URL e credenciais",
                description="Não há sucesso confirmado no recorte atual; vale revisar conexão, permissões e disponibilidade do Metabase.",
                reason="Ausência de último sucesso registrado.",
                action_label="Revisar saúde",
                action_target="summary",
                context={"instance_id": instance.id},
            )
        )

    if unlinked > 0 or partially_linked > 0:
        recommendations.append(
            MetabaseIntegrationRecommendationOut(
                severity="warning" if unlinked == 0 else "critical",
                title="Revisar artefatos sem vínculo",
                description="Há artefatos sincronizados sem cobertura completa de vínculo com o catálogo.",
                reason=f"{unlinked} artefato(s) sem vínculo e {partially_linked} parcialmente vinculados.",
                action_label="Ver artefatos",
                action_target="artifacts",
                context={"unlinked": unlinked, "partially_linked": partially_linked, "total": total_artifacts},
            )
        )

    if top_tables:
        table_without_owner = next((item for item in top_tables if not item.owner), None)
        if table_without_owner is not None:
            recommendations.append(
                MetabaseIntegrationRecommendationOut(
                    severity="info",
                    title="Completar owner das tabelas mais consumidas",
                    description="Os ativos com maior consumo analítico devem ter owner, qualidade e certificação bem definidos.",
                    reason=f"A tabela {table_without_owner.table_fqn} ainda não tem owner claro.",
                    action_label="Abrir Explorer",
                    action_target="explorer",
                    context={"table_id": table_without_owner.table_id},
                )
            )
        low_certified = next((item for item in top_tables if (item.certification_status or "").lower() not in {"certified", "em_andamento"} and item.total_links_count > 0), None)
        if low_certified is not None:
            recommendations.append(
                MetabaseIntegrationRecommendationOut(
                    severity="info",
                    title="Priorizar certificação das tabelas mais consumidas",
                    description="Consumo alto justifica revisar prontidão, qualidade e elegibilidade dos ativos.",
                    reason=f"{low_certified.table_fqn} ainda não está certificada.",
                    action_label="Ver certificação",
                    action_target="certification",
                    context={"table_id": low_certified.table_id},
                )
            )

    if link_coverage is not None and link_coverage.coverage_percent < 50 and total_artifacts > 0:
        recommendations.append(
            MetabaseIntegrationRecommendationOut(
                severity="warning",
                title="Aumentar cobertura de vínculos",
                description="A cobertura de artefatos vinculados ainda está baixa para a base sincronizada.",
                reason=f"Cobertura atual de {link_coverage.coverage_percent}%.",
                action_label="Ver cobertura",
                action_target="summary",
                context={"coverage_percent": link_coverage.coverage_percent, "linked_artifacts": linked, "total_artifacts": total_artifacts},
            )
        )

    return recommendations[:5]


def _serialize_metabase_sync_runs_page(
    session: Session,
    instance: MetabaseInstance | None,
    *,
    page: int = 1,
    page_size: int = 20,
    status: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    finished_from: datetime | None = None,
    finished_to: datetime | None = None,
    query: str | None = None,
    only_failures: bool = False,
) -> PageOut[MetabaseSyncRunOut]:
    if instance is None:
        return PageOut[MetabaseSyncRunOut](page=max(int(page or 1), 1), page_size=max(int(page_size or 20), 1), total=0, total_pages=0, has_more=False, items=[])
    rows = session.scalars(
        select(MetabaseSyncRun)
        .where(MetabaseSyncRun.instance_id == instance.id)
        .order_by(MetabaseSyncRun.started_at.desc(), MetabaseSyncRun.id.desc())
    ).all()
    normalized_status = (status or "").strip().lower()
    normalized_query = (query or "").strip().lower()
    filtered: list[MetabaseSyncRunOut] = []
    for run in rows:
        run_status = (run.status or "").strip().lower()
        if normalized_status and run_status != normalized_status:
            continue
        if only_failures and run_status != "failed":
            continue
        if started_from is not None and run.started_at < started_from:
            continue
        if started_to is not None and run.started_at > started_to:
            continue
        if finished_from is not None and (run.finished_at is None or run.finished_at < finished_from):
            continue
        if finished_to is not None and (run.finished_at is None or run.finished_at > finished_to):
            continue
        serialized = _serialize_metabase_sync_run(run, instance_name=instance.name)
        if normalized_query:
            haystack = " ".join(
                str(value or "")
                for value in (
                    serialized.instance_name,
                    serialized.status,
                    serialized.error_message,
                    serialized.error_type,
                    serialized.summary,
                )
            ).lower()
            if normalized_query not in haystack:
                continue
        filtered.append(serialized)
    return paginate_items(filtered, page=page, page_size=page_size)


def _serialize_metabase_artifacts_page(
    session: Session,
    instance: MetabaseInstance | None,
    *,
    page: int = 1,
    page_size: int = 20,
    type: str | None = None,
    collection: str | None = None,
    linked_status: str | None = None,
    query: str | None = None,
    table_id: int | None = None,
) -> PageOut[MetabaseIntegrationArtifactOut]:
    if instance is None:
        return PageOut[MetabaseIntegrationArtifactOut](page=max(int(page or 1), 1), page_size=max(int(page_size or 20), 1), total=0, total_pages=0, has_more=False, items=[])
    rows = _metabase_artifact_detail_rows(session, instance)
    normalized_type = (type or "").strip().lower()
    normalized_collection = (collection or "").strip().lower()
    normalized_linked_status = (linked_status or "").strip().lower()
    normalized_query = (query or "").strip().lower()
    filtered: list[MetabaseIntegrationArtifactOut] = []
    for item in rows:
        if normalized_type and normalized_type != "all" and item.object_type != normalized_type:
            continue
        if normalized_collection and normalized_collection != "all":
            collection_candidates = " ".join(filter(None, [item.collection_name, item.collection_external_id])).lower()
            if normalized_collection not in collection_candidates:
                continue
        if normalized_linked_status and normalized_linked_status != "all" and (item.linked_status or "unknown") != normalized_linked_status:
            continue
        if table_id is not None and all(link.table_id != table_id for link in item.linked_tables):
            continue
        if normalized_query:
            haystack = " ".join(
                str(value or "")
                for value in (
                    item.title,
                    item.collection_name,
                    item.collection_external_id,
                    item.url,
                    item.metabase_id,
                    item.linked_status,
                    " ".join(link.full_name for link in item.linked_tables),
                    " ".join(item.unresolved_references),
                )
            ).lower()
            if normalized_query not in haystack:
                continue
        filtered.append(item)
    return paginate_items(filtered, page=page, page_size=page_size)


def load_metabase_integration_summary(session: Session) -> MetabaseIntegrationSummaryOut:
    latest_instance = _metabase_any_instance(session)
    if latest_instance is None:
        ensure_metabase_instance_from_settings(session)
        latest_instance = _metabase_any_instance(session)
    instance = _metabase_instance_or_none(session)
    if latest_instance is None:
        return MetabaseIntegrationSummaryOut(
            configured=False,
            enabled=False,
            available=False,
            integration_status="misconfigured",
            sync_status="never_synced",
            status_message="Nenhuma instância do Metabase está configurada.",
            message="Nenhuma instância do Metabase está configurada.",
        )

    latest_sync = session.scalar(
        select(MetabaseSyncRun)
        .where(MetabaseSyncRun.instance_id == latest_instance.id)
        .order_by(MetabaseSyncRun.started_at.desc(), MetabaseSyncRun.id.desc())
        .limit(1)
    )
    health_row = get_integration_health(session, "metabase")
    configured = bool((instance or latest_instance).base_url)
    enabled = bool((instance or latest_instance).enabled and configured)
    active_instance = instance or latest_instance
    _, _, _, semantic_status, semantic_message = _metabase_health_status(instance, latest_sync, health_row)
    integration_status = health_row.status if health_row is not None else semantic_status
    message = health_row.status_message if health_row is not None else semantic_message

    object_counts = dict(
        session.execute(
            select(MetabaseObject.object_type, func.count(MetabaseObject.id))
            .where(MetabaseObject.instance_id == active_instance.id, MetabaseObject.archived.is_(False))
            .group_by(MetabaseObject.object_type)
        ).all()
    )
    direct_links_count, indirect_links_count, total_links_count = _count_links(session, active_instance.id)
    tables_with_consumption_count = session.scalar(
        select(func.count(func.distinct(MetabaseObjectLink.table_id))).where(
            MetabaseObjectLink.instance_id == active_instance.id,
            MetabaseObjectLink.is_active.is_(True),
        )
    ) or 0
    recent_sync_runs = session.scalars(
        select(MetabaseSyncRun)
        .where(MetabaseSyncRun.instance_id == active_instance.id)
        .order_by(MetabaseSyncRun.started_at.desc(), MetabaseSyncRun.id.desc())
        .limit(3)
    ).all()
    recent_sync_runs_serialized = [_serialize_metabase_sync_run(run, instance_name=active_instance.name) for run in recent_sync_runs]
    artifact_rows = _metabase_artifact_detail_rows(session, active_instance)
    top_dashboards = sorted(
        [row for row in artifact_rows if row.object_type == "dashboard" and isinstance(row.view_count, int)],
        key=lambda row: row.view_count or 0,
        reverse=True,
    )[:5]
    top_tables = _metabase_top_tables(session, active_instance.id)
    link_coverage = _metabase_artifact_link_coverage(artifact_rows)
    artifact_link_summary = _metabase_artifact_link_summary(artifact_rows)
    sync_health_notes = _metabase_sync_health_notes(instance=active_instance, latest_sync=latest_sync, health_row=health_row)
    recommendations = _metabase_recommendations(
        instance=active_instance,
        health_row=health_row,
        latest_sync=latest_sync,
        artifact_rows=artifact_rows,
        top_tables=top_tables,
        link_coverage=link_coverage,
    )

    available = bool(
        int(object_counts.get("dashboard", 0))
        or int(object_counts.get("question", 0))
        or int(object_counts.get("collection", 0))
        or int(direct_links_count)
        or int(indirect_links_count)
        or int(total_links_count)
        or int(tables_with_consumption_count)
    )
    summary_counts = {
        "dashboards_count": int(object_counts.get("dashboard", 0)),
        "questions_count": int(object_counts.get("question", 0)),
        "collections_count": int(object_counts.get("collection", 0)),
        "direct_links_count": int(direct_links_count),
        "indirect_links_count": int(indirect_links_count),
        "total_links_count": int(total_links_count),
        "tables_with_consumption_count": int(tables_with_consumption_count),
        "unresolved_count": int(active_instance.last_sync_unresolved or 0),
        "warnings_count": int(active_instance.last_sync_warnings or 0),
    }
    status_contract = _metabase_status_contract(
        instance=active_instance,
        health_row=health_row,
        latest_sync=latest_sync,
        summary_counts=summary_counts,
        checked_at=health_row.checked_at if health_row is not None else (latest_sync.started_at if latest_sync is not None else _now()),
    )

    return MetabaseIntegrationSummaryOut(
        configured=configured,
        enabled=enabled,
        available=available,
        integration_status=integration_status,
        sync_status=(latest_sync.status if latest_sync is not None else "never_synced"),
        status_message=message,
        message=message,
        health_category=health_row.category if health_row is not None else _metabase_health_category_from_status(semantic_status),
        checked_at=health_row.checked_at if health_row is not None else (latest_sync.started_at if latest_sync is not None else _now()),
        last_success_at=health_row.last_success_at if health_row is not None else (instance.last_sync_at if instance.last_sync_at is not None else None),
        last_failure_at=health_row.last_failure_at if health_row is not None else (latest_sync.started_at if latest_sync is not None and (latest_sync.status or "").lower() == "failed" else None),
        consecutive_failures=health_row.consecutive_failures if health_row is not None else (1 if latest_sync is not None and (latest_sync.status or "").lower() == "failed" else 0),
        failure_count=health_row.failure_count if health_row is not None else (1 if latest_sync is not None and (latest_sync.status or "").lower() == "failed" else 0),
        latency_ms=health_row.latency_ms if health_row is not None else None,
        error_type=health_row.error_type if health_row is not None else None,
        error_summary=health_row.error_summary if health_row is not None else None,
        breaker_state=health_row.breaker_state if health_row is not None else "closed",
        breaker_open_until_at=health_row.breaker_open_until_at if health_row is not None else None,
        instance_id=active_instance.id,
        instance_name=active_instance.name,
        instance_base_url=active_instance.base_url,
        last_sync_at=active_instance.last_sync_at,
        last_sync_message=active_instance.last_sync_message,
        dashboards_count=int(object_counts.get("dashboard", 0)),
        questions_count=int(object_counts.get("question", 0)),
        collections_count=int(object_counts.get("collection", 0)),
        direct_links_count=int(direct_links_count),
        indirect_links_count=int(indirect_links_count),
        total_links_count=int(total_links_count),
        tables_with_consumption_count=int(tables_with_consumption_count),
        recent_sync_runs=recent_sync_runs_serialized,
        top_tables=top_tables,
        top_tables_enriched=top_tables,
        recent_artifacts=artifact_rows[:6],
        top_dashboards=top_dashboards,
        link_coverage=link_coverage,
        artifact_link_summary=artifact_link_summary,
        recommendations=recommendations,
        sync_health_notes=sync_health_notes,
        status_contract=status_contract,
    )


def list_metabase_integration_sync_runs(
    session: Session,
    *,
    instance_id: int | None = None,
    page: int = 1,
    page_size: int = 20,
    status: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    finished_from: datetime | None = None,
    finished_to: datetime | None = None,
    query: str | None = None,
    only_failures: bool = False,
) -> PageOut[MetabaseSyncRunOut]:
    instance = _metabase_instance_for_requests(session, instance_id)
    return _serialize_metabase_sync_runs_page(
        session,
        instance,
        page=page,
        page_size=page_size,
        status=status,
        started_from=started_from,
        started_to=started_to,
        finished_from=finished_from,
        finished_to=finished_to,
        query=query,
        only_failures=only_failures,
    )


def list_metabase_integration_artifacts(
    session: Session,
    *,
    instance_id: int | None = None,
    page: int = 1,
    page_size: int = 20,
    type: str | None = None,
    collection: str | None = None,
    linked_status: str | None = None,
    query: str | None = None,
    table_id: int | None = None,
) -> PageOut[MetabaseIntegrationArtifactOut]:
    instance = _metabase_instance_for_requests(session, instance_id)
    return _serialize_metabase_artifacts_page(
        session,
        instance,
        page=page,
        page_size=page_size,
        type=type,
        collection=collection,
        linked_status=linked_status,
        query=query,
        table_id=table_id,
    )
