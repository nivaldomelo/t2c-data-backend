from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from t2c_data.features.dashboard.executive_scoring import compute_priority_score
from t2c_data.features.pagination import normalize_page_params
from t2c_data.features.governance.rules import certification_review_due, owner_review_due, privacy_review_due
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.ingestion import load_ingestion_operational_overview_from_source
from t2c_data.features.platform.analytics import analytics_summary, legacy_api_usage_stats_by_module
from t2c_data.features.platform.jobs import integration_jobs_status_snapshot
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback


_SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2, "healthy": 3}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str:
    return (value or "").strip()


def _normalize_key(value: str | None) -> str:
    return _normalize_text(value).lower()


def _table_key(table_id: int | None, schema_name: str | None, table_name: str | None) -> str | None:
    if table_id is not None:
        return f"id:{int(table_id)}"
    schema = _normalize_key(schema_name)
    table = _normalize_key(table_name)
    if schema and table:
        return f"fqn:{schema}.{table}"
    return None


def _build_table_refs(tables: list[Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for table in tables:
        try:
            score, _signals = compute_priority_score(
                table,
                recent_incident_count=int(getattr(table, "open_incidents", 0) or 0),
                recent_occurrences=int(getattr(table, "open_incidents", 0) or 0),
            )
        except AttributeError:
            open_incidents = int(getattr(table, "open_incidents", 0) or 0)
            critical_open_incidents = int(getattr(table, "critical_open_incidents", 0) or 0)
            score = max(0, 100 - (open_incidents * 8) - (critical_open_incidents * 12))
        refs.append(
            {
                "table_id": getattr(table, "table_id", None),
                "table_name": getattr(table, "table_name", None),
                "table_fqn": getattr(table, "table_fqn", None),
                "schema_name": getattr(table, "schema_name", None),
                "criticality_score": int(score),
            }
        )
    return refs


def _queue_item(
    *,
    item_id: str,
    item_type: str,
    category: str,
    title: str,
    subtitle: str | None,
    severity: str,
    status: str,
    description: str,
    asset_id: int | None = None,
    asset_name: str | None = None,
    connection: str | None = None,
    database: str | None = None,
    schema_name: str | None = None,
    pipeline_name: str | None = None,
    dag_id: str | None = None,
    task_id: str | None = None,
    recommended_action: str | None = None,
    route: str | None = None,
    updated_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": item_id,
        "type": item_type,
        "category": category,
        "title": title,
        "subtitle": subtitle,
        "severity": severity,
        "status": status,
        "description": description,
        "asset_id": asset_id,
        "asset_name": asset_name,
        "connection": connection,
        "database": database,
        "schema": schema_name,
        "pipeline_name": pipeline_name,
        "dag_id": dag_id,
        "task_id": task_id,
        "recommended_action": recommended_action,
        "route": route,
        "updated_at": updated_at or _now(),
        "metadata": metadata or {},
    }


def _severity_from_ingestion_status(status_label: str | None, last_error: str | None, observacao: str | None) -> tuple[str, str]:
    status = _normalize_key(status_label)
    if "falha" in status or "error" in status or last_error or observacao:
        return "critical", "failure"
    if "degrad" in status or "warning" in status:
        return "warning", "degraded"
    if "pend" in status:
        return "warning", "pending"
    if "execução" in status or "running" in status:
        return "info", "running"
    if "sucesso" in status or "success" in status or "ok" in status:
        return "healthy", "healthy"
    return "info", "running"


def _ingestion_title(item: dict[str, Any]) -> str:
    return str(item.get("pipeline_name") or item.get("table_name") or item.get("table_fqn") or "Pipeline")


def _ingestion_subtitle(item: dict[str, Any]) -> str | None:
    parts = [item.get("schema_name"), item.get("dag_id"), item.get("task_name")]
    clean = [str(part) for part in parts if part]
    if not clean:
        return None
    return " · ".join(clean)


def _build_pipeline_queue_items(
    ingestion: dict[str, Any],
    *,
    category: str = "operacao",
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for raw_item in list(ingestion.get("items") or []):
        table_id = raw_item.get("table_id")
        severity, status = _severity_from_ingestion_status(
            raw_item.get("latest_status_label") or raw_item.get("last_status"),
            raw_item.get("last_error"),
            raw_item.get("observacao"),
        )
        last_success_at = raw_item.get("last_success_at")
        if status == "healthy" and last_success_at is None and raw_item.get("last_run_started_at") is None:
            status = "pending"
            severity = "warning"
        item_type = {
            "failure": "pipeline_failure",
            "degraded": "degraded_pipeline",
            "pending": "degraded_pipeline",
            "running": "pipeline_running",
            "healthy": "pipeline_healthy",
        }.get(status, "pipeline_running")
        description_map = {
            "failure": "Falha operacional ativa no pipeline associado.",
            "degraded": "Pipeline com degradação operacional no recorte atual.",
            "pending": "Pipeline mapeado, mas ainda sem confirmação operacional recente.",
            "running": "Pipeline em execução dentro da leitura disponível.",
            "healthy": "Pipeline saudável na última leitura disponível.",
        }
        route = raw_item.get("pipeline_history_href") or raw_item.get("target_url")
        metadata = {
            "rows_processed": raw_item.get("rows_processed"),
            "records_processed": raw_item.get("records_processed"),
            "last_status": raw_item.get("last_status"),
            "latest_status_label": raw_item.get("latest_status_label"),
            "last_error": raw_item.get("last_error"),
            "last_success_at": raw_item.get("last_success_at"),
            "last_run_started_at": raw_item.get("last_run_started_at"),
            "last_run_finished_at": raw_item.get("last_run_finished_at"),
            "last_execution_finished_at": raw_item.get("last_execution_finished_at"),
        }
        if int(raw_item.get("rows_processed") or 0) >= int(ingestion.get("high_volume_failed_threshold_rows") or 100000):
            metadata["high_volume"] = True
        items.append(
            _queue_item(
                item_id=f"ingestion:{table_id or raw_item.get('table_fqn')}",
                item_type=item_type,
                category=category,
                title=_ingestion_title(raw_item),
                subtitle=_ingestion_subtitle(raw_item),
                severity=severity,
                status=status,
                description=description_map.get(status, "Pipeline monitorado pela camada operacional."),
                asset_id=int(table_id) if table_id is not None else None,
                asset_name=str(raw_item.get("table_name") or "") or None,
                connection=str(raw_item.get("connection") or raw_item.get("datasource_name") or "") or None,
                database=str(raw_item.get("database_name") or "") or None,
                schema_name=str(raw_item.get("schema_name") or "") or None,
                pipeline_name=str(raw_item.get("pipeline_name") or "") or None,
                dag_id=str(raw_item.get("dag_id") or "") or None,
                task_id=str(raw_item.get("task_name") or "") or None,
                recommended_action=(
                    "Ver histórico operacional"
                    if status == "failure"
                    else "Ver pipeline"
                    if status in {"degraded", "running", "healthy", "pending"}
                    else "Revisar leitura"
                ),
                route=route,
                updated_at=_now(),
                metadata=metadata,
            )
        )

    for raw_item in list(ingestion.get("unmapped_items") or []):
        table_id = raw_item.get("table_id")
        items.append(
            _queue_item(
                item_id=f"unmapped:{table_id or raw_item.get('table_fqn')}",
                item_type="asset_without_pipeline",
                category="mapeamento",
                title=str(raw_item.get("table_name") or raw_item.get("table_fqn") or "Ativo sem pipeline"),
                subtitle=str(raw_item.get("table_fqn") or "") or None,
                severity="warning",
                status="pending",
                description="Ativo no catálogo, mas ainda sem pipeline operacional mapeado.",
                asset_id=int(table_id) if table_id is not None else None,
                asset_name=str(raw_item.get("table_name") or "") or None,
                connection=str(raw_item.get("connection") or "") or None,
                database=str(raw_item.get("database_name") or "") or None,
                schema_name=str(raw_item.get("schema_name") or "") or None,
                recommended_action="Mapear pipeline",
                route=raw_item.get("target_url") or (f"/explorer?tableId={int(table_id)}" if table_id is not None else None),
                updated_at=_now(),
                metadata={"reason": raw_item.get("hint")},
            )
        )
    return items


def _build_governance_queue_items(tables: list[Any], settings_snapshot: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    now = _now()
    for table in tables:
        table_id = getattr(table, "table_id", None)
        table_name = getattr(table, "table_name", None)
        schema_name = getattr(table, "schema_name", None)
        table_fqn = getattr(table, "table_fqn", None)
        owner_defined = bool(getattr(table, "owner_defined", False))
        open_incidents = int(getattr(table, "open_incidents", 0) or 0)
        critical_open_incidents = int(getattr(table, "critical_open_incidents", 0) or 0)
        dq_score = getattr(table, "dq_score", None)
        classification_defined = bool(getattr(table, "classification_defined", False))
        has_personal_data = bool(getattr(table, "has_personal_data", False))
        has_sensitive_personal_data = bool(getattr(table, "has_sensitive_personal_data", False))
        if not owner_defined and (critical_open_incidents > 0 or (dq_score is not None and float(dq_score) < 70)):
            items.append(
                _queue_item(
                    item_id=f"governance:{table_id}:owner",
                    item_type="governance_gap",
                    category="governanca",
                    title=str(table_name or table_fqn or "Ativo sem responsável"),
                    subtitle=str(table_fqn or "") or None,
                    severity="warning",
                    status="pending",
                    description="Ativo crítico sem owner definido.",
                    asset_id=int(table_id) if table_id is not None else None,
                    asset_name=str(table_name or "") or None,
                    database=str(getattr(table, "database_name", None) or "") or None,
                    schema_name=str(schema_name or "") or None,
                    recommended_action="Definir owner",
                    route=f"/owners?tableId={int(table_id)}" if table_id is not None else "/owners",
                    updated_at=now,
                    metadata={"critical_open_incidents": critical_open_incidents, "dq_score": dq_score},
                )
            )
        if owner_review_due(table, now=now, settings_snapshot=settings_snapshot):
            items.append(
                _queue_item(
                    item_id=f"governance:{table_id}:owner_review",
                    item_type="governance_gap",
                    category="governanca",
                    title=str(table_name or table_fqn or "Revisão de owner"),
                    subtitle=str(table_fqn or "") or None,
                    severity="warning",
                    status="pending",
                    description="Revisão de owner vencida ou próxima do vencimento.",
                    asset_id=int(table_id) if table_id is not None else None,
                    asset_name=str(table_name or "") or None,
                    database=str(getattr(table, "database_name", None) or "") or None,
                    schema_name=str(schema_name or "") or None,
                    recommended_action="Revisar owner",
                    route=f"/governance/owners?tableId={int(table_id)}" if table_id is not None else "/governance/owners",
                    updated_at=now,
                    metadata={},
                )
            )
        if certification_review_due(table, now=now, settings_snapshot=settings_snapshot):
            items.append(
                _queue_item(
                    item_id=f"governance:{table_id}:certification_review",
                    item_type="governance_gap",
                    category="governanca",
                    title=str(table_name or table_fqn or "Revisão de certificação"),
                    subtitle=str(table_fqn or "") or None,
                    severity="warning",
                    status="pending",
                    description="Revisão de certificação vencida ou próxima do vencimento.",
                    asset_id=int(table_id) if table_id is not None else None,
                    asset_name=str(table_name or "") or None,
                    database=str(getattr(table, "database_name", None) or "") or None,
                    schema_name=str(schema_name or "") or None,
                    recommended_action="Revisar certificação",
                    route=f"/certification?tableId={int(table_id)}" if table_id is not None else "/certification",
                    updated_at=now,
                    metadata={},
                )
            )
    return items


def _build_privacy_and_quality_queue_items(tables: list[Any], settings_snapshot: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    now = _now()
    for table in tables:
        table_id = getattr(table, "table_id", None)
        table_name = getattr(table, "table_name", None)
        table_fqn = getattr(table, "table_fqn", None)
        schema_name = getattr(table, "schema_name", None)
        database_name = getattr(table, "database_name", None)
        owner_defined = bool(getattr(table, "owner_defined", False))
        classification_defined = bool(getattr(table, "classification_defined", False))
        has_personal_data = bool(getattr(table, "has_personal_data", False))
        has_sensitive_personal_data = bool(getattr(table, "has_sensitive_personal_data", False))
        dq_score = getattr(table, "dq_score", None)
        open_incidents = int(getattr(table, "open_incidents", 0) or 0)
        critical_open_incidents = int(getattr(table, "critical_open_incidents", 0) or 0)

        if (has_personal_data or has_sensitive_personal_data) and not classification_defined:
            items.append(
                _queue_item(
                    item_id=f"privacy:{table_id}:classification",
                    item_type="privacy_review",
                    category="privacidade",
                    title=str(table_name or table_fqn or "Ativo sensível"),
                    subtitle=str(table_fqn or "") or None,
                    severity="warning",
                    status="pending",
                    description="Ativo sensível ainda sem classificação definida.",
                    asset_id=int(table_id) if table_id is not None else None,
                    asset_name=str(table_name or "") or None,
                    database=str(database_name or "") or None,
                    schema_name=str(schema_name or "") or None,
                    recommended_action="Classificar ativo",
                    route=f"/privacy-access?tableId={int(table_id)}" if table_id is not None else "/privacy-access",
                    updated_at=now,
                    metadata={"has_personal_data": has_personal_data, "has_sensitive_personal_data": has_sensitive_personal_data},
                )
            )

        if privacy_review_due(table, now=now, settings_snapshot=settings_snapshot):
            items.append(
                _queue_item(
                    item_id=f"privacy:{table_id}:review",
                    item_type="privacy_review",
                    category="privacidade",
                    title=str(table_name or table_fqn or "Revisão de privacidade"),
                    subtitle=str(table_fqn or "") or None,
                    severity="warning",
                    status="pending",
                    description="Revisão de privacidade vencida ou próxima do vencimento.",
                    asset_id=int(table_id) if table_id is not None else None,
                    asset_name=str(table_name or "") or None,
                    database=str(database_name or "") or None,
                    schema_name=str(schema_name or "") or None,
                    recommended_action="Abrir revisão de privacidade",
                    route=f"/privacy-access?tableId={int(table_id)}" if table_id is not None else "/privacy-access",
                    updated_at=now,
                    metadata={},
                )
            )

        dq_score_value = float(dq_score) if dq_score is not None else None
        if dq_score_value is not None and dq_score_value < 90:
            items.append(
                _queue_item(
                    item_id=f"quality:{table_id}:dq",
                    item_type="dq_failure",
                    category="qualidade",
                    title=str(table_name or table_fqn or "DQ abaixo do esperado"),
                    subtitle=str(table_fqn or "") or None,
                    severity="critical" if dq_score_value < 70 or critical_open_incidents > 0 else "warning",
                    status="degraded",
                    description="Ativo com degradação de Data Quality no recorte atual.",
                    asset_id=int(table_id) if table_id is not None else None,
                    asset_name=str(table_name or "") or None,
                    database=str(database_name or "") or None,
                    schema_name=str(schema_name or "") or None,
                    recommended_action="Ver histórico de DQ",
                    route=f"/data-quality?tableId={int(table_id)}" if table_id is not None else "/data-quality",
                    updated_at=now,
                    metadata={"dq_score": dq_score_value, "open_incidents": open_incidents},
                )
            )
    return items


def _build_incident_queue_items(tables: list[Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    now = _now()
    for table in tables:
        open_incidents = int(getattr(table, "open_incidents", 0) or 0)
        critical_open_incidents = int(getattr(table, "critical_open_incidents", 0) or 0)
        if open_incidents <= 0:
            continue
        table_id = getattr(table, "table_id", None)
        table_name = getattr(table, "table_name", None)
        table_fqn = getattr(table, "table_fqn", None)
        schema_name = getattr(table, "schema_name", None)
        database_name = getattr(table, "database_name", None)
        items.append(
            _queue_item(
                item_id=f"incident:{table_id}",
                item_type="critical_incident",
                category="incidentes",
                title=str(table_name or table_fqn or "Incidente aberto"),
                subtitle=str(table_fqn or "") or None,
                severity="critical" if critical_open_incidents > 0 else "warning",
                status="failure" if critical_open_incidents > 0 else "pending",
                description="Incidente aberto associado ao ativo.",
                asset_id=int(table_id) if table_id is not None else None,
                asset_name=str(table_name or "") or None,
                database=str(database_name or "") or None,
                schema_name=str(schema_name or "") or None,
                recommended_action="Triar incidente",
                route=f"/incidents/tickets?tableId={int(table_id)}" if table_id is not None else "/incidents/tickets",
                updated_at=now,
                metadata={"open_incidents": open_incidents, "critical_open_incidents": critical_open_incidents},
            )
        )
    return items


def _legacy_action_items(session: Session, *, current_user: Any = None) -> list[dict[str, Any]]:
    analytics = analytics_summary(session, days=30, current_user=current_user)
    items: list[dict[str, Any]] = []
    legacy_hits = int(analytics.get("legacy_api_hits") or 0)
    for module in list(analytics.get("top_legacy_modules") or [])[:5]:
        module_name = str(module.get("label") or module.get("module") or "").strip()
        hits = int(module.get("value") or 0)
        if hits <= 0:
            continue
        items.append(
            {
                "id": f"legacy:{module_name}",
                "title": "Uso residual da API legada",
                "severity": "warning" if legacy_hits > 0 else "info",
                "origin": "legacy_api",
                "impact": "Ainda existem chamadas em rotas antigas e isso adia o desligamento seguro.",
                "reason": f"Módulo {module_name or 'legado'} ainda concentra {hits} chamada(s) no período.",
                "suggested_route": "/admin/governance",
                "primary_action_label": "Ver legado",
                "secondary_action_label": None,
                "context": {"module_name": module_name, "hits": hits},
                "priority": 30,
            }
        )
    return items


def build_platform_cockpit_queue_items(
    session: Session,
    *,
    current_user: Any = None,
) -> list[dict[str, Any]]:
    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    tables, _source = load_dashboard_profiles_with_fallback(session, now, current_user=current_user)
    table_refs = _build_table_refs(tables)
    ingestion = load_ingestion_operational_overview_from_source(
        session,
        table_refs=table_refs,
        limit=max(len(table_refs), 1),
        high_volume_threshold_rows=settings_snapshot.operational_high_volume_threshold_rows,
        stale_threshold_hours=settings_snapshot.platform_recent_success_window_hours,
        airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
    )

    queue_items: list[dict[str, Any]] = []
    queue_items.extend(_build_pipeline_queue_items(ingestion))
    queue_items.extend(_build_governance_queue_items(tables, settings_snapshot))
    queue_items.extend(_build_privacy_and_quality_queue_items(tables, settings_snapshot))
    queue_items.extend(_build_incident_queue_items(tables))

    deduped: dict[str, dict[str, Any]] = {}
    for item in queue_items:
        key = str(item.get("id"))
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = item
            continue
        current_rank = _SEVERITY_ORDER.get(str(item.get("severity") or "info"), 2)
        existing_rank = _SEVERITY_ORDER.get(str(existing.get("severity") or "info"), 2)
        if current_rank < existing_rank:
            deduped[key] = item

    return sorted(
        deduped.values(),
        key=lambda item: (
            _SEVERITY_ORDER.get(str(item.get("severity") or "info"), 2),
            0 if str(item.get("status") or "") in {"failure", "degraded"} else 1,
            -(int(item.get("updated_at").timestamp()) if isinstance(item.get("updated_at"), datetime) else 0),
            str(item.get("title") or ""),
        ),
    )


def _matches_filters(
    item: dict[str, Any],
    *,
    category: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    q: str | None = None,
) -> bool:
    if category:
        requested_category = _normalize_key(category)
        item_category = _normalize_key(str(item.get("category") or ""))
        if requested_category != "all" and item_category != requested_category:
            return False
    if status and _normalize_key(str(item.get("status") or "")) != _normalize_key(status):
        return False
    if severity and _normalize_key(str(item.get("severity") or "")) != _normalize_key(severity):
        return False
    if q:
        query = _normalize_key(q)
        haystack = " ".join(
            str(part)
            for part in [
                item.get("title"),
                item.get("subtitle"),
                item.get("description"),
                item.get("asset_name"),
                item.get("pipeline_name"),
                item.get("dag_id"),
                item.get("task_id"),
            ]
            if part
        ).lower()
        if query not in haystack:
            return False
    return True


def build_platform_cockpit_queue_page(
    session: Session,
    *,
    current_user: Any = None,
    category: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    q: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    items = [item for item in build_platform_cockpit_queue_items(session, current_user=current_user) if _matches_filters(item, category=category, status=status, severity=severity, q=q)]
    normalized_page, normalized_page_size = normalize_page_params(
        page=page,
        page_size=page_size,
        default_page_size=20,
        max_page_size=100,
    )
    total = len(items)
    start = (normalized_page - 1) * normalized_page_size
    end = start + normalized_page_size
    page_items = items[start:end]
    total_pages = (total + normalized_page_size - 1) // normalized_page_size if total else 0
    return {
        "generated_at": _now(),
        "category": _normalize_key(category) if category else None,
        "page": normalized_page,
        "page_size": normalized_page_size,
        "total": total,
        "total_pages": total_pages,
        "has_more": end < total,
        "items": page_items,
    }


def build_platform_cockpit_recommended_actions(
    session: Session,
    *,
    current_user: Any = None,
    limit: int = 10,
) -> dict[str, Any]:
    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    jobs_snapshot = integration_jobs_status_snapshot(session, limit=max(int(limit or 10), 10))
    queue_items = build_platform_cockpit_queue_items(session, current_user=current_user)
    analytics = analytics_summary(session, days=30, current_user=current_user)

    actions: list[dict[str, Any]] = []
    stalled_job = next(
        (
            item
            for item in jobs_snapshot.get("items", [])
            if item.get("diagnostic_status") in {"stalled", "overdue_next_run", "attention"} or bool(item.get("is_stalled"))
        ),
        None,
    )
    if stalled_job is not None:
        actions.append(
            {
                "id": f"job-stalled-{stalled_job['id']}",
                "title": "Revisar job travado",
                "severity": "critical" if stalled_job.get("diagnostic_status") == "stalled" else "warning",
                "origin": "automation_jobs",
                "impact": "Execução running por tempo excessivo pode indicar lock preso, scheduler travado ou heartbeat ausente.",
                "reason": str(stalled_job.get("diagnostic_description") or stalled_job.get("diagnostic_label") or "Job em risco operacional."),
                "suggested_route": "/ops/cockpit#jobs",
                "primary_action_label": "Ver jobs",
                "secondary_action_label": "Ver histórico",
                "context": {
                    "job_name": stalled_job.get("job_key"),
                    "started_at": stalled_job.get("started_at"),
                    "running_duration_seconds": stalled_job.get("running_duration_seconds"),
                    "diagnostic_status": stalled_job.get("diagnostic_status"),
                },
                "priority": 100,
            }
        )

    for item in queue_items:
        if str(item.get("severity") or "info") == "healthy":
            continue
        item_type = str(item.get("type") or "")
        if item_type == "pipeline_failure":
            actions.append(
                {
                    "id": f"pipeline-failure-{item.get('id')}",
                    "title": f"Corrigir pipeline {item.get('title')}",
                    "severity": "critical",
                    "origin": "ingestion",
                    "impact": "Pipeline sem sucesso recente ou com falha ativa pode bloquear atualização de dados.",
                    "reason": str(item.get("description") or "Falha operacional ativa."),
                    "suggested_route": item.get("route"),
                    "primary_action_label": "Ver histórico operacional",
                    "secondary_action_label": "Abrir incidente",
                    "context": {
                        "asset_id": item.get("asset_id"),
                        "pipeline_name": item.get("pipeline_name"),
                        "dag_id": item.get("dag_id"),
                        "task_id": item.get("task_id"),
                    },
                    "priority": 90,
                }
            )
        elif item_type == "degraded_pipeline":
            actions.append(
                {
                    "id": f"degraded-pipeline-{item.get('id')}",
                    "title": f"Revisar degradação em {item.get('title')}",
                    "severity": "warning",
                    "origin": "ingestion",
                    "impact": "A degradação operacional aumenta o risco de atraso e falha silenciosa.",
                    "reason": str(item.get("description") or "Pipeline degradado."),
                    "suggested_route": item.get("route"),
                    "primary_action_label": "Ver pipeline",
                    "secondary_action_label": None,
                    "context": {
                        "asset_id": item.get("asset_id"),
                        "pipeline_name": item.get("pipeline_name"),
                    },
                    "priority": 70,
                }
            )
        elif item_type == "asset_without_pipeline":
            actions.append(
                {
                    "id": f"unmapped-{item.get('id')}",
                    "title": "Mapear ativo sem pipeline",
                    "severity": "warning",
                    "origin": "mapping",
                    "impact": "Ativo sem pipeline mapeado reduz a rastreabilidade operacional.",
                    "reason": str(item.get("description") or "Ativo sem pipeline."),
                    "suggested_route": item.get("route"),
                    "primary_action_label": "Ver mapeamento",
                    "secondary_action_label": None,
                    "context": {"asset_id": item.get("asset_id"), "asset_name": item.get("asset_name")},
                    "priority": 60,
                }
            )
        elif item_type == "critical_incident":
            actions.append(
                {
                    "id": f"incident-{item.get('id')}",
                    "title": "Triar incidente crítico",
                    "severity": "critical",
                    "origin": "incidents",
                    "impact": "Incidente aberto pode exigir ação imediata para limitar impacto ao negócio.",
                    "reason": str(item.get("description") or "Incidente aberto."),
                    "suggested_route": item.get("route"),
                    "primary_action_label": "Ver incidente",
                    "secondary_action_label": None,
                    "context": {"asset_id": item.get("asset_id"), "open_incidents": item.get("metadata", {}).get("open_incidents")},
                    "priority": 80,
                }
            )
        elif item_type == "dq_failure":
            actions.append(
                {
                    "id": f"dq-{item.get('id')}",
                    "title": "Corrigir Data Quality",
                    "severity": "critical" if item.get("severity") == "critical" else "warning",
                    "origin": "quality",
                    "impact": "A degradação de DQ pode afetar consumo e certificação do ativo.",
                    "reason": str(item.get("description") or "DQ abaixo do esperado."),
                    "suggested_route": item.get("route"),
                    "primary_action_label": "Ver DQ",
                    "secondary_action_label": None,
                    "context": {"asset_id": item.get("asset_id"), "dq_score": item.get("metadata", {}).get("dq_score")},
                    "priority": 75,
                }
            )
        elif item_type == "governance_gap":
            actions.append(
                {
                    "id": f"governance-{item.get('id')}",
                    "title": "Resolver lacuna de governança",
                    "severity": "warning",
                    "origin": "governance",
                    "impact": "Sem owner ou com revisão vencida, o ativo perde rastreabilidade de tratamento.",
                    "reason": str(item.get("description") or "Lacuna de governança."),
                    "suggested_route": item.get("route"),
                    "primary_action_label": "Ver owner",
                    "secondary_action_label": None,
                    "context": {"asset_id": item.get("asset_id")},
                    "priority": 55,
                }
            )
        elif item_type == "privacy_review":
            actions.append(
                {
                    "id": f"privacy-{item.get('id')}",
                    "title": "Concluir revisão de privacidade",
                    "severity": "warning",
                    "origin": "privacy",
                    "impact": "Pendências de classificação ou revisão de privacidade reduzem segurança e conformidade.",
                    "reason": str(item.get("description") or "Revisão pendente."),
                    "suggested_route": item.get("route"),
                    "primary_action_label": "Ver privacidade",
                    "secondary_action_label": None,
                    "context": {"asset_id": item.get("asset_id"), "schema": item.get("schema")},
                    "priority": 50,
                }
            )
        if len(actions) >= max(int(limit or 10), 1) * 2:
            break

    actions.extend(_legacy_action_items(session, current_user=current_user))
    actions.sort(
        key=lambda item: (
            _SEVERITY_ORDER.get(str(item.get("severity") or "info"), 2),
            -int(item.get("priority") or 0),
            str(item.get("title") or ""),
        )
    )
    unique_actions: dict[str, dict[str, Any]] = {}
    for action in actions:
        key = str(action.get("id"))
        if key not in unique_actions:
            unique_actions[key] = action
    total = len(unique_actions)
    items = list(unique_actions.values())[: max(int(limit or 10), 1)]
    return {"generated_at": now, "total": total, "items": items}


def build_platform_cockpit_export_rows(
    session: Session,
    *,
    current_user: Any = None,
    category: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    q: str | None = None,
) -> list[dict[str, Any]]:
    queue_items = [
        item
        for item in build_platform_cockpit_queue_items(session, current_user=current_user)
        if _matches_filters(item, category=category, status=status, severity=severity, q=q)
    ]
    actions = build_platform_cockpit_recommended_actions(session, current_user=current_user, limit=10)["items"]
    analytics = analytics_summary(session, days=30, current_user=current_user)
    legacy_stats = legacy_api_usage_stats_by_module(session, days=30)

    rows: list[dict[str, Any]] = []
    for action in actions:
        rows.append(
            {
                "record_type": "recommended_action",
                "severity": action.get("severity"),
                "title": action.get("title"),
                "asset_name": action.get("context", {}).get("asset_name") if isinstance(action.get("context"), dict) else None,
                "database": action.get("context", {}).get("database") if isinstance(action.get("context"), dict) else None,
                "schema": action.get("context", {}).get("schema") if isinstance(action.get("context"), dict) else None,
                "pipeline_name": action.get("context", {}).get("pipeline_name") if isinstance(action.get("context"), dict) else None,
                "dag_id": action.get("context", {}).get("dag_id") if isinstance(action.get("context"), dict) else None,
                "task_id": action.get("context", {}).get("task_id") if isinstance(action.get("context"), dict) else None,
                "status": "actionable",
                "reason": action.get("reason"),
                "impact": action.get("impact"),
                "recommended_action": action.get("primary_action_label"),
                "route": action.get("suggested_route"),
                "updated_at": action.get("context", {}).get("started_at") if isinstance(action.get("context"), dict) else None,
                "metadata_json": action.get("context") or {},
            }
        )

    for item in queue_items:
        if str(item.get("severity") or "info") not in {"critical", "warning"}:
            continue
        rows.append(
            {
                "record_type": str(item.get("type") or "queue_item"),
                "severity": item.get("severity"),
                "title": item.get("title"),
                "asset_name": item.get("asset_name"),
                "database": item.get("database"),
                "schema": item.get("schema"),
                "pipeline_name": item.get("pipeline_name"),
                "dag_id": item.get("dag_id"),
                "task_id": item.get("task_id"),
                "status": item.get("status"),
                "reason": item.get("description"),
                "impact": item.get("metadata", {}).get("reason") if isinstance(item.get("metadata"), dict) else None,
                "recommended_action": item.get("recommended_action"),
                "route": item.get("route"),
                "updated_at": item.get("updated_at"),
                "metadata_json": item.get("metadata") or {},
            }
        )

    for module, payload in legacy_stats.items():
        hits = int(payload.get("hits_in_window", 0) or 0)
        if hits <= 0:
            continue
        rows.append(
            {
                "record_type": "legacy_api_usage",
                "severity": "warning" if int(analytics.get("legacy_api_hits") or 0) > 0 else "info",
                "title": f"Uso residual da API legada em {module}",
                "asset_name": None,
                "database": None,
                "schema": None,
                "pipeline_name": None,
                "dag_id": None,
                "task_id": None,
                "status": "legacy_usage",
                "reason": f"{hits} chamada(s) no período.",
                "impact": "Ainda existe tráfego em rotas antigas.",
                "recommended_action": "Planejar desligamento seguro.",
                "route": "/admin/governance",
                "updated_at": payload.get("last_hit_at"),
                "metadata_json": {"module": module, "hits_in_window": hits, "hits_total": int(payload.get("hits_total", 0) or 0)},
            }
        )

    rows.sort(
        key=lambda item: (
            _SEVERITY_ORDER.get(str(item.get("severity") or "info"), 2),
            str(item.get("record_type") or ""),
            str(item.get("title") or ""),
        )
    )
    return rows
