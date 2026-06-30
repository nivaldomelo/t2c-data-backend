from __future__ import annotations

import json
import csv
from math import ceil
from datetime import datetime, timezone
from io import StringIO

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.db import get_db
from t2c_data.core.deps import get_current_user, require_permission, require_roles
from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, redact_export_value, resolve_export_limit
from t2c_data.features.platform import (
    analytics_summary,
    cockpit_summary,
    build_platform_cockpit_export_rows,
    build_platform_cockpit_queue_page,
    build_platform_cockpit_recommended_actions,
    legacy_api_surface_summary,
    list_platform_domain_events,
    refresh_platform_read_models,
    serialize_platform_domain_event,
)
from t2c_data.features.platform.event_catalog import (
    list_supported_platform_events,
    supported_platform_event_categories,
    supported_platform_event_keys,
)
from t2c_data.features.platform.api_keys import (
    create_api_key,
    list_api_keys,
    list_external_api_scopes,
    get_api_key,
    rotate_api_key,
    update_api_key,
    revoke_api_key,
    serialize_api_key_out,
)
from t2c_data.features.platform.analytics import track_usage_event
from t2c_data.features.platform.automations import (
    create_automation_rule,
    delete_automation_rule,
    evaluate_automation_rules,
    execute_automation_action,
    list_available_automation_actions,
    list_automation_executions,
    list_automation_rules,
    run_automation_rule,
    _serialize_execution,
    update_automation_rule,
)
from t2c_data.features.platform.events import PlatformEventFilters
from t2c_data.features.platform.operational_actions import open_operational_incident, reprocess_datasource_scan, rerun_table_profiling
from t2c_data.features.pagination import paginate_items
from t2c_data.features.platform.scheduler import scheduler_status_snapshot
from t2c_data.features.platform.jobs import integration_jobs_status_snapshot, list_integration_jobs_history, run_platform_job
from t2c_data.features.ingestion import operational_source_diagnostics
from t2c_data.models.auth import User
from t2c_data.models.platform import (
    AssetVisibilityRule,
    PlatformAutomationExecution,
    PlatformAutomationRule,
    PlatformDomainEvent,
    PlatformApiKey,
)
from t2c_data.schemas.platform import (
    AssetVisibilityRuleIn,
    AssetVisibilityRuleOut,
    PlatformDomainEventOut,
    PlatformDomainEventsOut,
    PlatformSupportedEventsOut,
    PlatformActionOut,
    PlatformAutomationActionOut,
    PlatformAutomationActionsOut,
    PlatformAutomationEvaluationOut,
    PlatformAutomationExecuteIn,
    PlatformAutomationExecutionOut,
    PlatformAutomationExecutionsOut,
    PlatformAutomationRuleIn,
    PlatformAutomationRuleOut,
    PlatformAutomationRulesOut,
    PlatformAnalyticsSummaryOut,
    PlatformCockpitOut,
    PlatformCockpitQueuePageOut,
    PlatformCockpitRecommendedActionsOut,
    IntegrationSyncJobOut,
    IntegrationSyncJobRunIn,
    PlatformJobsStatusOut,
    PlatformLegacyApiSurfaceOut,
    PlatformOperationalSourceOut,
    PlatformSchedulerStatusOut,
    PlatformUsageEventIn,
    PlatformUsageEventOut,
    ReadModelRefreshOut,
)
from t2c_data.schemas.external_api import (
    ExternalApiKeyCreateIn,
    ExternalApiKeyCreatedOut,
    ExternalApiKeyOut,
    ExternalApiKeyRotateOut,
    ExternalApiKeyUpdateIn,
    ExternalApiScopeOut,
)
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter(prefix="/platform", tags=["platform"])

def _serialize_visibility_rule(rule: AssetVisibilityRule) -> AssetVisibilityRuleOut:
    return AssetVisibilityRuleOut(
        id=rule.id,
        entity_type=rule.entity_type,
        entity_id=rule.entity_id,
        rule_scope=rule.rule_scope,
        match_value=rule.match_value,
        allowed_role=rule.allowed_role,
        allowed_user_id=rule.allowed_user_id,
        visibility_scope=rule.visibility_scope,
        mask_sensitive_fields=bool(rule.mask_sensitive_fields),
        reason=rule.reason,
        is_active=rule.is_active,
        created_at=rule.created_at,
    )


@router.post("/read-models/refresh", response_model=ReadModelRefreshOut)
def platform_refresh_read_models_endpoint(
    request: Request,
    mode: str = Query(default="full", pattern="^(full|incremental|auto)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ReadModelRefreshOut:
    payload = refresh_platform_read_models(db, mode=mode)
    search_payload = payload["search"]
    dashboard_payload = payload["dashboard"]
    write_audit_log_sync(
        db,
        action="platform.read_models.refresh",
        entity_type="platform_read_model",
        entity_id="all",
        after={
            "search_entries": int(search_payload["entries"]),
            "dashboard_entries": int(dashboard_payload["entries"]),
            "refreshed_at": dashboard_payload["refreshed_at"],
            "mode": mode,
            "search_strategy": search_payload.get("mode"),
            "dashboard_strategy": dashboard_payload.get("mode"),
        },
        metadata={"message": "Platform read models refreshed"},
        source_module="platform",
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return ReadModelRefreshOut(
        refreshed_at=dashboard_payload["refreshed_at"],
        search_entries=int(search_payload["entries"]),
        dashboard_entries=int(dashboard_payload["entries"]),
        mode=str(mode),
    )


@router.get("/cockpit/summary", response_model=PlatformCockpitOut)
def platform_cockpit(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformCockpitOut:
    return PlatformCockpitOut(**cockpit_summary(db, current_user=current_user))


@router.get("/cockpit/recommended-actions", response_model=PlatformCockpitRecommendedActionsOut)
def platform_cockpit_recommended_actions(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformCockpitRecommendedActionsOut:
    payload = build_platform_cockpit_recommended_actions(db, current_user=current_user, limit=limit)
    return PlatformCockpitRecommendedActionsOut(**payload)


@router.get("/cockpit/queues", response_model=PlatformCockpitQueuePageOut)
def platform_cockpit_queues(
    category: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    severity: str | None = Query(default=None),
    q: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformCockpitQueuePageOut:
    payload = build_platform_cockpit_queue_page(
        db,
        current_user=current_user,
        category=category,
        status=status_filter,
        severity=severity,
        q=q,
        page=page,
        page_size=page_size,
    )
    return PlatformCockpitQueuePageOut(**payload)


@router.get("/cockpit/export.csv", response_model=None)
def platform_cockpit_export_csv(
    request: Request,
    category: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    severity: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("ops.export")),
) -> StreamingResponse:
    export_limit = resolve_export_limit(source_module="platform", entity_type="platform_cockpit")
    rows = build_platform_cockpit_export_rows(
        db,
        current_user=current_user,
        category=category,
        status=status_filter,
        severity=severity,
        q=q,
    )
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="platform.cockpit.export_csv",
        entity_type="platform_cockpit",
        source_module="platform",
        row_count=len(rows),
        truncated=truncated,
        limit=export_limit,
        filters={"category": category, "status": status_filter, "severity": severity, "q": q},
    )
    headers = [
        "record_type",
        "severity",
        "title",
        "asset_name",
        "database",
        "schema",
        "pipeline_name",
        "dag_id",
        "task_id",
        "status",
        "reason",
        "impact",
        "recommended_action",
        "route",
        "updated_at",
        "metadata_json",
    ]
    buffer = StringIO()
    buffer.write("\ufeff")
    writer = csv.writer(buffer)
    writer.writerow(headers)

    def _csv_value(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (dict, list)):
            return redact_export_value(value, field_name="metadata_json")
        return redact_export_value(value)

    for row in rows:
        writer.writerow([_csv_value(row.get(header, "")) for header in headers])
    payload = buffer.getvalue().encode("utf-8")
    return StreamingResponse(
        iter([payload]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="ops_cockpit_operational_export.csv"'},
    )


@router.get("/scheduler/status", response_model=PlatformSchedulerStatusOut)
def platform_scheduler_status(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformSchedulerStatusOut:
    return PlatformSchedulerStatusOut(**scheduler_status_snapshot(db))


@router.get("/jobs/status", response_model=PlatformJobsStatusOut)
def platform_jobs_status(
    limit: int = Query(default=12, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformJobsStatusOut:
    return PlatformJobsStatusOut(**integration_jobs_status_snapshot(db, limit=limit))


@router.get("/jobs/history", response_model=PageOut[IntegrationSyncJobOut])
def platform_jobs_history(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    source: str | None = None,
    job_type: str | None = None,
    status: str | None = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PageOut[IntegrationSyncJobOut]:
    return list_integration_jobs_history(
        db,
        page=page,
        page_size=page_size,
        source=source,
        job_type=job_type,
        status=status,
    )


@router.post("/jobs/run", response_model=IntegrationSyncJobOut)
def platform_jobs_run(
    payload: IntegrationSyncJobRunIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> IntegrationSyncJobOut:
    job = run_platform_job(
        db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return job


@router.get("/ingestion/source", response_model=PlatformOperationalSourceOut)
def platform_operational_ingestion_source(
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformOperationalSourceOut:
    return PlatformOperationalSourceOut(**operational_source_diagnostics())


@router.get("/analytics/summary", response_model=PlatformAnalyticsSummaryOut)
def platform_analytics(
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAnalyticsSummaryOut:
    return PlatformAnalyticsSummaryOut(**analytics_summary(db, days=days, current_user=current_user))


@router.get("/events", response_model=PlatformDomainEventsOut)
def platform_domain_events(
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=100, ge=1, le=500),
    table_id: int | None = None,
    entity_type: str | None = None,
    event_key: str | None = None,
    category: str | None = None,
    severity: str | None = None,
    q: str | None = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformDomainEventsOut:
    payload = list_platform_domain_events(
        db,
        filters=PlatformEventFilters(
            days=days,
            limit=limit,
            table_id=table_id,
            entity_type=entity_type,
            event_key=event_key,
            category=category,
            severity=severity,
            q=q,
        ),
    )
    return PlatformDomainEventsOut(**payload)


@router.get("/events/catalog", response_model=PlatformSupportedEventsOut)
def platform_supported_event_catalog(
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformSupportedEventsOut:
    payload = list_supported_platform_events()
    return PlatformSupportedEventsOut(
        generated_at=datetime.now(timezone.utc),
        total=int(payload["total"]),
        items=payload["items"],
    )


@router.get("/automations/actions", response_model=PlatformAutomationActionsOut)
def platform_automation_actions(
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationActionsOut:
    payload = list_available_automation_actions()
    return PlatformAutomationActionsOut(
        generated_at=datetime.now(timezone.utc),
        total=int(payload["total"]),
        items=[PlatformAutomationActionOut(**item) for item in payload["items"]],
    )


@router.get("/automations/rules", response_model=PlatformAutomationRulesOut)
def platform_automation_rules_list(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationRulesOut:
    payload = list_automation_rules(db)
    return PlatformAutomationRulesOut(
        generated_at=datetime.fromisoformat(payload["generated_at"]),
        total=int(payload["total"]),
        items=[PlatformAutomationRuleOut(**item) for item in payload["items"]],
    )


@router.post("/automations/rules", response_model=PlatformAutomationRuleOut, status_code=status.HTTP_201_CREATED)
def platform_automation_rules_create(
    payload: PlatformAutomationRuleIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationRuleOut:
    rule = create_automation_rule(
        db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    executions = db.scalars(
        select(PlatformAutomationExecution).where(PlatformAutomationExecution.rule_id == rule.id)
    ).all()
    rule_payload = serialize_model(rule)
    return PlatformAutomationRuleOut(
        **{
            **rule_payload,
            "execution_count": len(executions),
            "suggested_count": sum(1 for execution in executions if execution.status == "suggested"),
            "succeeded_count": sum(1 for execution in executions if execution.status == "succeeded"),
            "failed_count": sum(1 for execution in executions if execution.status == "failed"),
        }
    )


@router.put("/automations/rules/{rule_id}", response_model=PlatformAutomationRuleOut)
def platform_automation_rules_update(
    rule_id: int,
    payload: PlatformAutomationRuleIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationRuleOut:
    rule = update_automation_rule(
        db,
        rule_id=rule_id,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    executions = db.scalars(
        select(PlatformAutomationExecution).where(PlatformAutomationExecution.rule_id == rule.id)
    ).all()
    rule_payload = serialize_model(rule)
    return PlatformAutomationRuleOut(
        **{
            **rule_payload,
            "execution_count": len(executions),
            "suggested_count": sum(1 for execution in executions if execution.status == "suggested"),
            "succeeded_count": sum(1 for execution in executions if execution.status == "succeeded"),
            "failed_count": sum(1 for execution in executions if execution.status == "failed"),
        }
    )


@router.delete("/automations/rules/{rule_id}", response_model=PlatformUsageEventOut)
def platform_automation_rules_delete(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformUsageEventOut:
    delete_automation_rule(
        db,
        rule_id=rule_id,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return PlatformUsageEventOut(ok=True)


@router.post("/automations/rules/{rule_id}/run", response_model=PlatformAutomationExecutionOut)
def platform_automation_rule_run(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationExecutionOut:
    execution = run_automation_rule(
        db,
        rule_id=rule_id,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return PlatformAutomationExecutionOut(**_serialize_execution(execution))


@router.post("/automations/evaluate", response_model=PlatformAutomationEvaluationOut)
def platform_automation_evaluate(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationEvaluationOut:
    payload = evaluate_automation_rules(db, current_user=current_user, audit_kwargs=request_audit_kwargs(request, current_user))
    return PlatformAutomationEvaluationOut(
        generated_at=datetime.fromisoformat(payload["generated_at"]),
        rules_evaluated=int(payload["rules_evaluated"]),
        suggestions_created=int(payload["suggestions_created"]),
        actions_executed=int(payload["actions_executed"]),
        skipped=int(payload["skipped"]),
        items=[PlatformAutomationExecutionOut(**item) for item in payload["items"]],
    )


@router.get("/automations/executions", response_model=PlatformAutomationExecutionsOut)
def platform_automation_executions_list(
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationExecutionsOut:
    payload = list_automation_executions(db, limit=limit)
    return PlatformAutomationExecutionsOut(
        generated_at=datetime.fromisoformat(payload["generated_at"]),
        total=int(payload["total"]),
        items=[PlatformAutomationExecutionOut(**item) for item in payload["items"]],
    )


@router.post("/automations/execute", response_model=PlatformAutomationExecutionOut)
def platform_automation_execute(
    payload: PlatformAutomationExecuteIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformAutomationExecutionOut:
    execution = execute_automation_action(
        db,
        action_key=payload.action_key,
        current_user=current_user,
        table_id=payload.table_id,
        datasource_id=payload.datasource_id,
        dq_rule_id=payload.dq_rule_id,
        delivery_id=payload.delivery_id,
        incident_id=payload.incident_id,
        data_owner_id=payload.data_owner_id,
        request_type=payload.request_type,
        scope_kind=payload.scope_kind,
        scope_value=payload.scope_value,
        target_json=payload.target_json,
        execution_mode="manual",
        trigger_source="manual",
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return PlatformAutomationExecutionOut(**serialize_model(execution))


@router.get("/api-keys/scopes", response_model=PageOut[ExternalApiScopeOut])
def platform_api_key_scopes(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    _: User = Depends(require_roles("admin")),
) -> PageOut[ExternalApiScopeOut]:
    return paginate_items([ExternalApiScopeOut(**item) for item in list_external_api_scopes()], page=page, page_size=page_size)

@router.get("/api-keys", response_model=PageOut[ExternalApiKeyOut])
def platform_api_keys_list(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    _: User = Depends(require_roles("admin")),
) -> PageOut[ExternalApiKeyOut]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 25), 1)
    total = int(db.scalar(select(func.count(PlatformApiKey.id))) or 0)
    keys = list_api_keys(db, offset=(normalized_page - 1) * normalized_page_size, limit=normalized_page_size)
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    return PageOut[ExternalApiKeyOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=[serialize_api_key_out(key) for key in keys],
    )


@router.post("/api-keys", response_model=ExternalApiKeyCreatedOut, status_code=status.HTTP_201_CREATED)
def platform_api_keys_create(
    payload: ExternalApiKeyCreateIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> ExternalApiKeyCreatedOut:
    key, token = create_api_key(
        db,
        name=payload.name,
        description=payload.description,
        scopes=payload.scopes,
        environment=payload.environment,
        allowed_ips=payload.allowed_ips,
        status_value=payload.status,
        expires_at=payload.expires_at,
        expires_in_days=payload.expires_in_days,
        created_by=current_user,
    )
    write_audit_log_sync(
        db,
        action="platform.api_key.created",
        entity_type="platform_api_key",
        entity_id=key.id,
        after={
            "name": key.name,
            "scopes": key.scopes_json,
            "environment": key.environment,
            "allowed_ips": key.allowed_ips_json,
            "status": key.status,
            "expires_at": key.expires_at,
            "permission_summary": serialize_api_key_out(key).permission_summary.model_dump(),
        },
        source_module="platform",
        is_sensitive_change=True,
        sensitive_category="credential",
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    token_preview = f"{key.token_prefix}..."
    return ExternalApiKeyCreatedOut(key=serialize_api_key_out(key), token=token, token_preview=token_preview)


@router.get("/api-keys/{key_id}", response_model=ExternalApiKeyOut)
def platform_api_keys_get(
    key_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> ExternalApiKeyOut:
    key = get_api_key(db, key_id)
    if key is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API key não encontrada.")
    return serialize_api_key_out(key)


@router.put("/api-keys/{key_id}", response_model=ExternalApiKeyOut)
def platform_api_keys_update(
    key_id: int,
    payload: ExternalApiKeyUpdateIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> ExternalApiKeyOut:
    key = get_api_key(db, key_id)
    if key is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API key não encontrada.")
    before = serialize_model(key)
    update_api_key(
        key,
        name=payload.name,
        description=payload.description,
        scopes=payload.scopes,
        environment=payload.environment,
        allowed_ips=payload.allowed_ips,
        status_value=payload.status,
        expires_at=payload.expires_at,
        expires_in_days=payload.expires_in_days,
    )
    write_audit_log_sync(
        db,
        action="platform.api_key.updated",
        entity_type="platform_api_key",
        entity_id=key.id,
        before=before,
        after=serialize_model(key),
        source_module="platform",
        is_sensitive_change=True,
        sensitive_category="credential",
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return serialize_api_key_out(key)


@router.post("/api-keys/{key_id}/rotate", response_model=ExternalApiKeyRotateOut)
def platform_api_keys_rotate(
    key_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> ExternalApiKeyRotateOut:
    key = get_api_key(db, key_id)
    if key is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API key não encontrada.")
    token = rotate_api_key(key)
    write_audit_log_sync(
        db,
        action="platform.api_key.rotated",
        entity_type="platform_api_key",
        entity_id=key.id,
        after={
            "name": key.name,
            "scopes": key.scopes_json,
            "status": key.status,
            "permission_summary": serialize_api_key_out(key).permission_summary.model_dump(),
        },
        source_module="platform",
        is_sensitive_change=True,
        sensitive_category="credential",
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    token_preview = f"{key.token_prefix}..."
    return ExternalApiKeyRotateOut(key=serialize_api_key_out(key), token=token, token_preview=token_preview)


@router.post("/api-keys/{key_id}/revoke", response_model=ExternalApiKeyOut)
def platform_api_keys_revoke(
    key_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> ExternalApiKeyOut:
    key = get_api_key(db, key_id)
    if key is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API key não encontrada.")
    revoke_api_key(key)
    write_audit_log_sync(
        db,
        action="platform.api_key.revoked",
        entity_type="platform_api_key",
        entity_id=key.id,
        after={"status": key.status},
        source_module="platform",
        is_sensitive_change=True,
        sensitive_category="credential",
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return serialize_api_key_out(key)


@router.get("/events/{event_id}", response_model=PlatformDomainEventOut)
def platform_domain_event_detail(
    event_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformDomainEventOut:
    event = db.get(PlatformDomainEvent, event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Evento não encontrado.")
    return PlatformDomainEventOut(**serialize_platform_domain_event(event))


@router.get("/legacy-api/surface", response_model=PlatformLegacyApiSurfaceOut)
def platform_legacy_api_surface(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> PlatformLegacyApiSurfaceOut:
    return PlatformLegacyApiSurfaceOut(**legacy_api_surface_summary(db))


@router.post("/analytics/events", response_model=PlatformUsageEventOut)
def platform_track_event(
    payload: PlatformUsageEventIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> PlatformUsageEventOut:
    track_usage_event(
        db,
        user=current_user,
        event_name=payload.event_name,
        module_name=payload.module_name,
        page_path=payload.page_path,
        entity_type=payload.entity_type,
        entity_id=payload.entity_id,
        target_url=payload.target_url,
        metadata=payload.metadata,
    )
    db.commit()
    return PlatformUsageEventOut(ok=True)


@router.get("/visibility/rules", response_model=list[AssetVisibilityRuleOut])
def list_visibility_rules(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> list[AssetVisibilityRuleOut]:
    rows = db.scalars(select(AssetVisibilityRule).order_by(AssetVisibilityRule.created_at.desc(), AssetVisibilityRule.id.desc())).all()
    return [_serialize_visibility_rule(row) for row in rows]


@router.post("/visibility/rules", response_model=AssetVisibilityRuleOut, status_code=status.HTTP_201_CREATED)
def create_visibility_rule(
    payload: AssetVisibilityRuleIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> AssetVisibilityRuleOut:
    if not payload.allowed_role and payload.allowed_user_id is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe um papel ou um usuário para permitir acesso.")
    if payload.rule_scope == "asset" and payload.entity_id is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe o ativo para regras no escopo asset.")
    if payload.rule_scope in {"domain", "classification"} and not (payload.match_value or "").strip():
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe o valor de domínio ou classificação para a regra.")

    rule = AssetVisibilityRule(**payload.model_dump())
    db.add(rule)
    db.commit()
    db.refresh(rule)
    write_audit_log_sync(
        db,
        action="platform.visibility_rule.create",
        entity_type="asset_visibility_rule",
        entity_id=rule.id,
        after=serialize_model(rule),
        metadata={"message": "Asset visibility rule created"},
        source_module="platform.visibility",
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return _serialize_visibility_rule(rule)


@router.delete("/visibility/rules/{rule_id}", response_model=PlatformUsageEventOut)
def delete_visibility_rule(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> PlatformUsageEventOut:
    rule = db.get(AssetVisibilityRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Regra de visibilidade não encontrada.")
    before = serialize_model(rule)
    db.delete(rule)
    db.commit()
    write_audit_log_sync(
        db,
        action="platform.visibility_rule.delete",
        entity_type="asset_visibility_rule",
        entity_id=rule_id,
        before=before,
        metadata={"message": "Asset visibility rule removed"},
        source_module="platform.visibility",
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return PlatformUsageEventOut(ok=True)


@router.post("/actions/datasources/{datasource_id}/scan/reprocess", response_model=PlatformActionOut)
def platform_reprocess_scan(
    datasource_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> PlatformActionOut:
    return PlatformActionOut(
        **reprocess_datasource_scan(
            db,
            datasource_id=datasource_id,
            current_user=current_user,
            audit_kwargs=request_audit_kwargs(request, current_user),
        )
    )


@router.post("/actions/tables/{table_id}/profiling/rerun", response_model=PlatformActionOut)
def platform_rerun_profiling(
    table_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> PlatformActionOut:
    return PlatformActionOut(
        **rerun_table_profiling(
            db,
            table_id=table_id,
            current_user=current_user,
            audit_kwargs=request_audit_kwargs(request, current_user),
        )
    )


@router.post("/actions/tables/{table_id}/incidents/open", response_model=PlatformActionOut)
def platform_open_incident(
    table_id: int,
    request: Request,
    mode: str = Query(default="manual", pattern="^(manual|auto_if_missing)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> PlatformActionOut:
    return PlatformActionOut(
        **open_operational_incident(
            db,
            table_id=table_id,
            current_user=current_user,
            audit_kwargs=request_audit_kwargs(request, current_user),
            mode=mode,
        )
    )
