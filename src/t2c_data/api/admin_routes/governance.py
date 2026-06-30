from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.export_jobs import ExportArtifactResult, enqueue_export_job, register_export_request_audit, serialize_export_job
from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, redact_export_value, resolve_export_limit
from t2c_data.features.governance import get_or_create_governance_settings
from t2c_data.features.governance.score_config import normalize_governance_policy_rules, normalize_governance_score_weights
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.models.auth import User
from t2c_data.models.audit import AccessLog, AccessLogArchive, AuditLog, AuditLogArchive
from t2c_data.models.platform import PlatformSchedulerStatus, PlatformUsageEvent
from t2c_data.models.search import SearchResultClick
from t2c_data.schemas.governance import GovernanceRetentionSummaryOut, GovernanceSettingsOut, GovernanceSettingsUpdate, RetentionTableSummaryOut
from t2c_data.schemas.platform import IntegrationSyncJobOut
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter()


def _count_rows(db: Session, model, *, since_dt: datetime | None = None) -> int:
    stmt = select(func.count()).select_from(model)
    if since_dt is not None:
        stmt = stmt.where(model.created_at >= since_dt)
    return int(db.scalar(stmt) or 0)


def _estimate_relation_size_mb(db: Session, table_name: str) -> float | None:
    relation_name = f"{settings.db_schema}.{table_name}"
    try:
        size_bytes = db.execute(
            text("SELECT pg_total_relation_size(to_regclass(:relation_name))"),
            {"relation_name": relation_name},
        ).scalar_one_or_none()
    except Exception:
        return None
    if size_bytes is None:
        return None
    return round(float(size_bytes) / (1024 * 1024), 2)


def _pressure_level(*, projected_hot_rows: int, eligible_for_archive: int, eligible_for_purge: int, projected_storage_mb_30d: float | None) -> str:
    if eligible_for_archive > 100_000 or eligible_for_purge > 100_000:
        return "high"
    if projected_storage_mb_30d is not None and projected_storage_mb_30d >= 1024:
        return "high"
    if projected_hot_rows >= 250_000 or eligible_for_archive > 10_000 or eligible_for_purge > 10_000:
        return "medium"
    return "normal"


def _retention_item(
    *,
    db: Session,
    table_name: str,
    hot_model,
    archive_model=None,
    hot_retention_days: int | None,
    archive_retention_days: int | None,
    hot_cutoff: datetime | None,
    archive_cutoff: datetime | None,
    last_archived_count: int = 0,
    last_purged_count: int = 0,
) -> RetentionTableSummaryOut:
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=30)
    hot_rows = _count_rows(db, hot_model)
    archived_rows = _count_rows(db, archive_model) if archive_model is not None else 0
    estimated_rows_per_day = round(_count_rows(db, hot_model, since_dt=since) / 30, 1)
    projected_rows_30d = int(round(estimated_rows_per_day * 30))
    projected_hot_rows_at_retention = int(round(estimated_rows_per_day * hot_retention_days)) if hot_retention_days else hot_rows
    estimated_storage_mb = _estimate_relation_size_mb(db, table_name)
    if estimated_storage_mb is not None and hot_rows > 0:
        projected_storage_mb_30d = round(estimated_storage_mb * ((hot_rows + projected_rows_30d) / hot_rows), 2)
    else:
        projected_storage_mb_30d = estimated_storage_mb
    eligible_for_archive = 0
    if hot_cutoff is not None:
        eligible_for_archive = int(db.scalar(select(func.count()).select_from(hot_model).where(hot_model.created_at < hot_cutoff)) or 0)
    eligible_for_purge = 0
    if archive_model is not None and archive_cutoff is not None:
        eligible_for_purge = int(db.scalar(select(func.count()).select_from(archive_model).where(archive_model.created_at < archive_cutoff)) or 0)
    pressure_level = _pressure_level(
        projected_hot_rows=projected_hot_rows_at_retention,
        eligible_for_archive=eligible_for_archive,
        eligible_for_purge=eligible_for_purge,
        projected_storage_mb_30d=projected_storage_mb_30d,
    )
    return RetentionTableSummaryOut(
        table_name=table_name,
        hot_rows=hot_rows,
        archived_rows=archived_rows,
        eligible_for_archive=eligible_for_archive,
        eligible_for_purge=eligible_for_purge,
        last_archived_count=last_archived_count,
        last_purged_count=last_purged_count,
        estimated_rows_per_day=estimated_rows_per_day,
        projected_rows_30d=projected_rows_30d,
        projected_hot_rows_at_retention=projected_hot_rows_at_retention,
        estimated_storage_mb=estimated_storage_mb,
        projected_storage_mb_30d=projected_storage_mb_30d,
        pressure_level=pressure_level,
        hot_retention_days=hot_retention_days,
        archive_retention_days=archive_retention_days,
    )


def _governance_settings_out(db: Session) -> GovernanceSettingsOut:
    settings_row = get_or_create_governance_settings(db)
    snapshot = get_governance_settings_snapshot(db)
    approver_ids = sorted({int(item["approver_user_id"]) for item in snapshot.stewardship_assignment_rules if item.get("approver_user_id")})
    approver_lookup = {
        int(user.id): user
        for user in db.scalars(select(User).where(User.id.in_(approver_ids))).all()
    } if approver_ids else {}
    return GovernanceSettingsOut(
        owner_review_interval_days=snapshot.owner_review_interval_days,
        privacy_review_interval_days=snapshot.privacy_review_interval_days,
        sensitive_privacy_review_interval_days=snapshot.sensitive_privacy_review_interval_days,
        certification_review_interval_days=snapshot.certification_review_interval_days,
        certification_review_sla_days=snapshot.certification_review_sla_days,
        certification_revalidation_window_days=snapshot.certification_revalidation_window_days,
        audit_log_retention_days=snapshot.audit_log_retention_days,
        audit_log_archive_retention_days=snapshot.audit_log_archive_retention_days,
        access_log_retention_days=snapshot.access_log_retention_days,
        access_log_archive_retention_days=snapshot.access_log_archive_retention_days,
        platform_usage_event_retention_days=snapshot.platform_usage_event_retention_days,
        search_result_click_retention_days=snapshot.search_result_click_retention_days,
        legacy_api_cutoff_window_days=snapshot.legacy_api_cutoff_window_days,
        legacy_api_disabled_modules=list(snapshot.legacy_api_disabled_modules),
        legacy_api_force_enabled_modules=list(snapshot.legacy_api_force_enabled_modules),
        stewardship_assignment_rules=[
            {
                "key": str(item["key"]),
                "request_type": str(item["request_type"]),
                "domain_name": item.get("domain_name"),
                "owner_area": item.get("owner_area"),
                "approver_user_id": int(item["approver_user_id"]),
                "approver_name": (approver_lookup.get(int(item["approver_user_id"])).name or approver_lookup.get(int(item["approver_user_id"])).full_name)
                if approver_lookup.get(int(item["approver_user_id"])) is not None
                else None,
                "approver_email": approver_lookup.get(int(item["approver_user_id"])).email
                if approver_lookup.get(int(item["approver_user_id"])) is not None
                else None,
                "priority": int(item.get("priority", 100) or 100),
                "is_active": bool(item.get("is_active", True)),
            }
            for item in snapshot.stewardship_assignment_rules
        ],
        governance_policy_rules=[
            {
                "key": str(item["key"]),
                "name": str(item.get("name") or item["key"]),
                "description": item.get("description"),
                "trigger_key": str(item.get("trigger_key") or ""),
                "scope": str(item.get("scope") or "table"),
                "domain_name": item.get("domain_name"),
                "datasource_name": item.get("datasource_name"),
                "criticality": item.get("criticality"),
                "sensitivity_level": item.get("sensitivity_level"),
                "min_trust_score": item.get("min_trust_score"),
                "min_risk_score": item.get("min_risk_score"),
                "min_search_clicks": item.get("min_search_clicks"),
                "severity": str(item.get("severity") or "medium"),
                "impact": str(item.get("impact") or "medium"),
                "sla_days": item.get("sla_days"),
                "action_key": str(item.get("action_key") or ""),
                "action_label": str(item.get("action_label") or ""),
                "recommendation_title": str(item.get("recommendation_title") or ""),
                "recommendation_detail": str(item.get("recommendation_detail") or ""),
                "auto_create_recommendation": bool(item.get("auto_create_recommendation", True)),
                "requires_owner": bool(item.get("requires_owner", False)),
                "requires_classification": bool(item.get("requires_classification", False)),
                "requires_dictionary": bool(item.get("requires_dictionary", False)),
                "requires_active_dq": bool(item.get("requires_active_dq", False)),
                "priority": int(item.get("priority", 100) or 100),
                "is_active": bool(item.get("is_active", True)),
            }
            for item in snapshot.governance_policy_rules
        ],
        governance_score_weights=snapshot.governance_score_weights or normalize_governance_score_weights(None),
        trust_score_domain_adjustments=snapshot.trust_score_domain_adjustments or {},
        trust_score_criticality_adjustments=snapshot.trust_score_criticality_adjustments or {},
        governance_notifications_enabled=snapshot.governance_notifications_enabled,
        governance_notification_repeat_days=snapshot.governance_notification_repeat_days,
        governance_notification_critical_repeat_hours=snapshot.governance_notification_critical_repeat_hours,
        pipeline_failure_owner_sla_hours=snapshot.pipeline_failure_owner_sla_hours,
        platform_job_running_attention_minutes=snapshot.platform_job_running_attention_minutes,
        platform_job_running_critical_hours=snapshot.platform_job_running_critical_hours,
        platform_job_next_expected_delay_minutes=snapshot.platform_job_next_expected_delay_minutes,
        platform_recent_success_window_hours=snapshot.platform_recent_success_window_hours,
        operational_high_volume_threshold_rows=snapshot.operational_high_volume_threshold_rows,
        governance_high_usage_click_threshold=snapshot.governance_high_usage_click_threshold,
        dq_operational_failure_penalty_points=snapshot.dq_operational_failure_penalty_points,
        dq_operational_stale_penalty_points=snapshot.dq_operational_stale_penalty_points,
        dq_operational_recurrent_penalty_points=snapshot.dq_operational_recurrent_penalty_points,
        airflow_ui_base_url=snapshot.airflow_ui_base_url,
        updated_at=settings_row.updated_at,
    )


@router.get("/governance-settings", response_model=GovernanceSettingsOut)
def get_governance_settings(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> GovernanceSettingsOut:
    return _governance_settings_out(db)


@router.put("/governance-settings", response_model=GovernanceSettingsOut)
def update_governance_settings(
    payload: GovernanceSettingsUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> GovernanceSettingsOut:
    settings_row = get_or_create_governance_settings(db)
    before = serialize_model(settings_row)
    data = payload.model_dump()
    data["legacy_api_disabled_modules"] = ",".join(sorted({item.strip().lower() for item in data.get("legacy_api_disabled_modules", []) if item.strip()}))
    data["legacy_api_force_enabled_modules"] = ",".join(
        sorted({item.strip().lower() for item in data.get("legacy_api_force_enabled_modules", []) if item.strip()})
    )
    data["stewardship_assignment_rules"] = json.dumps(
        [
            {
                "key": (str(item.get("key") or "").strip() or None),
                "request_type": str(item.get("request_type") or "any").strip().lower() or "any",
                "domain_name": (str(item.get("domain_name") or "").strip() or None),
                "owner_area": (str(item.get("owner_area") or "").strip() or None),
                "approver_user_id": int(item["approver_user_id"]),
                "priority": int(item.get("priority", 100) or 100),
                "is_active": bool(item.get("is_active", True)),
            }
            for item in data.get("stewardship_assignment_rules", [])
        ],
        sort_keys=True,
    )
    data["governance_policy_rules"] = json.dumps(
        normalize_governance_policy_rules(data.get("governance_policy_rules")),
        sort_keys=True,
    )
    data["governance_score_weights"] = json.dumps(
        normalize_governance_score_weights(data.get("governance_score_weights")),
        sort_keys=True,
    )
    data["trust_score_domain_adjustments"] = json.dumps(
        data.get("trust_score_domain_adjustments") or {},
        sort_keys=True,
    )
    data["trust_score_criticality_adjustments"] = json.dumps(
        data.get("trust_score_criticality_adjustments") or {},
        sort_keys=True,
    )
    data["airflow_ui_base_url"] = (str(data.get("airflow_ui_base_url") or "").strip() or None)
    for key, value in data.items():
        setattr(settings_row, key, value)
    db.add(settings_row)
    db.commit()
    db.refresh(settings_row)
    write_audit_log_sync(
        db,
        action="admin.governance_settings.update",
        entity_type="governance_settings",
        entity_id=settings_row.id,
        before=before,
        after=serialize_model(settings_row),
        metadata={"message": "Governance settings updated"},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return _governance_settings_out(db)


@router.get("/governance-retention-summary", response_model=GovernanceRetentionSummaryOut)
def get_governance_retention_summary(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> GovernanceRetentionSummaryOut:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(db)
    scheduler_status = db.get(PlatformSchedulerStatus, 1)
    last_run_summary = dict((scheduler_status.last_run_summary_json or {})) if scheduler_status else {}
    maintenance_summary = dict(last_run_summary.get("maintenance") or {})
    items = [
        _retention_item(
            db=db,
            table_name="audit_log",
            hot_model=AuditLog,
            archive_model=AuditLogArchive,
            hot_retention_days=settings_snapshot.audit_log_retention_days,
            archive_retention_days=settings_snapshot.audit_log_archive_retention_days,
            hot_cutoff=now - timedelta(days=settings_snapshot.audit_log_retention_days),
            archive_cutoff=now - timedelta(days=settings_snapshot.audit_log_archive_retention_days),
            last_archived_count=int(maintenance_summary.get("audit_archived", 0) or 0),
            last_purged_count=int(maintenance_summary.get("audit_archive_deleted", 0) or 0),
        ),
        _retention_item(
            db=db,
            table_name="access_log",
            hot_model=AccessLog,
            archive_model=AccessLogArchive,
            hot_retention_days=settings_snapshot.access_log_retention_days,
            archive_retention_days=settings_snapshot.access_log_archive_retention_days,
            hot_cutoff=now - timedelta(days=settings_snapshot.access_log_retention_days),
            archive_cutoff=now - timedelta(days=settings_snapshot.access_log_archive_retention_days),
            last_archived_count=int(maintenance_summary.get("access_archived", 0) or 0),
            last_purged_count=int(maintenance_summary.get("access_archive_deleted", 0) or 0),
        ),
        _retention_item(
            db=db,
            table_name="platform_usage_events",
            hot_model=PlatformUsageEvent,
            hot_retention_days=settings_snapshot.platform_usage_event_retention_days,
            archive_retention_days=None,
            hot_cutoff=now - timedelta(days=settings_snapshot.platform_usage_event_retention_days),
            archive_cutoff=None,
            last_purged_count=int(maintenance_summary.get("platform_usage_deleted", 0) or 0),
        ),
        _retention_item(
            db=db,
            table_name="search_result_clicks",
            hot_model=SearchResultClick,
            hot_retention_days=settings_snapshot.search_result_click_retention_days,
            archive_retention_days=None,
            hot_cutoff=now - timedelta(days=settings_snapshot.search_result_click_retention_days),
            archive_cutoff=None,
            last_purged_count=int(maintenance_summary.get("search_click_deleted", 0) or 0),
        ),
    ]
    return GovernanceRetentionSummaryOut(generated_at=now, items=items)


def _access_log_archive_rows(
    db: Session,
    *,
    module_name: str | None,
    api_version: str | None,
    days: int,
):
    since = datetime.now(timezone.utc) - timedelta(days=max(days, 1))
    stmt = select(AccessLogArchive).where(AccessLogArchive.created_at >= since)
    if module_name and module_name.strip():
        stmt = stmt.where(AccessLogArchive.module_name == module_name.strip().lower())
    if api_version and api_version.strip():
        stmt = stmt.where(AccessLogArchive.api_version == api_version.strip().lower())
    return db.scalars(stmt.order_by(AccessLogArchive.created_at.desc(), AccessLogArchive.id.desc()).limit(10000)).all()


def build_access_log_archive_csv_export_artifact(
    db: Session,
    *,
    current_user: User,
    module_name: str | None = None,
    api_version: str | None = None,
    days: int = 30,
    **_: Any,
) -> ExportArtifactResult:
    export_limit = resolve_export_limit(source_module="audit", entity_type="access_log_archive")
    rows = _access_log_archive_rows(db, module_name=module_name, api_version=api_version, days=days)
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["created_at", "user_email", "actor_name", "api_version", "module_name", "route", "method", "status_code", "duration_ms", "request_id"])
    for row in rows:
        writer.writerow([
            row.created_at.isoformat(),
            redact_export_value(row.user_email, field_name="user_email"),
            row.actor_name or "",
            row.api_version,
            row.module_name or "",
            row.route,
            row.method or "",
            row.status_code or "",
            row.duration_ms or "",
            row.request_id or "",
        ])
    return ExportArtifactResult(
        payload=buffer.getvalue().encode("utf-8"),
        filename="access_log_archive.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(rows),
        truncated=truncated,
        export_format="csv",
    )


def build_access_log_archive_xlsx_export_artifact(
    db: Session,
    *,
    current_user: User,
    module_name: str | None = None,
    api_version: str | None = None,
    days: int = 30,
    **_: Any,
) -> ExportArtifactResult:
    from openpyxl import Workbook

    export_limit = resolve_export_limit(source_module="audit", entity_type="access_log_archive")
    rows = _access_log_archive_rows(db, module_name=module_name, api_version=api_version, days=days)
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Access Log Archive"
    sheet.append(["created_at", "user_email", "actor_name", "api_version", "module_name", "route", "method", "status_code", "duration_ms", "request_id"])
    for row in rows:
        sheet.append([
            row.created_at.isoformat(),
            redact_export_value(row.user_email, field_name="user_email"),
            row.actor_name or "",
            row.api_version,
            row.module_name or "",
            row.route,
            row.method or "",
            row.status_code or "",
            row.duration_ms or "",
            row.request_id or "",
        ])
    buffer = BytesIO()
    workbook.save(buffer)
    return ExportArtifactResult(
        payload=buffer.getvalue(),
        filename="access_log_archive.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        row_count=len(rows),
        truncated=truncated,
        export_format="xlsx",
    )


def _audit_log_archive_rows(
    db: Session,
    *,
    entity_type: str | None,
    change_type: str | None,
    source_module: str | None,
    days: int,
):
    since = datetime.now(timezone.utc) - timedelta(days=max(days, 1))
    stmt = select(AuditLogArchive).where(AuditLogArchive.created_at >= since)
    if entity_type and entity_type.strip():
        stmt = stmt.where(AuditLogArchive.entity_type == entity_type.strip().lower())
    if change_type and change_type.strip():
        stmt = stmt.where(AuditLogArchive.change_type == change_type.strip().lower())
    if source_module and source_module.strip():
        stmt = stmt.where(AuditLogArchive.source_module == source_module.strip().lower())
    return db.scalars(stmt.order_by(AuditLogArchive.created_at.desc(), AuditLogArchive.id.desc()).limit(10000)).all()


def build_audit_log_archive_csv_export_artifact(
    db: Session,
    *,
    current_user: User,
    entity_type: str | None = None,
    change_type: str | None = None,
    source_module: str | None = None,
    days: int = 90,
    **_: Any,
) -> ExportArtifactResult:
    export_limit = resolve_export_limit(source_module="audit", entity_type="audit_log_archive")
    rows = _audit_log_archive_rows(db, entity_type=entity_type, change_type=change_type, source_module=source_module, days=days)
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["created_at", "actor_name", "user_email", "entity_type", "entity_id", "field_name", "change_type", "source_module", "is_sensitive_change", "before", "after"])
    for row in rows:
        writer.writerow([
            row.created_at.isoformat(),
            row.actor_name or "",
            row.user_email or "",
            row.entity_type or "",
            row.entity_id or "",
            row.field_name or "",
            row.change_type or "",
            row.source_module or "",
            "true" if row.is_sensitive_change else "false",
            redact_export_value(row.before_json, field_name=row.field_name or "before") if row.before_json is not None else "",
            redact_export_value(row.after_json, field_name=row.field_name or "after") if row.after_json is not None else "",
        ])
    return ExportArtifactResult(
        payload=buffer.getvalue().encode("utf-8"),
        filename="audit_log_archive.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(rows),
        truncated=truncated,
        export_format="csv",
    )


def build_audit_log_archive_xlsx_export_artifact(
    db: Session,
    *,
    current_user: User,
    entity_type: str | None = None,
    change_type: str | None = None,
    source_module: str | None = None,
    days: int = 90,
    **_: Any,
) -> ExportArtifactResult:
    from openpyxl import Workbook

    export_limit = resolve_export_limit(source_module="audit", entity_type="audit_log_archive")
    rows = _audit_log_archive_rows(db, entity_type=entity_type, change_type=change_type, source_module=source_module, days=days)
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Audit Log Archive"
    sheet.append(["created_at", "actor_name", "user_email", "entity_type", "entity_id", "field_name", "change_type", "source_module", "is_sensitive_change", "before", "after"])
    for row in rows:
        sheet.append([
            row.created_at.isoformat(),
            row.actor_name or "",
            redact_export_value(row.user_email, field_name="user_email"),
            row.entity_type or "",
            row.entity_id or "",
            row.field_name or "",
            row.change_type or "",
            row.source_module or "",
            bool(row.is_sensitive_change),
            redact_export_value(row.before_json, field_name=row.field_name or "before") if row.before_json is not None else "",
            redact_export_value(row.after_json, field_name=row.field_name or "after") if row.after_json is not None else "",
        ])
    buffer = BytesIO()
    workbook.save(buffer)
    return ExportArtifactResult(
        payload=buffer.getvalue(),
        filename="audit_log_archive.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        row_count=len(rows),
        truncated=truncated,
        export_format="xlsx",
    )
@router.get("/access-log/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_access_log_archive_csv(
    request: Request,
    module_name: str | None = Query(default=None),
    api_version: str | None = Query(default=None),
    days: int = Query(default=30, ge=1, le=3650),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("audit:export")),
) -> IntegrationSyncJobOut:
    job = enqueue_export_job(
        db,
        job_type="admin.access_log_archive.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "module_name": module_name,
            "api_version": api_version,
            "days": days,
            "export_format": "csv",
        },
        context_json={"filters": {"module_name": module_name, "api_version": api_version, "days": days}},
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="governance.access_log_archive.export_requested",
        entity_type="access_log_archive",
        source_module="governance",
        export_format="csv",
        filters={"module_name": module_name, "api_version": api_version, "days": days},
    )
    return serialize_export_job(job, request=request)


@router.get("/access-log/export.xlsx", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_access_log_archive_xlsx(
    request: Request,
    module_name: str | None = Query(default=None),
    api_version: str | None = Query(default=None),
    days: int = Query(default=30, ge=1, le=3650),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("audit:export")),
) -> IntegrationSyncJobOut:
    job = enqueue_export_job(
        db,
        job_type="admin.access_log_archive.xlsx",
        requested_by_user_id=current_user.id,
        payload_json={
            "module_name": module_name,
            "api_version": api_version,
            "days": days,
            "export_format": "xlsx",
        },
        context_json={"filters": {"module_name": module_name, "api_version": api_version, "days": days}},
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="governance.access_log_archive.export_requested",
        entity_type="access_log_archive",
        source_module="governance",
        export_format="xlsx",
        filters={"module_name": module_name, "api_version": api_version, "days": days},
    )
    return serialize_export_job(job, request=request)



def _audit_log_archive_query(
    db: Session,
    *,
    entity_type: str | None,
    change_type: str | None,
    source_module: str | None,
    days: int,
):
    since = datetime.now(timezone.utc) - timedelta(days=max(days, 1))
    stmt = select(AuditLogArchive).where(AuditLogArchive.created_at >= since)
    if entity_type and entity_type.strip():
        stmt = stmt.where(AuditLogArchive.entity_type == entity_type.strip().lower())
    if change_type and change_type.strip():
        stmt = stmt.where(AuditLogArchive.change_type == change_type.strip().lower())
    if source_module and source_module.strip():
        stmt = stmt.where(AuditLogArchive.source_module == source_module.strip().lower())
    return db.scalars(stmt.order_by(AuditLogArchive.created_at.desc(), AuditLogArchive.id.desc()).limit(10000)).all()


@router.get("/audit-log/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_audit_log_archive_csv(
    request: Request,
    entity_type: str | None = Query(default=None),
    change_type: str | None = Query(default=None),
    source_module: str | None = Query(default=None),
    days: int = Query(default=90, ge=1, le=3650),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("audit:export")),
) -> IntegrationSyncJobOut:
    job = enqueue_export_job(
        db,
        job_type="admin.audit_log_archive.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "entity_type": entity_type,
            "change_type": change_type,
            "source_module": source_module,
            "days": days,
            "export_format": "csv",
        },
        context_json={"filters": {"entity_type": entity_type, "change_type": change_type, "source_module": source_module, "days": days}},
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="governance.audit_log_archive.export_requested",
        entity_type="audit_log_archive",
        source_module="governance",
        export_format="csv",
        filters={"entity_type": entity_type, "change_type": change_type, "source_module": source_module, "days": days},
    )
    return serialize_export_job(job, request=request)


@router.get("/audit-log/export.xlsx", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_audit_log_archive_xlsx(
    request: Request,
    entity_type: str | None = Query(default=None),
    change_type: str | None = Query(default=None),
    source_module: str | None = Query(default=None),
    days: int = Query(default=90, ge=1, le=3650),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("audit:export")),
) -> IntegrationSyncJobOut:
    job = enqueue_export_job(
        db,
        job_type="admin.audit_log_archive.xlsx",
        requested_by_user_id=current_user.id,
        payload_json={
            "entity_type": entity_type,
            "change_type": change_type,
            "source_module": source_module,
            "days": days,
            "export_format": "xlsx",
        },
        context_json={"filters": {"entity_type": entity_type, "change_type": change_type, "source_module": source_module, "days": days}},
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="governance.audit_log_archive.export_requested",
        entity_type="audit_log_archive",
        source_module="governance",
        export_format="xlsx",
        filters={"entity_type": entity_type, "change_type": change_type, "source_module": source_module, "days": days},
    )
    return serialize_export_job(job, request=request)
