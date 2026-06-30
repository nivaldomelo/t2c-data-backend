from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, insert, select
from sqlalchemy.orm import Session

from t2c_data.features.platform.retention_policy import RetentionPolicySnapshot, get_retention_policy_snapshot
from t2c_data.models.audit import AccessLog, AccessLogArchive, AuditLog, AuditLogArchive
from t2c_data.models.platform import PlatformUsageEvent
from t2c_data.models.search import SearchResultClick


def purge_operational_history(session: Session, *, retention_policy: RetentionPolicySnapshot | None = None) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    retention = retention_policy or get_retention_policy_snapshot(session)
    audit_cutoff = now - timedelta(days=retention.audit_log_retention_days)
    audit_archive_cutoff = now - timedelta(days=retention.audit_log_archive_retention_days)
    access_cutoff = now - timedelta(days=retention.access_log_retention_days)
    access_archive_cutoff = now - timedelta(days=retention.access_log_archive_retention_days)
    usage_cutoff = now - timedelta(days=retention.platform_usage_event_retention_days)
    click_cutoff = now - timedelta(days=retention.search_result_click_retention_days)

    access_archived_count = 0
    access_rows = session.scalars(
        select(AccessLog).where(AccessLog.created_at < access_cutoff).order_by(AccessLog.created_at.asc()).limit(10000)
    ).all()
    if access_rows:
        session.execute(
            insert(AccessLogArchive),
            [
                {
                    "id": row.id,
                    "created_at": row.created_at,
                    "user_id": row.user_id,
                    "actor_name": row.actor_name,
                    "user_email": row.user_email,
                    "ip": row.ip,
                    "user_agent": row.user_agent,
                    "route": row.route,
                    "method": row.method,
                    "status_code": row.status_code,
                    "request_id": row.request_id,
                    "api_version": row.api_version,
                    "module_name": row.module_name,
                    "duration_ms": row.duration_ms,
                    "metadata_json": row.metadata_json,
                }
                for row in access_rows
            ],
        )
        session.execute(delete(AccessLog).where(AccessLog.id.in_([row.id for row in access_rows])))
        access_archived_count = len(access_rows)

    audit_archived_count = 0
    audit_rows = session.scalars(
        select(AuditLog).where(AuditLog.created_at < audit_cutoff).order_by(AuditLog.created_at.asc()).limit(10000)
    ).all()
    if audit_rows:
        session.execute(
            insert(AuditLogArchive),
            [
                {
                    "id": row.id,
                    "created_at": row.created_at,
                    "user_id": row.user_id,
                    "actor_name": row.actor_name,
                    "user_email": row.user_email,
                    "ip": row.ip,
                    "user_agent": row.user_agent,
                    "action": row.action,
                    "entity_type": row.entity_type,
                    "entity_id": row.entity_id,
                    "parent_entity_type": row.parent_entity_type,
                    "parent_entity_id": row.parent_entity_id,
                    "change_set_id": row.change_set_id,
                    "change_type": row.change_type,
                    "field_name": row.field_name,
                    "source_module": row.source_module,
                    "is_sensitive_change": row.is_sensitive_change,
                    "sensitive_category": row.sensitive_category,
                    "route": row.route,
                    "method": row.method,
                    "status_code": row.status_code,
                    "request_id": row.request_id,
                    "before_json": row.before_json,
                    "after_json": row.after_json,
                    "metadata_json": row.metadata_json,
                }
                for row in audit_rows
            ],
        )
        session.execute(delete(AuditLog).where(AuditLog.id.in_([row.id for row in audit_rows])))
        audit_archived_count = len(audit_rows)

    access_archive_deleted = session.execute(
        delete(AccessLogArchive).where(AccessLogArchive.created_at < access_archive_cutoff)
    ).rowcount or 0

    audit_archive_deleted = session.execute(delete(AuditLogArchive).where(AuditLogArchive.created_at < audit_archive_cutoff)).rowcount or 0
    usage_deleted = session.execute(
        delete(PlatformUsageEvent).where(
            PlatformUsageEvent.created_at < usage_cutoff,
        )
    ).rowcount or 0
    click_deleted = session.execute(
        delete(SearchResultClick).where(
            SearchResultClick.created_at < click_cutoff,
        )
    ).rowcount or 0
    session.flush()
    return {
        "audit_archived": int(audit_archived_count),
        "audit_archive_deleted": int(audit_archive_deleted),
        "access_archived": int(access_archived_count),
        "access_archive_deleted": int(access_archive_deleted),
        "platform_usage_deleted": int(usage_deleted),
        "search_click_deleted": int(click_deleted),
    }
