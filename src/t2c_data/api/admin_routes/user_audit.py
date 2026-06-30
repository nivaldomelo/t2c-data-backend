from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import csv
import io

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission
from t2c_data.core.network import get_request_client_ip
from t2c_data.models.audit import AccessLog, AuditLog
from t2c_data.models.auth import User, UserAccessEvent, UserSession
from t2c_data.schemas.pagination import PageOut
from t2c_data.schemas.user_audit import (
    UserAuditAccessEventOut,
    UserAuditChangeEventOut,
    UserAuditSessionOut,
    UserAuditSummaryCountOut,
    UserAuditSummaryOut,
)
from t2c_data.services.audit import request_audit_kwargs, write_access_log_sync, write_audit_log_sync

router = APIRouter()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_q(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text.lower() or None


def _session_status(session: UserSession, now: datetime | None = None) -> str:
    current = now or _now()
    if session.ended_at is not None:
        return "encerrada"
    if session.revoked_at is not None:
        return "revogada"
    expires_at = session.expires_at if session.expires_at.tzinfo else session.expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= current:
        return "expirada"
    return "em_andamento"


def _session_duration_seconds(session: UserSession, now: datetime | None = None) -> int | None:
    started = session.started_at or session.created_at
    if started is None:
        return None
    current = now or _now()
    if session.ended_at is not None:
        end = session.ended_at
    elif session.revoked_at is not None:
        end = session.revoked_at
    else:
        end = min(session.last_seen_at or started, session.expires_at or current)
        if end < started:
            end = started
    return max(int((end - started).total_seconds()), 0)


def _paginate(query, count_query, *, page: int, page_size: int, db: Session):
    total = db.scalar(count_query) or 0
    rows = db.execute(query.offset((page - 1) * page_size).limit(page_size)).all()
    return rows, total


def _access_event_filters(
    *,
    since: datetime | None = None,
    q: str | None = None,
    user_id: int | None = None,
    event_type: str | None = None,
    page_key: str | None = None,
    resource_type: str | None = None,
    datasource_id: int | None = None,
    schema_name: str | None = None,
    table_id: int | None = None,
    action: str | None = None,
    sensitivity_level: str | None = None,
    sensitive_only: bool = False,
    export_only: bool = False,
):
    conditions = []
    if since is not None:
        conditions.append(UserAccessEvent.created_at >= since)
    normalized_q = _normalize_q(q)
    if user_id is not None:
        conditions.append(UserAccessEvent.user_id == user_id)
    if event_type:
        conditions.append(UserAccessEvent.event_type == event_type)
    if page_key:
        conditions.append(UserAccessEvent.page_key == page_key)
    if resource_type:
        conditions.append(UserAccessEvent.resource_type == resource_type)
    if datasource_id is not None:
        conditions.append(UserAccessEvent.datasource_id == datasource_id)
    if schema_name:
        conditions.append(UserAccessEvent.schema_name == schema_name)
    if table_id is not None:
        conditions.append(UserAccessEvent.table_id == table_id)
    if action:
        conditions.append(UserAccessEvent.action == action)
    if sensitivity_level:
        conditions.append(UserAccessEvent.sensitivity_level == sensitivity_level)
    if sensitive_only:
        conditions.append(or_(UserAccessEvent.has_sensitive_data.is_(True), UserAccessEvent.has_personal_data.is_(True)))
    if export_only:
        conditions.append(UserAccessEvent.event_type == "export")
    if normalized_q:
        pattern = f"%{normalized_q}%"
        conditions.append(
            or_(
                func.lower(func.coalesce(User.name, "")).like(pattern),
                func.lower(func.coalesce(User.full_name, "")).like(pattern),
                func.lower(func.coalesce(User.email, "")).like(pattern),
                func.lower(func.coalesce(UserAccessEvent.route_path, "")).like(pattern),
                func.lower(func.coalesce(UserAccessEvent.resource_fqn, "")).like(pattern),
                func.lower(func.coalesce(UserAccessEvent.table_name, "")).like(pattern),
                func.lower(func.coalesce(UserAccessEvent.column_name, "")).like(pattern),
            )
        )
    return conditions


def _change_filters(*, since: datetime | None = None, q: str | None = None, user_id: int | None = None, module: str | None = None, action: str | None = None, sensitive_only: bool = False):
    conditions = []
    if since is not None:
        conditions.append(AuditLog.created_at >= since)
    normalized_q = _normalize_q(q)
    if user_id is not None:
        conditions.append(AuditLog.user_id == user_id)
    if module:
        conditions.append(AuditLog.source_module == module)
    if action:
        conditions.append(AuditLog.action == action)
    if sensitive_only:
        conditions.append(AuditLog.is_sensitive_change.is_(True))
    if normalized_q:
        pattern = f"%{normalized_q}%"
        conditions.append(
            or_(
                func.lower(func.coalesce(AuditLog.actor_name, "")).like(pattern),
                func.lower(func.coalesce(AuditLog.user_email, "")).like(pattern),
                func.lower(func.coalesce(AuditLog.action, "")).like(pattern),
                func.lower(func.coalesce(AuditLog.entity_type, "")).like(pattern),
                func.lower(func.coalesce(AuditLog.entity_id, "")).like(pattern),
                func.lower(func.coalesce(AuditLog.source_module, "")).like(pattern),
            )
        )
    return conditions


def _session_filters(*, since: datetime | None = None, q: str | None = None, user_id: int | None = None, status_value: str | None = None, auth_method: str | None = None):
    conditions = []
    if since is not None:
        conditions.append(UserSession.started_at >= since)
    normalized_q = _normalize_q(q)
    if user_id is not None:
        conditions.append(UserSession.user_id == user_id)
    if auth_method:
        conditions.append(UserSession.auth_method == auth_method)
    if status_value:
        normalized_status = status_value.strip().lower()
        if normalized_status == "em_andamento":
            conditions.append(UserSession.ended_at.is_(None))
            conditions.append(UserSession.revoked_at.is_(None))
        elif normalized_status == "encerrada":
            conditions.append(UserSession.ended_at.is_not(None))
        elif normalized_status == "revogada":
            conditions.append(UserSession.revoked_at.is_not(None))
        elif normalized_status == "expirada":
            conditions.append(UserSession.ended_at.is_(None))
            conditions.append(UserSession.revoked_at.is_(None))
            conditions.append(UserSession.expires_at <= _now())
    if normalized_q:
        pattern = f"%{normalized_q}%"
        conditions.append(
            or_(
                func.lower(func.coalesce(User.name, "")).like(pattern),
                func.lower(func.coalesce(User.full_name, "")).like(pattern),
                func.lower(func.coalesce(User.email, "")).like(pattern),
                func.lower(func.coalesce(UserSession.jti, "")).like(pattern),
                func.lower(func.coalesce(UserSession.ip_address, "")).like(pattern),
            )
        )
    return conditions


def _session_out(session: UserSession, user_name: str | None, user_email: str | None) -> UserAuditSessionOut:
    now = _now()
    return UserAuditSessionOut(
        id=session.id,
        user_id=session.user_id,
        user_name=user_name,
        user_email=user_email,
        session_jti=session.jti,
        started_at=session.started_at,
        last_seen_at=session.last_seen_at,
        ended_at=session.ended_at,
        duration_seconds=session.duration_seconds if session.duration_seconds is not None else _session_duration_seconds(session, now),
        end_reason=session.end_reason,
        status=_session_status(session, now),
        ip_address=session.ip_address,
        user_agent=session.user_agent,
        device_type=session.device_type,
        browser=session.browser,
        os=session.os,
        country=session.country,
        city=session.city,
        auth_method=session.auth_method,
        mfa_used=bool(session.mfa_used),
        success=bool(session.success),
        failure_reason=session.failure_reason,
    )


def _access_event_out(event: UserAccessEvent, user_name: str | None, user_email: str | None, session_jti: str | None) -> UserAuditAccessEventOut:
    return UserAuditAccessEventOut(
        id=event.id,
        created_at=event.created_at,
        user_id=event.user_id,
        user_name=user_name,
        user_email=user_email,
        session_id=event.session_id,
        session_jti=session_jti,
        event_type=event.event_type,
        page_key=event.page_key,
        route_path=event.route_path,
        http_method=event.http_method,
        resource_type=event.resource_type,
        resource_id=event.resource_id,
        resource_fqn=event.resource_fqn,
        datasource_id=event.datasource_id,
        schema_name=event.schema_name,
        table_id=event.table_id,
        table_name=event.table_name,
        column_id=event.column_id,
        column_name=event.column_name,
        action=event.action,
        sensitivity_level=event.sensitivity_level,
        has_personal_data=bool(event.has_personal_data),
        has_sensitive_data=bool(event.has_sensitive_data),
        privacy_classification=event.privacy_classification,
        metadata_json=event.metadata_json,
        ip_address=str(event.ip_address) if event.ip_address is not None else None,
        user_agent=event.user_agent,
        request_id=event.request_id,
        correlation_id=event.correlation_id,
    )


def _change_event_out(log: AuditLog) -> UserAuditChangeEventOut:
    return UserAuditChangeEventOut(
        id=log.id,
        created_at=log.created_at,
        user_id=log.user_id,
        actor_name=log.actor_name,
        user_email=log.user_email,
        action=log.action,
        entity_type=log.entity_type,
        entity_id=log.entity_id,
        parent_entity_type=log.parent_entity_type,
        parent_entity_id=log.parent_entity_id,
        change_set_id=log.change_set_id,
        change_type=log.change_type,
        field_name=log.field_name,
        source_module=log.source_module,
        is_sensitive_change=bool(log.is_sensitive_change),
        sensitive_category=log.sensitive_category,
        route=log.route,
        method=log.method,
        status_code=log.status_code,
        request_id=log.request_id,
        before_json=log.before_json,
        after_json=log.after_json,
        metadata_json=log.metadata_json,
    )


def _summary_counts_from_rows(rows, *, label_field: int = 0, value_field: int = 1) -> list[UserAuditSummaryCountOut]:
    return [UserAuditSummaryCountOut(label=str(row[label_field] or "—"), value=int(row[value_field] or 0)) for row in rows]


def _build_summary(db: Session, *, period_days: int) -> UserAuditSummaryOut:
    now = _now()
    since = now - timedelta(days=max(period_days, 1))
    since_24h = now - timedelta(days=1)

    users_active_today = db.scalar(
        select(func.count(func.distinct(UserSession.user_id))).where(UserSession.started_at >= since_24h)
    ) or 0
    logins_last_24h = db.scalar(
        select(func.count(UserSession.id)).where(UserSession.started_at >= since_24h, UserSession.success.is_(True))
    ) or 0
    open_sessions = db.scalar(
        select(func.count(UserSession.id)).where(UserSession.ended_at.is_(None), UserSession.revoked_at.is_(None), UserSession.expires_at > now)
    ) or 0
    avg_session_seconds = db.scalar(
        select(func.avg(func.coalesce(UserSession.duration_seconds, 0))).where(UserSession.started_at >= since)
    )
    page_views_last_24h = db.scalar(
        select(func.count(UserAccessEvent.id)).where(UserAccessEvent.created_at >= since_24h, UserAccessEvent.event_type == "page_view")
    ) or 0
    asset_views_last_24h = db.scalar(
        select(func.count(UserAccessEvent.id)).where(
            UserAccessEvent.created_at >= since_24h,
            UserAccessEvent.event_type.in_(["asset_view", "sensitive_view"]),
        )
    ) or 0
    changes_last_24h = db.scalar(select(func.count(AuditLog.id)).where(AuditLog.created_at >= since_24h)) or 0
    exports_last_24h = db.scalar(
        select(func.count(AuditLog.id)).where(AuditLog.created_at >= since_24h, func.lower(AuditLog.action).like("%export%"))
    ) or 0
    sensitive_access_last_24h = db.scalar(
        select(func.count(UserAccessEvent.id)).where(
            UserAccessEvent.created_at >= since_24h,
            or_(UserAccessEvent.has_sensitive_data.is_(True), UserAccessEvent.has_personal_data.is_(True)),
        )
    ) or 0
    denied_requests_last_24h = db.scalar(
        select(func.count(AccessLog.id)).where(
            AccessLog.created_at >= since_24h,
            AccessLog.status_code.in_([401, 403]),
        )
    ) or 0

    top_pages_rows = db.execute(
        select(UserAccessEvent.page_key, func.count(UserAccessEvent.id).label("total"))
        .where(UserAccessEvent.created_at >= since, UserAccessEvent.event_type == "page_view")
        .group_by(UserAccessEvent.page_key)
        .order_by(desc("total"))
        .limit(5)
    ).all()
    top_assets_rows = db.execute(
        select(
            func.coalesce(UserAccessEvent.resource_fqn, UserAccessEvent.table_name, UserAccessEvent.column_name, UserAccessEvent.route_path),
            func.count(UserAccessEvent.id).label("total"),
        )
        .where(UserAccessEvent.created_at >= since, UserAccessEvent.event_type.in_(["asset_view", "sensitive_view"]))
        .group_by(
            func.coalesce(UserAccessEvent.resource_fqn, UserAccessEvent.table_name, UserAccessEvent.column_name, UserAccessEvent.route_path)
        )
        .order_by(desc("total"))
        .limit(5)
    ).all()
    top_users_rows = db.execute(
        select(func.coalesce(User.name, User.full_name, User.email), func.count(UserSession.id).label("total"))
        .select_from(UserSession)
        .join(User, User.id == UserSession.user_id, isouter=True)
        .where(UserSession.started_at >= since)
        .group_by(func.coalesce(User.name, User.full_name, User.email))
        .order_by(desc("total"))
        .limit(5)
    ).all()

    return UserAuditSummaryOut(
        generated_at=now,
        period_days=period_days,
        users_active_today=int(users_active_today or 0),
        logins_last_24h=int(logins_last_24h or 0),
        open_sessions=int(open_sessions or 0),
        avg_session_seconds=int(avg_session_seconds) if avg_session_seconds is not None else None,
        page_views_last_24h=int(page_views_last_24h or 0),
        asset_views_last_24h=int(asset_views_last_24h or 0),
        changes_last_24h=int(changes_last_24h or 0),
        exports_last_24h=int(exports_last_24h or 0),
        sensitive_access_last_24h=int(sensitive_access_last_24h or 0),
        denied_requests_last_24h=int(denied_requests_last_24h or 0),
        top_pages=_summary_counts_from_rows(top_pages_rows),
        top_assets=_summary_counts_from_rows(top_assets_rows),
        top_users=_summary_counts_from_rows(top_users_rows),
    )


@router.get("/user-audit/summary", response_model=UserAuditSummaryOut)
def user_audit_summary(
    period_days: int = Query(default=30, ge=1, le=3650),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("admin.user_audit.read")),
) -> UserAuditSummaryOut:
    return _build_summary(db, period_days=period_days)


@router.get("/user-audit/sessions", response_model=PageOut[UserAuditSessionOut])
def user_audit_sessions(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    period_days: int = Query(default=30, ge=1, le=3650),
    user_id: int | None = Query(default=None, ge=1),
    q: str | None = Query(default=None),
    status_value: str | None = Query(default=None, alias="status"),
    auth_method: str | None = Query(default=None),
    _: User = Depends(require_permission("admin.user_audit.read")),
) -> PageOut[UserAuditSessionOut]:
    since = _now() - timedelta(days=max(period_days, 1))
    filters = _session_filters(since=since, q=q, user_id=user_id, status_value=status_value, auth_method=auth_method)
    base = (
        select(UserSession, User.name, User.full_name, User.email)
        .select_from(UserSession)
        .join(User, User.id == UserSession.user_id, isouter=True)
        .where(*filters)
        .order_by(UserSession.started_at.desc(), UserSession.id.desc())
    )
    count_query = select(func.count()).select_from(UserSession).join(User, User.id == UserSession.user_id, isouter=True).where(*filters)
    rows, total = _paginate(base, count_query, page=page, page_size=page_size, db=db)
    items = [
        _session_out(session, name or full_name, email)
        for session, name, full_name, email in rows
    ]
    return PageOut[UserAuditSessionOut](
        page=page,
        page_size=page_size,
        total=total,
        total_pages=(total + page_size - 1) // page_size if total else 0,
        has_more=(page * page_size) < total,
        items=items,
    )


@router.get("/user-audit/events", response_model=PageOut[UserAuditAccessEventOut])
def user_audit_events(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    period_days: int = Query(default=30, ge=1, le=3650),
    q: str | None = Query(default=None),
    user_id: int | None = Query(default=None, ge=1),
    event_type: str | None = Query(default=None),
    page_key: str | None = Query(default=None),
    resource_type: str | None = Query(default=None),
    datasource_id: int | None = Query(default=None, ge=1),
    schema_name: str | None = Query(default=None),
    table_id: int | None = Query(default=None, ge=1),
    action: str | None = Query(default=None),
    sensitivity_level: str | None = Query(default=None),
    sensitive_only: bool = Query(default=False),
    export_only: bool = Query(default=False),
    _: User = Depends(require_permission("admin.user_audit.read")),
) -> PageOut[UserAuditAccessEventOut]:
    since = _now() - timedelta(days=max(period_days, 1))
    return _build_access_events_page(
        db,
        page=page,
        page_size=page_size,
        since=since,
        q=q,
        user_id=user_id,
        event_type=event_type,
        page_key=page_key,
        resource_type=resource_type,
        datasource_id=datasource_id,
        schema_name=schema_name,
        table_id=table_id,
        action=action,
        sensitivity_level=sensitivity_level,
        sensitive_only=sensitive_only,
        export_only=export_only,
    )


def _build_access_events_page(
    db: Session,
    *,
    page: int,
    page_size: int,
    since: datetime | None = None,
    q: str | None = None,
    user_id: int | None = None,
    event_type: str | None = None,
    page_key: str | None = None,
    resource_type: str | None = None,
    datasource_id: int | None = None,
    schema_name: str | None = None,
    table_id: int | None = None,
    action: str | None = None,
    sensitivity_level: str | None = None,
    sensitive_only: bool = False,
    export_only: bool = False,
) -> PageOut[UserAuditAccessEventOut]:
    filters = _access_event_filters(
        since=since,
        q=q,
        user_id=user_id,
        event_type=event_type,
        page_key=page_key,
        resource_type=resource_type,
        datasource_id=datasource_id,
        schema_name=schema_name,
        table_id=table_id,
        action=action,
        sensitivity_level=sensitivity_level,
        sensitive_only=sensitive_only,
        export_only=export_only,
    )
    base = (
        select(UserAccessEvent, User.name, User.full_name, User.email, UserSession.jti)
        .select_from(UserAccessEvent)
        .join(User, User.id == UserAccessEvent.user_id, isouter=True)
        .join(UserSession, UserSession.id == UserAccessEvent.session_id, isouter=True)
        .where(*filters)
        .order_by(UserAccessEvent.created_at.desc(), UserAccessEvent.id.desc())
    )
    count_query = (
        select(func.count())
        .select_from(UserAccessEvent)
        .join(User, User.id == UserAccessEvent.user_id, isouter=True)
        .join(UserSession, UserSession.id == UserAccessEvent.session_id, isouter=True)
        .where(*filters)
    )
    rows, total = _paginate(base, count_query, page=page, page_size=page_size, db=db)
    items = [
        _access_event_out(event, name or full_name, email, session_jti)
        for event, name, full_name, email, session_jti in rows
    ]
    return PageOut[UserAuditAccessEventOut](
        page=page,
        page_size=page_size,
        total=total,
        total_pages=(total + page_size - 1) // page_size if total else 0,
        has_more=(page * page_size) < total,
        items=items,
    )


@router.get("/user-audit/changes", response_model=PageOut[UserAuditChangeEventOut])
def user_audit_changes(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    period_days: int = Query(default=30, ge=1, le=3650),
    q: str | None = Query(default=None),
    user_id: int | None = Query(default=None, ge=1),
    module: str | None = Query(default=None),
    action: str | None = Query(default=None),
    sensitive_only: bool = Query(default=False),
    _: User = Depends(require_permission("admin.user_audit.change_read")),
) -> PageOut[UserAuditChangeEventOut]:
    since = _now() - timedelta(days=max(period_days, 1))
    filters = _change_filters(since=since, q=q, user_id=user_id, module=module, action=action, sensitive_only=sensitive_only)
    base = (
        select(AuditLog)
        .where(*filters)
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
    )
    count_query = select(func.count()).select_from(AuditLog).where(*filters)
    rows, total = _paginate(base, count_query, page=page, page_size=page_size, db=db)
    items = [_change_event_out(row[0]) for row in rows]
    return PageOut[UserAuditChangeEventOut](
        page=page,
        page_size=page_size,
        total=total,
        total_pages=(total + page_size - 1) // page_size if total else 0,
        has_more=(page * page_size) < total,
        items=items,
    )


@router.get("/user-audit/sensitive-access", response_model=PageOut[UserAuditAccessEventOut])
def user_audit_sensitive_access(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    period_days: int = Query(default=30, ge=1, le=3650),
    q: str | None = Query(default=None),
    user_id: int | None = Query(default=None, ge=1),
    current_user: User = Depends(require_permission("admin.user_audit.sensitive_read")),
) -> PageOut[UserAuditAccessEventOut]:
    since = _now() - timedelta(days=max(period_days, 1))
    return _build_access_events_page(
        db=db,
        page=page,
        page_size=page_size,
        since=since,
        q=q,
        user_id=user_id,
        sensitive_only=True,
    )


@router.get("/user-audit/exports", response_model=PageOut[UserAuditChangeEventOut])
def user_audit_exports(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    period_days: int = Query(default=30, ge=1, le=3650),
    q: str | None = Query(default=None),
    user_id: int | None = Query(default=None, ge=1),
    _: User = Depends(require_permission("admin.user_audit.export")),
) -> PageOut[UserAuditChangeEventOut]:
    since = _now() - timedelta(days=max(period_days, 1))
    filters = _change_filters(since=since, q=q, user_id=user_id, action=None, module=None, sensitive_only=False)
    filters.append(func.lower(AuditLog.action).like("%export%"))
    base = select(AuditLog).where(*filters).order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
    count_query = select(func.count()).select_from(AuditLog).where(*filters)
    rows, total = _paginate(base, count_query, page=page, page_size=page_size, db=db)
    items = [_change_event_out(row[0]) for row in rows]
    return PageOut[UserAuditChangeEventOut](
        page=page,
        page_size=page_size,
        total=total,
        total_pages=(total + page_size - 1) // page_size if total else 0,
        has_more=(page * page_size) < total,
        items=items,
    )


@router.get("/user-audit/users/{user_id}/audit-summary", response_model=UserAuditSummaryOut)
def user_audit_user_summary(
    user_id: int,
    period_days: int = Query(default=30, ge=1, le=3650),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("admin.user_audit.read")),
) -> UserAuditSummaryOut:
    now = _now()
    since = now - timedelta(days=max(period_days, 1))
    since_24h = now - timedelta(days=1)
    if db.scalar(select(User.id).where(User.id == user_id)) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    summary = _build_summary(db, period_days=period_days)
    summary.users_active_today = int(db.scalar(select(func.count(func.distinct(UserSession.user_id))).where(UserSession.user_id == user_id, UserSession.started_at >= since_24h)) or 0)
    summary.logins_last_24h = int(db.scalar(select(func.count(UserSession.id)).where(UserSession.user_id == user_id, UserSession.started_at >= since_24h, UserSession.success.is_(True))) or 0)
    summary.open_sessions = int(db.scalar(select(func.count(UserSession.id)).where(UserSession.user_id == user_id, UserSession.ended_at.is_(None), UserSession.revoked_at.is_(None), UserSession.expires_at > now)) or 0)
    summary.page_views_last_24h = int(db.scalar(select(func.count(UserAccessEvent.id)).where(UserAccessEvent.user_id == user_id, UserAccessEvent.created_at >= since_24h, UserAccessEvent.event_type == "page_view")) or 0)
    summary.asset_views_last_24h = int(db.scalar(select(func.count(UserAccessEvent.id)).where(UserAccessEvent.user_id == user_id, UserAccessEvent.created_at >= since_24h, UserAccessEvent.event_type.in_(["asset_view", "sensitive_view"]))) or 0)
    summary.changes_last_24h = int(db.scalar(select(func.count(AuditLog.id)).where(AuditLog.user_id == user_id, AuditLog.created_at >= since_24h)) or 0)
    summary.exports_last_24h = int(db.scalar(select(func.count(AuditLog.id)).where(AuditLog.user_id == user_id, AuditLog.created_at >= since_24h, func.lower(AuditLog.action).like("%export%"))) or 0)
    summary.sensitive_access_last_24h = int(db.scalar(select(func.count(UserAccessEvent.id)).where(UserAccessEvent.user_id == user_id, UserAccessEvent.created_at >= since_24h, or_(UserAccessEvent.has_sensitive_data.is_(True), UserAccessEvent.has_personal_data.is_(True)))) or 0)
    summary.denied_requests_last_24h = int(db.scalar(select(func.count(AccessLog.id)).where(AccessLog.user_id == user_id, AccessLog.created_at >= since_24h, AccessLog.status_code.in_([401, 403]))) or 0)
    summary.top_pages = _summary_counts_from_rows(
        db.execute(
            select(UserAccessEvent.page_key, func.count(UserAccessEvent.id).label("total"))
            .where(UserAccessEvent.user_id == user_id, UserAccessEvent.created_at >= since, UserAccessEvent.event_type == "page_view")
            .group_by(UserAccessEvent.page_key)
            .order_by(desc("total"))
            .limit(5)
        ).all()
    )
    summary.top_assets = _summary_counts_from_rows(
        db.execute(
            select(
                func.coalesce(UserAccessEvent.resource_fqn, UserAccessEvent.table_name, UserAccessEvent.column_name, UserAccessEvent.route_path),
                func.count(UserAccessEvent.id).label("total"),
            )
            .where(UserAccessEvent.user_id == user_id, UserAccessEvent.created_at >= since, UserAccessEvent.event_type.in_(["asset_view", "sensitive_view"]))
            .group_by(
                func.coalesce(UserAccessEvent.resource_fqn, UserAccessEvent.table_name, UserAccessEvent.column_name, UserAccessEvent.route_path)
            )
            .order_by(desc("total"))
            .limit(5)
        ).all()
    )
    summary.top_users = _summary_counts_from_rows(
        db.execute(
            select(func.coalesce(User.name, User.full_name, User.email), func.count(UserSession.id).label("total"))
            .select_from(UserSession)
            .join(User, User.id == UserSession.user_id, isouter=True)
            .where(UserSession.user_id == user_id, UserSession.started_at >= since)
            .group_by(func.coalesce(User.name, User.full_name, User.email))
            .order_by(desc("total"))
            .limit(5)
        ).all()
    )
    return summary


@router.post("/user-audit/events/export.csv")
def user_audit_export_csv(
    request: Request,
    db: Session = Depends(get_db),
    period_days: int = Query(default=30, ge=1, le=3650),
    q: str | None = Query(default=None),
    user_id: int | None = Query(default=None, ge=1),
    event_type: str | None = Query(default=None),
    page_key: str | None = Query(default=None),
    resource_type: str | None = Query(default=None),
    datasource_id: int | None = Query(default=None, ge=1),
    schema_name: str | None = Query(default=None),
    table_id: int | None = Query(default=None, ge=1),
    action: str | None = Query(default=None),
    sensitivity_level: str | None = Query(default=None),
    sensitive_only: bool = Query(default=False),
    _: User = Depends(require_permission("admin.user_audit.export")),
) -> Response:
    export_limit = 1000
    since = _now() - timedelta(days=max(period_days, 1))
    filters = _access_event_filters(
        since=since,
        q=q,
        user_id=user_id,
        event_type=event_type,
        page_key=page_key,
        resource_type=resource_type,
        datasource_id=datasource_id,
        schema_name=schema_name,
        table_id=table_id,
        action=action,
        sensitivity_level=sensitivity_level,
        sensitive_only=sensitive_only,
    )
    # Fetch one extra row to detect (and signal) truncation without an extra COUNT query.
    rows = db.execute(
        select(UserAccessEvent, User.name, User.full_name, User.email)
        .select_from(UserAccessEvent)
        .join(User, User.id == UserAccessEvent.user_id, isouter=True)
        .where(*filters)
        .order_by(UserAccessEvent.created_at.desc(), UserAccessEvent.id.desc())
        .limit(export_limit + 1)
    ).all()
    truncated = len(rows) > export_limit
    rows = rows[:export_limit]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "created_at",
        "user_name",
        "user_email",
        "event_type",
        "page_key",
        "route_path",
        "resource_type",
        "resource_fqn",
        "datasource_id",
        "schema_name",
        "table_name",
        "column_name",
        "action",
        "sensitivity_level",
        "has_sensitive_data",
    ])
    for event, name, full_name, email in rows:
        writer.writerow([
            event.created_at.isoformat(),
            name or full_name or "",
            email or "",
            event.event_type,
            event.page_key or "",
            event.route_path or "",
            event.resource_type or "",
            event.resource_fqn or "",
            event.datasource_id or "",
            event.schema_name or "",
            event.table_name or "",
            event.column_name or "",
            event.action or "",
            event.sensitivity_level or "",
            "yes" if event.has_sensitive_data else "no",
        ])

    write_audit_log_sync(
        db,
        action="admin.user_audit.export",
        entity_type="user_audit",
        entity_id="events",
        metadata={"q": q, "user_id": user_id, "event_type": event_type, "page_key": page_key},
        **request_audit_kwargs(request, getattr(request.state, "current_user", None)),
    )
    audit_kwargs = request_audit_kwargs(request, getattr(request.state, "current_user", None))
    audit_kwargs.pop("route", None)
    audit_kwargs.pop("method", None)
    write_access_log_sync(
        db,
        route="/api/v1/admin/user-audit/events/export.csv",
        method="POST",
        status_code=200,
        metadata={"q": q, "user_id": user_id, "event_type": event_type, "page_key": page_key},
        **audit_kwargs,
    )
    db.commit()
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="user-audit-events.csv"',
            "X-Export-Row-Count": str(len(rows)),
            "X-Export-Truncated": "true" if truncated else "false",
            "Access-Control-Expose-Headers": "X-Export-Row-Count, X-Export-Truncated",
        },
    )
