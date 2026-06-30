from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone

from sqlalchemy import inspect, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import settings
from t2c_data.core.json_utils import to_jsonable
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.settings import GovernanceSettingsSnapshot, get_governance_settings_snapshot
from t2c_data.models.catalog import DataOwner
from t2c_data.models.governance import GovernanceNotification

SEVERITY_PRIORITY = {"critical": 0, "high": 1, "medium": 2, "low": 3}
SEVERITY_LABELS = {
    "critical": "Crítica",
    "high": "Alta",
    "medium": "Média",
    "low": "Baixa",
}
STATUS_LABELS = {
    "active": "Ativa",
    "resolved": "Resolvida",
    "dismissed": "Dispensada",
}
MANAGED_PENDING_KEYS = {
    "no_owner",
    "no_description",
    "no_classification",
    "no_sla",
    "low_dq",
    "open_incident",
    "owner_review_due",
    "privacy_review_due",
    "certification_review_due",
    "no_pipeline_mapped",
    "stale_update",
    "operational_governance_risk",
    "critical_without_dq",
    "classification_high_usage",
    "dictionary_high_usage",
    "recurring_dq_failure_critical",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _notification_table_available(session: Session) -> bool:
    try:
        return inspect(session.get_bind()).has_table("governance_notifications", schema=settings.db_schema)
    except Exception:
        return False


def _notification_interval(item: dict[str, object], settings_snapshot: GovernanceSettingsSnapshot) -> timedelta:
    severity = str(item.get("severity") or "medium").lower()
    if severity == "critical":
        return timedelta(hours=max(settings_snapshot.governance_notification_critical_repeat_hours, 1))
    return timedelta(days=max(settings_snapshot.governance_notification_repeat_days, 1))


def _notification_message(item: dict[str, object]) -> str:
    table_fqn = str(item.get("table_fqn") or item.get("owner_name") or "Ativo")
    description = str(item.get("description") or "").strip()
    context_value = str(item.get("context_value") or "").strip()
    parts = [table_fqn]
    if description:
        parts.append(description)
    if context_value:
        parts.append(f"Contexto: {context_value}")
    return " · ".join(parts)


def _inactive_owner_notification_items(session: Session, *, now: datetime) -> list[dict[str, object]]:
    owners = session.scalars(select(DataOwner).order_by(DataOwner.name)).all()
    profiles = load_table_profiles(session, now, current_user=None)
    asset_counts = Counter(int(profile.data_owner_id) for profile in profiles if profile.data_owner_id is not None)
    items: list[dict[str, object]] = []
    for owner in owners:
        assets = int(asset_counts.get(owner.id, 0))
        if owner.is_active or assets <= 0:
            continue
        items.append(
            {
                "key": "inactive_owner_with_assets",
                "title": "Owner inativo com ativos",
                "description": "O responsável está inativo e precisa de reatribuição formal.",
                "severity": "high" if assets < 5 else "critical",
                "origin": "governance",
                "status": "open",
                "action_label": "Reatribuir owner",
                "action_href": "/data-owners",
                "detected_at": now,
                "due_at": None,
                "sla_days": None,
                "context_value": f"{assets} ativo(s) sob responsabilidade",
                "table_id": None,
                "table_name": None,
                "table_fqn": None,
                "datasource_name": None,
                "database_name": None,
                "schema_name": None,
                "owner_name": owner.name,
                "data_owner_id": owner.id,
                "owner_status": "inactive",
                "owner_assets_count": assets,
            }
        )
    return items


def _dedupe_key(item: dict[str, object]) -> str:
    entity_id = item.get("table_id") or item.get("data_owner_id") or "global"
    return f"{item.get('key') or 'pending'}:{entity_id}"


def _notification_context(item: dict[str, object]) -> dict[str, object]:
    governance_score = dict(item.get("governance_score") or {})
    links = dict(item.get("links") or {})
    return {
        "pending_key": item.get("key"),
        "severity_label": item.get("severity_label"),
        "origin": item.get("origin"),
        "origin_label": item.get("origin_label"),
        "status": item.get("status"),
        "status_label": item.get("status_label"),
        "governance_score": governance_score.get("score"),
        "governance_label": governance_score.get("label"),
        "action_label": item.get("action_label"),
        "action_href": item.get("action_href"),
        "links": links,
        "sla_days": item.get("sla_days"),
        "due_at": item.get("due_at"),
        "aging_days": item.get("aging_days"),
        "trust_score": item.get("trust_score"),
        "trust_label": item.get("trust_label"),
        "trust_tone": item.get("trust_tone"),
        "risk_score": item.get("risk_score"),
        "risk_label": item.get("risk_label"),
        "risk_tone": item.get("risk_tone"),
        "risk_reason": item.get("risk_reason"),
        "risk_components": item.get("risk_components"),
        "data_owner_id": item.get("data_owner_id"),
        "owner_status": item.get("owner_status"),
        "owner_assets_count": item.get("owner_assets_count"),
    }


def _is_due(notification: GovernanceNotification, now: datetime) -> bool:
    next_send_at = _normalize_dt(notification.next_send_at)
    return notification.status == "active" and (next_send_at is None or next_send_at <= now)


def _serialize_notification(notification: GovernanceNotification, *, now: datetime) -> dict[str, object]:
    return {
        "id": notification.id,
        "dedupe_key": notification.dedupe_key,
        "rule_key": notification.rule_key,
        "channel": notification.channel,
        "status": notification.status,
        "status_label": STATUS_LABELS.get(notification.status, notification.status),
        "severity": notification.severity,
        "severity_label": SEVERITY_LABELS.get(notification.severity, notification.severity),
        "origin": notification.origin,
        "title": notification.title,
        "message": notification.message,
        "entity_type": notification.entity_type,
        "table_id": notification.table_id,
        "table_name": getattr(notification.table, "name", None),
        "table_fqn": getattr(notification.table, "fqn", None),
        "owner_name": getattr(notification.data_owner, "name", None),
        "target_href": notification.target_href,
        "context": dict(notification.context_json or {}),
        "first_detected_at": notification.first_detected_at.isoformat() if notification.first_detected_at else None,
        "last_detected_at": notification.last_detected_at.isoformat() if notification.last_detected_at else None,
        "last_sent_at": notification.last_sent_at.isoformat() if notification.last_sent_at else None,
        "next_send_at": notification.next_send_at.isoformat() if notification.next_send_at else None,
        "resolved_at": notification.resolved_at.isoformat() if notification.resolved_at else None,
        "send_count": int(notification.send_count or 0),
        "last_delivery_status": notification.last_delivery_status,
        "last_delivery_error": notification.last_delivery_error,
        "is_due": _is_due(notification, now),
    }


def refresh_governance_notifications(session: Session) -> dict[str, object]:
    from t2c_data.features.governance.queries import get_governance_pending_center

    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    if not _notification_table_available(session):
        return {
            "enabled": settings_snapshot.governance_notifications_enabled,
            "generated_at": now.isoformat(),
            "candidates": 0,
            "created": 0,
            "updated": 0,
            "requeued": 0,
            "resolved": 0,
            "active_total": 0,
            "warning": "governance_notifications table unavailable",
        }
    if not settings_snapshot.governance_notifications_enabled:
        return {
            "enabled": False,
            "generated_at": now.isoformat(),
            "candidates": 0,
            "created": 0,
            "updated": 0,
            "requeued": 0,
            "resolved": 0,
            "active_total": _active_notification_total(session),
        }

    pending_payload = get_governance_pending_center(
        session,
        page=1,
        page_size=5000,
        current_user=None,
    )
    candidate_items = [
        item
        for item in pending_payload["items"]
        if str(item.get("key") or "") in MANAGED_PENDING_KEYS and str(item.get("status") or "") == "open"
    ]
    candidate_items.extend(_inactive_owner_notification_items(session, now=now))
    existing = {
        notification.dedupe_key: notification
        for notification in session.scalars(select(GovernanceNotification)).all()
    }
    active_keys: set[str] = set()
    created = 0
    updated = 0
    requeued = 0

    for item in candidate_items:
        dedupe_key = _dedupe_key(item)
        active_keys.add(dedupe_key)
        notification = existing.get(dedupe_key)
        interval = _notification_interval(item, settings_snapshot)
        target_href = str(item.get("action_href") or dict(item.get("links") or {}).get("explorer") or "/governance/pending-center")
        entity_type = str(item.get("entity_type") or ("data_owner" if item.get("data_owner_id") else "table"))
        table_id_value = item.get("table_id")
        table_id = int(table_id_value) if table_id_value is not None else None
        if notification is None:
            notification = GovernanceNotification(
                dedupe_key=dedupe_key,
                rule_key=str(item.get("key") or "pending"),
                channel="in_app",
                status="active",
                severity=str(item.get("severity") or "medium"),
                origin=str(item.get("origin") or "governance"),
                title=str(item.get("title") or "Pendência de governança"),
                message=_notification_message(item),
                entity_type=entity_type,
                table_id=table_id,
                data_owner_id=item.get("data_owner_id"),
                target_href=target_href,
                context_json=to_jsonable(_notification_context(item)),
                first_detected_at=now,
                last_detected_at=now,
                last_sent_at=now,
                next_send_at=now + interval,
                send_count=1,
                last_delivery_status="active",
            )
            session.add(notification)
            created += 1
            continue

        notification.status = "active"
        notification.severity = str(item.get("severity") or notification.severity)
        notification.origin = str(item.get("origin") or notification.origin)
        notification.title = str(item.get("title") or notification.title)
        notification.message = _notification_message(item)
        notification.entity_type = entity_type
        notification.table_id = table_id
        notification.data_owner_id = item.get("data_owner_id")
        notification.target_href = target_href
        notification.context_json = to_jsonable(_notification_context(item))
        notification.last_detected_at = now
        notification.resolved_at = None
        notification.resolved_reason = None
        if notification.last_sent_at is None:
            notification.last_sent_at = now
            notification.next_send_at = now + interval
            notification.send_count = max(int(notification.send_count or 0), 1)
            notification.last_delivery_status = "active"
            requeued += 1
        elif notification.next_send_at is None or notification.next_send_at <= now:
            notification.last_sent_at = now
            notification.next_send_at = now + interval
            notification.send_count = int(notification.send_count or 0) + 1
            notification.last_delivery_status = "requeued"
            requeued += 1
        else:
            notification.last_delivery_status = "active"
            updated += 1

    resolved = 0
    for dedupe_key, notification in existing.items():
        if dedupe_key in active_keys:
            continue
        if notification.status != "active":
            continue
        notification.status = "resolved"
        notification.resolved_at = now
        notification.resolved_reason = "condition_cleared"
        notification.last_delivery_status = "resolved"
        resolved += 1

    session.flush()
    return {
        "enabled": True,
        "generated_at": now.isoformat(),
        "candidates": len(candidate_items),
        "created": created,
        "updated": updated,
        "requeued": requeued,
        "resolved": resolved,
        "active_total": _active_notification_total(session),
    }


def _active_notification_total(session: Session) -> int:
    return len(
        session.scalars(
            select(GovernanceNotification.id).where(GovernanceNotification.status == "active")
        ).all()
    )


def get_governance_notification_summary(session: Session) -> dict[str, object]:
    now = _now()
    settings_snapshot = get_governance_settings_snapshot(session)
    if not _notification_table_available(session):
        return {
            "generated_at": now.isoformat(),
            "enabled": settings_snapshot.governance_notifications_enabled,
            "repeat_days": settings_snapshot.governance_notification_repeat_days,
            "critical_repeat_hours": settings_snapshot.governance_notification_critical_repeat_hours,
            "active_total": 0,
            "due_now_total": 0,
            "critical_total": 0,
            "review_total": 0,
            "operational_total": 0,
            "quality_total": 0,
            "incident_total": 0,
            "top_items": [],
        }
    notifications = session.scalars(
        select(GovernanceNotification)
        .options(
            selectinload(GovernanceNotification.table),
            selectinload(GovernanceNotification.data_owner),
        )
    ).all()
    active = [item for item in notifications if item.status == "active"]
    due_now = [item for item in active if _is_due(item, now)]
    review_related = [
        item
        for item in active
        if item.rule_key in {"owner_review_due", "privacy_review_due", "certification_review_due", "inactive_owner_with_assets"}
    ]
    operational = [item for item in active if item.origin == "operations"]
    quality = [item for item in active if item.origin == "quality"]
    incidents = [item for item in active if item.origin == "incidents"]
    top_items = sorted(
        active,
        key=lambda item: (
            SEVERITY_PRIORITY.get(item.severity, 99),
            0 if _is_due(item, now) else 1,
            _normalize_dt(item.next_send_at) or now,
            _normalize_dt(item.last_detected_at) or now,
        ),
    )[:8]
    return {
        "generated_at": now.isoformat(),
        "enabled": settings_snapshot.governance_notifications_enabled,
        "repeat_days": settings_snapshot.governance_notification_repeat_days,
        "critical_repeat_hours": settings_snapshot.governance_notification_critical_repeat_hours,
        "active_total": len(active),
        "due_now_total": len(due_now),
        "critical_total": sum(1 for item in active if item.severity == "critical"),
        "review_total": len(review_related),
        "operational_total": len(operational),
        "quality_total": len(quality),
        "incident_total": len(incidents),
        "top_items": [_serialize_notification(item, now=now) for item in top_items],
    }


def get_governance_notifications(
    session: Session,
    *,
    status_filter: str = "active",
    limit: int = 50,
) -> dict[str, object]:
    now = _now()
    if not _notification_table_available(session):
        return {
            "generated_at": now.isoformat(),
            "status": status_filter,
            "total": 0,
            "items": [],
        }
    stmt = (
        select(GovernanceNotification)
        .options(
            selectinload(GovernanceNotification.table),
            selectinload(GovernanceNotification.data_owner),
        )
        .order_by(
            GovernanceNotification.status.asc(),
            GovernanceNotification.next_send_at.asc().nullsfirst(),
            GovernanceNotification.last_detected_at.desc(),
            GovernanceNotification.id.desc(),
        )
        .limit(max(limit, 1))
    )
    if status_filter and status_filter != "all":
        stmt = stmt.where(GovernanceNotification.status == status_filter)
    notifications = session.scalars(stmt).all()
    return {
        "generated_at": now.isoformat(),
        "status": status_filter,
        "total": len(notifications),
        "items": [_serialize_notification(item, now=now) for item in notifications],
    }
