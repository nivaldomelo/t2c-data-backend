from __future__ import annotations

import logging
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import case, func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.config import settings
from t2c_data.core.json_utils import to_jsonable
from t2c_data.models.auth import Role, User
from t2c_data.models.governance import GovernanceNotification
from t2c_data.models.notifications import UserInboxNotification, UserNotificationPreference

logger = logging.getLogger(__name__)

CATEGORY_LABELS = {
    "governance": "Governança",
    "stewardship": "Stewardship",
    "operations": "Operação",
    "data_quality": "Qualidade de dados",
}
CATEGORY_PREFERENCE_FIELDS = {
    "governance": "governance_enabled",
    "stewardship": "stewardship_enabled",
    "operations": "operational_enabled",
    "data_quality": "operational_enabled",
}
DIGEST_TIMEZONE = ZoneInfo("America/Sao_Paulo")
DIGEST_HOUR = 8


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _display_name(user: User | None) -> str | None:
    if user is None:
        return None
    return user.name or user.full_name or user.email


def _user_search_payload(user: User) -> dict[str, Any]:
    return {
        "id": user.id,
        "display_name": _display_name(user) or user.email,
        "email": user.email,
    }


def get_or_create_user_notification_preference(session: Session, user: User | int) -> UserNotificationPreference:
    user_id = int(user.id if isinstance(user, User) else user)
    preference = session.scalar(
        select(UserNotificationPreference).where(UserNotificationPreference.user_id == user_id).limit(1)
    )
    if preference is None:
        preference = UserNotificationPreference(user_id=user_id)
        session.add(preference)
        session.flush()
    return preference


def _preference_allows_category(preference: UserNotificationPreference, category: str) -> bool:
    field = CATEGORY_PREFERENCE_FIELDS.get(category, "governance_enabled")
    return bool(getattr(preference, field, True))


def _serialize_preference(preference: UserNotificationPreference) -> dict[str, Any]:
    return {
        "in_app_enabled": bool(preference.in_app_enabled),
        "email_enabled": bool(preference.email_enabled),
        "governance_enabled": bool(preference.governance_enabled),
        "stewardship_enabled": bool(preference.stewardship_enabled),
        "operational_enabled": bool(preference.operational_enabled),
        "only_assigned_items": bool(preference.only_assigned_items),
        "daily_digest_enabled": bool(preference.daily_digest_enabled),
        "last_daily_digest_at": preference.last_daily_digest_at,
        "next_daily_digest_at": preference.next_daily_digest_at,
        "last_daily_digest_status": preference.last_daily_digest_status,
        "updated_at": preference.updated_at,
    }


def get_user_notification_preferences_payload(session: Session, user: User) -> dict[str, Any]:
    preference = get_or_create_user_notification_preference(session, user)
    return _serialize_preference(preference)


def update_user_notification_preferences(session: Session, *, user: User, payload) -> dict[str, Any]:
    preference = get_or_create_user_notification_preference(session, user)
    digest_enabled_before = bool(preference.daily_digest_enabled)
    preference.in_app_enabled = bool(payload.in_app_enabled)
    preference.email_enabled = bool(payload.email_enabled)
    preference.governance_enabled = bool(payload.governance_enabled)
    preference.stewardship_enabled = bool(payload.stewardship_enabled)
    preference.operational_enabled = bool(payload.operational_enabled)
    preference.only_assigned_items = bool(payload.only_assigned_items)
    preference.daily_digest_enabled = bool(payload.daily_digest_enabled)
    if preference.daily_digest_enabled and (not digest_enabled_before or preference.next_daily_digest_at is None):
        preference.next_daily_digest_at = _next_digest_slot(now=_now())
    if not preference.daily_digest_enabled:
        preference.next_daily_digest_at = None
        preference.last_daily_digest_status = None
    session.add(preference)
    session.commit()
    session.refresh(preference)
    return _serialize_preference(preference)


def _next_digest_slot(*, now: datetime) -> datetime:
    localized = now.astimezone(DIGEST_TIMEZONE)
    scheduled = localized.replace(hour=DIGEST_HOUR, minute=0, second=0, microsecond=0)
    if localized >= scheduled:
        scheduled = scheduled + timedelta(days=1)
    return scheduled.astimezone(timezone.utc)


def create_user_inbox_notification(
    session: Session,
    *,
    user_id: int,
    dedupe_key: str,
    category: str,
    severity: str,
    source_module: str,
    source_entity_type: str,
    source_entity_id: str | int,
    title: str,
    message: str,
    href: str | None = None,
    context_json: dict[str, Any] | None = None,
    forwarded_from_notification_id: int | None = None,
    forwarded_by_user_id: int | None = None,
    forwarded_at: datetime | None = None,
    ignore_category_preferences: bool = False,
) -> UserInboxNotification:
    preference = get_or_create_user_notification_preference(session, user_id)
    if not ignore_category_preferences and not _preference_allows_category(preference, category):
        raise ValueError(f"Notification category '{category}' is disabled for user {user_id}")

    now = _now()
    inbox = session.scalar(
        select(UserInboxNotification)
        .where(
            UserInboxNotification.user_id == user_id,
            UserInboxNotification.dedupe_key == dedupe_key,
        )
        .limit(1)
    )
    if inbox is None:
        inbox = UserInboxNotification(
            user_id=user_id,
            dedupe_key=dedupe_key,
            category=category,
            severity=severity,
            source_module=source_module,
            source_entity_type=source_entity_type,
            source_entity_id=str(source_entity_id),
            title=title,
            message=message,
            href=href,
            state="unread",
            delivery_state="none",
            context_json=to_jsonable(context_json or {}),
            forwarded_from_notification_id=forwarded_from_notification_id,
            forwarded_by_user_id=forwarded_by_user_id,
            forwarded_at=forwarded_at,
            first_seen_at=now,
            last_seen_at=now,
            next_delivery_at=None,
            delivery_channels_json={},
        )
        session.add(inbox)
        session.flush()
        return inbox

    inbox.category = category
    inbox.severity = severity
    inbox.title = title
    inbox.message = message
    inbox.href = href
    inbox.context_json = to_jsonable(context_json or {})
    if forwarded_from_notification_id is not None:
        inbox.forwarded_from_notification_id = forwarded_from_notification_id
    if forwarded_by_user_id is not None:
        inbox.forwarded_by_user_id = forwarded_by_user_id
    if forwarded_at is not None:
        inbox.forwarded_at = forwarded_at
    inbox.last_seen_at = now
    if inbox.state == "archived":
        inbox.state = "unread"
        inbox.archived_at = None
    if inbox.state == "read":
        inbox.state = "unread"
        inbox.read_at = None
    inbox.delivery_state = "none"
    inbox.next_delivery_at = None
    inbox.delivery_channels_json = {}
    session.add(inbox)
    session.flush()
    return inbox


def search_inbox_forward_recipients(
    session: Session,
    *,
    q: str | None = None,
    limit: int = 20,
    exclude_user_id: int | None = None,
) -> list[dict[str, Any]]:
    conditions = [User.is_active.is_(True)]
    if exclude_user_id is not None:
        conditions.append(User.id != exclude_user_id)
    query_text = (q or "").strip()
    if query_text:
        like = f"%{query_text}%"
        conditions.append(
            or_(
                User.email.ilike(like),
                User.name.ilike(like),
                User.full_name.ilike(like),
            )
        )
    users = session.scalars(
        select(User)
        .where(*conditions)
        .order_by(func.lower(func.coalesce(User.name, User.full_name, User.email)).asc(), User.id.asc())
        .limit(max(1, min(limit, 50)))
    ).all()
    return [_user_search_payload(user) for user in users]


def resolve_inbox_notification_recipients(
    session: Session,
    *,
    user_ids: list[int] | tuple[int, ...] | set[int] | None = None,
    include_admins: bool = True,
) -> list[User]:
    recipients: list[User] = []
    seen: set[int] = set()

    def add(user: User | None) -> None:
        if user is None or not user.is_active or user.id in seen:
            return
        seen.add(user.id)
        recipients.append(user)

    for raw_user_id in user_ids or []:
        try:
            user_id = int(raw_user_id)
        except Exception:  # noqa: BLE001
            continue
        add(session.get(User, user_id))

    if include_admins:
        for admin in get_active_admin_users(session):
            add(admin)

    return recipients


def get_active_admin_users(session: Session) -> list[User]:
    return session.scalars(
        select(User)
        .join(User.roles)
        .options(selectinload(User.roles))
        .where(User.is_active.is_(True), Role.name == "admin")
        .order_by(User.name.asc().nullslast(), User.email.asc())
    ).all()


def forward_user_inbox_notification(
    session: Session,
    *,
    user: User,
    notification_id: int,
    recipient_user_id: int,
) -> UserInboxNotification:
    original = session.get(UserInboxNotification, notification_id)
    if original is None or original.user_id != user.id:
        raise ValueError("Notificação não encontrada")

    recipient = session.scalar(
        select(User).where(User.id == recipient_user_id, User.is_active.is_(True)).limit(1)
    )
    if recipient is None:
        raise ValueError("Usuário destinatário não encontrado ou inativo")
    if recipient.id == user.id:
        raise ValueError("Não é possível encaminhar a notificação para o próprio usuário")

    now = _now()
    forwarded_context = dict(original.context_json or {})
    forwarded_context["forwarded"] = {
        "from_notification_id": original.id,
        "from_user_id": user.id,
        "from_user_name": _display_name(user),
        "from_user_email": user.email,
        "to_user_id": recipient.id,
        "to_user_name": _display_name(recipient),
        "to_user_email": recipient.email,
        "forwarded_at": now.isoformat(),
    }
    created: UserInboxNotification | None = None
    for target_user in resolve_inbox_notification_recipients(session, user_ids=[recipient.id], include_admins=True):
        inbox = create_user_inbox_notification(
            session,
            user_id=target_user.id,
            dedupe_key=f"forward:{original.id}:{recipient.id}",
            category=original.category,
            severity=original.severity,
            source_module=original.source_module,
            source_entity_type=original.source_entity_type,
            source_entity_id=original.source_entity_id,
            title=original.title,
            message=original.message,
            href=original.href,
            context_json=to_jsonable(forwarded_context),
            forwarded_from_notification_id=original.id,
            forwarded_by_user_id=user.id,
            forwarded_at=now,
            ignore_category_preferences=True,
        )
        if target_user.id == recipient.id:
            created = inbox
    session.flush()
    if created is None:
        raise ValueError("Usuário destinatário não encontrado ou inativo")
    return created


def _send_email(*, inbox: UserInboxNotification, user: User) -> tuple[str, dict[str, Any]]:
    if not settings.smtp_host or not settings.notification_from_email:
        return "skipped", {"reason": "smtp_not_configured"}
    if not user.email:
        return "skipped", {"reason": "recipient_without_email"}
    message = EmailMessage()
    message["Subject"] = inbox.title
    message["From"] = settings.notification_from_email
    message["To"] = user.email
    message.set_content(f"{inbox.title}\n\n{inbox.message}\n\nAção: {inbox.href or '-'}")
    server = smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10)
    try:
        if settings.smtp_use_tls:
            server.starttls()
        if settings.smtp_username:
            server.login(settings.smtp_username, settings.smtp_password or "")
        server.send_message(message)
    finally:
        try:
            server.quit()
        except Exception:
            pass
    return "sent", {"provider": "smtp", "recipient": user.email}


def send_user_alert_email(*, user: User, title: str, message: str, href: str | None = None) -> tuple[str, dict[str, Any]]:
    if not settings.smtp_host or not settings.notification_from_email:
        return "skipped", {"reason": "smtp_not_configured"}
    if not user.email:
        return "skipped", {"reason": "recipient_without_email"}
    alert = EmailMessage()
    alert["Subject"] = title
    alert["From"] = settings.notification_from_email
    alert["To"] = user.email
    body_lines = [title, "", message]
    if href:
        body_lines.extend(["", f"Ação: {href}"])
    alert.set_content("\n".join(body_lines))
    server = smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10)
    try:
        if settings.smtp_use_tls:
            server.starttls()
        if settings.smtp_username:
            server.login(settings.smtp_username, settings.smtp_password or "")
        server.send_message(alert)
    finally:
        try:
            server.quit()
        except Exception:
            pass
    return "sent", {"provider": "smtp", "recipient": user.email}


def _send_digest_email(*, user: User, subject: str, body: str) -> tuple[str, dict[str, Any]]:
    if not settings.smtp_host or not settings.notification_from_email:
        return "skipped", {"reason": "smtp_not_configured"}
    if not user.email:
        return "skipped", {"reason": "recipient_without_email"}
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.notification_from_email
    message["To"] = user.email
    message.set_content(body)
    server = smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10)
    try:
        if settings.smtp_use_tls:
            server.starttls()
        if settings.smtp_username:
            server.login(settings.smtp_username, settings.smtp_password or "")
        server.send_message(message)
    finally:
        try:
            server.quit()
        except Exception:
            pass
    return "sent", {"provider": "smtp", "recipient": user.email}


def queue_governance_notifications_for_users(session: Session) -> dict[str, Any]:
    items = session.scalars(
        select(GovernanceNotification)
        .options(
            selectinload(GovernanceNotification.table),
            selectinload(GovernanceNotification.data_owner),
        )
        .where(GovernanceNotification.status == "active")
    ).all()
    created = 0
    skipped = 0
    for item in items:
        owner_email = None
        if item.data_owner is not None:
            owner_email = item.data_owner.email
        elif item.table is not None and getattr(item.table, "data_owner", None) is not None:
            owner_email = item.table.data_owner.email
        if not owner_email:
            skipped += 1
            continue
        user = session.scalar(select(User).where(User.is_active.is_(True), User.email == owner_email).limit(1))
        if user is None:
            skipped += 1
            continue
        preference = get_or_create_user_notification_preference(session, user)
        if preference.only_assigned_items and item.origin not in {"governance", "operations"}:
            skipped += 1
            continue
        try:
            targets = resolve_inbox_notification_recipients(session, user_ids=[user.id], include_admins=True)
            for target_user in targets:
                create_user_inbox_notification(
                    session,
                    user_id=target_user.id,
                    dedupe_key=f"governance:{item.dedupe_key}",
                    category="governance",
                    severity=item.severity,
                    source_module="governance",
                    source_entity_type=item.entity_type,
                    source_entity_id=item.table_id or item.dedupe_key,
                    title=item.title,
                    message=item.message,
                    href=item.target_href,
                    context_json=dict(item.context_json or {}),
                )
            created += len(targets)
        except ValueError:
            skipped += 1
    session.flush()
    return {"queued": created, "skipped": skipped}


def _build_digest_payload(items: list[UserInboxNotification]) -> tuple[str, str]:
    unread_items = [item for item in items if item.state == "unread"]
    total = len(unread_items)
    by_category: dict[str, int] = {}
    for item in unread_items:
        by_category[item.category] = by_category.get(item.category, 0) + 1
    lines = [
        f"Você tem {total} item(ns) não lidos na inbox do T2C Data.",
        "",
        "Resumo por categoria:",
    ]
    for key, count in sorted(by_category.items(), key=lambda pair: (-pair[1], pair[0])):
        lines.append(f"- {CATEGORY_LABELS.get(key, key.title())}: {count}")
    lines.append("")
    lines.append("Principais itens:")
    for item in unread_items[:5]:
        lines.append(f"- [{item.severity.upper()}] {item.title}: {item.message}")
    lines.append("")
    lines.append("Abra sua inbox em /inbox para agir nos itens pendentes.")
    subject = f"T2C Data • Digest diário ({total} pendência(s))"
    return subject, "\n".join(lines)


def dispatch_daily_notification_digests(session: Session, *, limit: int = 50) -> dict[str, Any]:
    now = _now()
    preferences = session.scalars(
        select(UserNotificationPreference)
        .options(selectinload(UserNotificationPreference.user))
        .where(
            UserNotificationPreference.daily_digest_enabled.is_(True),
            or_(UserNotificationPreference.next_daily_digest_at.is_(None), UserNotificationPreference.next_daily_digest_at <= now),
        )
        .order_by(UserNotificationPreference.next_daily_digest_at.asc().nullsfirst(), UserNotificationPreference.id.asc())
        .limit(limit)
    ).all()
    processed = sent = skipped = failed = 0
    for preference in preferences:
        processed += 1
        user = preference.user
        if user is None or not user.is_active:
            preference.last_daily_digest_status = "skipped"
            preference.next_daily_digest_at = _next_digest_slot(now=now)
            session.add(preference)
            skipped += 1
            continue
        items = session.scalars(
            select(UserInboxNotification)
            .where(UserInboxNotification.user_id == user.id, UserInboxNotification.state == "unread")
            .order_by(UserInboxNotification.created_at.desc(), UserInboxNotification.id.desc())
            .limit(25)
        ).all()
        if not items:
            preference.last_daily_digest_at = now
            preference.last_daily_digest_status = "empty"
            preference.next_daily_digest_at = _next_digest_slot(now=now)
            session.add(preference)
            skipped += 1
            continue
        subject, body = _build_digest_payload(items)
        outcomes: list[str] = []
        try:
            if preference.email_enabled:
                outcome, _ = _send_digest_email(user=user, subject=subject, body=body)
                outcomes.append(outcome)
            if not outcomes:
                preference.last_daily_digest_status = "skipped"
                skipped += 1
            elif any(item == "sent" for item in outcomes):
                preference.last_daily_digest_status = "sent"
                preference.last_daily_digest_at = now
                sent += 1
            else:
                preference.last_daily_digest_status = "skipped"
                skipped += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("daily digest delivery failed user=%s error=%s", user.id, exc)
            preference.last_daily_digest_status = "failed"
            preference.next_daily_digest_at = now + timedelta(hours=1)
            session.add(preference)
            failed += 1
            continue
        preference.next_daily_digest_at = _next_digest_slot(now=now)
        session.add(preference)
    session.flush()
    return {
        "processed": processed,
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
    }


def get_inbox_notification_payload(item: UserInboxNotification) -> dict[str, Any]:
    return {
        "id": item.id,
        "category": item.category,
        "severity": item.severity,
        "source_module": item.source_module,
        "source_entity_type": item.source_entity_type,
        "source_entity_id": item.source_entity_id,
        "title": item.title,
        "message": item.message,
        "href": item.href,
        "state": item.state,
        "delivery_state": item.delivery_state,
        "context_json": dict(item.context_json or {}),
        "forwarded_from_notification_id": item.forwarded_from_notification_id,
        "forwarded_by_user_id": item.forwarded_by_user_id,
        "forwarded_by_user_name": _display_name(item.forwarded_by_user),
        "forwarded_by_user_email": item.forwarded_by_user.email if item.forwarded_by_user else None,
        "forwarded_at": item.forwarded_at,
        "first_seen_at": item.first_seen_at,
        "last_seen_at": item.last_seen_at,
        "last_notified_at": item.last_notified_at,
        "next_delivery_at": item.next_delivery_at,
        "read_at": item.read_at,
        "archived_at": item.archived_at,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def get_user_inbox(
    session: Session,
    *,
    user: User,
    state_filter: str | None = None,
    category: str | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict[str, Any]:
    conditions = [UserInboxNotification.user_id == user.id]
    if state_filter:
        conditions.append(UserInboxNotification.state == state_filter)
    if category:
        conditions.append(UserInboxNotification.category == category)
    safe_limit = max(1, min(limit, 200))
    safe_page = max(int(page or 1), 1)
    offset = (safe_page - 1) * safe_limit
    state_order = case(
        (UserInboxNotification.state == "unread", 0),
        (UserInboxNotification.state == "read", 1),
        (UserInboxNotification.state == "archived", 2),
        else_=3,
    )
    total = int(
        session.scalar(select(func.count()).select_from(UserInboxNotification).where(*conditions)) or 0
    )
    items = session.scalars(
        select(UserInboxNotification)
        .options(selectinload(UserInboxNotification.forwarded_by_user))
        .where(*conditions)
        .order_by(state_order.asc(), UserInboxNotification.created_at.desc(), UserInboxNotification.id.desc())
        .offset(offset)
        .limit(safe_limit)
    ).all()
    return {
        "generated_at": _now().isoformat(),
        "total": total,
        "page": safe_page,
        "page_size": safe_limit,
        "has_more": offset + len(items) < total,
        "items": [get_inbox_notification_payload(item) for item in items],
    }


def get_user_inbox_summary(session: Session, *, user: User) -> dict[str, Any]:
    rows = session.execute(
        select(
            UserInboxNotification.category,
            func.count(UserInboxNotification.id).label("total"),
            func.sum(case((UserInboxNotification.state == "unread", 1), else_=0)).label("unread"),
            func.sum(case((UserInboxNotification.delivery_state == "pending", 1), else_=0)).label("due_delivery"),
        )
        .where(UserInboxNotification.user_id == user.id)
        .group_by(UserInboxNotification.category)
    ).all()
    total = sum(int(row.total or 0) for row in rows)
    unread = sum(int(row.unread or 0) for row in rows)
    due_delivery = sum(int(row.due_delivery or 0) for row in rows)
    return {
        "total": total,
        "unread": unread,
        "due_delivery": due_delivery,
        "by_category": [
            {"key": str(row.category), "count": int(row.total or 0)}
            for row in sorted(rows, key=lambda item: str(item.category or ""))
        ],
    }


def mark_user_inbox_notification_read(session: Session, *, user: User, notification_id: int) -> dict[str, Any]:
    item = session.get(UserInboxNotification, notification_id)
    if item is None or item.user_id != user.id:
        raise ValueError("Notification not found")
    item.state = "read"
    item.read_at = _now()
    item.archived_at = None
    session.add(item)
    session.commit()
    session.refresh(item)
    return get_inbox_notification_payload(item)


def mark_user_inbox_notification_unread(session: Session, *, user: User, notification_id: int) -> dict[str, Any]:
    item = session.get(UserInboxNotification, notification_id)
    if item is None or item.user_id != user.id:
        raise ValueError("Notification not found")
    item.state = "unread"
    item.read_at = None
    item.archived_at = None
    session.add(item)
    session.commit()
    session.refresh(item)
    return get_inbox_notification_payload(item)


def mark_user_inbox_notification_archived(session: Session, *, user: User, notification_id: int) -> dict[str, Any]:
    item = session.get(UserInboxNotification, notification_id)
    if item is None or item.user_id != user.id:
        raise ValueError("Notification not found")
    item.state = "archived"
    item.archived_at = _now()
    session.add(item)
    session.commit()
    session.refresh(item)
    return get_inbox_notification_payload(item)


__all__ = [
    "create_user_inbox_notification",
    "dispatch_daily_notification_digests",
    "get_inbox_notification_payload",
    "forward_user_inbox_notification",
    "get_or_create_user_notification_preference",
    "get_user_inbox",
    "get_user_inbox_summary",
    "get_user_notification_preferences_payload",
    "mark_user_inbox_notification_archived",
    "mark_user_inbox_notification_read",
    "mark_user_inbox_notification_unread",
    "send_user_alert_email",
    "queue_governance_notifications_for_users",
    "search_inbox_forward_recipients",
    "update_user_notification_preferences",
]
