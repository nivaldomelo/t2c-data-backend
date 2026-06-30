from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.db_sequences import SequenceAlignmentResult, align_integer_pk_sequence
from t2c_data.features.audit import finalize_audit_json, request_audit_kwargs, serialize_model
from t2c_data.features.audit.support import AuditFieldChange, classify_sensitive_change
from t2c_data.models.audit import AccessLog, AuditLog

logger = logging.getLogger(__name__)


def write_audit_log_sync(
    session: Session,
    *,
    action: str,
    user_id: int | None = None,
    actor_name: str | None = None,
    user_email: str | None = None,
    ip: str | None = None,
    user_agent: str | None = None,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    parent_entity_type: str | None = None,
    parent_entity_id: str | int | None = None,
    change_set_id: str | None = None,
    change_type: str | None = None,
    field_name: str | None = None,
    source_module: str | None = None,
    is_sensitive_change: bool | None = None,
    sensitive_category: str | None = None,
    route: str | None = None,
    method: str | None = None,
    status_code: int | None = None,
    request_id: str | None = None,
    before: Any = None,
    after: Any = None,
    metadata: Any = None,
) -> None:
    try:
        entry = AuditLog(
            user_id=user_id,
            actor_name=actor_name,
            user_email=user_email,
            ip=ip,
            user_agent=user_agent,
            action=action,
            entity_type=entity_type,
            entity_id=None if entity_id is None else str(entity_id),
            parent_entity_type=parent_entity_type,
            parent_entity_id=None if parent_entity_id is None else str(parent_entity_id),
            change_set_id=change_set_id,
            change_type=change_type,
            field_name=field_name,
            source_module=source_module,
            is_sensitive_change=bool(is_sensitive_change),
            sensitive_category=sensitive_category,
            route=route,
            method=method,
            status_code=status_code,
            request_id=request_id,
            before_json=finalize_audit_json(before),
            after_json=finalize_audit_json(after),
            metadata_json=finalize_audit_json(metadata),
        )
        session.add(entry)
        try:
            from t2c_data.features.platform.events import record_platform_domain_event_from_audit

            record_platform_domain_event_from_audit(
                session,
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                parent_entity_type=parent_entity_type,
                parent_entity_id=parent_entity_id,
                source_module=source_module,
                user_id=user_id,
                actor_name=actor_name,
                actor_email=user_email,
                change_set_id=change_set_id,
                before=before,
                after=after,
                metadata=metadata if isinstance(metadata, dict) else None,
            )
        except Exception:  # noqa: BLE001
            logger.warning("platform domain event emission skipped action=%s", action)
    except Exception as exc:  # noqa: BLE001
        logger.warning("audit_log write failed action=%s error=%s", action, exc)


async def write_audit_log(session: Session, **kwargs: Any) -> None:
    write_audit_log_sync(session, **kwargs)


def request_audit_kwargs_without_user_email(request: Any, user: Any = None) -> dict[str, Any]:
    payload = request_audit_kwargs(request, user)
    payload.pop("user_email", None)
    return payload


def write_access_log_sync(
    session: Session,
    *,
    user_id: int | None = None,
    actor_name: str | None = None,
    user_email: str | None = None,
    ip: str | None = None,
    user_agent: str | None = None,
    route: str,
    method: str | None = None,
    status_code: int | None = None,
    request_id: str | None = None,
    api_version: str = "v1",
    module_name: str | None = None,
    duration_ms: float | int | None = None,
    metadata: Any = None,
) -> None:
    try:
        entry = AccessLog(
            user_id=user_id,
            actor_name=actor_name,
            user_email=user_email,
            ip=ip,
            user_agent=user_agent,
            route=route,
            method=method,
            status_code=status_code,
            request_id=request_id,
            api_version=api_version,
            module_name=module_name,
            duration_ms=int(duration_ms) if duration_ms is not None else None,
            metadata_json=finalize_audit_json(metadata),
        )
        session.add(entry)
    except Exception as exc:  # noqa: BLE001
        logger.warning("access_log write failed route=%s error=%s", route, exc)


def _is_sequence_pk_conflict(exc: IntegrityError, *, constraint_name: str) -> bool:
    original = getattr(exc, "orig", None)
    diagnostic = getattr(original, "diag", None)
    detected_constraint = getattr(diagnostic, "constraint_name", None)
    message = str(original or exc).lower()
    return (
        detected_constraint == constraint_name
        and "duplicate key value violates unique constraint" in message
    )


def commit_access_log_with_repair(session: Session, **kwargs: Any) -> SequenceAlignmentResult | None:
    try:
        write_access_log_sync(session, **kwargs)
        session.commit()
        return None
    except IntegrityError as exc:
        session.rollback()
        if not _is_sequence_pk_conflict(exc, constraint_name="access_log_pkey"):
            raise
        repair = align_integer_pk_sequence(
            session,
            schema=settings.db_schema,
            table_name=AccessLog.__tablename__,
        )
        session.commit()
        logger.warning(
            "access_log sequence misaligned; repaired table=%s sequence=%s max_id=%s created_sequence=%s",
            repair.table_name,
            repair.sequence_name,
            repair.max_value,
            repair.created_sequence,
        )
        write_access_log_sync(session, **kwargs)
        session.commit()
        return repair


def add_audit_log(
    session: Session,
    actor_user_id: int | None,
    action: str,
    entity_type: str,
    entity_id: int | None,
    message: str | None,
    changes: dict,
) -> None:
    """Backward-compatible wrapper for existing call sites."""
    metadata: dict[str, Any] = {}
    if message:
        metadata["message"] = message
    write_audit_log_sync(
        session,
        user_id=actor_user_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        after=changes,
        metadata=metadata or None,
    )


def log_field_changes(
    session: Session,
    *,
    action: str,
    entity_type: str,
    entity_id: int | str,
    changes: list[AuditFieldChange],
    parent_entity_type: str | None = None,
    parent_entity_id: int | str | None = None,
    source_module: str | None = None,
    metadata: dict[str, Any] | None = None,
    audit_kwargs: dict[str, Any] | None = None,
    actor_user_id: int | None = None,
) -> int:
    payload = dict(audit_kwargs or {})
    if actor_user_id is not None and payload.get("user_id") is None:
        payload["user_id"] = actor_user_id
    change_set_id = payload.get("request_id") or str(uuid4())
    created = 0
    for change in changes:
        if finalize_audit_json(change.before) == finalize_audit_json(change.after):
            continue
        item_metadata = dict(metadata or {})
        if change.metadata:
            item_metadata.update(change.metadata)
        sensitive, category = classify_sensitive_change(
            field_name=change.field_name,
            change_type=change.change_type,
            metadata=item_metadata,
        )
        if sensitive:
            item_metadata.setdefault("is_sensitive_change", True)
        if category:
            item_metadata.setdefault("sensitive_category", category)
        write_audit_log_sync(
            session,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            parent_entity_type=parent_entity_type,
            parent_entity_id=parent_entity_id,
            change_set_id=change_set_id,
            change_type=change.change_type,
            field_name=change.field_name,
            source_module=source_module,
            is_sensitive_change=sensitive,
            sensitive_category=category,
            before=change.before,
            after=change.after,
            metadata=item_metadata or None,
            **payload,
        )
        created += 1
    return created


__all__ = [
    "AuditFieldChange",
    "add_audit_log",
    "commit_access_log_with_repair",
    "log_field_changes",
    "request_audit_kwargs",
    "request_audit_kwargs_without_user_email",
    "serialize_model",
    "write_access_log_sync",
    "write_audit_log",
    "write_audit_log_sync",
]
