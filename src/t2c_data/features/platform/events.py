from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from t2c_data.features.audit import safe_jsonable
from t2c_data.features.platform.sensitive_data import redact_sensitive_metadata

from t2c_data.models.platform import PlatformDomainEvent

_CATEGORY_LABELS = {
    "governance": "Governança",
    "quality": "Qualidade",
    "operation": "Operação",
    "incident": "Incidente",
    "audit": "Auditoria",
    "platform": "Plataforma",
    "tags": "Tags",
    "classification": "Classificação",
    "certification": "Certificação",
}

_EMITTED_PREFIXES = (
    "governance.",
    "platform.",
    "tags.",
    "glossary.",
    "certification.",
    "datasource.",
    "dashboard.",
    "search.",
    "data_quality.",
    "dq.",
    "incident.",
    "incidents.",
    "lineage.",
    "stewardship.",
    "catalog.",
)


@dataclass(frozen=True)
class PlatformEventFilters:
    days: int = 30
    limit: int = 100
    table_id: int | None = None
    entity_type: str | None = None
    event_key: str | None = None
    category: str | None = None
    severity: str | None = None
    q: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str:
    return (value or "").strip()


def _humanize_action(action: str | None) -> str:
    raw = _normalize_text(action)
    if not raw:
        return "Evento"
    tail = raw.split(".")[-1].replace("_", " ").strip()
    return tail.title() or "Evento"


def _event_category(action: str | None, source_module: str | None, entity_type: str | None) -> str:
    normalized_action = _normalize_text(action).lower()
    normalized_module = _normalize_text(source_module).lower()
    normalized_entity = _normalize_text(entity_type).lower()
    if normalized_action.startswith(("incident.", "incidents.")) or normalized_module == "incidents":
        return "incident"
    if normalized_action.startswith(("dq.", "data_quality.")) or normalized_module in {"dq", "data_quality"}:
        return "quality"
    if normalized_action.startswith("certification.") or normalized_module == "certification":
        return "certification"
    if normalized_action.startswith("tags.") or normalized_module == "tags":
        return "tags"
    if normalized_action.startswith("glossary.") or normalized_module == "glossary":
        return "governance"
    if normalized_action.startswith("stewardship.") or normalized_module == "stewardship":
        return "governance"
    if normalized_action.startswith("lineage.") or normalized_module == "lineage":
        return "operation"
    if normalized_action.startswith("platform.cockpit.") or normalized_action.startswith("platform.scheduler.") or normalized_action.startswith("platform.read_models.") or normalized_action.startswith("platform.visibility.") or normalized_module == "platform":
        return "platform"
    if normalized_entity in {"column"}:
        return "classification"
    return "governance"


def _event_severity(action: str | None, *, metadata: dict[str, Any] | None = None, before: Any = None, after: Any = None) -> str:
    payload = metadata or {}
    explicit = str(payload.get("severity") or payload.get("severity_label") or "").strip().lower()
    if explicit in {"critical", "high", "medium", "low"}:
        return explicit
    normalized_action = _normalize_text(action).lower()
    if any(token in normalized_action for token in ("delete", "revoke", "reject", "fail", "open", "close", "expire", "disable")):
        return "high"
    if any(token in normalized_action for token in ("approve", "apply", "update", "create", "resolve", "review")):
        return "medium"
    if before is not None or after is not None:
        return "medium"
    return "low"


def _manual_mode(*, user_id: int | None, actor_name: str | None, actor_email: str | None, source_module: str | None) -> str:
    if user_id is not None or actor_name or actor_email:
        return "manual"
    normalized_module = _normalize_text(source_module).lower()
    if normalized_module in {"governance", "tags", "glossary", "certification", "privacy_access", "stewardship"}:
        return "manual"
    if normalized_module in {"ingestion", "dq", "data_quality", "ops", "platform", "system", "lineage", "catalog"}:
        return "automatic"
    return "unknown"


def should_emit_platform_domain_event(action: str | None, source_module: str | None = None) -> bool:
    normalized_action = _normalize_text(action).lower()
    if not normalized_action:
        return False
    if normalized_action.startswith(_EMITTED_PREFIXES):
        return True
    normalized_module = _normalize_text(source_module).lower()
    return normalized_module in {
        "governance",
        "governance.playbooks",
        "governance.assistant",
        "governance.recommendations",
        "tags",
        "glossary",
        "certification",
        "dq",
        "data_quality",
        "incidents",
        "lineage",
        "stewardship",
        "platform",
        "datasource",
        "datasources",
        "dashboard",
        "search",
        "catalog",
    }


def should_emit_platform_usage_domain_event(event_name: str | None, module_name: str | None = None) -> bool:
    normalized_event = _normalize_text(event_name).lower()
    if not normalized_event:
        return False
    if any(token in normalized_event for token in ("view", "page", "query", "search")):
        return False
    normalized_module = _normalize_text(module_name).lower()
    return normalized_module in {
        "ops_cockpit",
        "ops",
        "platform",
        "governance",
        "governance_playbooks",
        "governance_playbook",
        "recommendations",
        "assistant",
        "playbooks",
        "playbook",
        "certification",
        "data_quality",
        "dq",
        "incidents",
        "lineage",
        "datasources",
        "datasource",
        "dashboard",
        "search",
        "tags",
        "glossary",
    }


def _entity_refs(
    *,
    entity_type: str | None,
    entity_id: str | int | None,
    parent_entity_type: str | None,
    parent_entity_id: str | int | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, int | None]:
    payload = metadata or {}
    table_id = payload.get("table_id")
    column_id = payload.get("column_id")
    datasource_id = payload.get("datasource_id")
    entity_type_norm = _normalize_text(entity_type).lower()
    parent_type_norm = _normalize_text(parent_entity_type).lower()
    try:
        entity_id_int = int(entity_id) if entity_id is not None and str(entity_id).strip() else None
    except Exception:
        entity_id_int = None
    try:
        parent_entity_id_int = int(parent_entity_id) if parent_entity_id is not None and str(parent_entity_id).strip() else None
    except Exception:
        parent_entity_id_int = None
    if entity_type_norm == "table":
        table_id = entity_id_int if entity_id_int is not None else table_id
    elif parent_type_norm == "table":
        table_id = parent_entity_id_int if parent_entity_id_int is not None else table_id
    if entity_type_norm == "column":
        column_id = entity_id_int if entity_id_int is not None else column_id
    if datasource_id is not None:
        try:
            datasource_id = int(datasource_id)
        except Exception:
            datasource_id = None
    if table_id is not None:
        try:
            table_id = int(table_id)
        except Exception:
            table_id = None
    if column_id is not None:
        try:
            column_id = int(column_id)
        except Exception:
            column_id = None
    return {"table_id": table_id, "column_id": column_id, "datasource_id": datasource_id}


def build_platform_domain_event_payload(
    *,
    action: str,
    entity_type: str | None,
    entity_id: str | int | None,
    parent_entity_type: str | None = None,
    parent_entity_id: str | int | None = None,
    source_module: str | None = None,
    user_id: int | None = None,
    actor_name: str | None = None,
    actor_email: str | None = None,
    change_set_id: str | None = None,
    before: Any = None,
    after: Any = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(metadata or {})
    refs = _entity_refs(
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        metadata=payload,
    )
    event_key = _normalize_text(action).lower()
    category = _event_category(action, source_module, entity_type)
    title = _normalize_text(payload.get("title")) or _humanize_action(action)
    summary = _normalize_text(payload.get("message")) or _normalize_text(payload.get("summary"))
    if not summary:
        summary = _normalize_text(payload.get("description"))
    if not summary and before is not None and after is not None:
        summary = "Atualização registrada"
    severity = _event_severity(action, metadata=payload, before=before, after=after)
    return {
        "event_key": event_key,
        "category": category,
        "severity": severity,
        "title": title,
        "summary": summary or None,
        "source_module": _normalize_text(source_module) or None,
        "source_action": event_key,
        "entity_type": _normalize_text(entity_type) or None,
        "entity_id": int(entity_id) if entity_id is not None and str(entity_id).strip().isdigit() else None,
        "table_id": refs["table_id"],
        "column_id": refs["column_id"],
        "datasource_id": refs["datasource_id"],
        "actor_user_id": user_id,
        "actor_name": _normalize_text(actor_name) or None,
        "actor_email": _normalize_text(actor_email) or None,
        "manual_mode": _manual_mode(
            user_id=user_id,
            actor_name=actor_name,
            actor_email=actor_email,
            source_module=source_module,
        ),
        "correlation_key": _normalize_text(change_set_id) or None,
        "payload_json": {
            "before": redact_sensitive_metadata(safe_jsonable(before)),
            "after": redact_sensitive_metadata(safe_jsonable(after)),
            "metadata": redact_sensitive_metadata(safe_jsonable(payload)),
        },
    }


def record_platform_domain_event_from_audit(
    session: Session,
    *,
    action: str,
    entity_type: str | None,
    entity_id: str | int | None,
    parent_entity_type: str | None = None,
    parent_entity_id: str | int | None = None,
    source_module: str | None = None,
    user_id: int | None = None,
    actor_name: str | None = None,
    actor_email: str | None = None,
    change_set_id: str | None = None,
    before: Any = None,
    after: Any = None,
    metadata: dict[str, Any] | None = None,
) -> PlatformDomainEvent | None:
    if not should_emit_platform_domain_event(action, source_module):
        return None
    payload = build_platform_domain_event_payload(
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module=source_module,
        user_id=user_id,
        actor_name=actor_name,
        actor_email=actor_email,
        change_set_id=change_set_id,
        before=before,
        after=after,
        metadata=metadata,
    )
    event = PlatformDomainEvent(**payload)
    session.add(event)
    session.flush()
    return event


def record_platform_domain_event_from_usage(
    session: Session,
    *,
    event_name: str,
    module_name: str,
    page_path: str | None = None,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    target_url: str | None = None,
    user_id: int | None = None,
    actor_name: str | None = None,
    actor_email: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> PlatformDomainEvent | None:
    if not should_emit_platform_usage_domain_event(event_name, module_name):
        return None
    payload = dict(metadata or {})
    if page_path:
        payload.setdefault("page_path", page_path)
    if target_url:
        payload.setdefault("target_url", target_url)
    payload.setdefault("module_name", module_name)
    payload.setdefault("event_name", event_name)
    event = PlatformDomainEvent(
        **build_platform_domain_event_payload(
            action=f"platform.usage.{_normalize_text(module_name) or 'module'}.{_normalize_text(event_name) or 'event'}",
            entity_type=entity_type,
            entity_id=entity_id,
            source_module="platform.analytics",
            user_id=user_id,
            actor_name=actor_name,
            actor_email=actor_email,
            metadata=payload,
        )
    )
    session.add(event)
    session.flush()
    return event


def serialize_platform_domain_event(event: PlatformDomainEvent) -> dict[str, Any]:
    payload = event.payload_json or {}
    if not isinstance(payload, dict):
        payload = {"raw": payload}
    return {
        "id": int(event.id),
        "event_key": event.event_key,
        "category": event.category,
        "category_label": _CATEGORY_LABELS.get(event.category, event.category.replace("_", " ").title()),
        "severity": event.severity,
        "title": event.title,
        "summary": event.summary,
        "source_module": event.source_module,
        "source_action": event.source_action,
        "entity_type": event.entity_type,
        "entity_id": event.entity_id,
        "table_id": event.table_id,
        "column_id": event.column_id,
        "datasource_id": event.datasource_id,
        "actor_user_id": event.actor_user_id,
        "actor_name": event.actor_name,
        "actor_email": event.actor_email,
        "manual_mode": event.manual_mode,
        "correlation_key": event.correlation_key,
        "payload_json": payload,
        "occurred_at": event.created_at,
    }


def list_platform_domain_events(
    session: Session,
    *,
    filters: PlatformEventFilters | None = None,
) -> dict[str, Any]:
    filters = filters or PlatformEventFilters()
    now = _now()
    since = now - timedelta(days=max(int(filters.days), 1)) if filters.days and filters.days > 0 else now - timedelta(days=30)
    stmt = select(PlatformDomainEvent)
    count_stmt = select(func.count(PlatformDomainEvent.id))
    conditions = [PlatformDomainEvent.created_at >= since]
    if filters.table_id is not None:
        conditions.append(PlatformDomainEvent.table_id == int(filters.table_id))
    if filters.entity_type:
        conditions.append(PlatformDomainEvent.entity_type == filters.entity_type)
    if filters.event_key:
        conditions.append(PlatformDomainEvent.event_key == filters.event_key)
    if filters.category:
        conditions.append(PlatformDomainEvent.category == filters.category)
    if filters.severity:
        conditions.append(PlatformDomainEvent.severity == filters.severity)
    if filters.q:
        q = f"%{filters.q.strip().lower()}%"
        conditions.append(
            or_(
                func.lower(PlatformDomainEvent.title).like(q),
                func.lower(func.coalesce(PlatformDomainEvent.summary, "")).like(q),
                func.lower(func.coalesce(PlatformDomainEvent.event_key, "")).like(q),
            )
        )
    stmt = stmt.where(and_(*conditions)).order_by(PlatformDomainEvent.created_at.desc(), PlatformDomainEvent.id.desc()).limit(max(1, min(int(filters.limit), 500)))
    count_stmt = count_stmt.where(and_(*conditions))
    total = int(session.scalar(count_stmt) or 0)
    items = [serialize_platform_domain_event(row) for row in session.scalars(stmt).all()]
    return {
        "generated_at": now,
        "total": total,
        "limit": max(1, min(int(filters.limit), 500)),
        "days": int(filters.days),
        "items": items,
    }


__all__ = [
    "PlatformEventFilters",
    "build_platform_domain_event_payload",
    "list_platform_domain_events",
    "record_platform_domain_event_from_audit",
    "record_platform_domain_event_from_usage",
    "serialize_platform_domain_event",
    "should_emit_platform_domain_event",
    "should_emit_platform_usage_domain_event",
]
