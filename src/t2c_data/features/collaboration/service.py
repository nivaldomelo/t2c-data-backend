from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.notifications import create_user_inbox_notification, resolve_inbox_notification_recipients
from t2c_data.features.stewardship.workflow import create_stewardship_request
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner, TableEntity
from t2c_data.models.dq import DQRule
from t2c_data.models.incident import Incident
from t2c_data.models.platform import DashboardAssetReadModel
from t2c_data.models.semantic import SemanticDataProduct, SemanticDomain
from t2c_data.models.collaboration import CollaborationComment, CollaborationEvent, CollaborationTask
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync
from t2c_data.schemas.stewardship import StewardshipRequestCreateIn


ENTITY_KIND_ALIASES = {
    "asset": "table",
    "table": "table",
    "incident": "incident",
    "dq": "dq_rule",
    "dq_rule": "dq_rule",
    "domain": "semantic_domain",
    "semantic_domain": "semantic_domain",
    "product": "semantic_product",
    "semantic_product": "semantic_product",
}

TASK_TYPE_TO_STEWARDSHIP = {
    "update_documentation": "table_description",
    "define_owner": "owner_assignment",
    "review_contract": "certification_review",
    "validate_quality": "owner_review",
    "request_review": "owner_review",
}

RESPONSIBILITY_ROLE_LABELS = {
    "owner": "Owner",
    "steward": "Steward",
    "quality": "Qualidade",
    "domain_owner": "Responsável por domínio",
    "product_owner": "Responsável por produto",
}


@dataclass(slots=True)
class CollaborationEntity:
    entity_type: str
    entity_id: int
    entity_label: str
    href: str | None = None
    owner_name: str | None = None
    steward_name: str | None = None
    owner_user_id: int | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_entity_type(entity_type: str) -> str:
    normalized = (entity_type or "").strip().lower()
    return ENTITY_KIND_ALIASES.get(normalized, normalized or "table")


def _display_name(user: User | None) -> str | None:
    if user is None:
        return None
    return user.name or user.full_name or user.email


def _table_label(session: Session, entity_id: int) -> CollaborationEntity:
    table = session.scalar(
        select(TableEntity)
        .options(selectinload(TableEntity.data_owner))
        .where(TableEntity.id == entity_id)
    )
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ativo não encontrado.")
    datasource_name = table.schema.database.datasource.name
    database_name = table.schema.database.name
    schema_name = table.schema.name
    entity_label = f"{datasource_name}.{database_name}.{schema_name}.{table.name}"
    owner_user_id = None
    if table.data_owner is not None:
        owner = session.scalar(select(User).where(User.is_active.is_(True), User.email == table.data_owner.email).limit(1))
        owner_user_id = owner.id if owner is not None else None
    return CollaborationEntity(
        entity_type="table",
        entity_id=table.id,
        entity_label=entity_label,
        href=f"/explorer?tableId={table.id}",
        owner_name=table.data_owner.name if table.data_owner else table.owner or None,
        owner_user_id=owner_user_id,
    )


def _incident_label(session: Session, entity_id: int) -> CollaborationEntity:
    incident = session.get(Incident, entity_id)
    if incident is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Incidente não encontrado.")
    return CollaborationEntity(
        entity_type="incident",
        entity_id=incident.id,
        entity_label=incident.title,
        href="/incidents/tickets",
        owner_name=incident.owner_team or _display_name(incident.owner_user),
        owner_user_id=incident.owner_user_id,
    )


def _dq_rule_label(session: Session, entity_id: int) -> CollaborationEntity:
    rule = session.get(DQRule, entity_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Regra de DQ não encontrada.")
    return CollaborationEntity(
        entity_type="dq_rule",
        entity_id=rule.id,
        entity_label=rule.name,
        href=f"/data-quality/rules?rule_id={rule.id}",
    )


def _semantic_domain_label(session: Session, entity_id: int) -> CollaborationEntity:
    domain = session.get(SemanticDomain, entity_id)
    if domain is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domínio não encontrado.")
    owner = session.scalar(select(User).where(User.is_active.is_(True), User.email == (domain.owner or "")).limit(1))
    steward = session.scalar(select(User).where(User.is_active.is_(True), User.email == (domain.steward or "")).limit(1))
    return CollaborationEntity(
        entity_type="semantic_domain",
        entity_id=domain.id,
        entity_label=domain.name,
        href=f"/governance/domains/{domain.slug}",
        owner_name=domain.owner,
        steward_name=domain.steward,
        owner_user_id=owner.id if owner is not None else steward.id if steward is not None else None,
    )


def _semantic_product_label(session: Session, entity_id: int) -> CollaborationEntity:
    product = session.get(SemanticDataProduct, entity_id)
    if product is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Produto de dados não encontrado.")
    domain = session.get(SemanticDomain, product.domain_id)
    owner = session.scalar(select(User).where(User.is_active.is_(True), User.email == (product.owner or "")).limit(1))
    steward = session.scalar(select(User).where(User.is_active.is_(True), User.email == (product.steward or "")).limit(1))
    return CollaborationEntity(
        entity_type="semantic_product",
        entity_id=product.id,
        entity_label=product.name,
        href=f"/governance/data-products/{product.slug}",
        owner_name=product.owner,
        steward_name=product.steward,
        owner_user_id=owner.id if owner is not None else steward.id if steward is not None else None,
    )


def _resolve_entity(session: Session, entity_type: str, entity_id: int) -> CollaborationEntity:
    normalized = _normalize_entity_type(entity_type)
    if normalized == "table":
        return _table_label(session, entity_id)
    if normalized == "incident":
        return _incident_label(session, entity_id)
    if normalized == "dq_rule":
        return _dq_rule_label(session, entity_id)
    if normalized == "semantic_domain":
        return _semantic_domain_label(session, entity_id)
    if normalized == "semantic_product":
        return _semantic_product_label(session, entity_id)
    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Tipo de entidade de colaboração não suportado.")


def _event_payload(comment: CollaborationComment) -> dict[str, Any]:
    return {
        "id": comment.id,
        "entity_type": comment.entity_type,
        "entity_id": comment.entity_id,
        "entity_label": comment.entity_label,
        "body": comment.body,
        "comment_kind": comment.comment_kind,
        "task_id": comment.task_id,
        "parent_comment_id": comment.parent_comment_id,
        "visibility_scope": comment.visibility_scope,
        "is_resolved": comment.is_resolved,
        "resolved_at": comment.resolved_at,
        "author_user_id": comment.author_user_id,
        "author_name": comment.author_name,
        "author_email": comment.author_email,
        "resolved_by_user_id": comment.resolved_by_user_id,
        "context_json": comment.context_json,
        "created_at": comment.created_at,
        "updated_at": comment.updated_at,
    }


def _task_payload(task: CollaborationTask, comments_count: int = 0, event_count: int = 0) -> dict[str, Any]:
    return {
        "id": task.id,
        "entity_type": task.entity_type,
        "entity_id": task.entity_id,
        "entity_label": task.entity_label,
        "title": task.title,
        "description": task.description,
        "task_type": task.task_type,
        "status": task.status,
        "priority": task.priority,
        "responsibility_role": task.responsibility_role,
        "assigned_to_user_id": task.assigned_to_user_id,
        "assigned_by_user_id": task.assigned_by_user_id,
        "due_at": task.due_at,
        "completed_at": task.completed_at,
        "completed_by_user_id": task.completed_by_user_id,
        "linked_request_type": task.linked_request_type,
        "context_json": task.context_json,
        "comments_count": comments_count,
        "event_count": event_count,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


def _event_payload_row(event: CollaborationEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "entity_type": event.entity_type,
        "entity_id": event.entity_id,
        "event_type": event.event_type,
        "title": event.title,
        "detail": event.detail,
        "status_from": event.status_from,
        "status_to": event.status_to,
        "actor_user_id": event.actor_user_id,
        "actor_name": event.actor_name,
        "actor_email": event.actor_email,
        "comment_id": event.comment_id,
        "task_id": event.task_id,
        "payload_json": event.payload_json,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
    }


def _record_event(
    session: Session,
    *,
    entity_type: str,
    entity_id: int,
    event_type: str,
    title: str,
    detail: str | None = None,
    actor_user: User | None = None,
    status_from: str | None = None,
    status_to: str | None = None,
    comment_id: int | None = None,
    task_id: int | None = None,
    payload_json: dict[str, Any] | None = None,
) -> CollaborationEvent:
    event = CollaborationEvent(
        entity_type=entity_type,
        entity_id=entity_id,
        event_type=event_type,
        title=title,
        detail=detail,
        status_from=status_from,
        status_to=status_to,
        actor_user_id=actor_user.id if actor_user is not None else None,
        actor_name=_display_name(actor_user),
        actor_email=actor_user.email if actor_user is not None else None,
        comment_id=comment_id,
        task_id=task_id,
        payload_json=payload_json,
    )
    session.add(event)
    session.flush()
    return event


def _notify_user(session: Session, *, user_id: int | None, title: str, message: str, href: str | None, source_entity_type: str, source_entity_id: int, category: str = "governance", severity: str = "medium") -> None:
    if user_id is None:
        return
    for recipient in resolve_inbox_notification_recipients(session, user_ids=[user_id]):
        create_user_inbox_notification(
            session,
            user_id=recipient.id,
            dedupe_key=f"collaboration:{source_entity_type}:{source_entity_id}:{recipient.id}:{title}",
            category=category,
            severity=severity,
            source_module="collaboration",
            source_entity_type=source_entity_type,
            source_entity_id=source_entity_id,
            title=title,
            message=message,
            href=href,
            context_json={"entity_type": source_entity_type, "entity_id": source_entity_id},
            ignore_category_preferences=False,
        )


def enforce_collaboration_entity_visibility(
    session: Session,
    *,
    entity_type: str | None,
    entity_id: int | None,
    user: User | None,
) -> None:
    """Block object-level enumeration: a user may only read/edit collaboration data for a
    table entity they are allowed to view. No-op for non-table entities or missing ids."""
    if entity_id is None:
        return
    if _normalize_entity_type(entity_type or "") != "table":
        return
    table = session.get(TableEntity, entity_id)
    if table is None:
        return
    from t2c_data.features.access_control.policy import can_view_table

    if not can_view_table(user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Acesso negado ao recurso solicitado.")


def list_collaboration_comments(
    session: Session,
    *,
    entity_type: str | None = None,
    entity_id: int | None = None,
    task_id: int | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    stmt = select(CollaborationComment).order_by(CollaborationComment.created_at.desc(), CollaborationComment.id.desc())
    if entity_type:
        stmt = stmt.where(CollaborationComment.entity_type == _normalize_entity_type(entity_type))
    if entity_id is not None:
        stmt = stmt.where(CollaborationComment.entity_id == entity_id)
    if task_id is not None:
        stmt = stmt.where(CollaborationComment.task_id == task_id)
    rows = session.scalars(stmt.limit(max(1, min(limit, 200)))).all()
    items = [_event_payload(comment) for comment in rows]
    return {"generated_at": _utcnow().isoformat(), "total": len(items), "items": items}


def list_collaboration_tasks(
    session: Session,
    *,
    entity_type: str | None = None,
    entity_id: int | None = None,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    stmt = select(CollaborationTask).order_by(CollaborationTask.created_at.desc(), CollaborationTask.id.desc())
    if entity_type:
        stmt = stmt.where(CollaborationTask.entity_type == _normalize_entity_type(entity_type))
    if entity_id is not None:
        stmt = stmt.where(CollaborationTask.entity_id == entity_id)
    if status:
        stmt = stmt.where(CollaborationTask.status == status)
    rows = session.scalars(stmt.limit(max(1, min(limit, 200)))).all()
    items = []
    for task in rows:
        comments_count = session.scalar(select(func.count()).select_from(CollaborationComment).where(CollaborationComment.task_id == task.id)) or 0
        event_count = session.scalar(select(func.count()).select_from(CollaborationEvent).where(CollaborationEvent.task_id == task.id)) or 0
        items.append(_task_payload(task, int(comments_count), int(event_count)))
    return {"generated_at": _utcnow().isoformat(), "total": len(items), "items": items}


def list_collaboration_events(
    session: Session,
    *,
    entity_type: str | None = None,
    entity_id: int | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    stmt = select(CollaborationEvent).order_by(CollaborationEvent.created_at.desc(), CollaborationEvent.id.desc())
    if entity_type:
        stmt = stmt.where(CollaborationEvent.entity_type == _normalize_entity_type(entity_type))
    if entity_id is not None:
        stmt = stmt.where(CollaborationEvent.entity_id == entity_id)
    rows = session.scalars(stmt.limit(max(1, min(limit, 200)))).all()
    items = [_event_payload_row(event) for event in rows]
    return {"generated_at": _utcnow().isoformat(), "total": len(items), "items": items}


def get_collaboration_summary(session: Session) -> dict[str, Any]:
    now = _utcnow()
    total_comments = session.scalar(select(func.count()).select_from(CollaborationComment)) or 0
    total_tasks = session.scalar(select(func.count()).select_from(CollaborationTask)) or 0
    open_tasks = session.scalar(select(func.count()).select_from(CollaborationTask).where(CollaborationTask.status.in_(["open", "in_progress", "blocked"]))) or 0
    overdue_tasks = session.scalar(
        select(func.count())
        .select_from(CollaborationTask)
        .where(
            CollaborationTask.status.in_(["open", "in_progress", "blocked"]),
            CollaborationTask.due_at.is_not(None),
            CollaborationTask.due_at < now,
        )
    ) or 0
    completed_tasks = session.scalar(select(func.count()).select_from(CollaborationTask).where(CollaborationTask.status == "done")) or 0
    recent_comments = session.scalar(
        select(func.count()).select_from(CollaborationComment).where(CollaborationComment.created_at >= now - timedelta(days=7))
    ) or 0
    recent_events = session.scalar(
        select(func.count()).select_from(CollaborationEvent).where(CollaborationEvent.created_at >= now - timedelta(days=7))
    ) or 0
    assets_without_owner = session.scalar(
        select(func.count()).select_from(DashboardAssetReadModel).where(DashboardAssetReadModel.owner_defined.is_(False))
    ) or 0
    domains_without_steward = session.scalar(
        select(func.count()).select_from(SemanticDomain).where(or_(SemanticDomain.steward.is_(None), SemanticDomain.steward == ""))
    ) or 0
    documentation_stale = session.scalar(
        select(func.count()).select_from(DashboardAssetReadModel).where(
            or_(
                DashboardAssetReadModel.description_complete.is_(False),
                DashboardAssetReadModel.dictionary_complete.is_(False),
                DashboardAssetReadModel.last_review_at.is_(None),
            )
        )
    ) or 0
    pending_governance_tasks = session.scalar(
        select(func.count()).select_from(CollaborationTask).where(
            CollaborationTask.status.in_(["open", "in_progress", "blocked"]),
            CollaborationTask.entity_type.in_(["table", "semantic_domain", "semantic_product"]),
        )
    ) or 0
    recent_task_rows = session.scalars(
        select(CollaborationTask).order_by(CollaborationTask.updated_at.desc(), CollaborationTask.id.desc()).limit(12)
    ).all()
    comments = session.scalars(
        select(CollaborationComment).order_by(CollaborationComment.created_at.desc(), CollaborationComment.id.desc()).limit(12)
    ).all()
    events = session.scalars(
        select(CollaborationEvent).order_by(CollaborationEvent.created_at.desc(), CollaborationEvent.id.desc()).limit(12)
    ).all()
    return {
        "generated_at": now,
        "total_comments": int(total_comments),
        "total_tasks": int(total_tasks),
        "open_tasks": int(open_tasks),
        "overdue_tasks": int(overdue_tasks),
        "completed_tasks": int(completed_tasks),
        "assets_without_owner": int(assets_without_owner),
        "domains_without_steward": int(domains_without_steward),
        "documentation_stale": int(documentation_stale),
        "pending_governance_tasks": int(pending_governance_tasks),
        "recent_comments": int(recent_comments),
        "recent_events": int(recent_events),
        "items": [_task_payload(task, int(session.scalar(select(func.count()).select_from(CollaborationComment).where(CollaborationComment.task_id == task.id)) or 0), int(session.scalar(select(func.count()).select_from(CollaborationEvent).where(CollaborationEvent.task_id == task.id)) or 0)) for task in recent_task_rows],
        "comments": [_event_payload(comment) for comment in comments],
        "events": [_event_payload_row(event) for event in events],
    }


def create_collaboration_comment(
    session: Session,
    *,
    payload,
    current_user: User,
    audit_kwargs: dict[str, Any] | None = None,
) -> CollaborationComment:
    entity = _resolve_entity(session, payload.entity_type, payload.entity_id)
    comment = CollaborationComment(
        entity_type=entity.entity_type,
        entity_id=entity.entity_id,
        entity_label=payload.entity_label.strip() or entity.entity_label,
        body=payload.body.strip(),
        comment_kind=(payload.comment_kind or "comment").strip() or "comment",
        task_id=payload.task_id,
        parent_comment_id=payload.parent_comment_id,
        visibility_scope=(payload.visibility_scope or "collaboration").strip() or "collaboration",
        author_user_id=current_user.id,
        author_name=_display_name(current_user),
        author_email=current_user.email,
        context_json=payload.context_json,
    )
    session.add(comment)
    session.flush()
    _record_event(
        session,
        entity_type=comment.entity_type,
        entity_id=comment.entity_id,
        event_type="comment_created",
        title="Comentário registrado",
        detail=comment.body,
        actor_user=current_user,
        comment_id=comment.id,
        task_id=comment.task_id,
        payload_json={"comment_kind": comment.comment_kind, "visibility_scope": comment.visibility_scope},
    )
    if entity.owner_user_id is not None:
        _notify_user(
            session,
            user_id=entity.owner_user_id,
            title=f"Novo comentário em {entity.entity_label}",
            message=comment.body[:180],
            href=entity.href,
            source_entity_type=comment.entity_type,
            source_entity_id=comment.entity_id,
            category="governance",
            severity="low",
        )
    write_audit_log_sync(
        session,
        action="collaboration.comment.create",
        entity_type="collaboration_comment",
        entity_id=comment.id,
        after=serialize_model(comment),
        metadata={"message": "Collaboration comment created", "entity_type": comment.entity_type},
        source_module="collaboration",
        **(audit_kwargs or {}),
    )
    session.commit()
    session.refresh(comment)
    return comment


def _link_stewardship_request(
    session: Session,
    *,
    task: CollaborationTask,
    current_user: User,
    audit_kwargs: dict[str, Any] | None = None,
) -> None:
    if task.entity_type != "table":
        return
    request_type = TASK_TYPE_TO_STEWARDSHIP.get(task.task_type)
    if request_type is None:
        return
    request_payload = StewardshipRequestCreateIn(
        table_id=task.entity_id,
        request_type=request_type,
        requester_comment=task.description or task.title,
        approver_user_id=task.assigned_to_user_id,
        request_origin="collaboration",
        description_manual=task.context_json.get("description_manual") if isinstance(task.context_json, dict) else None,
        data_owner_id=task.context_json.get("data_owner_id") if isinstance(task.context_json, dict) else None,
    )
    create_stewardship_request(
        session,
        payload=request_payload,
        user=current_user,
        audit_kwargs=audit_kwargs,
    )


def create_collaboration_task(
    session: Session,
    *,
    payload,
    current_user: User,
    audit_kwargs: dict[str, Any] | None = None,
) -> CollaborationTask:
    entity = _resolve_entity(session, payload.entity_type, payload.entity_id)
    task = CollaborationTask(
        entity_type=entity.entity_type,
        entity_id=entity.entity_id,
        entity_label=payload.entity_label.strip() or entity.entity_label,
        title=payload.title.strip(),
        description=(payload.description or "").strip() or None,
        task_type=(payload.task_type or "governance_task").strip(),
        status=(payload.status or "open").strip() or "open",
        priority=(payload.priority or "medium").strip() or "medium",
        responsibility_role=(payload.responsibility_role or None),
        assigned_to_user_id=payload.assigned_to_user_id,
        assigned_by_user_id=current_user.id,
        due_at=payload.due_at,
        linked_request_type=(payload.linked_request_type or None),
        context_json=payload.context_json,
    )
    session.add(task)
    session.flush()
    initial_comment: CollaborationComment | None = None
    comment_body = (getattr(payload, "comment", None) or "").strip()
    if comment_body:
        initial_comment = CollaborationComment(
            entity_type=task.entity_type,
            entity_id=task.entity_id,
            entity_label=task.entity_label,
            task_id=task.id,
            comment_kind="task_note",
            body=comment_body,
            visibility_scope="collaboration",
            author_user_id=current_user.id,
            author_name=_display_name(current_user),
            author_email=current_user.email,
            context_json=task.context_json,
        )
        session.add(initial_comment)
        session.flush()
    _record_event(
        session,
        entity_type=task.entity_type,
        entity_id=task.entity_id,
        event_type="task_created",
        title=task.title,
        detail=task.description,
        actor_user=current_user,
        task_id=task.id,
        payload_json={"task_type": task.task_type, "priority": task.priority, "responsibility_role": task.responsibility_role},
    )
    if initial_comment is not None:
        _record_event(
            session,
            entity_type=task.entity_type,
            entity_id=task.entity_id,
            event_type="comment_created",
            title="Comentário da tarefa",
            detail=initial_comment.body,
            actor_user=current_user,
            comment_id=initial_comment.id,
            task_id=task.id,
            payload_json={"comment_kind": initial_comment.comment_kind, "visibility_scope": initial_comment.visibility_scope},
        )
    notify_user_id = task.assigned_to_user_id if task.assigned_to_user_id is not None else entity.owner_user_id
    if notify_user_id is not None:
        _notify_user(
            session,
            user_id=notify_user_id,
            title=f"Nova tarefa: {task.title}",
            message=task.description or f"Tarefa pendente em {task.entity_label}.",
            href=entity.href,
            source_entity_type=task.entity_type,
            source_entity_id=task.entity_id,
            category="governance",
            severity="medium" if task.priority != "high" else "high",
        )
    if task.linked_request_type:
        _link_stewardship_request(session, task=task, current_user=current_user, audit_kwargs=audit_kwargs)
    write_audit_log_sync(
        session,
        action="collaboration.task.create",
        entity_type="collaboration_task",
        entity_id=task.id,
        after=serialize_model(task),
        metadata={"message": "Collaboration task created", "entity_type": task.entity_type},
        source_module="collaboration",
        **(audit_kwargs or {}),
    )
    session.commit()
    session.refresh(task)
    return task


def update_collaboration_task(
    session: Session,
    *,
    task_id: int,
    payload,
    current_user: User,
    audit_kwargs: dict[str, Any] | None = None,
) -> CollaborationTask:
    task = session.get(CollaborationTask, task_id)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tarefa não encontrada.")
    enforce_collaboration_entity_visibility(
        session, entity_type=task.entity_type, entity_id=task.entity_id, user=current_user
    )
    before = serialize_model(task)
    status_before = task.status
    fields_set = set(getattr(payload, "model_fields_set", set()))
    if "title" in fields_set and payload.title is not None:
        task.title = payload.title.strip()
    if "description" in fields_set:
        task.description = (payload.description or "").strip() or None
    if "task_type" in fields_set and payload.task_type is not None:
        task.task_type = payload.task_type.strip()
    if "status" in fields_set and payload.status is not None:
        task.status = payload.status.strip()
    if "priority" in fields_set and payload.priority is not None:
        task.priority = payload.priority.strip()
    if "responsibility_role" in fields_set:
        task.responsibility_role = payload.responsibility_role
    if "assigned_to_user_id" in fields_set:
        task.assigned_to_user_id = payload.assigned_to_user_id
    if "due_at" in fields_set:
        task.due_at = payload.due_at
    if "context_json" in fields_set:
        task.context_json = payload.context_json
    if task.status == "done" and task.completed_at is None:
        task.completed_at = _utcnow()
        task.completed_by_user_id = current_user.id
    if task.status != "done":
        task.completed_at = None
        task.completed_by_user_id = None
    session.flush()
    status_changed = status_before != task.status
    _record_event(
        session,
        entity_type=task.entity_type,
        entity_id=task.entity_id,
        event_type="task_status_changed" if status_changed else "task_updated",
        title=task.title,
        detail=task.description,
        actor_user=current_user,
        status_from=status_before if status_changed else None,
        status_to=task.status if status_changed else None,
        task_id=task.id,
        payload_json={"task_type": task.task_type, "priority": task.priority, "responsibility_role": task.responsibility_role},
    )
    if task.assigned_to_user_id is not None and task.assigned_to_user_id != before.get("assigned_to_user_id"):
        _notify_user(
            session,
            user_id=task.assigned_to_user_id,
            title=f"Tarefa atribuída: {task.title}",
            message=task.description or f"Tarefa pendente em {task.entity_label}.",
            href=None,
            source_entity_type=task.entity_type,
            source_entity_id=task.entity_id,
            category="governance",
            severity="medium" if task.priority != "high" else "high",
        )
    write_audit_log_sync(
        session,
        action="collaboration.task.update",
        entity_type="collaboration_task",
        entity_id=task.id,
        before=before,
        after=serialize_model(task),
        metadata={"message": "Collaboration task updated"},
        source_module="collaboration",
        **(audit_kwargs or {}),
    )
    session.commit()
    session.refresh(task)
    return task


def get_collaboration_timeline(
    session: Session,
    *,
    entity_type: str | None = None,
    entity_id: int | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return list_collaboration_events(session, entity_type=entity_type, entity_id=entity_id, limit=limit)


def build_collaboration_activity_summary(session: Session) -> dict[str, Any]:
    return get_collaboration_summary(session)
