from __future__ import annotations

from fastapi import APIRouter, Depends, Request, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.collaboration.service import (
    build_collaboration_activity_summary,
    create_collaboration_comment,
    create_collaboration_task,
    enforce_collaboration_entity_visibility,
    get_collaboration_timeline,
    list_collaboration_comments,
    list_collaboration_events,
    list_collaboration_tasks,
    update_collaboration_task,
)
from t2c_data.models.auth import User
from t2c_data.schemas.collaboration import (
    CollaborationCommentIn,
    CollaborationCommentListOut,
    CollaborationCommentOut,
    CollaborationEventListOut,
    CollaborationSummaryOut,
    CollaborationTaskIn,
    CollaborationTaskListOut,
    CollaborationTaskOut,
    CollaborationTaskUpdateIn,
)
from t2c_data.services.audit import request_audit_kwargs

router = APIRouter(prefix="/collaboration", tags=["collaboration"])


@router.get("/summary", response_model=CollaborationSummaryOut)
def collaboration_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer", "stewardship", "data_owner")),
) -> CollaborationSummaryOut:
    return CollaborationSummaryOut(**build_collaboration_activity_summary(db))


@router.get("/tasks", response_model=CollaborationTaskListOut)
def collaboration_tasks(
    entity_type: str | None = None,
    entity_id: int | None = None,
    status: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer", "stewardship", "data_owner")),
) -> CollaborationTaskListOut:
    enforce_collaboration_entity_visibility(db, entity_type=entity_type, entity_id=entity_id, user=current_user)
    return CollaborationTaskListOut(**list_collaboration_tasks(db, entity_type=entity_type, entity_id=entity_id, status=status, limit=limit))


@router.get("/comments", response_model=CollaborationCommentListOut)
def collaboration_comments(
    entity_type: str | None = None,
    entity_id: int | None = None,
    task_id: int | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer", "stewardship", "data_owner")),
) -> CollaborationCommentListOut:
    enforce_collaboration_entity_visibility(db, entity_type=entity_type, entity_id=entity_id, user=current_user)
    return CollaborationCommentListOut(
        **list_collaboration_comments(db, entity_type=entity_type, entity_id=entity_id, task_id=task_id, limit=limit)
    )


@router.get("/events", response_model=CollaborationEventListOut)
def collaboration_events(
    entity_type: str | None = None,
    entity_id: int | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer", "stewardship", "data_owner")),
) -> CollaborationEventListOut:
    enforce_collaboration_entity_visibility(db, entity_type=entity_type, entity_id=entity_id, user=current_user)
    return CollaborationEventListOut(**list_collaboration_events(db, entity_type=entity_type, entity_id=entity_id, limit=limit))


@router.get("/timeline", response_model=CollaborationEventListOut)
def collaboration_timeline(
    entity_type: str | None = None,
    entity_id: int | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer", "stewardship", "data_owner")),
) -> CollaborationEventListOut:
    enforce_collaboration_entity_visibility(db, entity_type=entity_type, entity_id=entity_id, user=current_user)
    return CollaborationEventListOut(**get_collaboration_timeline(db, entity_type=entity_type, entity_id=entity_id, limit=limit))


@router.post("/comments", response_model=CollaborationCommentOut, status_code=status.HTTP_201_CREATED)
def collaboration_create_comment(
    payload: CollaborationCommentIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> CollaborationCommentOut:
    comment = create_collaboration_comment(
        db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return CollaborationCommentOut.model_validate(comment, from_attributes=True)


@router.post("/tasks", response_model=CollaborationTaskOut, status_code=status.HTTP_201_CREATED)
def collaboration_create_task(
    payload: CollaborationTaskIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> CollaborationTaskOut:
    task = create_collaboration_task(
        db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return CollaborationTaskOut.model_validate(task, from_attributes=True)


@router.patch("/tasks/{task_id}", response_model=CollaborationTaskOut)
def collaboration_update_task(
    task_id: int,
    payload: CollaborationTaskUpdateIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> CollaborationTaskOut:
    task = update_collaboration_task(
        db,
        task_id=task_id,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return CollaborationTaskOut.model_validate(task, from_attributes=True)
