from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CollaborationCommentIn(BaseModel):
    entity_type: str = Field(min_length=1, max_length=40)
    entity_id: int = Field(ge=1)
    entity_label: str = Field(min_length=1, max_length=255)
    body: str = Field(min_length=1)
    comment_kind: str = Field(default="comment", max_length=40)
    task_id: int | None = Field(default=None, ge=1)
    parent_comment_id: int | None = Field(default=None, ge=1)
    visibility_scope: str = Field(default="collaboration", max_length=24)
    context_json: dict[str, Any] | None = None


class CollaborationCommentOut(BaseModel):
    id: int
    entity_type: str
    entity_id: int
    entity_label: str
    body: str
    comment_kind: str
    task_id: int | None = None
    parent_comment_id: int | None = None
    visibility_scope: str
    is_resolved: bool = False
    resolved_at: datetime | None = None
    author_user_id: int | None = None
    author_name: str | None = None
    author_email: str | None = None
    resolved_by_user_id: int | None = None
    context_json: dict[str, Any] | list | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CollaborationTaskIn(BaseModel):
    entity_type: str = Field(min_length=1, max_length=40)
    entity_id: int = Field(ge=1)
    entity_label: str = Field(min_length=1, max_length=255)
    title: str = Field(min_length=1, max_length=220)
    description: str | None = None
    task_type: str = Field(default="governance_task", max_length=60)
    status: str = Field(default="open", max_length=24)
    priority: str = Field(default="medium", max_length=20)
    responsibility_role: str | None = Field(default=None, max_length=80)
    assigned_to_user_id: int | None = Field(default=None, ge=1)
    due_at: datetime | None = None
    linked_request_type: str | None = Field(default=None, max_length=40)
    context_json: dict[str, Any] | None = None
    comment: str | None = None


class CollaborationTaskUpdateIn(BaseModel):
    title: str | None = Field(default=None, max_length=220)
    description: str | None = None
    task_type: str | None = Field(default=None, max_length=60)
    status: str | None = Field(default=None, max_length=24)
    priority: str | None = Field(default=None, max_length=20)
    responsibility_role: str | None = Field(default=None, max_length=80)
    assigned_to_user_id: int | None = Field(default=None, ge=1)
    due_at: datetime | None = None
    context_json: dict[str, Any] | None = None


class CollaborationTaskOut(BaseModel):
    id: int
    entity_type: str
    entity_id: int
    entity_label: str
    title: str
    description: str | None = None
    task_type: str
    status: str
    priority: str
    responsibility_role: str | None = None
    assigned_to_user_id: int | None = None
    assigned_by_user_id: int | None = None
    due_at: datetime | None = None
    completed_at: datetime | None = None
    completed_by_user_id: int | None = None
    linked_request_type: str | None = None
    context_json: dict[str, Any] | list | None = None
    comments_count: int = 0
    event_count: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CollaborationEventOut(BaseModel):
    id: int
    entity_type: str
    entity_id: int
    event_type: str
    title: str
    detail: str | None = None
    status_from: str | None = None
    status_to: str | None = None
    actor_user_id: int | None = None
    actor_name: str | None = None
    actor_email: str | None = None
    comment_id: int | None = None
    task_id: int | None = None
    payload_json: dict[str, Any] | list | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CollaborationCommentListOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[CollaborationCommentOut] = Field(default_factory=list)


class CollaborationTaskListOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[CollaborationTaskOut] = Field(default_factory=list)


class CollaborationEventListOut(BaseModel):
    generated_at: datetime
    total: int = 0
    items: list[CollaborationEventOut] = Field(default_factory=list)


class CollaborationSummaryOut(BaseModel):
    generated_at: datetime
    total_comments: int = 0
    total_tasks: int = 0
    open_tasks: int = 0
    overdue_tasks: int = 0
    completed_tasks: int = 0
    assets_without_owner: int = 0
    domains_without_steward: int = 0
    documentation_stale: int = 0
    pending_governance_tasks: int = 0
    recent_comments: int = 0
    recent_events: int = 0
    items: list[CollaborationTaskOut] = Field(default_factory=list)
    comments: list[CollaborationCommentOut] = Field(default_factory=list)
    events: list[CollaborationEventOut] = Field(default_factory=list)
