from __future__ import annotations

from math import ceil

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.operations.backups import list_backups, run_backup
from t2c_data.features.operations.failures import failure_summary
from t2c_data.models.operations import BackupExecution, OperationalFailureEvent, OperationalFailureTaxonomy
from t2c_data.models.auth import User
from t2c_data.schemas.operations import (
    BackupExecutionOut,
    OperationalFailureEventOut,
    OperationalFailureTaxonomyOut,
)
from t2c_data.schemas.pagination import PageOut


router = APIRouter(prefix="/operations", tags=["operations"])


@router.get("/backups", response_model=PageOut[BackupExecutionOut])
def list_backup_executions(
    scope: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[BackupExecutionOut]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 25), 1)
    count_stmt = select(func.count(BackupExecution.id))
    if scope:
        count_stmt = count_stmt.where(BackupExecution.scope == scope)
    total = int(db.scalar(count_stmt) or 0)
    items = [
        BackupExecutionOut.model_validate(item)
        for item in list_backups(
            db,
            scope=scope,
            offset=(normalized_page - 1) * normalized_page_size,
            limit=normalized_page_size,
        )
    ]
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    return PageOut[BackupExecutionOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=items,
    )


@router.post("/backups/run", response_model=BackupExecutionOut)
def run_backup_execution(
    scope: str = Query(default="platform"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> BackupExecutionOut:
    record = run_backup(
        db,
        scope=scope,
        triggered_by_user_id=current_user.id,
        trigger_source="manual",
    )
    return BackupExecutionOut.model_validate(record)


@router.get("/failures", response_model=PageOut[OperationalFailureEventOut])
def list_operational_failures(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[OperationalFailureEventOut]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 25), 1)
    total = int(db.scalar(select(func.count(OperationalFailureEvent.id))) or 0)
    rows = db.scalars(
        select(OperationalFailureEvent)
        .order_by(OperationalFailureEvent.occurred_at.desc(), OperationalFailureEvent.id.desc())
        .offset((normalized_page - 1) * normalized_page_size)
        .limit(normalized_page_size)
    ).all()
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    return PageOut[OperationalFailureEventOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=[OperationalFailureEventOut.model_validate(item) for item in rows],
    )


@router.get("/failures/summary", response_model=dict[str, object])
def operational_failure_summary(
    limit: int = Query(default=30, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> dict[str, object]:
    return failure_summary(db, limit=limit)


@router.get("/failures/taxonomy", response_model=PageOut[OperationalFailureTaxonomyOut])
def operational_failure_taxonomy(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[OperationalFailureTaxonomyOut]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 25), 1)
    total = int(db.scalar(select(func.count(OperationalFailureTaxonomy.code))) or 0)
    rows = db.scalars(
        select(OperationalFailureTaxonomy)
        .order_by(OperationalFailureTaxonomy.code)
        .offset((normalized_page - 1) * normalized_page_size)
        .limit(normalized_page_size)
    ).all()
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    return PageOut[OperationalFailureTaxonomyOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=[OperationalFailureTaxonomyOut.model_validate(item) for item in rows],
    )
