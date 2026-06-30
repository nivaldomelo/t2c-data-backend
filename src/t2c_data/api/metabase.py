from __future__ import annotations

from math import ceil

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.metabase import (
    create_metabase_instance,
    enqueue_metabase_instance_sync,
    get_metabase_instance,
    get_table_metabase_consumption,
    list_metabase_instances,
    list_metabase_sync_runs,
    serialize_metabase_instance,
    update_metabase_instance,
)
from t2c_data.models.metabase import MetabaseInstance, MetabaseSyncRun
from t2c_data.models.auth import User
from t2c_data.schemas.metabase import (
    MetabaseConsumptionSummaryOut,
    MetabaseInstanceCreate,
    MetabaseInstanceOut,
    MetabaseInstanceUpdate,
    MetabaseSyncRunOut,
)
from t2c_data.schemas.pagination import PageOut

router = APIRouter(prefix="/metabase", tags=["metabase"])


@router.get("/instances", response_model=PageOut[MetabaseInstanceOut])
def metabase_instances(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[MetabaseInstanceOut]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 25), 1)
    total = int(db.scalar(select(func.count(MetabaseInstance.id))) or 0)
    items = list_metabase_instances(
        db,
        offset=(normalized_page - 1) * normalized_page_size,
        limit=normalized_page_size,
    )
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    return PageOut[MetabaseInstanceOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=items,
    )


@router.post("/instances", response_model=MetabaseInstanceOut, status_code=status.HTTP_201_CREATED)
def create_metabase_instance_route(
    payload: MetabaseInstanceCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> MetabaseInstanceOut:
    instance = create_metabase_instance(db, payload)
    db.commit()
    return serialize_metabase_instance(instance)


@router.get("/instances/{instance_id}", response_model=MetabaseInstanceOut)
def get_metabase_instance_route(
    instance_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> MetabaseInstanceOut:
    try:
        instance = get_metabase_instance(db, instance_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Metabase instance not found") from exc
    return serialize_metabase_instance(instance)


@router.patch("/instances/{instance_id}", response_model=MetabaseInstanceOut)
def update_metabase_instance_route(
    instance_id: int,
    payload: MetabaseInstanceUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> MetabaseInstanceOut:
    try:
        instance = get_metabase_instance(db, instance_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Metabase instance not found") from exc
    updated = update_metabase_instance(db, instance, payload)
    db.commit()
    return serialize_metabase_instance(updated)


@router.post("/instances/{instance_id}/sync", response_model=MetabaseSyncRunOut)
def sync_metabase_instance_route(
    instance_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> MetabaseSyncRunOut:
    try:
        result = enqueue_metabase_instance_sync(db, instance_id, current_user=current_user)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Metabase instance not found") from exc
    return result


@router.get("/instances/{instance_id}/sync-runs", response_model=PageOut[MetabaseSyncRunOut])
def list_metabase_sync_runs_route(
    instance_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[MetabaseSyncRunOut]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 25), 1)
    total = int(
        db.scalar(
            select(func.count(MetabaseSyncRun.id)).where(MetabaseSyncRun.instance_id == instance_id)
        )
        or 0
    )
    items = list_metabase_sync_runs(
        db,
        instance_id,
        offset=(normalized_page - 1) * normalized_page_size,
        limit=normalized_page_size,
    )
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    return PageOut[MetabaseSyncRunOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=items,
    )


@router.get("/tables/{table_id}/consumption", response_model=MetabaseConsumptionSummaryOut)
def metabase_table_consumption_route(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> MetabaseConsumptionSummaryOut:
    return get_table_metabase_consumption(db, table_id)
