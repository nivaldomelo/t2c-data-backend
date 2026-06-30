from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.lineage.application import (
    create_lineage_source_with_audit,
    run_lineage_source_sync,
    run_lineage_table_sync,
    update_lineage_source_with_audit,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.features.lineage.source_configs import list_source_statuses, serialize_source_config
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import (
    LineageSourceConfigCreate,
    LineageSourceConfigOut,
    LineageSourceConfigUpdate,
    LineageSourceSyncIn,
    LineageSourceSyncOut,
    LineageSourceStatusOut,
)
from t2c_data.schemas.pagination import PageOut

router = APIRouter(tags=["lineage"])


@router.get("/sources", response_model=PageOut[LineageSourceStatusOut])
def get_lineage_sources(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[LineageSourceStatusOut]:
    return paginate_items(list_source_statuses(db), page=page, page_size=page_size)


@router.post("/sources", response_model=LineageSourceConfigOut, status_code=status.HTTP_201_CREATED)
def create_lineage_source(
    payload: LineageSourceConfigCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
) -> LineageSourceConfigOut:
    source = create_lineage_source_with_audit(db=db, payload=payload, user=user)
    return serialize_source_config(source, current_user=user)


@router.patch("/sources/{source_id}", response_model=LineageSourceConfigOut)
def patch_lineage_source(
    source_id: int,
    payload: LineageSourceConfigUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
) -> LineageSourceConfigOut:
    updated = update_lineage_source_with_audit(db=db, source_id=source_id, payload=payload, user=user)
    return serialize_source_config(updated, current_user=user)


@router.post("/sources/{source_id}/sync", response_model=LineageSourceSyncOut)
def sync_lineage_source(
    source_id: int,
    payload: LineageSourceSyncIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
) -> LineageSourceSyncOut:
    return run_lineage_source_sync(
        db=db,
        source_id=source_id,
        namespace=payload.namespace,
        node_id=payload.node_id,
        depth=payload.depth,
        table_id=payload.table_id,
        user=user,
    )


@router.post("/tables/{table_id}/sync", response_model=LineageSourceSyncOut)
def sync_lineage_for_table(
    table_id: int,
    payload: LineageSourceSyncIn | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin")),
) -> LineageSourceSyncOut:
    sync_payload = payload or LineageSourceSyncIn()
    return run_lineage_table_sync(db=db, table_id=table_id, depth=sync_payload.depth, user=user)
