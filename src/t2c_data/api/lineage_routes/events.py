from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.lineage.openlineage_sync import (
    ingest_openlineage_event,
    ingest_openlineage_events_bulk,
    rebuild_openlineage_source,
    rebuild_openlineage_source_for_table,
)
from t2c_data.features.lineage.source_configs import get_source_config, list_source_configs
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import LineageEventBulkIn, LineageEventIn, LineageEventIngestionOut, LineageEventsBulkOut, LineageRebuildIn, LineageSourceSyncOut

router = APIRouter(tags=["lineage"])


@router.post("/events", response_model=LineageEventIngestionOut, status_code=status.HTTP_201_CREATED)
def post_lineage_event(
    payload: LineageEventIn,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> LineageEventIngestionOut:
    return ingest_openlineage_event(db, payload=payload)


@router.post("/events/bulk", response_model=LineageEventsBulkOut, status_code=status.HTTP_201_CREATED)
def post_lineage_events_bulk(
    payload: LineageEventBulkIn,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> LineageEventsBulkOut:
    return ingest_openlineage_events_bulk(db, payload=payload)


@router.post("/rebuild", response_model=LineageSourceSyncOut)
def rebuild_lineage(
    payload: LineageRebuildIn,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> LineageSourceSyncOut:
    if payload.table_id is not None:
        return rebuild_openlineage_source_for_table(db, table_id=payload.table_id, depth=payload.depth)
    if payload.source_id is not None:
        source = get_source_config(db, payload.source_id)
        return rebuild_openlineage_source(
            db,
            source=source,
            depth=payload.depth,
            namespace=payload.namespace,
            node_id=payload.node_id,
            table_id=payload.table_id,
        )
    source = next((item for item in list_source_configs(db) if item.source_type == "openlineage" and item.enabled), None)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No lineage source configured")
    return rebuild_openlineage_source(
        db,
        source=source,
        depth=payload.depth,
        namespace=payload.namespace,
        node_id=payload.node_id,
        table_id=payload.table_id,
    )
