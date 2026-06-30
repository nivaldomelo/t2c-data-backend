from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.lineage.api_support import get_asset_or_404
from t2c_data.features.lineage.column_edges import serialize_column_edge
from t2c_data.features.lineage.table_summary import get_asset_summary, get_table_summary
from t2c_data.features.lineage.visibility import asset_visible_to_user
from t2c_data.features.pagination import paginate_items
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageAsset, LineageColumnEdge
from t2c_data.schemas.lineage import LineageAssetRefOut, LineageAssetSummaryOut, LineageColumnEdgeOut, LineageGraphOut, LineageImpactOut, LineageJobSummaryOut
from t2c_data.schemas.pagination import PageOut

router = APIRouter(tags=["lineage"])


@router.get("/graph", response_model=LineageGraphOut)
def get_lineage_graph(
    asset_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    max_relations: int | None = Query(default=None, ge=1, le=1000),
    max_depth: int = Query(default=1, ge=1, le=10),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageGraphOut:
    if asset_id is not None:
        summary = get_asset_summary(db, asset_id, current_user=current_user, max_relations=max_relations, max_depth=max_depth)
    elif table_id is not None:
        summary = get_table_summary(db, table_id, current_user=current_user, max_relations=max_relations, max_depth=max_depth)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asset_id or table_id is required")
    return LineageGraphOut(summary=summary, nodes=summary.graph_nodes, edges=summary.graph_edges)


@router.get("/upstream", response_model=PageOut[LineageAssetRefOut])
def get_lineage_upstream(
    asset_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    max_depth: int = Query(default=1, ge=1, le=10),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[LineageAssetRefOut]:
    if asset_id is not None:
        summary = get_asset_summary(db, asset_id, current_user=current_user, max_depth=max_depth)
    elif table_id is not None:
        summary = get_table_summary(db, table_id, current_user=current_user, max_depth=max_depth)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asset_id or table_id is required")
    return paginate_items(summary.upstream, page=page, page_size=page_size)


@router.get("/downstream", response_model=PageOut[LineageAssetRefOut])
def get_lineage_downstream(
    asset_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    max_depth: int = Query(default=1, ge=1, le=10),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[LineageAssetRefOut]:
    if asset_id is not None:
        summary = get_asset_summary(db, asset_id, current_user=current_user, max_depth=max_depth)
    elif table_id is not None:
        summary = get_table_summary(db, table_id, current_user=current_user, max_depth=max_depth)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asset_id or table_id is required")
    return paginate_items(summary.downstream, page=page, page_size=page_size)


@router.get("/impact", response_model=LineageImpactOut)
def get_lineage_impact(
    asset_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    max_depth: int = Query(default=1, ge=1, le=10),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageImpactOut:
    if asset_id is not None:
        summary = get_asset_summary(db, asset_id, current_user=current_user, max_depth=max_depth)
    elif table_id is not None:
        summary = get_table_summary(db, table_id, current_user=current_user, max_depth=max_depth)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asset_id or table_id is required")
    return summary.impact


@router.get("/columns", response_model=PageOut[LineageColumnEdgeOut])
def get_lineage_columns(
    asset_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[LineageColumnEdgeOut]:
    if asset_id is not None:
        asset = get_asset_or_404(db, asset_id, current_user=current_user)
    elif table_id is not None:
        asset = db.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == table_id))
        if not asset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage asset not found")
        asset = get_asset_or_404(db, asset.id, current_user=current_user)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asset_id or table_id is required")
    edges = db.scalars(
        select(LineageColumnEdge)
        .options(selectinload(LineageColumnEdge.source_asset), selectinload(LineageColumnEdge.target_asset))
        .where(LineageColumnEdge.is_active.is_(True))
        .where(
            (LineageColumnEdge.source_asset_id == asset.id)
            | (LineageColumnEdge.target_asset_id == asset.id)
        )
        .order_by(LineageColumnEdge.updated_at.desc(), LineageColumnEdge.id.desc())
    ).all()
    if current_user is not None:
        edges = [
            edge
            for edge in edges
            if asset_visible_to_user(db, current_user, edge.source_asset)
            and asset_visible_to_user(db, current_user, edge.target_asset)
        ]
    return paginate_items([serialize_column_edge(edge, focus_asset=asset) for edge in edges], page=page, page_size=page_size)


@router.get("/runs", response_model=PageOut[LineageJobSummaryOut])
def get_lineage_runs(
    asset_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    max_depth: int = Query(default=1, ge=1, le=10),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[LineageJobSummaryOut]:
    if asset_id is not None:
        summary = get_asset_summary(db, asset_id, current_user=current_user, max_depth=max_depth)
    elif table_id is not None:
        summary = get_table_summary(db, table_id, current_user=current_user, max_depth=max_depth)
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asset_id or table_id is required")
    return paginate_items(summary.related_jobs, page=page, page_size=page_size)
