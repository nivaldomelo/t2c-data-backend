from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.lineage.api_support import get_asset_or_404, relation_items_for_asset, serialize_assets_out
from t2c_data.features.lineage.application import (
    build_table_lineage_document,
    create_lineage_asset_with_audit,
    ensure_lineage_asset_from_table_with_audit,
    update_lineage_asset_with_audit,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.features.lineage.queries import (
    get_asset_summary,
    get_table_summary,
    list_asset_candidates,
    list_assets,
)
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import (
    LineageAssetCandidateOut,
    LineageAssetCreate,
    LineageAssetOut,
    LineageAssetSummaryOut,
    LineageAssetUpdate,
    LineageRelationOut,
    TableLineageOut,
)
from t2c_data.schemas.pagination import PageOut

router = APIRouter(tags=["lineage"])


@router.get("/assets", response_model=PageOut[LineageAssetOut])
def get_assets(
    q: str | None = Query(default=None),
    asset_type: str | None = Query(default=None),
    layer: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[LineageAssetOut]:
    return paginate_items(
        serialize_assets_out(
            list_assets(db, current_user=current_user, query=q, asset_type=asset_type, layer=layer, status=status_filter)
        ),
        page=page,
        page_size=page_size,
    )


@router.get("/assets/candidates", response_model=list[LineageAssetCandidateOut])
def get_asset_candidates(
    q: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[LineageAssetCandidateOut]:
    return list_asset_candidates(db, current_user=current_user, query=q, limit=limit)


@router.post("/assets", response_model=LineageAssetOut, status_code=status.HTTP_201_CREATED)
def create_lineage_asset(
    payload: LineageAssetCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageAssetOut:
    asset = create_lineage_asset_with_audit(db=db, payload=payload, user=user)
    return LineageAssetOut.model_validate(asset, from_attributes=True)


@router.post("/assets/from-table/{table_id}", response_model=LineageAssetOut, status_code=status.HTTP_201_CREATED)
def create_asset_from_table(
    table_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageAssetOut:
    asset = ensure_lineage_asset_from_table_with_audit(db=db, table_id=table_id, user=user)
    return LineageAssetOut.model_validate(asset, from_attributes=True)


@router.get("/assets/{asset_id}", response_model=LineageAssetOut)
def get_asset(
    asset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageAssetOut:
    asset = get_asset_or_404(db, asset_id, current_user=current_user)
    return LineageAssetOut.model_validate(asset, from_attributes=True)


@router.patch("/assets/{asset_id}", response_model=LineageAssetOut)
def patch_asset(
    asset_id: int,
    payload: LineageAssetUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageAssetOut:
    asset = get_asset_or_404(db, asset_id, current_user=user)
    updated = update_lineage_asset_with_audit(db=db, asset=asset, payload=payload, user=user)
    return LineageAssetOut.model_validate(updated, from_attributes=True)


@router.get("/assets/{asset_id}/upstream", response_model=list[LineageRelationOut])
def get_asset_upstream(
    asset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[LineageRelationOut]:
    return relation_items_for_asset(db, asset_id, direction="upstream", current_user=current_user)


@router.get("/assets/{asset_id}/downstream", response_model=list[LineageRelationOut])
def get_asset_downstream(
    asset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[LineageRelationOut]:
    return relation_items_for_asset(db, asset_id, direction="downstream", current_user=current_user)


@router.get("/assets/{asset_id}/impact", response_model=LineageAssetSummaryOut)
def get_asset_impact(
    asset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageAssetSummaryOut:
    return get_asset_summary(db, asset_id, current_user=current_user)


@router.get("/assets/{asset_id}/summary", response_model=LineageAssetSummaryOut)
def get_asset_summary_route(
    asset_id: int,
    max_relations: int | None = Query(default=None, ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageAssetSummaryOut:
    return get_asset_summary(db, asset_id, current_user=current_user, max_relations=max_relations)


@router.get("/tables/{table_id}/summary", response_model=LineageAssetSummaryOut)
def get_table_summary_route(
    table_id: int,
    max_relations: int | None = Query(default=None, ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageAssetSummaryOut:
    return get_table_summary(db, table_id, current_user=current_user, max_relations=max_relations)


@router.get("/tables/{table_id}/graph", response_model=LineageAssetSummaryOut)
def get_table_graph_route(
    table_id: int,
    max_relations: int | None = Query(default=None, ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageAssetSummaryOut:
    return get_table_summary(db, table_id, current_user=current_user, max_relations=max_relations)


@router.get("/tables/{table_id}", response_model=TableLineageOut)
def get_table_lineage_document(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableLineageOut:
    return build_table_lineage_document(db=db, table_id=table_id, current_user=current_user)
