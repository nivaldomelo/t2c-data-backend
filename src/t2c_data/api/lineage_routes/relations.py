from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.lineage.api_support import get_relation_or_404
from t2c_data.features.lineage.application import (
    create_manual_relation_with_audit,
    deactivate_manual_relation_with_audit,
    update_manual_relation_with_audit,
)
from t2c_data.features.lineage.queries import list_relations_out
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageRelationVersion
from t2c_data.schemas.lineage import (
    LineageRelationCreate,
    LineageRelationListOut,
    LineageRelationOut,
    LineageRelationUpdate,
    LineageRelationVersionOut,
)

router = APIRouter(tags=["lineage"])

# Manual lineage entry has been discontinued: lineage is built automatically from
# OpenLineage events and detected consumption (e.g. Metabase). Reads stay available.
_MANUAL_DISABLED_DETAIL = (
    "Linhagem manual foi descontinuada. A linhagem é construída automaticamente "
    "via OpenLineage e consumo detectado."
)


def _manual_lineage_disabled() -> None:
    raise HTTPException(status_code=status.HTTP_410_GONE, detail=_MANUAL_DISABLED_DETAIL)


@router.get("/edges/manual", response_model=LineageRelationListOut)
def get_manual_relations(
    q: str | None = Query(default=None),
    layer: str | None = Query(default=None),
    asset_type: str | None = Query(default=None),
    relation_type: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    process: str | None = Query(default=None),
    dashboard: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=120, ge=1, le=500),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageRelationListOut:
    return list_relations_out(
        db,
        current_user=current_user,
        query=q,
        layer=layer,
        asset_type=asset_type,
        relation_type=relation_type,
        origin=origin,
        status=status_filter,
        process_name=process,
        dashboard_name=dashboard,
        page=page,
        page_size=page_size,
    )


@router.post("/edges/manual", response_model=LineageRelationOut, status_code=status.HTTP_201_CREATED)
def create_manual_relation(
    payload: LineageRelationCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageRelationOut:
    _manual_lineage_disabled()
    return create_manual_relation_with_audit(db=db, payload=payload, user=user)


@router.patch("/edges/manual/{relation_id}", response_model=LineageRelationOut)
def patch_manual_relation(
    relation_id: int,
    payload: LineageRelationUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageRelationOut:
    _manual_lineage_disabled()
    relation = get_relation_or_404(db, relation_id, current_user=user)
    return update_manual_relation_with_audit(db=db, relation=relation, payload=payload, user=user)


@router.delete("/edges/manual/{relation_id}", response_model=dict[str, bool])
def deactivate_manual_relation(
    relation_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, bool]:
    _manual_lineage_disabled()
    relation = get_relation_or_404(db, relation_id, current_user=user)
    return deactivate_manual_relation_with_audit(db=db, relation=relation, user=user)


@router.get("/edges/manual/{relation_id}/versions", response_model=list[LineageRelationVersionOut])
def get_manual_relation_versions(
    relation_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[LineageRelationVersionOut]:
    relation = get_relation_or_404(db, relation_id, current_user=user)
    versions = db.scalars(
        select(LineageRelationVersion)
        .where(LineageRelationVersion.lineage_relation_id == relation.id)
        .order_by(LineageRelationVersion.version_number.desc(), LineageRelationVersion.id.desc())
    ).all()
    return [LineageRelationVersionOut.model_validate(version, from_attributes=True) for version in versions]
