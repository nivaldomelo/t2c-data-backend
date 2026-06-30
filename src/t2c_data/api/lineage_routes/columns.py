from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.lineage.api_support import get_column_edge_or_404
from t2c_data.features.lineage.column_actions import (
    create_or_update_manual_column_edge_with_audit,
    deactivate_manual_column_edge_with_audit,
    update_manual_column_edge_with_audit,
)
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageColumnEdgeVersion
from t2c_data.schemas.lineage import (
    LineageColumnEdgeCreate,
    LineageColumnEdgeOut,
    LineageColumnEdgeUpdate,
    LineageColumnEdgeVersionOut,
)

router = APIRouter(tags=["lineage"])

# Manual column lineage has been discontinued (lineage is automatic). Reads stay available.
_MANUAL_DISABLED_DETAIL = (
    "Linhagem manual foi descontinuada. A linhagem é construída automaticamente "
    "via OpenLineage e consumo detectado."
)


def _manual_lineage_disabled() -> None:
    raise HTTPException(status_code=status.HTTP_410_GONE, detail=_MANUAL_DISABLED_DETAIL)


@router.post("/columns/manual", response_model=LineageColumnEdgeOut, status_code=status.HTTP_201_CREATED)
def create_manual_column_edge(
    payload: LineageColumnEdgeCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageColumnEdgeOut:
    _manual_lineage_disabled()
    return create_or_update_manual_column_edge_with_audit(db=db, payload=payload, user=user)


@router.patch("/columns/manual/{edge_id}", response_model=LineageColumnEdgeOut)
def patch_manual_column_edge(
    edge_id: int,
    payload: LineageColumnEdgeUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageColumnEdgeOut:
    _manual_lineage_disabled()
    edge = get_column_edge_or_404(db, edge_id, user=user)
    return update_manual_column_edge_with_audit(db=db, edge=edge, payload=payload, user=user)


@router.delete("/columns/manual/{edge_id}", response_model=dict[str, bool])
def deactivate_manual_column_edge(
    edge_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, bool]:
    _manual_lineage_disabled()
    edge = get_column_edge_or_404(db, edge_id, user=user)
    return deactivate_manual_column_edge_with_audit(db=db, edge=edge, user=user)


@router.get("/columns/manual/{edge_id}/versions", response_model=list[LineageColumnEdgeVersionOut])
def get_manual_column_edge_versions(
    edge_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[LineageColumnEdgeVersionOut]:
    edge = get_column_edge_or_404(db, edge_id, user=user)
    versions = db.scalars(
        select(LineageColumnEdgeVersion)
        .where(LineageColumnEdgeVersion.lineage_column_edge_id == edge.id)
        .order_by(LineageColumnEdgeVersion.version_number.desc(), LineageColumnEdgeVersion.id.desc())
    ).all()
    return [LineageColumnEdgeVersionOut.model_validate(version, from_attributes=True) for version in versions]
