from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.lineage.application import upsert_lineage_spec_with_audit
from t2c_data.features.lineage.queries import get_lineage_spec_for_table, get_lineage_spec_lookup_by_fqn
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import LineageSpecIn, LineageSpecLookupOut, LineageSpecOut

router = APIRouter(tags=["lineage"])


@router.get("/spec/tables/{table_id}", response_model=LineageSpecOut)
def get_lineage_spec(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageSpecOut:
    return get_lineage_spec_for_table(db, table_id, current_user=current_user)


@router.get("/spec/by-fqn", response_model=LineageSpecLookupOut)
def get_lineage_spec_by_fqn(
    table_fqn: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> LineageSpecLookupOut:
    return get_lineage_spec_lookup_by_fqn(db, table_fqn, current_user=current_user)


@router.put("/spec/tables/{table_id}", response_model=LineageSpecOut)
def put_lineage_spec(
    table_id: int,
    payload: LineageSpecIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> LineageSpecOut:
    return upsert_lineage_spec_with_audit(db=db, table_id=table_id, payload=payload, user=user)
