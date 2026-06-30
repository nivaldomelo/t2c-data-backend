from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.intelligence import build_asset_intelligence
from t2c_data.models.auth import User
from t2c_data.schemas.asset_intelligence import AssetIntelligenceOut

router = APIRouter(prefix="/asset-intelligence", tags=["asset-intelligence"])


@router.get("/{asset_id}", response_model=AssetIntelligenceOut)
def get_asset_intelligence(
    asset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AssetIntelligenceOut:
    return build_asset_intelligence(db, asset_id=asset_id, current_user=current_user)
