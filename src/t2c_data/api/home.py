from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.home.queries import get_home_summary
from t2c_data.models.auth import User
from t2c_data.schemas.home import HomeSummaryOut

router = APIRouter(prefix="/home", tags=["home"])


@router.get("/summary", response_model=HomeSummaryOut)
def home_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> HomeSummaryOut:
    return HomeSummaryOut(**get_home_summary(db, current_user))
