from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.assistant import build_assistant_explanation, execute_assistant_action
from t2c_data.models.auth import User
from t2c_data.schemas.assistant import AssistantActionIn, AssistantActionOut, AssistantExplainOut
from t2c_data.services.audit import request_audit_kwargs

router = APIRouter(prefix="/assistant", tags=["assistant"])


@router.post("/explain/{asset_ref:path}", response_model=AssistantExplainOut)
def assistant_explain(
    asset_ref: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner", "viewer")),
) -> AssistantExplainOut:
    return build_assistant_explanation(db, asset_ref=asset_ref, current_user=current_user)


@router.post("/actions/{asset_ref:path}", response_model=AssistantActionOut)
def assistant_action(
    asset_ref: str,
    payload: AssistantActionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> AssistantActionOut:
    return execute_assistant_action(
        db,
        asset_ref=asset_ref,
        payload=payload,
        current_user=current_user,
        request_audit=request_audit_kwargs(request, current_user),
    )
