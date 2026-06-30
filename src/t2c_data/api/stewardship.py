from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.stewardship import (
    create_stewardship_request,
    decide_stewardship_request,
    get_stewardship_request_context,
    get_stewardship_request_payload,
    get_stewardship_requests,
)
from t2c_data.models.auth import User
from t2c_data.schemas.stewardship import (
    StewardshipCancelIn,
    StewardshipRequestContextOut,
    StewardshipDecisionIn,
    StewardshipRequestCreateIn,
    StewardshipRequestListOut,
    StewardshipRequestOut,
)
from t2c_data.services.audit import request_audit_kwargs

router = APIRouter(prefix="/stewardship", tags=["stewardship"])


@router.get("/context", response_model=StewardshipRequestContextOut)
def stewardship_request_context(
    table_id: int = Query(ge=1),
    request_type: str = Query(),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StewardshipRequestContextOut:
    return StewardshipRequestContextOut(**get_stewardship_request_context(db, table_id=table_id, request_type=request_type))


@router.get("/requests", response_model=StewardshipRequestListOut)
def list_stewardship_requests(
    status_filter: str | None = Query(default=None, alias="status"),
    request_type: str | None = Query(default=None),
    table_id: int | None = Query(default=None, ge=1),
    approver_user_id: int | None = Query(default=None, ge=1),
    data_owner_id: int | None = Query(default=None, ge=1),
    sla_status_filter: str | None = Query(default=None, alias="sla_status"),
    mine_only: bool = Query(default=False, alias="mine"),
    sort: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StewardshipRequestListOut:
    return StewardshipRequestListOut(
        **get_stewardship_requests(
            db,
            status_filter=status_filter,
            request_type=request_type,
            table_id=table_id,
            approver_user_id=approver_user_id,
            data_owner_id=data_owner_id,
            sla_status_filter=sla_status_filter,
            mine_only=mine_only,
            sort=sort,
            page=page,
            page_size=page_size,
            current_user=user,
        )
    )


@router.post("/requests", response_model=StewardshipRequestOut, status_code=status.HTTP_201_CREATED)
def create_request(
    payload: StewardshipRequestCreateIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "stewardship", "data_owner")),
) -> StewardshipRequestOut:
    item = create_stewardship_request(
        db,
        payload=payload,
        user=user,
        audit_kwargs=request_audit_kwargs(request, user),
    )
    return StewardshipRequestOut(**get_stewardship_request_payload(db, item.id))


@router.post("/requests/{request_id}/approve", response_model=StewardshipRequestOut)
def approve_request(
    request_id: int,
    payload: StewardshipDecisionIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("stewardship:approve")),
) -> StewardshipRequestOut:
    item = decide_stewardship_request(
        db,
        request_id=request_id,
        decision="approved",
        actor=user,
        payload=payload,
        audit_kwargs=request_audit_kwargs(request, user),
    )
    return StewardshipRequestOut(**get_stewardship_request_payload(db, item.id))


@router.post("/requests/{request_id}/reject", response_model=StewardshipRequestOut)
def reject_request(
    request_id: int,
    payload: StewardshipDecisionIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("stewardship:reject")),
) -> StewardshipRequestOut:
    item = decide_stewardship_request(
        db,
        request_id=request_id,
        decision="rejected",
        actor=user,
        payload=payload,
        audit_kwargs=request_audit_kwargs(request, user),
    )
    return StewardshipRequestOut(**get_stewardship_request_payload(db, item.id))


@router.post("/requests/{request_id}/cancel", response_model=StewardshipRequestOut)
def cancel_request(
    request_id: int,
    payload: StewardshipCancelIn,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "stewardship", "data_owner")),
) -> StewardshipRequestOut:
    item = decide_stewardship_request(
        db,
        request_id=request_id,
        decision="cancelled",
        actor=user,
        payload=payload,
        audit_kwargs=request_audit_kwargs(request, user),
    )
    return StewardshipRequestOut(**get_stewardship_request_payload(db, item.id))
