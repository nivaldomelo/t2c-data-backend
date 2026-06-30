from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import get_current_user
from t2c_data.core.network import get_request_client_ip
from t2c_data.models.auth import User
from t2c_data.schemas.user_audit import UserActivityHeartbeatOut, UserActivityPageViewIn
from t2c_data.services.user_activity_tracker import (
    record_access_event,
    record_page_view,
    record_session_heartbeat,
)

router = APIRouter(prefix="/activity", tags=["activity"])


@router.post("/page-view", response_model=UserActivityHeartbeatOut)
def capture_page_view(
    payload: UserActivityPageViewIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserActivityHeartbeatOut:
    session_jti = getattr(request.state, "current_user_session_jti", None)
    if payload.event_type == "page_view":
        event = record_page_view(
            db,
            user=current_user,
            session_jti=session_jti,
            route_path=payload.route_path,
            page_key=payload.page_key,
            metadata=payload.metadata,
            request_id=getattr(request.state, "request_id", None),
            correlation_id=getattr(request.state, "correlation_id", None),
            ip_address=get_request_client_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    else:
        event = record_access_event(
            db,
            user=current_user,
            session_jti=session_jti,
            event_type=payload.event_type,
            route_path=payload.route_path,
            action=payload.action,
            resource_type=payload.resource_type,
            resource_id=payload.resource_id,
            resource_fqn=payload.resource_fqn,
            datasource_id=payload.datasource_id,
            schema_name=payload.schema_name,
            table_id=payload.table_id,
            table_name=payload.table_name,
            column_id=payload.column_id,
            column_name=payload.column_name,
            sensitivity_level=payload.sensitivity_level,
            has_personal_data=payload.has_personal_data,
            has_sensitive_data=payload.has_sensitive_data,
            privacy_classification=payload.privacy_classification,
            metadata=payload.metadata,
            request_id=getattr(request.state, "request_id", None),
            correlation_id=getattr(request.state, "correlation_id", None),
            ip_address=get_request_client_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    db.commit()
    return UserActivityHeartbeatOut(ok=True, updated=event is not None)


@router.post("/heartbeat", response_model=UserActivityHeartbeatOut)
def capture_heartbeat(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserActivityHeartbeatOut:
    session_jti = getattr(request.state, "current_user_session_jti", None)
    session = record_session_heartbeat(
        db,
        user=current_user,
        session_jti=session_jti,
        user_agent=request.headers.get("user-agent"),
        ip_address=get_request_client_ip(request),
        force=False,
    )
    db.commit()
    return UserActivityHeartbeatOut(ok=True, updated=session is not None)
