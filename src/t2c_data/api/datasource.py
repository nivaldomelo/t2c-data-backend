from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission
from t2c_data.features.datasource.application import (
    create_datasource_with_audit,
    delete_datasource_with_audit,
    get_datasource_detail,
    list_datasource_schemas_via_connector,
    list_datasource_tables_via_connector,
    list_datasources_out,
    retest_saved_datasource_connection as retest_saved_datasource_connection_use_case,
    test_datasource_connection as run_datasource_connection_test,
    update_datasource_with_audit,
)
from t2c_data.features.datasource.schedules import (
    delete_scan_schedule,
    get_scan_schedule_for_datasource,
    list_scan_schedules,
    scheduler_status_snapshot,
    search_datasource_schedule_users,
    upsert_scan_schedule,
)
from t2c_data.features.datasource.api_support import (
    CONNECTOR_GATEWAY,
    capabilities_out,
    connector_definitions_out,
    datasource_detail,
    datasource_out,
    normalize_connection,
    normalize_schema_list,
    normalize_secrets,
    resolved_connection,
    run_connection_test,
    sanitize_error_message,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.models.auth import User
from t2c_data.models.datasource_scheduler import DataSourceScanSchedule
from t2c_data.schemas.datasource import (
    ConnectorDefinitionOut,
    DataSourceConnectionTestOut,
    DataSourceCreate,
    DataSourceDetail,
    DataSourceOut,
    DataSourceSchemaListOut,
    DataSourceTableListOut,
    DataSourceTestRequest,
    DataSourceUpdate,
)
from t2c_data.schemas.datasource_schedules import (
    DataSourceScanScheduleCreate,
    DataSourceScanScheduleOut,
    DataSourceScanSchedulerStatusOut,
    DataSourceScheduleUserOption,
)
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/datasources", tags=["datasources"])


@router.get("/scheduler/status", response_model=DataSourceScanSchedulerStatusOut)
def datasource_scheduler_status(
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("datasource:read")),
) -> DataSourceScanSchedulerStatusOut:
    return DataSourceScanSchedulerStatusOut.model_validate(scheduler_status_snapshot(db))


@router.get("/schedules/users", response_model=PageOut[DataSourceScheduleUserOption])
def datasource_schedule_users(
    q: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("datasource:read")),
) -> PageOut[DataSourceScheduleUserOption]:
    return paginate_items(
        [DataSourceScheduleUserOption.model_validate(item) for item in search_datasource_schedule_users(db=db, q=q, limit=max(page * page_size, page_size))],
        page=page,
        page_size=page_size,
    )


@router.get("/schedules", response_model=PageOut[DataSourceScanScheduleOut])
def datasource_scan_schedules(
    datasource_id: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("datasource:read")),
) -> PageOut[DataSourceScanScheduleOut]:
    return paginate_items(list_scan_schedules(db, datasource_id=datasource_id), page=page, page_size=page_size)


@router.post("/schedules", response_model=DataSourceScanScheduleOut, status_code=status.HTTP_201_CREATED)
def datasource_scan_schedule_save(
    payload: DataSourceScanScheduleCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("datasource:write")),
) -> DataSourceScanScheduleOut:
    try:
        existing = get_scan_schedule_for_datasource(db, payload.datasource_id)
        schedule = upsert_scan_schedule(db, payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    try:
        action = "datasource_scan_schedule_updated" if existing is not None else "datasource_scan_schedule_created"
        write_audit_log_sync(
            db,
            **request_audit_kwargs(request, current_user),
            action=action,
            entity_type="datasource_scan_schedule",
            entity_id=payload.datasource_id,
            metadata={"schedule_mode": payload.schedule_mode, "schedule_enabled": payload.schedule_enabled},
        )
        db.commit()
    except Exception:  # noqa: BLE001
        db.rollback()
        logger.exception("datasource scan schedule audit log failed datasource_id=%s", payload.datasource_id)
    return schedule


@router.delete("/schedules/{schedule_id}", response_model=None, status_code=status.HTTP_204_NO_CONTENT)
def datasource_scan_schedule_delete(
    schedule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("datasource:write")),
) -> None:
    schedule = db.get(DataSourceScanSchedule, schedule_id)
    delete_scan_schedule(db, schedule_id)
    if schedule is not None:
        try:
            write_audit_log_sync(
                db,
                **request_audit_kwargs(request, current_user),
                action="datasource_scan_schedule_deleted",
                entity_type="datasource_scan_schedule",
                entity_id=schedule.datasource_id,
                metadata={"schedule_mode": schedule.schedule_mode, "schedule_enabled": schedule.schedule_enabled},
            )
            db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception("datasource scan schedule delete audit log failed schedule_id=%s", schedule_id)

@router.get("/definitions", response_model=PageOut[ConnectorDefinitionOut])
def list_connector_definitions(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    _: User = Depends(require_permission("datasource:read")),
) -> PageOut[ConnectorDefinitionOut]:
    return paginate_items(connector_definitions_out(), page=page, page_size=page_size)


@router.post("", response_model=DataSourceOut, status_code=status.HTTP_201_CREATED)
def create_datasource(
    payload: DataSourceCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("datasource:write")),
) -> DataSourceOut:
    return create_datasource_with_audit(
        db=db,
        payload=payload,
        normalize_connection=normalize_connection,
        normalize_schema_list=normalize_schema_list,
        normalize_secrets=normalize_secrets,
        to_out=datasource_out,
        audit_kwargs=request_audit_kwargs(request, current_user),
        connector_gateway=CONNECTOR_GATEWAY,
    )


@router.get("", response_model=PageOut[DataSourceOut])
def list_datasources(
    db: Session = Depends(get_db),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    _: User = Depends(require_permission("datasource:read")),
) -> PageOut[DataSourceOut]:
    return paginate_items(list_datasources_out(db=db, to_out=datasource_out), page=page, page_size=page_size)


@router.post("/test", response_model=DataSourceConnectionTestOut)
def test_datasource_connection(
    payload: DataSourceTestRequest,
    _: User = Depends(require_permission("datasource:write")),
) -> DataSourceConnectionTestOut:
    return run_datasource_connection_test(
        payload=payload,
        normalize_connection=normalize_connection,
        normalize_secrets=normalize_secrets,
        run_connection_test=run_connection_test,
        connector_gateway=CONNECTOR_GATEWAY,
    )


@router.post("/{datasource_id}/test", response_model=DataSourceConnectionTestOut)
def retest_saved_datasource_connection(
    datasource_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("datasource:write")),
) -> DataSourceConnectionTestOut:
    return retest_saved_datasource_connection_use_case(
        db=db,
        datasource_id=datasource_id,
        resolved_connection=resolved_connection,
        run_connection_test=run_connection_test,
        connector_gateway=CONNECTOR_GATEWAY,
    )


@router.get("/{datasource_id}/schemas", response_model=DataSourceSchemaListOut)
def list_datasource_schemas(
    datasource_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("datasource:read")),
) -> DataSourceSchemaListOut:
    return DataSourceSchemaListOut.model_validate(
        list_datasource_schemas_via_connector(
            db=db,
            datasource_id=datasource_id,
            resolved_connection=resolved_connection,
            capabilities_out=capabilities_out,
            sanitize_error_message=sanitize_error_message,
            connector_gateway=CONNECTOR_GATEWAY,
        )
    )


@router.get("/{datasource_id}/tables", response_model=DataSourceTableListOut)
def list_datasource_tables(
    datasource_id: int,
    schema: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("datasource:read")),
) -> DataSourceTableListOut:
    return DataSourceTableListOut.model_validate(
        list_datasource_tables_via_connector(
            db=db,
            datasource_id=datasource_id,
            schema=schema,
            resolved_connection=resolved_connection,
            capabilities_out=capabilities_out,
            sanitize_error_message=sanitize_error_message,
            connector_gateway=CONNECTOR_GATEWAY,
        )
    )


@router.get("/{datasource_id}", response_model=DataSourceDetail)
def get_datasource(
    datasource_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("datasource:read")),
) -> DataSourceDetail:
    return get_datasource_detail(db=db, datasource_id=datasource_id, to_detail=datasource_detail)


@router.put("/{datasource_id}", response_model=DataSourceOut)
def update_datasource(
    datasource_id: int,
    payload: DataSourceUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("datasource:write")),
) -> DataSourceOut:
    return update_datasource_with_audit(
        db=db,
        datasource_id=datasource_id,
        payload=payload,
        normalize_connection=normalize_connection,
        normalize_schema_list=normalize_schema_list,
        normalize_secrets=normalize_secrets,
        resolved_connection=resolved_connection,
        to_out=datasource_out,
        audit_kwargs=request_audit_kwargs(request, current_user),
        connector_gateway=CONNECTOR_GATEWAY,
    )


@router.delete("/{datasource_id}", response_model=None, status_code=status.HTTP_204_NO_CONTENT)
def delete_datasource(
    datasource_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("datasource:write")),
) -> Response:
    return delete_datasource_with_audit(
        db=db,
        datasource_id=datasource_id,
        user=user,
        audit_kwargs=request_audit_kwargs(request, user),
    )
