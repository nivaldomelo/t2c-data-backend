from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.integrations import (
    build_metabase_artifact_detail,
    load_airflow_integration_failures,
    load_airflow_integration_health,
    load_airflow_integration_pipelines,
    load_airflow_integration_summary,
    load_metabase_integration_health,
    list_metabase_integration_artifacts,
    list_metabase_integration_sync_runs,
    load_metabase_integration_summary,
)
from t2c_data.features.metabase import enqueue_metabase_instance_sync
from t2c_data.features.integrations.data_lake import (
    create_data_lake_connection,
    delete_data_lake_connection_safe,
    list_data_lake_connections,
    test_data_lake_connection,
    update_data_lake_connection,
    get_data_lake_connection_or_404,
    serialize_data_lake_connection,
)
from t2c_data.features.integrations.data_lake_inventory import (
    get_data_lake_catalog_page,
    get_data_lake_inventory_page,
    list_data_lake_inventory_scans,
    scan_data_lake_inventory,
    update_data_lake_inventory_table_freshness_sla,
)
from t2c_data.features.integrations.data_lake_governance import update_data_lake_inventory_table_governance
from t2c_data.features.integrations.data_lake_operations import load_data_lake_operations_summary, load_data_lake_troubleshooting
from t2c_data.features.integrations.data_lake_schedules import (
    delete_data_lake_scan_schedule,
    get_data_lake_scan_schedule,
    list_data_lake_scan_schedules,
    scheduler_status_snapshot as load_data_lake_scan_scheduler_status,
    serialize_data_lake_scan_schedule,
    upsert_data_lake_scan_schedule,
)
from t2c_data.features.integrations.data_lake_detail import get_data_lake_table_detail, get_data_lake_table_detail_by_id, list_data_lake_table_files
from t2c_data.models.auth import User
from t2c_data.schemas.integrations import (
    AirflowIntegrationFailuresOut,
    AirflowIntegrationHealthOut,
    AirflowIntegrationPipelinesOut,
    AirflowIntegrationSummaryOut,
    MetabaseArtifactDetailOut,
    DataLakeCatalogPageOut,
    DataLakeConnectionIn,
    DataLakeConnectionOut,
    DataLakeInventoryPageOut,
    DataLakeInventoryScanOut,
    DataLakeInventoryScanRunOut,
    DataLakeInventoryTableOut,
    DataLakeConnectionTestOut,
    DataLakeTableFileOut,
    DataLakeTableFilesPageOut,
    DataLakeTableDetailOut,
    DataLakeTableFreshnessSlaIn,
    DataLakeInventoryTableGovernanceIn,
    DataLakeOperationsSummaryOut,
    DataLakeScanScheduleIn,
    DataLakeScanScheduleOut,
    DataLakeTroubleshootingOut,
    MetabaseIntegrationHealthOut,
    MetabaseIntegrationArtifactOut,
    MetabaseIntegrationSyncNowIn,
    MetabaseIntegrationSummaryOut,
    MetabaseSyncRunOut,
)
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs

router = APIRouter(prefix="/integrations", tags=["integrations"])


@router.get("/airflow/summary", response_model=AirflowIntegrationSummaryOut)
def airflow_integration_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AirflowIntegrationSummaryOut:
    return load_airflow_integration_summary(db)


@router.get("/airflow/pipelines", response_model=AirflowIntegrationPipelinesOut)
def airflow_integration_pipelines(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    search: str | None = Query(default=None, max_length=200),
    status: str | None = Query(default=None, pattern="^(all|active|paused|failing)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AirflowIntegrationPipelinesOut:
    return load_airflow_integration_pipelines(db, page=page, page_size=page_size, search=search, status=status)


@router.get("/airflow/failures", response_model=AirflowIntegrationFailuresOut)
def airflow_integration_failures(
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AirflowIntegrationFailuresOut:
    return load_airflow_integration_failures(db, limit=limit)


@router.get("/airflow/health", response_model=AirflowIntegrationHealthOut)
def airflow_integration_health(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AirflowIntegrationHealthOut:
    return load_airflow_integration_health(db)


@router.get("/metabase/summary", response_model=MetabaseIntegrationSummaryOut)
def metabase_integration_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> MetabaseIntegrationSummaryOut:
    return load_metabase_integration_summary(db)


@router.get("/metabase/health", response_model=MetabaseIntegrationHealthOut)
def metabase_integration_health(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> MetabaseIntegrationHealthOut:
    return load_metabase_integration_health(db)


@router.post("/metabase/sync-now", response_model=MetabaseSyncRunOut)
def metabase_integration_sync_now(
    request: Request,
    payload: MetabaseIntegrationSyncNowIn | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> MetabaseSyncRunOut:
    payload = payload or MetabaseIntegrationSyncNowIn()
    summary = load_metabase_integration_summary(db)
    instance_id = payload.instance_id or summary.instance_id
    if instance_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Nenhuma instância do Metabase está configurada.")
    return enqueue_metabase_instance_sync(
        db,
        int(instance_id),
        current_user=current_user,
        force=bool(payload.force),
    )


@router.get("/metabase/sync-runs", response_model=PageOut[MetabaseSyncRunOut])
def metabase_integration_sync_runs(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
    instance_id: int | None = Query(default=None, ge=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    finished_from: datetime | None = None,
    finished_to: datetime | None = None,
    query: str | None = None,
    only_failures: bool = False,
) -> PageOut[MetabaseSyncRunOut]:
    return list_metabase_integration_sync_runs(
        db,
        instance_id=instance_id,
        page=page,
        page_size=page_size,
        status=status,
        started_from=started_from,
        started_to=started_to,
        finished_from=finished_from,
        finished_to=finished_to,
        query=query,
        only_failures=only_failures,
    )


@router.get("/metabase/artifacts", response_model=PageOut[MetabaseIntegrationArtifactOut])
def metabase_integration_artifacts(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
    instance_id: int | None = Query(default=None, ge=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    type: str | None = None,
    collection: str | None = None,
    linked_status: str | None = None,
    query: str | None = None,
    table_id: int | None = Query(default=None, ge=1),
) -> PageOut[MetabaseIntegrationArtifactOut]:
    return list_metabase_integration_artifacts(
        db,
        instance_id=instance_id,
        page=page,
        page_size=page_size,
        type=type,
        collection=collection,
        linked_status=linked_status,
        query=query,
        table_id=table_id,
    )


@router.get("/metabase/artifacts/{object_id}", response_model=MetabaseArtifactDetailOut)
def metabase_integration_artifact_detail(
    object_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> MetabaseArtifactDetailOut:
    detail = build_metabase_artifact_detail(db, object_id=object_id)
    if detail is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Metabase artifact not found")
    return detail


@router.get("/data-lake/connections", response_model=list[DataLakeConnectionOut])
def data_lake_connections(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> list[DataLakeConnectionOut]:
    return [DataLakeConnectionOut.model_validate(item) for item in list_data_lake_connections(db)]


@router.get("/data-lake/catalog", response_model=DataLakeCatalogPageOut)
def data_lake_catalog(
    page: int = 1,
    page_size: int = 25,
    connection_id: int | None = None,
    bucket: str | None = None,
    layer: str | None = None,
    status: str | None = None,
    has_partitions: bool | None = None,
    has_parquet: bool | None = None,
    freshness_state: str | None = None,
    search: str | None = None,
    sort_by: str = "last_modified",
    sort_dir: str = "desc",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataLakeCatalogPageOut:
    return get_data_lake_catalog_page(
        db,
        page=page,
        page_size=page_size,
        connection_id=connection_id,
        bucket=bucket,
        layer=layer,
        status=status,
        has_partitions=has_partitions,
        has_parquet=has_parquet,
        freshness_state=freshness_state,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.get("/data-lake/connections/{connection_id}", response_model=DataLakeConnectionOut)
def data_lake_connection_detail(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeConnectionOut:
    connection = get_data_lake_connection_or_404(db, connection_id)
    return DataLakeConnectionOut.model_validate(serialize_data_lake_connection(connection))


@router.post("/data-lake/connections", response_model=DataLakeConnectionOut, status_code=201)
def create_data_lake_connection_route(
    payload: DataLakeConnectionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeConnectionOut:
    return DataLakeConnectionOut.model_validate(
        create_data_lake_connection(db, payload, current_user=current_user, audit_kwargs=request_audit_kwargs(request, current_user))
    )


@router.put("/data-lake/connections/{connection_id}", response_model=DataLakeConnectionOut)
def update_data_lake_connection_route(
    connection_id: int,
    payload: DataLakeConnectionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeConnectionOut:
    return DataLakeConnectionOut.model_validate(
        update_data_lake_connection(
            db,
            connection_id,
            payload,
            current_user=current_user,
            audit_kwargs=request_audit_kwargs(request, current_user),
        )
    )


@router.post("/data-lake/connections/{connection_id}/test", response_model=DataLakeConnectionTestOut)
def test_data_lake_connection_route(
    connection_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeConnectionTestOut:
    payload = test_data_lake_connection(
        db,
        connection_id,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return DataLakeConnectionTestOut.model_validate(payload)


@router.delete("/data-lake/connections/{connection_id}", response_model=None, status_code=204)
def delete_data_lake_connection_route(
    connection_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> Response:
    return delete_data_lake_connection_safe(db, connection_id, current_user=current_user, audit_kwargs=request_audit_kwargs(request, current_user))


@router.get("/data-lake/connections/{connection_id}/inventory", response_model=DataLakeInventoryPageOut)
def data_lake_inventory(
    connection_id: int,
    page: int = 1,
    page_size: int = 25,
    layer: str | None = None,
    name: str | None = None,
    status: str | None = None,
    has_partitions: bool | None = None,
    freshness_state: str | None = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeInventoryPageOut:
    return get_data_lake_inventory_page(
        db,
        connection_id,
        page=page,
        page_size=page_size,
        layer=layer,
        name=name,
        status=status,
        has_partitions=has_partitions,
        freshness_state=freshness_state,
    )


@router.post("/data-lake/connections/{connection_id}/inventory/scan", response_model=DataLakeInventoryScanOut)
def data_lake_inventory_scan(
    connection_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeInventoryScanOut:
    return scan_data_lake_inventory(
        db,
        connection_id,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
        correlation_id=getattr(request.state, "correlation_id", None),
    )


@router.get("/data-lake/connections/{connection_id}/inventory/scans", response_model=PageOut[DataLakeInventoryScanRunOut])
def data_lake_inventory_scans(
    connection_id: int,
    page: int = 1,
    page_size: int = 25,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> PageOut[DataLakeInventoryScanRunOut]:
    return list_data_lake_inventory_scans(db, connection_id, page=page, page_size=page_size)


@router.get("/data-lake/connections/{connection_id}/inventory/tables/{table_id}", response_model=DataLakeTableDetailOut)
def data_lake_inventory_table_detail(
    connection_id: int,
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeTableDetailOut:
    return get_data_lake_table_detail(db, connection_id, table_id)


@router.get("/data-lake/tables/{table_id}", response_model=DataLakeTableDetailOut)
def data_lake_table_detail_by_id(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataLakeTableDetailOut:
    return get_data_lake_table_detail_by_id(db, table_id)


@router.get("/data-lake/tables/{table_id}/files", response_model=DataLakeTableFilesPageOut)
def data_lake_table_files(
    table_id: int,
    page: int = 1,
    page_size: int = 25,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataLakeTableFilesPageOut:
    return list_data_lake_table_files(db, table_id, page=page, page_size=page_size)


@router.patch("/data-lake/connections/{connection_id}/inventory/tables/{table_id}", response_model=DataLakeInventoryTableOut)
def data_lake_inventory_table_update(
    connection_id: int,
    table_id: int,
    payload: DataLakeTableFreshnessSlaIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeInventoryTableOut:
    return update_data_lake_inventory_table_freshness_sla(
        db,
        connection_id,
        table_id,
        payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )


@router.patch("/data-lake/connections/{connection_id}/inventory/tables/{table_id}/governance", response_model=DataLakeInventoryTableOut)
def data_lake_inventory_table_governance_update(
    connection_id: int,
    table_id: int,
    payload: DataLakeInventoryTableGovernanceIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeInventoryTableOut:
    return update_data_lake_inventory_table_governance(
        db,
        connection_id,
        table_id,
        payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )


@router.get("/data-lake/connections/{connection_id}/schedule", response_model=DataLakeScanScheduleOut | None)
def data_lake_scan_schedule(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeScanScheduleOut | None:
    schedule = get_data_lake_scan_schedule(db, connection_id)
    return serialize_data_lake_scan_schedule(schedule, db) if schedule is not None else None


@router.get("/data-lake/connections/{connection_id}/schedules", response_model=list[DataLakeScanScheduleOut])
def data_lake_scan_schedules(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> list[DataLakeScanScheduleOut]:
    return list_data_lake_scan_schedules(db, connection_id)


@router.put("/data-lake/connections/{connection_id}/schedule", response_model=DataLakeScanScheduleOut)
def data_lake_scan_schedule_upsert(
    connection_id: int,
    payload: DataLakeScanScheduleIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeScanScheduleOut:
    return upsert_data_lake_scan_schedule(
        db,
        connection_id,
        payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )


@router.delete("/data-lake/connections/{connection_id}/schedule", status_code=204, response_model=None)
def data_lake_scan_schedule_delete(
    connection_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> Response:
    delete_data_lake_scan_schedule(db, connection_id, current_user=current_user, audit_kwargs=request_audit_kwargs(request, current_user))
    return Response(status_code=204)


@router.get("/data-lake/connections/{connection_id}/operations/summary", response_model=DataLakeOperationsSummaryOut)
def data_lake_operations_summary(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeOperationsSummaryOut:
    return load_data_lake_operations_summary(db, connection_id)


@router.get("/data-lake/connections/{connection_id}/troubleshooting", response_model=DataLakeTroubleshootingOut)
def data_lake_troubleshooting(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DataLakeTroubleshootingOut:
    return load_data_lake_troubleshooting(db, connection_id)


@router.get("/data-lake/scheduler/status", response_model=dict[str, object])
def data_lake_scan_scheduler_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> dict[str, object]:
    return load_data_lake_scan_scheduler_status(db)
