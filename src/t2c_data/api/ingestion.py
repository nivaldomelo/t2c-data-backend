from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.ingestion import (
    list_execution_logs_from_source,
    load_ingestion_operational_overview_from_source,
    list_table_ingestion_executions_from_source,
    load_table_ingestion_detail_from_source,
    load_table_ingestion_summary_from_source,
)
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.privacy_access import can_view_table
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.schemas.ingestion import (
    IngestionExecutionLogsOut,
    IngestionExecutionPageOut,
    IngestionOperationalOverviewOut,
    IngestionTableDetailOut,
    IngestionTableSummaryOut,
)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


@router.get("/overview", response_model=IngestionOperationalOverviewOut)
def get_ingestion_operational_overview(
    limit: int = Query(default=8, ge=1, le=50),
    schema: str | None = Query(default=None),
    table: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionOperationalOverviewOut:
    table_refs = None
    if schema and table:
        table_refs = [{"schema_name": schema, "table_name": table}]

    return IngestionOperationalOverviewOut(
        **load_ingestion_operational_overview_from_source(
            db,
            limit=limit,
            table_refs=table_refs,
        )
    )


def _resolve_visible_table(db: Session, table_id: int, current_user: User) -> tuple[TableEntity, DataSource, str, str]:
    row = db.execute(
        select(TableEntity, DataSource, Schema.name.label("schema_name"))
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.id == table_id)
    ).first()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tabela não encontrada.")
    table, datasource, schema_name = row
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Tabela não visível para este perfil.")
    return table, datasource, str(schema_name), str(table.name)


def _resolve_visible_table_by_name(db: Session, schema_name: str, table_name: str, current_user: User) -> tuple[TableEntity, DataSource, str, str]:
    rows = db.execute(
        select(TableEntity, DataSource, Schema.name.label("schema_name"))
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(Schema.name == schema_name, TableEntity.name == table_name)
        .order_by(TableEntity.updated_at.desc(), TableEntity.id.desc())
    ).all()
    if not rows:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tabela não encontrada.")
    for table, datasource, resolved_schema_name in rows:
        if can_view_table(current_user, table):
            return table, datasource, str(resolved_schema_name), str(table.name)
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Tabela não visível para este perfil.")


@router.get("/tables/{table_id}", response_model=IngestionTableDetailOut)
def get_table_ingestion_detail(
    table_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionTableDetailOut:
    _, _, schema_name, table_name = _resolve_visible_table(db, table_id, current_user)
    settings_snapshot = get_governance_settings_snapshot(db)
    return IngestionTableDetailOut(
        **load_table_ingestion_detail_from_source(
            db,
            schema_name=schema_name,
            table_name=table_name,
            page=page,
            page_size=page_size,
            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
        )
    )


@router.get("/table/{schema_name}/{table_name}", response_model=IngestionTableDetailOut)
def get_table_ingestion_detail_by_name(
    schema_name: str,
    table_name: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionTableDetailOut:
    _, _, resolved_schema_name, resolved_table_name = _resolve_visible_table_by_name(db, schema_name, table_name, current_user)
    settings_snapshot = get_governance_settings_snapshot(db)
    return IngestionTableDetailOut(
        **load_table_ingestion_detail_from_source(
            db,
            schema_name=resolved_schema_name,
            table_name=resolved_table_name,
            page=page,
            page_size=page_size,
            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
        )
    )


@router.get("/tables/{table_id}/summary", response_model=IngestionTableSummaryOut)
def get_table_ingestion_summary(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionTableSummaryOut:
    _, _, schema_name, table_name = _resolve_visible_table(db, table_id, current_user)
    settings_snapshot = get_governance_settings_snapshot(db)
    return IngestionTableSummaryOut(
        **load_table_ingestion_summary_from_source(
            db,
            schema_name=schema_name,
            table_name=table_name,
            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
        )
    )


@router.get("/tables/{table_id}/executions", response_model=IngestionExecutionPageOut)
def get_table_ingestion_executions(
    table_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionExecutionPageOut:
    _, _, schema_name, table_name = _resolve_visible_table(db, table_id, current_user)
    settings_snapshot = get_governance_settings_snapshot(db)
    return IngestionExecutionPageOut(
        **list_table_ingestion_executions_from_source(
            db,
            schema_name=schema_name,
            table_name=table_name,
            page=page,
            page_size=page_size,
            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
        )
    )


@router.get("/history", response_model=IngestionExecutionPageOut)
def get_ingestion_history_by_name(
    schema: str = Query(...),
    table: str = Query(...),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionExecutionPageOut:
    _resolve_visible_table_by_name(db, schema, table, current_user)
    settings_snapshot = get_governance_settings_snapshot(db)
    return IngestionExecutionPageOut(
        **list_table_ingestion_executions_from_source(
            db,
            schema_name=schema,
            table_name=table,
            page=page,
            page_size=page_size,
            airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
        )
    )


@router.get("/executions/{execution_id}/logs", response_model=IngestionExecutionLogsOut)
def get_execution_logs(
    execution_id: str,
    datasource_id: int | None = Query(default=None, ge=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionExecutionLogsOut:
    return IngestionExecutionLogsOut(
        **list_execution_logs_from_source(db, execution_id=execution_id, page=page, page_size=page_size)
    )


@router.get("/logs", response_model=IngestionExecutionLogsOut)
def get_execution_logs_by_query(
    execucao_id: str = Query(...),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> IngestionExecutionLogsOut:
    return IngestionExecutionLogsOut(
        **list_execution_logs_from_source(db, execution_id=execucao_id, page=page, page_size=page_size)
    )
