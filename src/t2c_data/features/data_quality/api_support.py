from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.data_quality.application import build_dq_job_out, build_dq_run_progress_out
from t2c_data.features.data_quality.spark_runs import get_dq_job_run, list_dq_run_children
from t2c_data.features.privacy_access import can_view_table
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRun
from t2c_data.schemas.dq import (
    DQJobRunOut,
    DQRunItemOut,
    DQRunProgressOut,
    DQTreeDatasourceChildrenOut,
    DQTreeDatasourceOut,
    DQTreeTableOut,
)


def get_dq_job_run_or_404(run_id: int, db: Session) -> DQJobRunOut:
    run = get_dq_job_run(run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="DQ run not found")
    return build_dq_job_out(run, db)


def get_dq_profiling_run_or_404(run_id: int, db: Session) -> DQRunProgressOut:
    run = db.get(DQRun, run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="DQ profiling run not found")
    return build_dq_run_progress_out(run, db)


def build_dq_run_items_out(run_id: int, db: Session) -> list[DQRunItemOut]:
    parent = db.get(DQRun, run_id)
    if not parent:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="DQ profiling run not found")
    items = list_dq_run_children(run_id)
    table_ids = [item.table_id for item in items if item.table_id is not None]
    tables_by_id = {t.id: t for t in db.scalars(select(TableEntity).where(TableEntity.id.in_(table_ids))).all()} if table_ids else {}
    schema_ids = [t.schema_id for t in tables_by_id.values()]
    schemas_by_id = {s.id: s for s in db.scalars(select(Schema).where(Schema.id.in_(schema_ids))).all()} if schema_ids else {}
    return [
        DQRunItemOut(
            id=item.id,
            parent_run_id=item.parent_run_id,
            table_id=item.table_id,
            table_fqn=(
                None
                if item.table_id is None or item.table_id not in tables_by_id
                else (
                    f"{schemas_by_id.get(tables_by_id[item.table_id].schema_id).name}.{tables_by_id[item.table_id].name}"
                    if schemas_by_id.get(tables_by_id[item.table_id].schema_id)
                    else tables_by_id[item.table_id].name
                )
            ),
            status=item.status,
            execution_engine=item.execution_engine,
            queued_at=item.queued_at,
            started_at=item.started_at,
            finished_at=item.finished_at,
            duration_ms=item.duration_ms,
            error_message=item.error_message,
            spark_app_id=item.spark_app_id,
            log_tail=item.log_tail,
        )
        for item in items
    ]


def build_dq_tree(db: Session, current_user: User) -> list[DQTreeDatasourceOut]:
    rows = db.scalars(select(DataSource).order_by(DataSource.name)).all()
    visible_datasource_ids = {
        table.schema.database.datasource_id
        for table in db.scalars(
            select(TableEntity).join(Schema, TableEntity.schema_id == Schema.id).join(Database, Schema.database_id == Database.id)
        ).all()
        if can_view_table(current_user, table)
    }
    return [
        DQTreeDatasourceOut(id=d.id, name=d.name, db_type=d.db_type, database=d.database)
        for d in rows
        if d.id in visible_datasource_ids
    ]


def build_dq_tree_datasource(datasource_id: int, db: Session, current_user: User) -> DQTreeDatasourceChildrenOut:
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")
    database = db.scalar(
        select(Database).where(Database.datasource_id == datasource_id).order_by(Database.id.desc()).limit(1)
    )
    if not database:
        return DQTreeDatasourceChildrenOut(datasource_id=datasource.id, database_id=None, database=datasource.database, schemas=[])

    schemas = db.scalars(select(Schema).where(Schema.database_id == database.id).order_by(Schema.name)).all()
    visible_schema_ids = {
        table.schema_id
        for table in db.scalars(select(TableEntity).where(TableEntity.schema_id.in_([schema.id for schema in schemas]))).all()
        if can_view_table(current_user, table)
    }
    return DQTreeDatasourceChildrenOut(
        datasource_id=datasource.id,
        database_id=database.id,
        database=database.name,
        schemas=[{"id": s.id, "name": s.name} for s in schemas if s.id in visible_schema_ids],
    )


def build_dq_tree_tables(schema_id: int, db: Session, current_user: User) -> list[DQTreeTableOut]:
    schema = db.get(Schema, schema_id)
    if not schema:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found")
    tables = db.scalars(select(TableEntity).where(TableEntity.schema_id == schema_id).order_by(TableEntity.name)).all()
    return [
        DQTreeTableOut(id=t.id, name=t.name, kind="table" if t.table_type == "table" else "view")
        for t in tables
        if can_view_table(current_user, t)
    ]
