from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.data_quality.operational_policy import apply_operational_dq_policy
from t2c_data.features.data_quality.queries import table_metrics_with_history
from t2c_data.features.privacy_access import can_view_table
from t2c_data.models.auth import User
from t2c_data.models.catalog import Database, Schema, TableEntity


def resolve_table_from_fqn(db: Session, table_fqn: str) -> TableEntity | None:
    parts = [p for p in table_fqn.split(".") if p]
    if len(parts) < 2:
        return None
    schema_name, table_name = parts[-2], parts[-1]
    datasource_name = parts[-3] if len(parts) >= 3 else None

    query = (
        select(TableEntity)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(Database.datasource)
        .where(Schema.name == schema_name, TableEntity.name == table_name)
    )
    if datasource_name:
        from t2c_data.models.catalog import DataSource

        query = query.where(DataSource.name == datasource_name)
    return db.scalar(query.order_by(TableEntity.id.desc()).limit(1))


def get_latest_metrics_by_fqn(*, db: Session, table_fqn: str, history_runs: int, current_user: User):
    table = resolve_table_from_fqn(db, table_fqn)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    latest = table_metrics_with_history(db, table, history_runs=history_runs, current_user=current_user)
    if not latest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No DQ metrics for table")
    return apply_operational_dq_policy(db, table=table, payload=latest)


def get_latest_metrics_by_table_id(*, db: Session, table_id: int, history_runs: int, current_user: User):
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    latest = table_metrics_with_history(db, table, history_runs=history_runs, current_user=current_user)
    if not latest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No DQ metrics for table")
    return apply_operational_dq_policy(db, table=table, payload=latest)


__all__ = [
    "get_latest_metrics_by_fqn",
    "get_latest_metrics_by_table_id",
    "resolve_table_from_fqn",
]
