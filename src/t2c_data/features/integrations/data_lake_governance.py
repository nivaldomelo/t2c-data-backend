from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.integrations.data_lake import get_data_lake_connection_or_404
from t2c_data.features.integrations.data_lake_inventory import serialize_data_lake_inventory_table
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner
from t2c_data.models.platform import DataLakeInventoryTable
from t2c_data.schemas.integrations import DataLakeInventoryTableGovernanceIn, DataLakeInventoryTableOut
from t2c_data.services.audit import write_audit_log_sync


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def update_data_lake_inventory_table_governance(
    session: Session,
    connection_id: int,
    table_id: int,
    payload: DataLakeInventoryTableGovernanceIn,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
) -> DataLakeInventoryTableOut:
    connection = get_data_lake_connection_or_404(session, connection_id)
    table = session.scalar(
        select(DataLakeInventoryTable).where(
            DataLakeInventoryTable.connection_id == connection.id,
            DataLakeInventoryTable.id == table_id,
        )
    )
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Lake table not found")

    before = serialize_data_lake_inventory_table(table)
    owner_id = payload.data_owner_id
    if owner_id is not None:
        owner = session.get(DataOwner, owner_id)
        if owner is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found")
        if not owner.is_active:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Data owner is inactive")
        table.data_owner_id = owner.id
    else:
        table.data_owner_id = None
    table.domain_name = _normalize_text(payload.domain_name)
    table.description = _normalize_text(payload.description)
    table.classification = _normalize_text(payload.classification)
    table.criticality = _normalize_text(payload.criticality)
    table.is_monitored = bool(payload.is_monitored)
    table.governance_last_updated_at = datetime.now(timezone.utc)
    session.add(table)
    session.commit()
    session.refresh(table)
    write_audit_log_sync(
        session,
        action="integrations.data_lake.inventory_table_governance_update",
        entity_type="data_lake_inventory_table",
        entity_id=table.id,
        before=before,
        after=serialize_data_lake_inventory_table(table),
        metadata={
            "connection_id": connection.id,
            "connection_name": connection.name,
            "table_name": table.table_name,
            "data_owner_id": table.data_owner_id,
            "domain_name": table.domain_name,
            "classification": table.classification,
            "criticality": table.criticality,
            "is_monitored": table.is_monitored,
        },
        **audit_kwargs,
    )
    session.commit()
    return DataLakeInventoryTableOut.model_validate(serialize_data_lake_inventory_table(table))

