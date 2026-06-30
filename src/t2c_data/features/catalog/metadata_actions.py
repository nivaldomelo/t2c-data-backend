from __future__ import annotations

from fastapi import HTTPException, status
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.audit import build_table_history_snapshot, table_history_changes
from t2c_data.features.tags.intelligence import reprocess_table_tag_intelligence
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner, TableEntity
from t2c_data.services.audit import log_field_changes


def patch_table_with_audit(
    *,
    db: Session,
    table_id: int,
    payload,
    user,
    audit_kwargs: dict | None = None,
    commit: bool = True,
):
    table = db.scalar(
        select(TableEntity)
        .options(
            selectinload(TableEntity.columns),
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.certification_decided_by_user),
        )
        .where(TableEntity.id == table_id)
    )
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")

    before = build_table_history_snapshot(table)

    updates = payload.model_dump(exclude_unset=True)
    if "data_owner_id" in updates:
        data_owner_id = updates.pop("data_owner_id")
        if data_owner_id is None:
            table.data_owner_id = None
            table.data_owner = None
            table.owner = None
            table.owner_email = None
        else:
            owner = db.get(DataOwner, data_owner_id)
            if not owner:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Data owner not found")
            table.data_owner = owner
            table.data_owner_id = owner.id
            table.owner = owner.name
            table.owner_email = owner.email
    if "steward_user_id" in updates:
        steward_user_id = updates.pop("steward_user_id")
        if steward_user_id is None:
            table.steward_user_id = None
        else:
            steward = db.get(User, steward_user_id)
            if not steward:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Steward user not found")
            table.steward_user_id = steward.id
    for key, value in updates.items():
        setattr(table, key, value)

    owner_changed = False
    if "data_owner_id" in payload.model_dump(exclude_unset=True) or "owner" in updates or "owner_email" in updates:
        owner_changed = (
            before.owner
            != build_table_history_snapshot(table).owner
        )
    if owner_changed and getattr(user, "id", None) is not None:
        table.owner_reviewed_by_user_id = user.id
        table.owner_reviewed_at = datetime.now(timezone.utc)

    db.flush()
    after = build_table_history_snapshot(table)
    changes = table_history_changes(before, after)
    if changes:
        log_field_changes(
            db,
            action="table.patch",
            entity_type="table",
            entity_id=table.id,
            changes=changes,
            source_module="catalog",
            metadata={"message": "Manual metadata updated"},
            audit_kwargs=audit_kwargs,
            actor_user_id=user.id,
        )
        reprocess_table_tag_intelligence(
            db,
            table_id=table.id,
            actor_user_id=user.id,
            audit_kwargs=audit_kwargs,
            source_module="catalog.metadata",
            metadata={"origin": "table_patch"},
        )

    if commit:
        db.commit()
        db.refresh(table)
    else:
        db.flush()
    return table


def ensure_table_exists(*, db: Session, table_id: int) -> None:
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")


def get_table_datasource_id(*, db: Session, table_id: int) -> int:
    from t2c_data.models.catalog import Database, Schema

    datasource_id = db.scalar(
        select(Database.datasource_id)
        .join(Schema, Schema.database_id == Database.id)
        .join(TableEntity, TableEntity.schema_id == Schema.id)
        .where(TableEntity.id == table_id)
    )
    if datasource_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return int(datasource_id)
