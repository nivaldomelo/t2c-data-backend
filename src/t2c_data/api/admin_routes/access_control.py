from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.admin.access_control_support import (
    access_group_out,
    apply_access_group_updates,
    get_access_group_or_404,
    list_access_groups_out,
    validate_access_group_name_available,
)
from t2c_data.models.access_control import AccessGroup
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.schemas.admin import (
    AccessDatasourceOptionOut,
    AccessGroupCreate,
    AccessGroupOut,
    AccessGroupUpdate,
    AccessSchemaOptionOut,
    AccessTableOptionOut,
    AccessTargetOptionsOut,
)
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter(prefix="/access")


def _validate_access_group_payload(payload: AccessGroupCreate | AccessGroupUpdate) -> None:
    grants = getattr(payload, "grants", None)
    if grants is None:
        return
    for grant in grants:
        if sum(1 for value in [grant.datasource_id, grant.schema_id, grant.table_id] if value is not None) != 1:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Each grant must target exactly one datasource, schema, or object")
        if grant.effect not in {"allow", "deny"}:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid grant effect")


@router.get("/targets", response_model=AccessTargetOptionsOut)
def list_access_targets(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> AccessTargetOptionsOut:
    datasources = db.scalars(select(DataSource).order_by(DataSource.name)).all()
    schemas = db.scalars(
        select(Schema)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .order_by(DataSource.name, Database.name, Schema.name)
    ).all()
    tables = db.scalars(
        select(TableEntity)
        .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
        .order_by(TableEntity.name)
    ).all()
    return AccessTargetOptionsOut(
        datasources=[
            AccessDatasourceOptionOut(id=item.id, name=item.name, db_type=item.db_type, database=item.database)
            for item in datasources
        ],
        schemas=[
            AccessSchemaOptionOut(
                id=item.id,
                datasource_id=item.database.datasource_id,
                database_id=item.database_id,
                name=item.name,
            )
            for item in schemas
        ],
        tables=[
            AccessTableOptionOut(
                id=item.id,
                datasource_id=item.schema.database.datasource_id,
                database_id=item.schema.database_id,
                schema_id=item.schema_id,
                name=item.name,
                table_type=item.table_type,
                table_fqn=f"{item.schema.database.datasource.name}.{item.schema.database.name}.{item.schema.name}.{item.name}",
            )
            for item in tables
        ],
    )


@router.get("/groups", response_model=list[AccessGroupOut])
def list_access_groups(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin")),
) -> list[AccessGroupOut]:
    return list_access_groups_out(db)


@router.post("/groups", response_model=AccessGroupOut, status_code=status.HTTP_201_CREATED)
def create_access_group(
    payload: AccessGroupCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> AccessGroupOut:
    validate_access_group_name_available(db, payload.name)
    _validate_access_group_payload(payload)
    group = AccessGroup(name=payload.name, description=payload.description, is_active=payload.is_active)
    db.add(group)
    db.flush()
    apply_access_group_updates(db, group, payload.model_dump(exclude_unset=True))
    db.commit()
    db.refresh(group)
    write_audit_log_sync(
        db,
        action="admin.access_group.create",
        entity_type="access_group",
        entity_id=group.id,
        after=serialize_model(group),
        metadata={"member_user_ids": [user.id for user in group.users], "grant_ids": [grant.id for grant in group.grants]},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return access_group_out(group)


@router.put("/groups/{group_id}", response_model=AccessGroupOut)
def update_access_group(
    group_id: int,
    payload: AccessGroupUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> AccessGroupOut:
    group = get_access_group_or_404(db, group_id)
    before = serialize_model(group)
    updates = payload.model_dump(exclude_unset=True)
    if "name" in updates and updates["name"] != group.name:
        validate_access_group_name_available(db, updates["name"], exclude_group_id=group_id)
    _validate_access_group_payload(payload)
    apply_access_group_updates(db, group, updates)
    db.add(group)
    db.commit()
    db.refresh(group)
    write_audit_log_sync(
        db,
        action="admin.access_group.update",
        entity_type="access_group",
        entity_id=group.id,
        before=before,
        after=serialize_model(group),
        metadata={"member_user_ids": [user.id for user in group.users], "grant_ids": [grant.id for grant in group.grants]},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return access_group_out(group)


@router.delete("/groups/{group_id}", response_model=None, status_code=status.HTTP_204_NO_CONTENT)
def delete_access_group(
    group_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> Response:
    group = get_access_group_or_404(db, group_id)
    before = serialize_model(group)
    db.delete(group)
    db.commit()
    write_audit_log_sync(
        db,
        action="admin.access_group.delete",
        entity_type="access_group",
        entity_id=group_id,
        before=before,
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
