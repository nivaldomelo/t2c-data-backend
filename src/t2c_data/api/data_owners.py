from __future__ import annotations

from math import ceil

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.schemas.data_owner import (
    DataOwnerCreate,
    DataOwnerDetailOut,
    DataOwnerListItemOut,
    DataOwnerOut,
    DataOwnerTablePreviewOut,
    DataOwnerUpdate,
    OwnershipDeleteImpactOut,
    OwnershipReassignPreviewOut,
    OwnershipReassignRequestIn,
    OwnershipReassignResultOut,
)
from t2c_data.schemas.pagination import PageOut
from t2c_data.features.governance import get_ownership_delete_impact, get_ownership_reassign_preview, reassign_ownership_assets
from t2c_data.features.platform.sensitive_data import can_view_sensitive_data, mask_payload_by_policy
from t2c_data.services.audit import add_audit_log, request_audit_kwargs

router = APIRouter(prefix="/data-owners", tags=["data-owners"])


def _table_preview_rows(owner: DataOwner) -> list[DataOwnerTablePreviewOut]:
    previews: list[DataOwnerTablePreviewOut] = []
    for table in sorted(owner.tables, key=lambda item: (item.schema.name, item.name)):
        previews.append(
            DataOwnerTablePreviewOut(
                id=table.id,
                name=table.name,
                schema_name=table.schema.name,
                database_name=table.schema.database.name,
                datasource_name=table.schema.database.datasource.name,
                description=table.description_manual or table.description_source,
            )
        )
    return previews


@router.get("", response_model=PageOut[DataOwnerListItemOut])
def list_data_owners(
    q: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[DataOwnerListItemOut]:
    query = (
        select(DataOwner)
        .options(
            selectinload(DataOwner.tables)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource)
        )
        .order_by(DataOwner.name)
    )
    if q:
        pattern = f"%{q.strip()}%"
        query = query.where(
            or_(
                DataOwner.name.ilike(pattern),
                DataOwner.email.ilike(pattern),
                DataOwner.area.ilike(pattern),
            )
        )
    if active is not None:
        query = query.where(DataOwner.is_active == active)

    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 25), 1)
    total = int(db.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0)
    owners = db.scalars(
        query.offset((normalized_page - 1) * normalized_page_size).limit(normalized_page_size)
    ).all()
    items: list[DataOwnerListItemOut] = []
    for owner in owners:
        previews = _table_preview_rows(owner)
        payload = {
            "id": owner.id,
            "name": owner.name,
            "email": owner.email,
            "area": owner.area,
            "description": owner.description,
            "is_active": owner.is_active,
            "created_at": owner.created_at,
            "updated_at": owner.updated_at,
            "tables_count": len(previews),
            "tables_preview": previews[:4],
        }
        if not can_view_sensitive_data(current_user, table=owner.tables[0] if owner.tables else None):
            payload = mask_payload_by_policy(payload, can_view_sensitive=False)
            payload["name"] = "[masked]"
            payload["email"] = "[masked]"
        items.append(
            DataOwnerListItemOut(**payload)
        )
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    return PageOut[DataOwnerListItemOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=normalized_page * normalized_page_size < total,
        items=items,
    )


@router.get("/{owner_id}", response_model=DataOwnerDetailOut)
def get_data_owner(
    owner_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataOwnerDetailOut:
    owner = db.scalar(
        select(DataOwner)
        .options(
            selectinload(DataOwner.tables)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource)
        )
        .where(DataOwner.id == owner_id)
    )
    if not owner:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found")
    tables = _table_preview_rows(owner)
    payload = {
        "id": owner.id,
        "name": owner.name,
        "email": owner.email,
        "area": owner.area,
        "description": owner.description,
        "is_active": owner.is_active,
        "created_at": owner.created_at,
        "updated_at": owner.updated_at,
        "tables_count": len(tables),
        "tables": tables,
    }
    if not can_view_sensitive_data(current_user, table=owner.tables[0] if owner.tables else None):
        payload = mask_payload_by_policy(payload, can_view_sensitive=False)
        payload["name"] = "[masked]"
        payload["email"] = "[masked]"
    return DataOwnerDetailOut(**payload)


@router.post("", response_model=DataOwnerOut, status_code=status.HTTP_201_CREATED)
def create_data_owner(
    payload: DataOwnerCreate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> DataOwnerOut:
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Data owner name is required")
    existing = db.scalar(select(DataOwner).where(func.lower(DataOwner.email) == str(payload.email).lower()))
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Data owner email already exists")

    owner = DataOwner(
        name=name,
        email=payload.email.lower().strip(),
        area=payload.area.strip() if payload.area else None,
        description=payload.description.strip() if payload.description else None,
        is_active=payload.is_active,
    )
    db.add(owner)
    db.flush()
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="data_owner.create",
        entity_type="data_owner",
        entity_id=owner.id,
        message="Data owner created",
        changes={"after": {"name": owner.name, "email": owner.email, "area": owner.area, "is_active": owner.is_active}},
    )
    db.commit()
    db.refresh(owner)
    return owner


@router.put("/{owner_id}", response_model=DataOwnerOut)
def update_data_owner(
    owner_id: int,
    payload: DataOwnerUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> DataOwnerOut:
    owner = db.get(DataOwner, owner_id)
    if not owner:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found")

    before = {
        "name": owner.name,
        "email": owner.email,
        "area": owner.area,
        "description": owner.description,
        "is_active": owner.is_active,
    }
    updates = payload.model_dump(exclude_unset=True)
    if "email" in updates and updates["email"] is not None:
        existing = db.scalar(
            select(DataOwner).where(
                func.lower(DataOwner.email) == str(updates["email"]).lower(),
                DataOwner.id != owner_id,
            )
        )
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Data owner email already exists")
        updates["email"] = str(updates["email"]).lower().strip()
    if "name" in updates and isinstance(updates["name"], str):
        updates["name"] = updates["name"].strip()
        if not updates["name"]:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Data owner name is required")
    for field in ("area", "description"):
        if field in updates and isinstance(updates[field], str):
            updates[field] = updates[field].strip() or None
    for key, value in updates.items():
        setattr(owner, key, value)

    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="data_owner.update",
        entity_type="data_owner",
        entity_id=owner.id,
        message="Data owner updated",
        changes={
            "before": before,
            "after": {
                "name": owner.name,
                "email": owner.email,
                "area": owner.area,
                "description": owner.description,
                "is_active": owner.is_active,
            },
        },
    )
    db.commit()
    db.refresh(owner)
    return owner


@router.delete("/{owner_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_data_owner(
    owner_id: int,
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> None:
    owner = db.scalar(select(DataOwner).options(selectinload(DataOwner.tables)).where(DataOwner.id == owner_id))
    if not owner:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found")

    try:
        impact_payload, _, _ = get_ownership_delete_impact(db, current_user=user, owner_id=owner_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found") from None

    if impact_payload.impact.asset_count > 0 and not force:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "message": "Este owner possui ativos associados. Use force=true para remover conscientemente ou reatribua os ativos antes.",
                **impact_payload.model_dump(),
            },
        )

    for table in owner.tables:
        table.data_owner_id = None
        table.owner = None
        table.owner_email = None

    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="data_owner.delete",
        entity_type="data_owner",
        entity_id=owner.id,
        message="Data owner deleted",
        changes={"before": {"name": owner.name, "email": owner.email}, "after": {"force": force}},
    )
    db.delete(owner)
    db.commit()


@router.get("/{owner_id}/delete-impact", response_model=OwnershipDeleteImpactOut)
def get_data_owner_delete_impact(
    owner_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> OwnershipDeleteImpactOut:
    try:
        impact_payload, _, _ = get_ownership_delete_impact(db, current_user=current_user, owner_id=owner_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found") from None
    return impact_payload


@router.get("/{owner_id}/reassign-preview", response_model=OwnershipReassignPreviewOut)
def get_data_owner_reassign_preview(
    owner_id: int,
    target_owner_id: int | None = Query(default=None, ge=1),
    asset_ids: list[int] | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=5000),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> OwnershipReassignPreviewOut:
    try:
        return get_ownership_reassign_preview(
            db,
            current_user=current_user,
            owner_id=owner_id,
            target_owner_id=target_owner_id,
            asset_ids=asset_ids,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        detail = str(exc)
        if "Target data owner not found" in detail or "Data owner not found" in detail:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found") from None
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail) from None


@router.post("/{owner_id}/reassign-assets", response_model=OwnershipReassignResultOut)
def post_data_owner_reassign_assets(
    owner_id: int,
    payload: OwnershipReassignRequestIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> OwnershipReassignResultOut:
    try:
        return reassign_ownership_assets(
            db,
            current_user=current_user,
            owner_id=owner_id,
            payload=payload,
            audit_kwargs=request_audit_kwargs(request, current_user),
        )
    except ValueError as exc:
        detail = str(exc)
        if "Target data owner not found" in detail or "Data owner not found" in detail:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data owner not found") from None
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail) from None
