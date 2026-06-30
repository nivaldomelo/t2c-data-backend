from __future__ import annotations

from collections.abc import Iterable

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from t2c_data.models.access_control import AccessGroup, DataAccessGrant
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.schemas.admin import AccessGroupOut, AccessGroupMemberOut, DataScopeGrantOut


def _grant_table_fqn(grant: DataAccessGrant) -> str | None:
    if grant.table is None:
        return None
    table = grant.table
    schema = table.schema
    database = schema.database if schema else None
    datasource = database.datasource if database and database.datasource else None
    parts = [part for part in [
        datasource.name if datasource else None,
        database.name if database else None,
        schema.name if schema else None,
        table.name,
    ] if part]
    return ".".join(parts) if parts else None


def _grant_datasource_fqn(grant: DataAccessGrant) -> str | None:
    if grant.datasource is None:
        return None
    return grant.datasource.name


def data_scope_grant_out(grant: DataAccessGrant) -> DataScopeGrantOut:
    datasource_name = grant.datasource.name if grant.datasource else None
    schema_name = grant.schema.name if grant.schema else None
    table_name = grant.table.name if grant.table else None
    return DataScopeGrantOut(
        id=grant.id,
        effect=grant.effect,
        datasource_id=grant.datasource_id,
        schema_id=grant.schema_id,
        table_id=grant.table_id,
        note=grant.note,
        scope_kind=grant.scope_kind,
        datasource_name=datasource_name,
        schema_name=schema_name,
        table_name=table_name,
        datasource_fqn=_grant_datasource_fqn(grant),
        table_fqn=_grant_table_fqn(grant),
        created_at=grant.created_at,
        updated_at=grant.updated_at,
    )


def access_group_out(group: AccessGroup) -> AccessGroupOut:
    members = [
        AccessGroupMemberOut(
            id=user.id,
            email=user.email,
            name=user.name,
            full_name=user.full_name,
            is_active=user.is_active,
        )
        for user in sorted(group.users, key=lambda item: item.email)
    ]
    grants = [data_scope_grant_out(grant) for grant in sorted(group.grants, key=lambda item: item.id)]
    return AccessGroupOut(
        id=group.id,
        name=group.name,
        description=group.description,
        is_active=group.is_active,
        members=members,
        grants=grants,
        created_at=group.created_at,
        updated_at=group.updated_at,
    )


def list_access_groups_out(db: Session) -> list[AccessGroupOut]:
    groups = db.scalars(
        select(AccessGroup)
        .options(
            selectinload(AccessGroup.users),
            selectinload(AccessGroup.grants).selectinload(DataAccessGrant.datasource),
            selectinload(AccessGroup.grants).selectinload(DataAccessGrant.schema),
            selectinload(AccessGroup.grants).selectinload(DataAccessGrant.table).selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
        .order_by(AccessGroup.id)
    ).all()
    return [access_group_out(group) for group in groups]


def get_access_group_or_404(db: Session, group_id: int) -> AccessGroup:
    group = db.scalar(
        select(AccessGroup)
        .options(
            selectinload(AccessGroup.users),
            selectinload(AccessGroup.grants).selectinload(DataAccessGrant.datasource),
            selectinload(AccessGroup.grants).selectinload(DataAccessGrant.schema),
            selectinload(AccessGroup.grants).selectinload(DataAccessGrant.table).selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
        .where(AccessGroup.id == group_id)
    )
    if not group:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Access group not found")
    return group


def validate_access_group_name_available(db: Session, name: str, *, exclude_group_id: int | None = None) -> None:
    query = select(AccessGroup).where(AccessGroup.name == name)
    if exclude_group_id is not None:
        query = query.where(AccessGroup.id != exclude_group_id)
    if db.scalar(query):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Access group already exists")


def _resolve_users_by_ids(db: Session, user_ids: Iterable[int] | None) -> list[User]:
    ids = [int(user_id) for user_id in (user_ids or []) if user_id is not None]
    if not ids:
        return []
    return db.scalars(select(User).where(User.id.in_(ids))).all()


def _resolve_group_grants(db: Session, grants: list[dict]) -> list[DataAccessGrant]:
    payloads = grants or []
    resolved: list[DataAccessGrant] = []
    for payload in payloads:
        target_count = sum(1 for value in [payload.get("datasource_id"), payload.get("schema_id"), payload.get("table_id")] if value is not None)
        if target_count != 1:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Each grant must target exactly one datasource, schema, or object",
            )
        effect = str(payload.get("effect") or "allow")
        if effect not in {"allow", "deny"}:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid grant effect")
        grant = DataAccessGrant(
            effect=effect,
            datasource_id=payload.get("datasource_id"),
            schema_id=payload.get("schema_id"),
            table_id=payload.get("table_id"),
            note=payload.get("note") or None,
        )
        resolved.append(grant)
    return resolved


def apply_access_group_updates(
    db: Session,
    group: AccessGroup,
    updates: dict,
) -> None:
    if "member_user_ids" in updates:
        member_user_ids = updates.pop("member_user_ids") or []
        group.users = _resolve_users_by_ids(db, member_user_ids)
    if "grants" in updates:
        grants = updates.pop("grants") or []
        group.grants = _resolve_group_grants(db, grants)
    for key, value in updates.items():
        setattr(group, key, value)


def apply_user_access_scope_updates(db: Session, user: User, updates: dict) -> None:
    if "access_group_ids" in updates:
        group_ids = updates.pop("access_group_ids") or []
        groups = db.scalars(select(AccessGroup).where(AccessGroup.id.in_(group_ids))).all() if group_ids else []
        user.access_groups = list(groups)
    if "data_scope_grants" in updates:
        grants = updates.pop("data_scope_grants") or []
        user.access_grants = _resolve_group_grants(db, grants)
