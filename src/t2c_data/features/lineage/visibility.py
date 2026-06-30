from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.access_control.policy import visible_table_ids
from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.models.auth import User
from t2c_data.models.catalog import Database, Schema, TableEntity
from t2c_data.models.lineage import LineageAsset, LineageRelation


_MISSING = object()


def _scope_cache(db: Session, bucket: str, user: User | None):
    """Per-session memo so the heavy visibility sets are computed once per request.

    lineage_overview / list_relations_page call the visibility checks once per asset
    and relation; without this the full-catalog scan ran O(n) times and could time
    out once the graph had many (unmatched) assets.
    """
    cache = db.info.setdefault("_lineage_scope_cache", {})
    key = (bucket, getattr(user, "id", None))
    return cache, key


def visible_lineage_table_id_set(db: Session, user: User | None) -> set[int] | None:
    if user is None:
        return None
    if is_admin_role(user_role_names(user)):
        return None
    cache, key = _scope_cache(db, "tables", user)
    cached = cache.get(key, _MISSING)
    if cached is not _MISSING:
        return cached
    tables = db.scalars(
        select(TableEntity)
        .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
        .order_by(TableEntity.id.asc())
    ).all()
    result = set(visible_table_ids(user, tables))
    cache[key] = result
    return result


def visible_lineage_asset_ids(db: Session, user: User | None) -> set[int] | None:
    table_ids = visible_lineage_table_id_set(db, user)
    if table_ids is None:
        return None
    if not table_ids:
        return set()
    cache, key = _scope_cache(db, "assets", user)
    cached = cache.get(key, _MISSING)
    if cached is not _MISSING:
        return cached
    relations = db.scalars(
        select(LineageRelation)
        .options(selectinload(LineageRelation.source_asset), selectinload(LineageRelation.target_asset))
        .where(LineageRelation.is_active.is_(True))
    ).all()
    asset_ids: set[int] = set()
    for relation in relations:
        source_visible = relation.source_asset.catalog_table_id in table_ids if relation.source_asset.catalog_table_id else False
        target_visible = relation.target_asset.catalog_table_id in table_ids if relation.target_asset.catalog_table_id else False
        if source_visible and target_visible:
            asset_ids.add(relation.source_asset_id)
            asset_ids.add(relation.target_asset_id)
    for asset in db.scalars(select(LineageAsset).where(LineageAsset.catalog_table_id.in_(sorted(table_ids)))).all():
        asset_ids.add(asset.id)
    cache[key] = asset_ids
    return asset_ids


def relation_visible_to_user(db: Session, user: User | None, relation: LineageRelation) -> bool:
    table_ids = visible_lineage_table_id_set(db, user)
    if table_ids is None:
        return True
    source_visible = relation.source_asset.catalog_table_id in table_ids if relation.source_asset.catalog_table_id else False
    target_visible = relation.target_asset.catalog_table_id in table_ids if relation.target_asset.catalog_table_id else False
    return source_visible and target_visible


def asset_visible_to_user(db: Session, user: User | None, asset: LineageAsset) -> bool:
    table_ids = visible_lineage_table_id_set(db, user)
    if table_ids is None:
        return True
    if asset.catalog_table_id is not None:
        return asset.catalog_table_id in table_ids
    if asset.id is None:
        return False
    asset_ids = visible_lineage_asset_ids(db, user)
    if asset_ids is None:
        return True
    return asset.id in asset_ids


__all__ = [
    "asset_visible_to_user",
    "relation_visible_to_user",
    "visible_lineage_asset_ids",
    "visible_lineage_table_id_set",
]
