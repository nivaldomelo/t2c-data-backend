from __future__ import annotations

from fastapi import HTTPException, status

from t2c_data.features.lineage.persistence import create_asset, get_or_create_asset_for_table, update_asset
from t2c_data.features.lineage.relation_actions import (
    create_manual_relation_with_audit,
    deactivate_manual_relation_with_audit,
    serialize_lineage_relation,
    update_manual_relation_with_audit,
)
from t2c_data.features.lineage.spec_actions import build_table_lineage_document, upsert_lineage_spec_with_audit
from t2c_data.features.lineage.source_configs import (
    create_source_config,
    get_source_config,
    serialize_source_config,
    update_source_config,
)
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import LineageAssetOut
from t2c_data.services.audit import add_audit_log


def create_lineage_source_with_audit(*, db, payload, user: User):
    source = create_source_config(db, payload)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.source.create",
        entity_type="lineage_source",
        entity_id=source.id,
        message="Lineage source created",
        changes={"name": source.name, "source_type": source.source_type, "base_url": source.base_url},
    )
    db.commit()
    db.refresh(source)
    return source


def update_lineage_source_with_audit(*, db, source_id: int, payload, user: User):
    source = get_source_config(db, source_id)
    before = serialize_source_config(source).model_dump(mode="json")
    updated = update_source_config(db, source, payload)
    after = serialize_source_config(updated).model_dump(mode="json")
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.source.update",
        entity_type="lineage_source",
        entity_id=updated.id,
        message="Lineage source updated",
        changes={"before": before, "after": after},
    )
    db.commit()
    db.refresh(updated)
    return updated


def create_lineage_asset_with_audit(*, db, payload, user: User):
    asset = create_asset(db, payload)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.asset.create",
        entity_type="lineage_asset",
        entity_id=asset.id,
        message="Lineage asset created",
        changes={"asset_key": asset.asset_key, "asset_name": asset.asset_name, "asset_type": asset.asset_type},
    )
    db.commit()
    db.refresh(asset)
    return asset


def update_lineage_asset_with_audit(*, db, asset, payload, user: User):
    before = LineageAssetOut.model_validate(asset, from_attributes=True).model_dump(mode="json")
    updated = update_asset(db, asset, payload.model_dump(exclude_unset=True))
    after = LineageAssetOut.model_validate(updated, from_attributes=True).model_dump(mode="json")
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.asset.update",
        entity_type="lineage_asset",
        entity_id=asset.id,
        message="Lineage asset updated",
        changes={"before": before, "after": after},
    )
    db.commit()
    db.refresh(updated)
    return updated

def ensure_lineage_asset_from_table_with_audit(*, db, table_id: int, user: User):
    asset = get_or_create_asset_for_table(db, table_id)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.asset.ensure_from_table",
        entity_type="lineage_asset",
        entity_id=asset.id,
        message="Catalog table prepared for lineage",
        changes={"catalog_table_id": table_id, "asset_key": asset.asset_key},
    )
    db.commit()
    db.refresh(asset)
    return asset
