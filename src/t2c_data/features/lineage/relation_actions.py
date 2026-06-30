from __future__ import annotations

from t2c_data.features.lineage.persistence import create_relation, update_relation
from t2c_data.features.lineage.versioning import confidence_tier
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import LineageRelationOut
from t2c_data.services.audit import add_audit_log


def serialize_lineage_relation(relation) -> LineageRelationOut:
    return LineageRelationOut.model_validate(
        {
            "id": relation.id,
            "source_asset": relation.source_asset,
            "target_asset": relation.target_asset,
            "relation_type": relation.relation_type,
            "process_name": relation.process_name,
            "process_type": relation.process_type,
            "dashboard_name": relation.dashboard_name,
            "notes": relation.notes,
            "evidence": relation.evidence,
            "discovery_method": relation.discovery_method,
            "confidence_score": relation.confidence_score,
            "confidence_tier": confidence_tier(relation.confidence_score, is_verified=bool(relation.is_verified)),
            "is_verified": bool(relation.is_verified),
            "version": int(relation.version or 1),
            "last_seen_at": relation.last_seen_at,
            "created_by_user_id": relation.created_by_user_id,
            "updated_by_user_id": relation.updated_by_user_id,
            "is_active": relation.is_active,
            "created_at": relation.created_at,
            "updated_at": relation.updated_at,
        }
    )


def create_manual_relation_with_audit(*, db, payload, user: User) -> LineageRelationOut:
    relation = create_relation(db, payload, user.id)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.relation.create",
        entity_type="lineage_relation",
        entity_id=relation.id,
        message="Lineage relation created",
        changes={
            "source_asset_id": relation.source_asset_id,
            "target_asset_id": relation.target_asset_id,
            "relation_type": relation.relation_type,
        },
    )
    db.commit()
    db.refresh(relation)
    return serialize_lineage_relation(relation)


def update_manual_relation_with_audit(*, db, relation, payload, user: User) -> LineageRelationOut:
    before = {
        "source_asset_id": relation.source_asset_id,
        "target_asset_id": relation.target_asset_id,
        "relation_type": relation.relation_type,
        "process_name": relation.process_name,
        "process_type": relation.process_type,
        "dashboard_name": relation.dashboard_name,
        "notes": relation.notes,
        "is_active": relation.is_active,
    }
    updated = update_relation(db, relation, payload, user.id)
    after = {
        "source_asset_id": updated.source_asset_id,
        "target_asset_id": updated.target_asset_id,
        "relation_type": updated.relation_type,
        "process_name": updated.process_name,
        "process_type": updated.process_type,
        "dashboard_name": updated.dashboard_name,
        "notes": updated.notes,
        "is_active": updated.is_active,
    }
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.relation.update",
        entity_type="lineage_relation",
        entity_id=updated.id,
        message="Lineage relation updated",
        changes={"before": before, "after": after},
    )
    db.commit()
    db.refresh(updated)
    return serialize_lineage_relation(updated)


def deactivate_manual_relation_with_audit(*, db, relation, user: User) -> dict[str, bool]:
    relation.is_active = False
    relation.updated_by_user_id = user.id
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.relation.deactivate",
        entity_type="lineage_relation",
        entity_id=relation.id,
        message="Lineage relation deactivated",
        changes={"is_active": False},
    )
    db.commit()
    return {"success": True}
