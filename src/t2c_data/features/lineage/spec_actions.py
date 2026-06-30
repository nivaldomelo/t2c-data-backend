from __future__ import annotations

from t2c_data.features.lineage.persistence import create_asset, create_relation, get_or_create_asset_for_table
from t2c_data.features.lineage.queries import get_lineage_spec_for_table, get_table_summary
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageRelation
from t2c_data.schemas.lineage import LineageAssetCreate, LineageRelationCreate, TableLineageOut
from t2c_data.services.audit import add_audit_log


def upsert_lineage_spec_with_audit(*, db, table_id: int, payload, user: User):
    from sqlalchemy import or_, select

    asset = get_or_create_asset_for_table(db, table_id)

    existing = db.scalars(
        select(LineageRelation).where(
            LineageRelation.is_active.is_(True),
            or_(LineageRelation.source_asset_id == asset.id, LineageRelation.target_asset_id == asset.id),
        )
    ).all()
    for relation in existing:
        relation.is_active = False
        relation.updated_by_user_id = user.id

    for upstream in payload.upstreams:
        source_asset = create_asset(
            db,
            LineageAssetCreate.model_validate(
                {
                    "asset_name": upstream.name or upstream.object or "Source",
                    "asset_type": "table",
                    "layer": "silver",
                    "schema_name": upstream.schema_name,
                    "object_name": upstream.object,
                    "system_name": upstream.database,
                    "datasource_id": upstream.datasource_id,
                    "description": None,
                }
            ),
        )
        create_relation(
            db,
            LineageRelationCreate(
                source={"asset_id": source_asset.id},
                target={"asset_id": asset.id},
                relation_type="transformation",
                process_name=payload.process.name if payload.process else None,
                process_type=payload.process.type if payload.process else None,
                notes=payload.notes,
            ),
            user.id,
        )

    for downstream in payload.downstreams:
        target_asset = create_asset(
            db,
            LineageAssetCreate.model_validate(
                {
                    "asset_name": downstream.name,
                    "asset_type": "dashboard" if downstream.type == "dashboard" else "table",
                    "layer": "dashboard" if downstream.type == "dashboard" else "gold",
                    "schema_name": None,
                    "object_name": downstream.name,
                    "system_name": "BI",
                    "description": None,
                }
            ),
        )
        create_relation(
            db,
            LineageRelationCreate(
                source={"asset_id": asset.id},
                target={"asset_id": target_asset.id},
                relation_type="consumption" if target_asset.asset_type in {"dashboard", "question"} else "load",
                process_name=payload.process.name if payload.process else None,
                process_type=payload.process.type if payload.process else None,
                dashboard_name=downstream.name if target_asset.asset_type in {"dashboard", "question"} else None,
                notes=payload.notes,
            ),
            user.id,
        )

    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.spec.upsert",
        entity_type="table",
        entity_id=table_id,
        message="Table lineage updated through compatibility endpoint",
        changes={"upstreams": len(payload.upstreams), "downstreams": len(payload.downstreams)},
    )
    db.commit()
    return get_lineage_spec_for_table(db, table_id, current_user=user)


def build_table_lineage_document(*, db, table_id: int, current_user: User | None = None) -> TableLineageOut:
    summary = get_table_summary(db, table_id, current_user=current_user)
    return TableLineageOut(
        table_id=table_id,
        nodes=[
            {
                "id": index + 1,
                "kind": node.kind,
                "label": node.label,
                "datasource_id": None,
                "table_id": node.asset_id,
                "meta": {"asset_type": node.asset_type, "layer": node.layer, "subtitle": node.subtitle},
            }
            for index, node in enumerate(summary.graph_nodes)
        ],
        edges=[
            {
                "id": index + 1,
                "from_node_id": index + 1,
                "to_node_id": index + 2,
                "edge_type": edge.relation_type,
                "transform": None,
                "notes": None,
            }
            for index, edge in enumerate(summary.graph_edges)
        ],
        notes=summary.notes[0] if summary.notes else None,
        updated_at=None,
    )
