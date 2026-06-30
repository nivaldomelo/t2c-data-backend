from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.persistence import create_asset
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.schemas.lineage import LineageAssetCreate


def load_existing_assets(session: Session) -> dict[str, LineageAsset]:
    return {
        asset.asset_key: asset
        for asset in session.scalars(select(LineageAsset)).all()
    }


def upsert_assets(
    session: Session,
    asset_defs: dict[str, dict[str, object]],
    *,
    existing_assets: dict[str, LineageAsset],
) -> tuple[int, int]:
    created_assets = 0
    updated_assets = 0

    for asset_key, item in asset_defs.items():
        asset = existing_assets.get(asset_key)
        if asset is None:
            asset = create_asset(
                session,
                LineageAssetCreate(
                    asset_key=asset_key,
                    asset_name=str(item.get("asset_name") or asset_key),
                    asset_type=str(item.get("asset_type") or "table"),
                    layer=str(item.get("layer") or "definir"),
                    schema_name=item.get("schema_name"),
                    object_name=item.get("object_name"),
                    system_name=item.get("system_name"),
                    description=item.get("description"),
                ),
            )
            created_assets += 1
        else:
            asset.asset_name = str(item.get("asset_name") or asset.asset_name)
            asset.asset_type = str(item.get("asset_type") or asset.asset_type)
            asset.layer = str(item.get("layer") or asset.layer)
            asset.schema_name = item.get("schema_name") or asset.schema_name
            asset.object_name = item.get("object_name") or asset.object_name
            asset.system_name = item.get("system_name") or asset.system_name
            asset.description = item.get("description") or asset.description
            asset.is_active = bool(item.get("is_active", True))
            updated_assets += 1
        existing_assets[asset_key] = asset

    return created_assets, updated_assets


def load_existing_relations(session: Session) -> dict[tuple[str, str, str], LineageRelation]:
    return {
        (relation.source_asset.asset_key, relation.target_asset.asset_key, relation.relation_type): relation
        for relation in session.scalars(select(LineageRelation).where(LineageRelation.is_active.is_(True))).all()
    }


def upsert_relations(
    session: Session,
    relation_defs: list[dict[str, object]],
    *,
    existing_assets: dict[str, LineageAsset],
    existing_relations: dict[tuple[str, str, str], LineageRelation],
    warnings: list[dict[str, object]],
) -> tuple[int, int, int]:
    created_relations = 0
    updated_relations = 0
    created_dashboard_assets = 0

    for item in relation_defs:
        source_asset = existing_assets.get(str(item["source_asset_key"]))
        target_asset = existing_assets.get(str(item["target_asset_key"]))
        if source_asset is None or target_asset is None:
            warnings.append(
                {
                    "sheet": item["sheet"],
                    "row_number": item["row_number"],
                    "message": f"Relação ignorada por ativo ausente: {item['source_asset_key']} -> {item['target_asset_key']}",
                }
            )
            continue

        key = (source_asset.asset_key, target_asset.asset_key, str(item["relation_type"]))
        relation = existing_relations.get(key)
        is_new_relation = relation is None
        if relation is None:
            relation = LineageRelation(
                source_asset_id=source_asset.id,
                target_asset_id=target_asset.id,
                relation_type=str(item["relation_type"]),
                process_name=item.get("process_name"),
                process_type=item.get("process_type"),
                dashboard_name=item.get("dashboard_name"),
                notes=item.get("notes"),
                discovery_method="spreadsheet",
                confidence_score=100,
                is_active=bool(item.get("is_active", True)),
            )
            session.add(relation)
            created_relations += 1
        else:
            relation.process_name = item.get("process_name") or relation.process_name
            relation.process_type = item.get("process_type") or relation.process_type
            relation.dashboard_name = item.get("dashboard_name") or relation.dashboard_name
            relation.notes = item.get("notes") or relation.notes
            relation.is_active = bool(item.get("is_active", True))
            relation.discovery_method = "spreadsheet"
            updated_relations += 1
        if is_new_relation and target_asset.asset_type in {"dashboard", "question"}:
            created_dashboard_assets += 1

    return created_relations, updated_relations, created_dashboard_assets


__all__ = [
    "load_existing_assets",
    "load_existing_relations",
    "upsert_assets",
    "upsert_relations",
]
