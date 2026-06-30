from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.shared import asset_display_name, build_asset_key, layer_from_table, table_context
from t2c_data.features.lineage.versioning import record_relation_version
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.schemas.lineage import LineageAssetCreate, LineageRelationAssetRefIn, LineageRelationCreate, LineageRelationUpdate


def get_or_create_asset_for_table(db: Session, table_id: int) -> LineageAsset:
    asset = db.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == table_id))
    if asset:
        return asset

    table, schema, _database, datasource = table_context(db, table_id)
    asset_type = "view" if table.table_type == "view" else "table"
    asset = db.scalar(
        select(LineageAsset).where(
            LineageAsset.datasource_id == datasource.id,
            LineageAsset.schema_name == (schema.name if schema.name != "default" else None),
            LineageAsset.object_name == table.name,
            LineageAsset.asset_type == asset_type,
        )
    )
    if asset:
        asset.catalog_table_id = table.id
        asset.asset_key = f"catalog_table:{table.id}"
        asset.asset_name = asset_display_name(table, schema)
        asset.layer = layer_from_table(table, schema)
        asset.system_name = datasource.name
        asset.description = table.description_manual or table.description_source
        asset.is_active = True
        db.flush()
        return asset

    asset = LineageAsset(
        catalog_table_id=table.id,
        datasource_id=datasource.id,
        asset_key=build_asset_key(
            asset_type=asset_type,
            layer=layer_from_table(table, schema),
            system_name=datasource.name,
            schema_name=schema.name if schema.name != "default" else None,
            object_name=table.name,
            asset_name=asset_display_name(table, schema),
            catalog_table_id=table.id,
        ),
        asset_name=asset_display_name(table, schema),
        asset_type=asset_type,
        layer=layer_from_table(table, schema),
        schema_name=schema.name if schema.name != "default" else None,
        object_name=table.name,
        system_name=datasource.name,
        description=table.description_manual or table.description_source,
        asset_origin="manual",
        is_active=True,
    )
    db.add(asset)
    db.flush()
    return asset


def create_asset(db: Session, payload: LineageAssetCreate) -> LineageAsset:
    if payload.catalog_table_id is not None:
        return get_or_create_asset_for_table(db, payload.catalog_table_id)

    asset_key = payload.asset_key or build_asset_key(
        asset_type=payload.asset_type,
        layer=payload.layer,
        system_name=payload.system_name,
        schema_name=payload.schema_name,
        object_name=payload.object_name,
        asset_name=payload.asset_name,
    )
    existing = db.scalar(select(LineageAsset).where(LineageAsset.asset_key == asset_key))
    if existing:
        return existing

    asset = LineageAsset(
        catalog_table_id=None,
        datasource_id=payload.datasource_id,
        asset_key=asset_key,
        asset_name=payload.asset_name,
        asset_type=payload.asset_type,
        layer=payload.layer,
        schema_name=payload.schema_name,
        object_name=payload.object_name,
        system_name=payload.system_name,
        description=payload.description,
        asset_origin="manual",
        is_active=True,
    )
    db.add(asset)
    db.flush()
    return asset


def update_asset(db: Session, asset: LineageAsset, payload: dict) -> LineageAsset:
    for field, value in payload.items():
        setattr(asset, field, value)
    if not asset.catalog_table_id:
        asset.asset_key = build_asset_key(
            asset_type=asset.asset_type,
            layer=asset.layer,
            system_name=asset.system_name,
            schema_name=asset.schema_name,
            object_name=asset.object_name,
            asset_name=asset.asset_name,
        )
    db.flush()
    return asset


def _resolve_asset_ref(db: Session, ref: LineageRelationAssetRefIn, side: str) -> LineageAsset:
    if ref.asset_id is not None:
        asset = db.get(LineageAsset, ref.asset_id)
        if not asset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{side} asset not found")
        return asset
    if ref.catalog_table_id is not None:
        return get_or_create_asset_for_table(db, ref.catalog_table_id)
    if ref.asset is not None:
        return create_asset(db, ref.asset)
    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"{side} asset is required")


def _ensure_no_duplicate_relation(
    db: Session,
    *,
    source_asset_id: int,
    target_asset_id: int,
    relation_type: str,
    process_name: str | None,
    process_type: str | None,
    dashboard_name: str | None,
    exclude_id: int | None = None,
) -> None:
    stmt = select(LineageRelation).where(
        LineageRelation.source_asset_id == source_asset_id,
        LineageRelation.target_asset_id == target_asset_id,
        LineageRelation.relation_type == relation_type,
        LineageRelation.process_name == process_name,
        LineageRelation.process_type == process_type,
        LineageRelation.dashboard_name == dashboard_name,
        LineageRelation.is_active.is_(True),
    )
    if exclude_id is not None:
        stmt = stmt.where(LineageRelation.id != exclude_id)
    existing = db.scalar(stmt)
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An identical lineage relation already exists")


def create_relation(db: Session, payload: LineageRelationCreate, actor_user_id: int | None) -> LineageRelation:
    source_asset = _resolve_asset_ref(db, payload.source, "Source")
    target_asset = _resolve_asset_ref(db, payload.target, "Target")
    if source_asset.id == target_asset.id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Source and target must be different")

    _ensure_no_duplicate_relation(
        db,
        source_asset_id=source_asset.id,
        target_asset_id=target_asset.id,
        relation_type=payload.relation_type,
        process_name=payload.process_name,
        process_type=payload.process_type,
        dashboard_name=payload.dashboard_name,
    )

    relation = LineageRelation(
        source_asset_id=source_asset.id,
        target_asset_id=target_asset.id,
        relation_type=payload.relation_type,
        process_name=payload.process_name,
        process_type=payload.process_type,
        dashboard_name=payload.dashboard_name,
        notes=payload.notes,
        evidence=payload.evidence,
        discovery_method=payload.discovery_method,
        confidence_score=payload.confidence_score,
        is_verified=bool(payload.is_verified) if payload.is_verified is not None else (payload.discovery_method in {"manual", "spreadsheet"}),
        is_active=True,
        created_by_user_id=actor_user_id,
        updated_by_user_id=actor_user_id,
    )
    db.add(relation)
    db.flush()
    record_relation_version(db, relation, actor_user_id=actor_user_id, force_version=True)
    db.refresh(relation)
    return relation


def update_relation(db: Session, relation: LineageRelation, payload: LineageRelationUpdate, actor_user_id: int | None) -> LineageRelation:
    source_asset = relation.source_asset
    target_asset = relation.target_asset
    if payload.source is not None:
        source_asset = _resolve_asset_ref(db, payload.source, "Source")
    if payload.target is not None:
        target_asset = _resolve_asset_ref(db, payload.target, "Target")
    if source_asset.id == target_asset.id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Source and target must be different")

    relation_type = payload.relation_type or relation.relation_type
    process_name = payload.process_name if payload.process_name is not None else relation.process_name
    process_type = payload.process_type if payload.process_type is not None else relation.process_type
    dashboard_name = payload.dashboard_name if payload.dashboard_name is not None else relation.dashboard_name
    _ensure_no_duplicate_relation(
        db,
        source_asset_id=source_asset.id,
        target_asset_id=target_asset.id,
        relation_type=relation_type,
        process_name=process_name,
        process_type=process_type,
        dashboard_name=dashboard_name,
        exclude_id=relation.id,
    )

    relation.source_asset_id = source_asset.id
    relation.target_asset_id = target_asset.id
    relation.relation_type = relation_type
    relation.process_name = process_name
    relation.process_type = process_type
    relation.dashboard_name = dashboard_name
    if payload.notes is not None:
        relation.notes = payload.notes
    if payload.evidence is not None:
        relation.evidence = payload.evidence
    if payload.confidence_score is not None:
        relation.confidence_score = payload.confidence_score
    if payload.is_verified is not None:
        relation.is_verified = payload.is_verified
    if payload.is_active is not None:
        relation.is_active = payload.is_active
    relation.updated_by_user_id = actor_user_id
    db.flush()
    record_relation_version(db, relation, actor_user_id=actor_user_id, force_version=True)
    db.refresh(relation)
    return relation
