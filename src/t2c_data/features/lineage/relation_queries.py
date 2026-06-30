from __future__ import annotations

from sqlalchemy import Select, and_, func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.models.auth import User
from t2c_data.features.lineage.visibility import asset_visible_to_user, relation_visible_to_user, visible_lineage_asset_ids, visible_lineage_table_id_set
from t2c_data.features.lineage.shared import (
    asset_display_name,
    canonical_lineage_asset_key,
    build_asset_key,
    layer_from_table,
    serialize_relation,
)
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.schemas.lineage import LineageAssetCandidateOut, LineageOverviewOut, LineageRelationListOut


def list_assets(
    db: Session,
    *,
    current_user: User | None = None,
    query: str | None = None,
    asset_type: str | None = None,
    layer: str | None = None,
    status: str | None = None,
) -> list[LineageAsset]:
    stmt: Select[tuple[LineageAsset]] = select(LineageAsset).order_by(LineageAsset.updated_at.desc(), LineageAsset.id.desc())
    if query:
        pattern = f"%{query.strip()}%"
        stmt = stmt.where(
            or_(
                LineageAsset.asset_name.ilike(pattern),
                LineageAsset.schema_name.ilike(pattern),
                LineageAsset.object_name.ilike(pattern),
                LineageAsset.system_name.ilike(pattern),
            )
        )
    if asset_type:
        stmt = stmt.where(LineageAsset.asset_type == asset_type)
    if layer:
        stmt = stmt.where(LineageAsset.layer == layer)
    if status == "active":
        stmt = stmt.where(LineageAsset.is_active.is_(True))
    elif status == "inactive":
        stmt = stmt.where(LineageAsset.is_active.is_(False))
    items = db.scalars(stmt).all()
    if current_user is None:
        visible_items = items
    else:
        visible_items = [item for item in items if asset_visible_to_user(db, current_user, item)]
    deduped: list[LineageAsset] = []
    seen: set[str] = set()
    for item in visible_items:
        key = canonical_lineage_asset_key(item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped



def list_asset_candidates(
    db: Session,
    *,
    current_user: User | None = None,
    query: str | None = None,
    limit: int = 25,
) -> list[LineageAssetCandidateOut]:
    from t2c_data.features.lineage.shared import serialize_asset_ref

    assets = list_assets(db, current_user=current_user, query=query, status="active")[:limit]
    items: list[LineageAssetCandidateOut] = [
        LineageAssetCandidateOut(
            **serialize_asset_ref(asset).model_dump(),
            lineage_asset_id=asset.id,
        )
        for asset in assets
    ]
    if len(items) >= limit:
        return items[:limit]

    table_stmt = (
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .order_by(TableEntity.updated_at.desc(), TableEntity.id.desc())
    )
    if query:
        pattern = f"%{query.strip()}%"
        table_stmt = table_stmt.where(
            or_(
                TableEntity.name.ilike(pattern),
                Schema.name.ilike(pattern),
                Database.name.ilike(pattern),
                DataSource.name.ilike(pattern),
            )
        )
    visible_table_ids = visible_lineage_table_id_set(db, current_user)
    if visible_table_ids is not None:
        table_stmt = table_stmt.where(TableEntity.id.in_(sorted(visible_table_ids)) if visible_table_ids else False)
    existing_catalog_ids = {item.catalog_table_id for item in items if item.catalog_table_id is not None}
    for table, schema, _database, datasource in db.execute(table_stmt.limit(limit * 2)).all():
        if table.id in existing_catalog_ids:
            continue
        items.append(
            LineageAssetCandidateOut(
                id=None,
                lineage_asset_id=None,
                catalog_table_id=table.id,
                datasource_id=datasource.id,
                asset_key=build_asset_key(
                    asset_type="view" if table.table_type == "view" else "table",
                    layer=layer_from_table(table, schema),
                    system_name=datasource.name,
                    schema_name=schema.name if schema.name != "default" else None,
                    object_name=table.name,
                    asset_name=asset_display_name(table, schema),
                    catalog_table_id=table.id,
                ),
                asset_name=asset_display_name(table, schema),
                asset_type="view" if table.table_type == "view" else "table",
                layer=layer_from_table(table, schema),
                schema_name=schema.name if schema.name != "default" else None,
                object_name=table.name,
                system_name=datasource.name,
                description=table.description_manual or table.description_source,
                is_active=True,
            )
        )
        if len(items) >= limit:
            break
    return items[:limit]



def list_relations(
    db: Session,
    *,
    current_user: User | None = None,
    query: str | None = None,
    layer: str | None = None,
    asset_type: str | None = None,
    relation_type: str | None = None,
    origin: str | None = None,
    status: str | None = None,
    process_name: str | None = None,
    dashboard_name: str | None = None,
) -> list[LineageRelation]:
    items, _total, _has_more = list_relations_page(
        db,
        current_user=current_user,
        query=query,
        layer=layer,
        asset_type=asset_type,
        relation_type=relation_type,
        origin=origin,
        status=status,
        process_name=process_name,
        dashboard_name=dashboard_name,
        page=1,
        page_size=500,
    )
    return items


def list_relations_page(
    db: Session,
    *,
    current_user: User | None = None,
    query: str | None = None,
    layer: str | None = None,
    asset_type: str | None = None,
    relation_type: str | None = None,
    origin: str | None = None,
    status: str | None = None,
    process_name: str | None = None,
    dashboard_name: str | None = None,
    page: int = 1,
    page_size: int = 200,
) -> tuple[list[LineageRelation], int, bool]:
    src = LineageAsset.__table__.alias("src_asset")
    tgt = LineageAsset.__table__.alias("tgt_asset")
    stmt = (
        select(LineageRelation)
        .join(LineageAsset, LineageRelation.source_asset_id == LineageAsset.id)
        .join(src, LineageRelation.source_asset_id == src.c.id)
        .join(tgt, LineageRelation.target_asset_id == tgt.c.id)
        .options(selectinload(LineageRelation.source_asset), selectinload(LineageRelation.target_asset))
        .order_by(LineageRelation.updated_at.desc(), LineageRelation.id.desc())
    )
    if query:
        pattern = f"%{query.strip()}%"
        stmt = stmt.where(
            or_(
                src.c.asset_name.ilike(pattern),
                tgt.c.asset_name.ilike(pattern),
                LineageRelation.process_name.ilike(pattern),
                LineageRelation.dashboard_name.ilike(pattern),
                LineageRelation.notes.ilike(pattern),
                LineageRelation.evidence.ilike(pattern),
            )
        )
    if layer:
        stmt = stmt.where(or_(src.c.layer == layer, tgt.c.layer == layer))
    if asset_type:
        stmt = stmt.where(or_(src.c.asset_type == asset_type, tgt.c.asset_type == asset_type))
    if relation_type:
        stmt = stmt.where(LineageRelation.relation_type == relation_type)
    if origin == "manual":
        stmt = stmt.where(LineageRelation.discovery_method.in_(["manual", "spreadsheet"]))
    elif origin == "automatic":
        stmt = stmt.where(LineageRelation.discovery_method == "automatic")
    elif origin == "merged":
        stmt = stmt.where(LineageRelation.discovery_method == "merged")
    if status == "active":
        stmt = stmt.where(LineageRelation.is_active.is_(True))
    elif status == "inactive":
        stmt = stmt.where(LineageRelation.is_active.is_(False))
    if process_name:
        stmt = stmt.where(LineageRelation.process_name.ilike(f"%{process_name.strip()}%"))
    if dashboard_name:
        stmt = stmt.where(
            or_(
                LineageRelation.dashboard_name.ilike(f"%{dashboard_name.strip()}%"),
                tgt.c.asset_name.ilike(f"%{dashboard_name.strip()}%"),
            )
        )
    visible_table_ids = visible_lineage_table_id_set(db, current_user)
    if visible_table_ids is not None:
        if not visible_table_ids:
            return [], 0, False
        stmt = stmt.where(
            src.c.catalog_table_id.in_(sorted(visible_table_ids)),
            tgt.c.catalog_table_id.in_(sorted(visible_table_ids)),
        )

    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(min(int(page_size or 200), 500), 1)
    count_stmt = stmt.with_only_columns(func.count(LineageRelation.id)).order_by(None)
    total = int(db.scalar(count_stmt) or 0)
    rows = db.scalars(
        stmt.offset((normalized_page - 1) * normalized_page_size).limit(normalized_page_size + 1)
    ).all()
    has_more = len(rows) > normalized_page_size
    items = rows[:normalized_page_size]
    if current_user is None or visible_table_ids is not None:
        return items, total, has_more
    return [item for item in items if relation_visible_to_user(db, current_user, item)], total, has_more



def lineage_overview(db: Session, *, current_user: User | None = None) -> LineageOverviewOut:
    assets_stmt = select(LineageAsset).where(LineageAsset.is_active.is_(True))
    relations_stmt = select(LineageRelation).where(LineageRelation.is_active.is_(True))
    assets = db.scalars(assets_stmt).all()
    relations = db.scalars(relations_stmt).all()
    if current_user is not None:
        assets = [asset for asset in assets if asset_visible_to_user(db, current_user, asset)]
        relations = [relation for relation in relations if relation_visible_to_user(db, current_user, relation)]
    total_assets = len(assets)
    total_relations = len(relations)
    automatic_relations = (
        len([relation for relation in relations if relation.discovery_method == "automatic"])
    )
    manual_relations = (
        len([relation for relation in relations if relation.discovery_method in ["manual", "spreadsheet"]])
    )
    merged_assets = (
        len([asset for asset in assets if asset.asset_origin == "merged"])
    )
    total_gold_tables_with_lineage = (
        len([asset for asset in assets if asset.asset_type in ["table", "view"] and asset.layer == "gold" and any(
            rel.source_asset_id == asset.id or rel.target_asset_id == asset.id for rel in relations
        )])
    )
    total_dashboards_related = (
        len([asset for asset in assets if asset.asset_type in ["dashboard", "question"] and any(rel.target_asset_id == asset.id for rel in relations)])
    )
    return LineageOverviewOut(
        total_assets=int(total_assets),
        total_relations=int(total_relations),
        total_gold_tables_with_lineage=int(total_gold_tables_with_lineage),
        total_dashboards_related=int(total_dashboards_related),
        automatic_relations=int(automatic_relations),
        manual_relations=int(manual_relations),
        merged_assets=int(merged_assets),
    )



def list_relations_out(db: Session, **filters: str | None) -> LineageRelationListOut:
    current_user = filters.pop("current_user", None)
    page = int(filters.pop("page", 1) or 1)
    page_size = int(filters.pop("page_size", 200) or 200)
    items, total, has_more = list_relations_page(
        db,
        current_user=current_user,
        page=page,
        page_size=page_size,
        **filters,
    )
    return LineageRelationListOut(
        summary=lineage_overview(db, current_user=current_user),
        page=page,
        page_size=page_size,
        total=total,
        has_more=has_more,
        items=[serialize_relation(item) for item in items],
    )
