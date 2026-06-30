from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.graph_summary import collect_asset_summary
from t2c_data.features.lineage.shared import (
    asset_display_name,
    layer_from_table,
    serialize_asset_ref,
    table_context,
)
from t2c_data.features.lineage.visibility import asset_visible_to_user
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageAsset
from t2c_data.schemas.lineage import LineageAssetSummaryOut, LineageImpactOut


def get_asset_summary(
    db: Session,
    asset_id: int,
    *,
    current_user: User | None = None,
    max_relations: int | None = None,
    max_depth: int = 1,
) -> LineageAssetSummaryOut:
    asset = db.get(LineageAsset, asset_id)
    if not asset or not asset.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage asset not found")
    if current_user is not None and not asset_visible_to_user(db, current_user, asset):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage asset not found")
    return collect_asset_summary(db, asset, current_user=current_user, max_relations=max_relations, max_depth=max_depth)


def get_table_summary(
    db: Session,
    table_id: int,
    *,
    current_user: User | None = None,
    max_relations: int | None = None,
    max_depth: int = 1,
) -> LineageAssetSummaryOut:
    asset = db.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == table_id))
    if asset:
        if current_user is not None and not asset_visible_to_user(db, current_user, asset):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage asset not found")
        return collect_asset_summary(db, asset, current_user=current_user, max_relations=max_relations, max_depth=max_depth)
    table, schema, _database, datasource = table_context(db, table_id)
    ephemeral = LineageAsset(
        id=0,
        catalog_table_id=table.id,
        datasource_id=datasource.id,
        asset_key=f"catalog_table:{table.id}",
        asset_name=asset_display_name(table, schema),
        asset_type="view" if table.table_type == "view" else "table",
        layer=layer_from_table(table, schema),
        schema_name=schema.name if schema.name != "default" else None,
        object_name=table.name,
        system_name=datasource.name,
        description=table.description_manual or table.description_source,
        asset_origin="manual",
        is_active=True,
    )
    return LineageAssetSummaryOut(
        asset=serialize_asset_ref(ephemeral),
        upstream=[],
        downstream=[],
        related_processes=[],
        related_dashboards=[],
        related_jobs=[],
        lineage_origin="manual",
        lineage_sources=[],
        recent_runs=[],
        impact=LineageImpactOut(
            upstream_count=0,
            downstream_count=0,
            process_count=0,
            dashboard_count=0,
            direct_dependencies_count=0,
            impact_level="low",
        ),
        graph_nodes=[],
        graph_edges=[],
        notes=[],
    )


__all__ = [
    "get_asset_summary",
    "get_table_summary",
]
