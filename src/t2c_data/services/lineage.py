from __future__ import annotations

"""Compatibility bridge for lineage domain services.

The canonical lineage application/domain modules now live under
`app.features.lineage`. This file remains only to preserve imports from the
older `app.services.lineage` surface while the migration finishes.
"""

from sqlalchemy.orm import Session

from t2c_data.features.lineage.persistence import create_asset as feature_create_asset
from t2c_data.features.lineage.persistence import create_relation as feature_create_relation
from t2c_data.features.lineage.persistence import get_or_create_asset_for_table as feature_get_or_create_asset_for_table
from t2c_data.features.lineage.persistence import update_asset as feature_update_asset
from t2c_data.features.lineage.persistence import update_relation as feature_update_relation
from t2c_data.features.lineage.queries import get_asset_summary as feature_get_asset_summary
from t2c_data.features.lineage.queries import get_lineage_spec_for_table as feature_get_lineage_spec_for_table
from t2c_data.features.lineage.queries import get_lineage_spec_lookup_by_fqn as feature_get_lineage_spec_lookup_by_fqn
from t2c_data.features.lineage.queries import get_table_summary as feature_get_table_summary
from t2c_data.features.lineage.queries import list_asset_candidates as feature_list_asset_candidates
from t2c_data.features.lineage.queries import list_assets as feature_list_assets
from t2c_data.features.lineage.queries import list_relations as feature_list_relations
from t2c_data.features.lineage.queries import list_relations_out as feature_list_relations_out
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.schemas.lineage import (
    LineageAssetCandidateOut,
    LineageAssetCreate,
    LineageAssetSummaryOut,
    LineageRelationCreate,
    LineageRelationListOut,
    LineageRelationUpdate,
    LineageSpecLookupOut,
    LineageSpecOut,
)

__all__ = [
    "create_asset",
    "create_relation",
    "get_asset_summary",
    "get_lineage_spec_for_table",
    "get_lineage_spec_lookup_by_fqn",
    "get_or_create_asset_for_table",
    "get_table_summary",
    "list_asset_candidates",
    "list_assets",
    "list_relations",
    "list_relations_out",
    "update_asset",
    "update_relation",
]


def get_or_create_asset_for_table(db: Session, table_id: int) -> LineageAsset:
    return feature_get_or_create_asset_for_table(db, table_id)


def create_asset(db: Session, payload: LineageAssetCreate) -> LineageAsset:
    return feature_create_asset(db, payload)


def update_asset(db: Session, asset: LineageAsset, payload: dict) -> LineageAsset:
    return feature_update_asset(db, asset, payload)


def create_relation(db: Session, payload: LineageRelationCreate, actor_user_id: int | None) -> LineageRelation:
    return feature_create_relation(db, payload, actor_user_id)


def update_relation(db: Session, relation: LineageRelation, payload: LineageRelationUpdate, actor_user_id: int | None) -> LineageRelation:
    return feature_update_relation(db, relation, payload, actor_user_id)


def list_assets(
    db: Session,
    *,
    query: str | None = None,
    asset_type: str | None = None,
    layer: str | None = None,
    status: str | None = None,
):
    return feature_list_assets(
        db,
        query=query,
        asset_type=asset_type,
        layer=layer,
        status=status,
    )


def list_asset_candidates(db: Session, *, query: str | None = None, limit: int = 25) -> list[LineageAssetCandidateOut]:
    return feature_list_asset_candidates(db, query=query, limit=limit)


def list_relations(
    db: Session,
    *,
    query: str | None = None,
    layer: str | None = None,
    asset_type: str | None = None,
    relation_type: str | None = None,
    origin: str | None = None,
    status: str | None = None,
    process_name: str | None = None,
    dashboard_name: str | None = None,
) -> list[LineageRelation]:
    return feature_list_relations(
        db,
        query=query,
        layer=layer,
        asset_type=asset_type,
        relation_type=relation_type,
        origin=origin,
        status=status,
        process_name=process_name,
        dashboard_name=dashboard_name,
    )


def list_relations_out(db: Session, **filters: str | None) -> LineageRelationListOut:
    return feature_list_relations_out(db, **filters)


def get_asset_summary(db: Session, asset_id: int) -> LineageAssetSummaryOut:
    return feature_get_asset_summary(db, asset_id)


def get_table_summary(db: Session, table_id: int) -> LineageAssetSummaryOut:
    return feature_get_table_summary(db, table_id)


def get_lineage_spec_for_table(db: Session, table_id: int) -> LineageSpecOut:
    return feature_get_lineage_spec_for_table(db, table_id)


def get_lineage_spec_lookup_by_fqn(db: Session, table_fqn: str) -> LineageSpecLookupOut:
    return feature_get_lineage_spec_lookup_by_fqn(db, table_fqn)
