"""Derive lineage from Metabase consumption.

Turns the Metabase artifacts the platform already syncs (dashboards/questions and
the tables they query) into automatic ``consumption`` lineage edges
(table -> dashboard/question). This populates the lineage graph from data that
already exists, without any manual entry.

Edges are idempotent (keyed by external_edge_key) and marked automatic. Catalog
matches are preferred (via MetabaseObjectLink, then the lineage table matcher);
references that don't resolve to a catalog table become lightweight "unmatched"
assets keyed by name so the consumption is still visible.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.openlineage_persistence import _normalize_slug, match_catalog_table
from t2c_data.features.lineage.persistence import get_or_create_asset_for_table
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.metabase import MetabaseInstance, MetabaseObject, MetabaseObjectLink

CONSUMPTION = "consumption"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_artifact_asset(db: Session, instance: MetabaseInstance, obj: MetabaseObject) -> LineageAsset:
    node_id = f"metabase:{instance.id}:{obj.object_type}:{obj.external_id}"
    title = obj.title or f"{obj.object_type} {obj.external_id}"
    asset_type = obj.object_type if obj.object_type in {"dashboard", "question"} else "dashboard"
    asset = db.scalar(select(LineageAsset).where(LineageAsset.external_node_id == node_id))
    if asset is None:
        asset = LineageAsset(
            asset_key=node_id,
            asset_name=title,
            asset_type=asset_type,
            layer="dashboard",
            system_name=instance.name,
            asset_origin="automatic",
            external_node_id=node_id,
            external_type="metabase",
            object_name=title,
            description=obj.description,
            is_active=True,
        )
        db.add(asset)
        db.flush()
    else:
        asset.asset_name = title
        asset.asset_type = asset_type
        asset.is_active = True
        db.flush()
    return asset


def _ensure_unmatched_table_asset(db: Session, ref: dict[str, Any]) -> LineageAsset | None:
    full_name = str(ref.get("full_name") or ref.get("name") or "").strip()
    name = str(ref.get("name") or full_name).strip()
    if not full_name or not name:
        return None
    key = "metabase_table:" + _normalize_slug(full_name)
    asset = db.scalar(select(LineageAsset).where(LineageAsset.asset_key == key))
    if asset is None:
        schema = ref.get("schema")
        asset = LineageAsset(
            asset_key=key,
            asset_name=full_name,
            asset_type="table",
            layer="definir",
            schema_name=str(schema).strip() if isinstance(schema, str) and schema.strip() else None,
            object_name=name,
            asset_origin="automatic",
            external_type="metabase_ref",
            is_active=True,
        )
        db.add(asset)
        db.flush()
    return asset


def _consumed_table_assets(db: Session, obj: MetabaseObject) -> list[LineageAsset]:
    assets: list[LineageAsset] = []
    seen_asset_ids: set[int] = set()

    def _add(asset: LineageAsset | None) -> None:
        if asset is not None and asset.id not in seen_asset_ids:
            seen_asset_ids.add(asset.id)
            assets.append(asset)

    # 1) Catalog-matched links (most reliable).
    matched_table_ids: set[int] = set()
    links = db.scalars(
        select(MetabaseObjectLink).where(
            MetabaseObjectLink.metabase_object_id == obj.id,
            MetabaseObjectLink.is_active.is_(True),
        )
    ).all()
    for link in links:
        if link.table_id is not None:
            matched_table_ids.add(int(link.table_id))
            _add(get_or_create_asset_for_table(db, int(link.table_id)))

    # 2) Referenced tables (resolved names): try the lineage matcher, else keep unmatched.
    refs = obj.referenced_tables_json if isinstance(obj.referenced_tables_json, list) else []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        full_name = str(ref.get("full_name") or ref.get("name") or "").strip()
        name = str(ref.get("name") or full_name).strip()
        if not full_name:
            continue
        match = match_catalog_table(
            db, dataset_name=full_name, physical_name=name or full_name, namespace=None, aliases=[full_name]
        )
        if match is not None:
            table = match[0]
            if int(table.id) in matched_table_ids:
                continue
            matched_table_ids.add(int(table.id))
            _add(get_or_create_asset_for_table(db, int(table.id)))
        else:
            _add(_ensure_unmatched_table_asset(db, ref))
    return assets


def _upsert_consumption(
    db: Session,
    *,
    table_asset: LineageAsset,
    artifact_asset: LineageAsset,
    dashboard_name: str | None,
    process_name: str | None,
) -> bool:
    edge_key = f"metabase-consumption:{table_asset.id}->{artifact_asset.id}"
    relation = db.scalar(select(LineageRelation).where(LineageRelation.external_edge_key == edge_key))
    if relation is None:
        relation = db.scalar(
            select(LineageRelation).where(
                LineageRelation.source_asset_id == table_asset.id,
                LineageRelation.target_asset_id == artifact_asset.id,
                LineageRelation.relation_type == CONSUMPTION,
            )
        )
    if relation is None:
        db.add(
            LineageRelation(
                source_asset_id=table_asset.id,
                target_asset_id=artifact_asset.id,
                relation_type=CONSUMPTION,
                discovery_method="automatic",
                confidence_score=100,
                process_type="metabase",
                process_name=process_name,
                dashboard_name=dashboard_name,
                notes="Consumo detectado automaticamente no Metabase.",
                evidence="Metabase",
                external_edge_key=edge_key,
                last_seen_at=_now(),
                is_active=True,
                is_verified=False,
            )
        )
        db.flush()
        return True
    relation.is_active = True
    relation.last_seen_at = _now()
    relation.external_edge_key = edge_key
    if (relation.discovery_method or "").lower() == "manual":
        relation.discovery_method = "merged"
    db.flush()
    return False


def sync_metabase_lineage(db: Session, *, instance: MetabaseInstance, commit: bool = True) -> dict[str, int]:
    """Build consumption lineage for one Metabase instance. Idempotent."""
    objects = db.scalars(
        select(MetabaseObject).where(
            MetabaseObject.instance_id == instance.id,
            MetabaseObject.object_type.in_(("dashboard", "question")),
            MetabaseObject.archived.is_(False),
        )
    ).all()
    artifacts = 0
    edges_total = 0
    edges_created = 0
    for obj in objects:
        table_assets = _consumed_table_assets(db, obj)
        if not table_assets:
            continue
        artifact_asset = _ensure_artifact_asset(db, instance, obj)
        artifacts += 1
        dashboard_name = obj.title if obj.object_type == "dashboard" else None
        for table_asset in table_assets:
            created = _upsert_consumption(
                db,
                table_asset=table_asset,
                artifact_asset=artifact_asset,
                dashboard_name=dashboard_name,
                process_name=obj.title,
            )
            edges_total += 1
            if created:
                edges_created += 1
    if commit:
        db.commit()
    return {"artifacts": artifacts, "edges_total": edges_total, "edges_created": edges_created}
