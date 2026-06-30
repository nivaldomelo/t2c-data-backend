from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.lineage import LineageAsset, LineageJob, LineageRelation, LineageSourceConfig
from t2c_data.features.lineage.versioning import record_relation_version


def parse_dataset_candidates(*values: str | None) -> list[tuple[str | None, str | None, str]]:
    candidates: list[tuple[str | None, str | None, str]] = []
    seen: set[tuple[str | None, str | None, str]] = set()
    for value in values:
        if not value:
            continue
        raw = value.strip().strip("`")
        if not raw:
            continue
        parts = [part.strip('`" ') for part in raw.split(".") if part.strip('`" ')]
        options: list[tuple[str | None, str | None, str]] = []
        if len(parts) >= 3:
            options.append((parts[-3], parts[-2], parts[-1]))
            options.append((None, parts[-2], parts[-1]))
        elif len(parts) == 2:
            options.append((None, parts[-2], parts[-1]))
        elif len(parts) == 1:
            options.append((None, None, parts[-1]))
        for option in options:
            if option not in seen:
                seen.add(option)
                candidates.append(option)
    return candidates


def parse_namespace_candidates(namespace: str | None, object_name: str | None) -> list[tuple[str | None, str | None, str]]:
    raw = (namespace or "").strip()
    target_name = (object_name or "").strip().strip('`" ')
    if not raw or not target_name:
        return []
    parsed = urlparse(raw)
    if not parsed.scheme:
        return []
    path_parts = [part.strip('`" ') for part in parsed.path.split("/") if part.strip('`" ')]
    candidates: list[tuple[str | None, str | None, str]] = []
    if parsed.scheme == "postgres":
        if len(path_parts) >= 2:
            candidates.append((path_parts[-2], path_parts[-1], target_name))
        elif len(path_parts) == 1:
            candidates.append((None, path_parts[-1], target_name))
    elif parsed.scheme == "mysql":
        if len(path_parts) >= 1:
            candidates.append((path_parts[-1], None, target_name))
    return candidates


def table_match_stmt() -> Select:
    return (
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    )


def match_catalog_table(
    db: Session,
    *,
    dataset_name: str | None,
    physical_name: str | None,
    namespace: str | None = None,
    aliases: list[str] | None = None,
) -> tuple[TableEntity, Schema, Database, DataSource] | None:
    parsed_candidates = parse_dataset_candidates(dataset_name, physical_name, *(aliases or []))
    if dataset_name:
        parsed_candidates = [
            *parse_namespace_candidates(namespace, dataset_name),
            *parsed_candidates,
        ]
    seen: set[tuple[str | None, str | None, str]] = set()
    for database_name, schema_name, object_name in parsed_candidates:
        candidate_key = (database_name, schema_name, object_name)
        if candidate_key in seen:
            continue
        seen.add(candidate_key)
        if not object_name:
            continue
        stmt = table_match_stmt().where(TableEntity.name == object_name)
        if schema_name:
            stmt = stmt.where(Schema.name == schema_name)
        if database_name:
            stmt = stmt.where(Database.name == database_name)
        matches = db.execute(stmt.limit(2)).all()
        if len(matches) == 1:
            return matches[0]
    return None


def layer_from_names(asset_type: str, schema_name: str | None, object_name: str | None) -> str:
    candidates = [schema_name or "", object_name or ""]
    for candidate in candidates:
        lowered = candidate.lower()
        for layer in ("bronze", "silver", "gold", "mart", "dashboard", "source"):
            if lowered == layer or lowered.startswith(f"{layer}_") or lowered.endswith(f"_{layer}") or f".{layer}." in lowered:
                return layer
    if asset_type == "dashboard":
        return "dashboard"
    if asset_type == "source":
        return "source"
    return "definir"


def _normalize_slug(value: str | None) -> str:
    raw = (value or "").strip().lower()
    return "".join(ch if ch.isalnum() else "_" for ch in raw).strip("_")


def asset_key(
    *,
    asset_type: str,
    layer: str,
    namespace: str | None,
    schema_name: str | None,
    object_name: str | None,
    display_name: str,
) -> str:
    parts = [
        _normalize_slug(asset_type),
        _normalize_slug(layer),
        _normalize_slug(namespace),
        _normalize_slug(schema_name),
        _normalize_slug(object_name),
        _normalize_slug(display_name),
    ]
    return ":".join(part for part in parts if part) or _normalize_slug(display_name) or "lineage_asset"


def apply_asset_origin(current: str | None, incoming: str) -> str:
    current_value = (current or "manual").lower()
    incoming_value = incoming.lower()
    if current_value == incoming_value:
        return current_value
    return "merged"


def upsert_dataset_asset(
    db: Session,
    *,
    source: LineageSourceConfig,
    meta: dict[str, Any],
) -> tuple[LineageAsset | None, bool, bool]:
    if not meta["name"]:
        return None, False, False
    asset = None
    if meta["node_id"]:
        asset = db.scalar(select(LineageAsset).where(LineageAsset.external_node_id == meta["node_id"]))
    if not asset and meta["namespace"] and meta["name"]:
        asset = db.scalar(
            select(LineageAsset).where(
                LineageAsset.external_namespace == meta["namespace"],
                LineageAsset.external_name == meta["name"],
            )
        )
    match = match_catalog_table(
        db,
        dataset_name=meta["name"],
        physical_name=meta["physical_name"],
        namespace=meta["namespace"],
        aliases=meta["aliases"],
    )
    matched_catalog = match is not None
    if not asset and match:
        table, schema, _database, datasource = match
        asset = db.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == table.id))
        if not asset:
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
                asset.asset_name = f"{schema.name}.{table.name}" if schema.name != "default" else table.name
                asset.asset_type = asset_type
                asset.layer = layer_from_names(asset_type, schema.name, table.name)
                asset.schema_name = schema.name if schema.name != "default" else None
                asset.object_name = table.name
                asset.system_name = datasource.name
                asset.description = table.description_manual or table.description_source
                asset.is_active = True
                db.flush()
                return asset, False, True
        if not asset:
            asset = LineageAsset(
                catalog_table_id=table.id,
                datasource_id=datasource.id,
                asset_key=f"catalog_table:{table.id}",
                asset_name=f"{schema.name}.{table.name}" if schema.name != "default" else table.name,
                asset_type="view" if table.table_type == "view" else "table",
                layer=layer_from_names(table.table_type, schema.name, table.name),
                schema_name=schema.name if schema.name != "default" else None,
                object_name=table.name,
                system_name=datasource.name,
                description=table.description_manual or table.description_source,
                is_active=True,
            )
            db.add(asset)
    created = asset is None
    if not asset:
        database_name, schema_name, object_name = (None, None, meta["name"])
        parsed = parse_dataset_candidates(meta["name"], meta["physical_name"], *(meta["aliases"] or []))
        if parsed:
            database_name, schema_name, object_name = parsed[0]
        display_name = meta["display_name"] or object_name or meta["name"]
        inferred_asset_type = "dashboard" if _normalize_slug(object_name).startswith("dashboard") else "table"
        layer = layer_from_names(inferred_asset_type, schema_name, object_name)
        inferred_asset_key = asset_key(
            asset_type=inferred_asset_type,
            layer=layer,
            namespace=meta["namespace"],
            schema_name=schema_name,
            object_name=object_name,
            display_name=display_name,
        )
        asset = db.scalar(select(LineageAsset).where(LineageAsset.asset_key == inferred_asset_key))
    if not asset:
        database_name, schema_name, object_name = (None, None, meta["name"])
        parsed = parse_dataset_candidates(meta["name"], meta["physical_name"], *(meta["aliases"] or []))
        if parsed:
            database_name, schema_name, object_name = parsed[0]
        display_name = meta["display_name"] or object_name or meta["name"]
        inferred_asset_type = "dashboard" if _normalize_slug(object_name).startswith("dashboard") else "table"
        layer = layer_from_names(inferred_asset_type, schema_name, object_name)
        asset = LineageAsset(
            asset_key=inferred_asset_key,
            asset_name=display_name,
            asset_type=inferred_asset_type,
            layer=layer,
            schema_name=schema_name,
            object_name=object_name,
            system_name=database_name or meta["namespace"],
            is_active=True,
        )
        db.add(asset)
    asset.lineage_source_id = source.id
    asset.asset_origin = apply_asset_origin(asset.asset_origin, "automatic")
    asset.external_node_id = meta["node_id"]
    asset.external_namespace = meta["namespace"]
    asset.external_name = meta["name"]
    asset.external_type = "dataset"
    asset.aliases_text = json.dumps(meta["aliases"], ensure_ascii=False) if meta["aliases"] else None
    asset.is_active = True
    db.flush()
    return asset, created, matched_catalog


def upsert_relation(
    db: Session,
    *,
    source: LineageSourceConfig,
    source_asset: LineageAsset,
    target_asset: LineageAsset,
    relation_type: str,
    process_name: str | None,
    process_type: str | None,
    discovery_method: str,
    notes: str | None = None,
    lineage_job: LineageJob | None = None,
    external_edge_key: str | None = None,
) -> tuple[LineageRelation, bool, bool]:
    relation = None
    if external_edge_key:
        relation = db.scalar(select(LineageRelation).where(LineageRelation.external_edge_key == external_edge_key))
    if not relation:
        relation = db.scalar(
            select(LineageRelation).where(
                LineageRelation.source_asset_id == source_asset.id,
                LineageRelation.target_asset_id == target_asset.id,
                LineageRelation.relation_type == relation_type,
                LineageRelation.is_active.is_(True),
            )
        )
    created = relation is None
    merged = False
    previous_state = None
    if not relation:
        relation = LineageRelation(
            source_asset_id=source_asset.id,
            target_asset_id=target_asset.id,
            relation_type=relation_type,
            discovery_method=discovery_method,
            confidence_score=100,
            is_active=True,
            is_verified=False,
        )
        db.add(relation)
    else:
        previous_state = {
            "source_asset_id": relation.source_asset_id,
            "target_asset_id": relation.target_asset_id,
            "relation_type": relation.relation_type,
            "process_name": relation.process_name,
            "process_type": relation.process_type,
            "dashboard_name": relation.dashboard_name,
            "notes": relation.notes,
            "evidence": relation.evidence,
            "discovery_method": relation.discovery_method,
            "confidence_score": relation.confidence_score,
            "is_verified": relation.is_verified,
            "external_edge_key": relation.external_edge_key,
            "is_active": relation.is_active,
            "created_by_user_id": relation.created_by_user_id,
            "updated_by_user_id": relation.updated_by_user_id,
        }
        current_method = (relation.discovery_method or "manual").lower()
        incoming_method = discovery_method.lower()
        merged = current_method == "merged" or {current_method, incoming_method} == {"manual", "automatic"} or {current_method, incoming_method} == {"spreadsheet", "automatic"}
        relation.discovery_method = "merged" if merged else incoming_method
    relation.source_asset_id = source_asset.id
    relation.target_asset_id = target_asset.id
    relation.process_name = process_name or relation.process_name
    relation.process_type = process_type or relation.process_type
    relation.notes = notes or relation.notes
    relation.evidence = notes or relation.evidence or external_edge_key or discovery_method
    relation.lineage_source_id = source.id
    relation.lineage_job_id = lineage_job.id if lineage_job else relation.lineage_job_id
    relation.external_edge_key = external_edge_key or relation.external_edge_key
    relation.is_active = True
    relation.last_seen_at = datetime.now(timezone.utc)
    db.flush()
    record_relation_version(db, relation, force_version=created, previous_state=previous_state)
    return relation, created, merged


__all__ = [
    "apply_asset_origin",
    "asset_key",
    "layer_from_names",
    "match_catalog_table",
    "parse_dataset_candidates",
    "table_match_stmt",
    "upsert_dataset_asset",
    "upsert_relation",
]
