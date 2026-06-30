from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.features.lineage.versioning import confidence_tier
from t2c_data.schemas.lineage import LineageAssetOut, LineageAssetRefOut, LineageRelationOut

ASSET_TYPES = {"table", "view", "dashboard", "source"}
LAYERS = {"bronze", "silver", "gold", "mart", "dashboard", "source", "definir"}
RELATION_TYPES = {"ingestion", "transformation", "load", "consumption"}
KNOWN_DATABASE_ENGINES = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "sqlserver": "sqlserver",
    "sql_server": "sqlserver",
    "mssql": "sqlserver",
    "oracle": "oracle",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "sqlite": "sqlite",
}
LINEAGE_SUPPORTED_DATABASE_ENGINES = set(KNOWN_DATABASE_ENGINES.values())


def normalize_slug(value: str | None) -> str:
    raw = (value or "").strip().lower()
    return "".join(ch if ch.isalnum() else "_" for ch in raw).strip("_")


def normalize_database_engine(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    if not normalized:
        return None
    return KNOWN_DATABASE_ENGINES.get(normalized)


def infer_engine_from_namespace(namespace: str | None) -> str | None:
    raw = (namespace or "").strip().lower()
    if "://" not in raw:
        return None
    scheme = raw.split("://", 1)[0]
    return normalize_database_engine(scheme)


def table_context(db: Session, table_id: int) -> tuple[TableEntity, Schema, Database, DataSource]:
    row = db.execute(
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.id == table_id)
    ).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return row


def layer_from_table(table: TableEntity, schema: Schema) -> str:
    candidates = [schema.name, table.name]
    for candidate in candidates:
        raw = (candidate or "").lower()
        for layer in ("bronze", "silver", "gold", "mart"):
            if raw == layer or raw.startswith(f"{layer}_") or raw.endswith(f"_{layer}") or f".{layer}." in raw:
                return layer
    return "gold" if table.table_type == "table" else "mart"


def build_asset_key(
    *,
    asset_type: str,
    layer: str,
    system_name: str | None,
    schema_name: str | None,
    object_name: str | None,
    asset_name: str,
    catalog_table_id: int | None = None,
) -> str:
    if catalog_table_id is not None:
        return f"catalog_table:{catalog_table_id}"
    parts = [
        normalize_slug(asset_type),
        normalize_slug(layer),
        normalize_slug(system_name),
        normalize_slug(schema_name),
        normalize_slug(object_name),
        normalize_slug(asset_name),
    ]
    return ":".join([part for part in parts if part]) or normalize_slug(asset_name) or "lineage_asset"


def asset_display_name(table: TableEntity, schema: Schema) -> str:
    if schema.name == "default":
        return table.name
    return f"{schema.name}.{table.name}"


def serialize_asset(asset: LineageAsset) -> LineageAssetOut:
    return LineageAssetOut(
        id=asset.id,
        catalog_table_id=asset.catalog_table_id,
        datasource_id=asset.datasource_id,
        asset_key=asset.asset_key,
        asset_name=asset.asset_name,
        asset_type=asset.asset_type,
        layer=asset.layer,
        schema_name=asset.schema_name,
        object_name=asset.object_name,
        system_name=asset.system_name,
        description=asset.description,
        is_active=asset.is_active,
        created_at=asset.created_at,
        updated_at=asset.updated_at,
    )


def serialize_asset_ref(asset: LineageAsset) -> LineageAssetRefOut:
    return LineageAssetRefOut(
        id=asset.id,
        catalog_table_id=asset.catalog_table_id,
        datasource_id=asset.datasource_id,
        asset_key=asset.asset_key,
        asset_name=asset.asset_name,
        asset_type=asset.asset_type,
        layer=asset.layer,
        schema_name=asset.schema_name,
        object_name=asset.object_name,
        system_name=asset.system_name,
        description=asset.description,
        asset_origin=asset.asset_origin,
        external_namespace=asset.external_namespace,
        external_name=asset.external_name,
        external_type=asset.external_type,
        external_node_id=asset.external_node_id,
        is_active=asset.is_active,
    )


def canonical_lineage_asset_key(asset: LineageAsset) -> str:
    if asset.catalog_table_id is not None:
        return f"catalog_table:{asset.catalog_table_id}"
    if asset.schema_name and asset.object_name:
        return f"{normalize_slug(asset.asset_type)}:{normalize_slug(asset.layer)}:{normalize_slug(asset.schema_name)}:{normalize_slug(asset.object_name)}"
    if asset.asset_name:
        return f"{normalize_slug(asset.asset_type)}:{normalize_slug(asset.layer)}:{normalize_slug(asset.asset_name)}"
    return f"asset:{asset.id or asset.asset_key}"


def normalized_relation_origin(discovery_method: str | None) -> str:
    value = (discovery_method or "manual").lower()
    if value in {"manual", "spreadsheet"}:
        return "manual"
    if value == "automatic":
        return "automatic"
    return "merged"


def serialize_relation(relation: LineageRelation) -> LineageRelationOut:
    return LineageRelationOut(
        id=relation.id,
        source_asset_id=relation.source_asset_id,
        target_asset_id=relation.target_asset_id,
        source_asset=serialize_asset_ref(relation.source_asset),
        target_asset=serialize_asset_ref(relation.target_asset),
        relation_type=relation.relation_type,
        process_name=relation.process_name,
        process_type=relation.process_type,
        dashboard_name=relation.dashboard_name,
        notes=relation.notes,
        evidence=relation.evidence,
        discovery_method=relation.discovery_method,
        lineage_origin=normalized_relation_origin(relation.discovery_method),
        lineage_source_name=relation.lineage_source.name if relation.lineage_source else None,
        lineage_namespace=relation.lineage_job.namespace if relation.lineage_job else None,
        lineage_job_name=relation.lineage_job.display_name if relation.lineage_job else relation.process_name,
        confidence_score=relation.confidence_score,
        confidence_tier=confidence_tier(relation.confidence_score, is_verified=bool(relation.is_verified)),
        is_verified=bool(relation.is_verified),
        version=int(relation.version or 1),
        last_seen_at=relation.last_seen_at,
        created_by_user_id=relation.created_by_user_id,
        updated_by_user_id=relation.updated_by_user_id,
        is_active=relation.is_active,
        created_at=relation.created_at,
        updated_at=relation.updated_at,
    )
