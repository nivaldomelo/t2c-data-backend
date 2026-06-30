from __future__ import annotations

from typing import Literal

from t2c_data.features.lineage.shared import serialize_asset_ref
from t2c_data.features.lineage.versioning import confidence_tier
from t2c_data.models.lineage import LineageAsset, LineageColumnEdge
from t2c_data.schemas.lineage import LineageColumnEdgeOut


def _asset_path(asset: LineageAsset | None) -> str | None:
    if asset is None:
        return None
    parts = [asset.system_name, asset.schema_name, asset.object_name]
    filtered = [part for part in parts if part]
    if filtered:
        return ".".join(filtered)
    return asset.asset_name or None


def _confidence_label(score: int) -> str:
    if score >= 90:
        return "Alta"
    if score >= 70:
        return "Média"
    return "Baixa"


def _evidence_label(edge: LineageColumnEdge) -> str:
    source = (edge.evidence_source or "").strip().lower()
    if source == "openlineage":
        return "OpenLineage"
    if source == "inferred_sql":
        return "SQL inferida"
    if source == "pipeline":
        return "Pipeline"
    if source == "imported":
        return "Importada"
    if source == "manual":
        return "Manual"
    if edge.discovery_method == "manual":
        return "Manual"
    return "Automática"


def _relative_direction(edge: LineageColumnEdge, focus_asset_id: int | None) -> Literal["upstream", "downstream"]:
    if focus_asset_id is not None and edge.target_asset_id == focus_asset_id:
        return "upstream"
    return "downstream"


def serialize_column_edge(edge: LineageColumnEdge, *, focus_asset: LineageAsset | None = None) -> LineageColumnEdgeOut:
    focus_asset_id = focus_asset.id if focus_asset is not None else None
    direction = _relative_direction(edge, focus_asset_id)
    local_asset = edge.target_asset if direction == "upstream" else edge.source_asset
    related_asset = edge.source_asset if direction == "upstream" else edge.target_asset
    local_column = edge.target_column_name if direction == "upstream" else edge.source_column_name
    related_column = edge.source_column_name if direction == "upstream" else edge.target_column_name

    return LineageColumnEdgeOut(
        id=edge.id,
        lineage_source_id=edge.lineage_source_id,
        lineage_job_id=edge.lineage_job_id,
        source_asset=serialize_asset_ref(edge.source_asset),
        target_asset=serialize_asset_ref(edge.target_asset),
        source_asset_id=edge.source_asset_id,
        target_asset_id=edge.target_asset_id,
        relative_direction=direction,
        local_asset_name=local_asset.asset_name,
        related_asset_name=related_asset.asset_name,
        local_asset_path=_asset_path(local_asset),
        related_asset_path=_asset_path(related_asset),
        local_column_name=local_column,
        related_column_name=related_column,
        source_column_name=edge.source_column_name,
        target_column_name=edge.target_column_name,
        relation_type=edge.relation_type,
        discovery_method=edge.discovery_method,
        evidence_source=edge.evidence_source,
        evidence_label=_evidence_label(edge),
        evidence=edge.evidence,
        confidence_score=edge.confidence_score,
        confidence_label=_confidence_label(edge.confidence_score),
        confidence_tier=confidence_tier(edge.confidence_score, is_verified=bool(edge.is_verified)),
        is_verified=bool(edge.is_verified),
        version=int(edge.version or 1),
        last_seen_at=edge.last_seen_at,
        created_by_user_id=edge.created_by_user_id,
        updated_by_user_id=edge.updated_by_user_id,
        transform_expression=edge.transform_expression,
        notes=edge.notes,
        external_edge_key=edge.external_edge_key,
        is_active=edge.is_active,
        created_at=edge.created_at,
        updated_at=edge.updated_at,
    )


__all__ = ["serialize_column_edge"]
