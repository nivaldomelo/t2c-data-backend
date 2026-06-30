from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.models.lineage import (
    LineageColumnEdge,
    LineageColumnEdgeVersion,
    LineageRelation,
    LineageRelationVersion,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def confidence_tier(confidence_score: int, *, is_verified: bool = False) -> str:
    score = int(confidence_score or 0)
    if is_verified and score >= 80:
        return "strong"
    if score >= 90:
        return "strong"
    if score >= 70:
        return "moderate"
    return "weak"


def relation_snapshot_payload(relation: LineageRelation) -> dict[str, object]:
    return {
        "lineage_relation_id": relation.id,
        "source_asset_id": relation.source_asset_id,
        "target_asset_id": relation.target_asset_id,
        "relation_type": relation.relation_type,
        "process_name": relation.process_name,
        "process_type": relation.process_type,
        "dashboard_name": relation.dashboard_name,
        "notes": relation.notes,
        "evidence": relation.evidence,
        "discovery_method": relation.discovery_method,
        "confidence_score": int(relation.confidence_score or 0),
        "is_verified": bool(relation.is_verified),
        "last_seen_at": relation.last_seen_at.isoformat() if relation.last_seen_at else None,
        "external_edge_key": relation.external_edge_key,
        "is_active": bool(relation.is_active),
        "created_by_user_id": relation.created_by_user_id,
        "updated_by_user_id": relation.updated_by_user_id,
    }


def column_edge_snapshot_payload(edge: LineageColumnEdge) -> dict[str, object]:
    return {
        "lineage_column_edge_id": edge.id,
        "lineage_source_id": edge.lineage_source_id,
        "lineage_job_id": edge.lineage_job_id,
        "source_asset_id": edge.source_asset_id,
        "target_asset_id": edge.target_asset_id,
        "source_column_name": edge.source_column_name,
        "target_column_name": edge.target_column_name,
        "relation_type": edge.relation_type,
        "discovery_method": edge.discovery_method,
        "confidence_score": int(edge.confidence_score or 0),
        "evidence_source": edge.evidence_source,
        "evidence": edge.evidence,
        "transform_expression": edge.transform_expression,
        "notes": edge.notes,
        "external_edge_key": edge.external_edge_key,
        "is_verified": bool(edge.is_verified),
        "last_seen_at": edge.last_seen_at.isoformat() if edge.last_seen_at else None,
        "is_active": bool(edge.is_active),
        "created_by_user_id": edge.created_by_user_id,
        "updated_by_user_id": edge.updated_by_user_id,
    }


def _material_relation_state(relation: LineageRelation) -> dict[str, object]:
    payload = relation_snapshot_payload(relation)
    payload.pop("last_seen_at", None)
    return payload


def _material_column_edge_state(edge: LineageColumnEdge) -> dict[str, object]:
    payload = column_edge_snapshot_payload(edge)
    payload.pop("last_seen_at", None)
    return payload


def _next_relation_version(session: Session, relation_id: int) -> int:
    current = session.scalar(
        select(func.max(LineageRelationVersion.version_number)).where(LineageRelationVersion.lineage_relation_id == relation_id)
    )
    return int(current or 0) + 1


def _next_column_edge_version(session: Session, edge_id: int) -> int:
    current = session.scalar(
        select(func.max(LineageColumnEdgeVersion.version_number)).where(LineageColumnEdgeVersion.lineage_column_edge_id == edge_id)
    )
    return int(current or 0) + 1


def record_relation_version(
    session: Session,
    relation: LineageRelation,
    *,
    actor_user_id: int | None = None,
    force_version: bool = False,
    previous_state: dict[str, object] | None = None,
) -> int:
    current_state = _material_relation_state(relation)
    should_version = force_version or previous_state is None or previous_state != current_state
    if not should_version:
        relation.last_seen_at = _now()
        session.flush()
        return int(relation.version or 1)

    next_version = _next_relation_version(session, relation.id)
    relation.version = next_version
    relation.last_seen_at = _now()
    version = LineageRelationVersion(
        lineage_relation_id=relation.id,
        version_number=next_version,
        source_asset_id=relation.source_asset_id,
        target_asset_id=relation.target_asset_id,
        relation_type=relation.relation_type,
        process_name=relation.process_name,
        process_type=relation.process_type,
        dashboard_name=relation.dashboard_name,
        notes=relation.notes,
        evidence=relation.evidence,
        discovery_method=relation.discovery_method,
        confidence_score=int(relation.confidence_score or 0),
        is_verified=bool(relation.is_verified),
        last_seen_at=relation.last_seen_at,
        external_edge_key=relation.external_edge_key,
        is_active=bool(relation.is_active),
        created_by_user_id=relation.created_by_user_id,
        updated_by_user_id=relation.updated_by_user_id,
        snapshot_json=json.dumps(current_state, ensure_ascii=False),
        recorded_at=_now(),
        recorded_by_user_id=actor_user_id,
    )
    session.add(version)
    session.flush()
    return next_version


def record_column_edge_version(
    session: Session,
    edge: LineageColumnEdge,
    *,
    actor_user_id: int | None = None,
    force_version: bool = False,
    previous_state: dict[str, object] | None = None,
) -> int:
    current_state = _material_column_edge_state(edge)
    should_version = force_version or previous_state is None or previous_state != current_state
    if not should_version:
        edge.last_seen_at = _now()
        session.flush()
        return int(edge.version or 1)

    next_version = _next_column_edge_version(session, edge.id)
    edge.version = next_version
    edge.last_seen_at = _now()
    version = LineageColumnEdgeVersion(
        lineage_column_edge_id=edge.id,
        version_number=next_version,
        lineage_source_id=edge.lineage_source_id,
        lineage_job_id=edge.lineage_job_id,
        source_asset_id=edge.source_asset_id,
        target_asset_id=edge.target_asset_id,
        source_column_name=edge.source_column_name,
        target_column_name=edge.target_column_name,
        relation_type=edge.relation_type,
        discovery_method=edge.discovery_method,
        confidence_score=int(edge.confidence_score or 0),
        evidence_source=edge.evidence_source,
        evidence=edge.evidence,
        transform_expression=edge.transform_expression,
        notes=edge.notes,
        external_edge_key=edge.external_edge_key,
        is_verified=bool(edge.is_verified),
        last_seen_at=edge.last_seen_at,
        is_active=bool(edge.is_active),
        created_by_user_id=edge.created_by_user_id,
        updated_by_user_id=edge.updated_by_user_id,
        snapshot_json=json.dumps(current_state, ensure_ascii=False),
        recorded_at=_now(),
        recorded_by_user_id=actor_user_id,
    )
    session.add(version)
    session.flush()
    return next_version


__all__ = [
    "confidence_tier",
    "column_edge_snapshot_payload",
    "record_column_edge_version",
    "record_relation_version",
    "relation_snapshot_payload",
]
