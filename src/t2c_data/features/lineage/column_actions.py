from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.column_edges import serialize_column_edge
from t2c_data.features.lineage.visibility import asset_visible_to_user
from t2c_data.features.lineage.versioning import record_column_edge_version
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageAsset, LineageColumnEdge
from t2c_data.schemas.lineage import LineageColumnEdgeCreate, LineageColumnEdgeOut, LineageColumnEdgeUpdate
from t2c_data.services.audit import add_audit_log


def _get_visible_asset_or_404(db: Session, asset_id: int, *, user: User) -> LineageAsset:
    asset = db.get(LineageAsset, asset_id)
    if not asset or not asset.is_active or not asset_visible_to_user(db, user, asset):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage asset not found")
    return asset


def get_column_edge_or_404(db: Session, edge_id: int, *, user: User) -> LineageColumnEdge:
    edge = db.scalar(
        select(LineageColumnEdge)
        .where(LineageColumnEdge.id == edge_id)
        .where(LineageColumnEdge.is_active.is_(True))
    )
    if not edge:
      raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage column edge not found")
    _get_visible_asset_or_404(db, edge.source_asset_id, user=user)
    _get_visible_asset_or_404(db, edge.target_asset_id, user=user)
    return edge


def _ensure_unique_edge(
    db: Session,
    *,
    source_asset_id: int,
    target_asset_id: int,
    source_column_name: str,
    target_column_name: str,
    relation_type: str,
    exclude_id: int | None = None,
) -> None:
    stmt = select(LineageColumnEdge).where(
        LineageColumnEdge.source_asset_id == source_asset_id,
        LineageColumnEdge.target_asset_id == target_asset_id,
        LineageColumnEdge.source_column_name == source_column_name,
        LineageColumnEdge.target_column_name == target_column_name,
        LineageColumnEdge.relation_type == relation_type,
        LineageColumnEdge.is_active.is_(True),
    )
    if exclude_id is not None:
        stmt = stmt.where(LineageColumnEdge.id != exclude_id)
    if db.scalar(stmt):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An identical column lineage already exists")


def _serialize(edge: LineageColumnEdge) -> LineageColumnEdgeOut:
    return serialize_column_edge(edge, focus_asset=edge.source_asset)


def create_or_update_manual_column_edge_with_audit(*, db: Session, payload: LineageColumnEdgeCreate, user: User) -> LineageColumnEdgeOut:
    source_asset = _get_visible_asset_or_404(db, payload.source_asset_id, user=user)
    target_asset = _get_visible_asset_or_404(db, payload.target_asset_id, user=user)
    if source_asset.id == target_asset.id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Source and target must be different")

    _ensure_unique_edge(
        db,
        source_asset_id=source_asset.id,
        target_asset_id=target_asset.id,
        source_column_name=payload.source_column_name,
        target_column_name=payload.target_column_name,
        relation_type=payload.relation_type,
    )

    edge = db.scalar(
        select(LineageColumnEdge).where(
            LineageColumnEdge.source_asset_id == source_asset.id,
            LineageColumnEdge.target_asset_id == target_asset.id,
            LineageColumnEdge.source_column_name == payload.source_column_name,
            LineageColumnEdge.target_column_name == payload.target_column_name,
            LineageColumnEdge.relation_type == payload.relation_type,
        )
    )
    created = edge is None
    if edge is None:
        edge = LineageColumnEdge(
            lineage_source_id=payload.lineage_source_id,
            lineage_job_id=payload.lineage_job_id,
            source_asset_id=source_asset.id,
            target_asset_id=target_asset.id,
            source_column_name=payload.source_column_name,
            target_column_name=payload.target_column_name,
            relation_type=payload.relation_type,
            discovery_method="manual",
            evidence_source=payload.evidence_source or "manual",
            evidence=payload.evidence,
            confidence_score=payload.confidence_score,
            transform_expression=payload.transform_expression,
            notes=payload.notes,
            external_edge_key=payload.external_edge_key,
            is_verified=bool(payload.is_verified) if payload.is_verified is not None else True,
            is_active=True,
            created_by_user_id=user.id,
            updated_by_user_id=user.id,
        )
        db.add(edge)
    else:
        edge.lineage_source_id = payload.lineage_source_id
        edge.lineage_job_id = payload.lineage_job_id
        edge.discovery_method = "manual"
        edge.evidence_source = payload.evidence_source or edge.evidence_source or "manual"
        if payload.evidence is not None:
            edge.evidence = payload.evidence
        edge.confidence_score = payload.confidence_score
        edge.transform_expression = payload.transform_expression
        edge.notes = payload.notes
        edge.external_edge_key = payload.external_edge_key or edge.external_edge_key
        if payload.is_verified is not None:
            edge.is_verified = payload.is_verified
        edge.is_active = True
        edge.updated_by_user_id = user.id
    db.flush()
    record_column_edge_version(db, edge, actor_user_id=user.id, force_version=True)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.column_edge.manual_upsert",
        entity_type="lineage_column_edge",
        entity_id=edge.id,
        message="Column lineage upserted manually",
        changes={
            "created": created,
            "source_asset_id": edge.source_asset_id,
            "target_asset_id": edge.target_asset_id,
            "source_column_name": edge.source_column_name,
            "target_column_name": edge.target_column_name,
            "relation_type": edge.relation_type,
            "confidence_score": edge.confidence_score,
            "version": edge.version,
        },
    )
    db.commit()
    db.refresh(edge)
    return _serialize(edge)


def update_manual_column_edge_with_audit(*, db: Session, edge: LineageColumnEdge, payload: LineageColumnEdgeUpdate, user: User) -> LineageColumnEdgeOut:
    before = {
        "source_asset_id": edge.source_asset_id,
        "target_asset_id": edge.target_asset_id,
        "source_column_name": edge.source_column_name,
        "target_column_name": edge.target_column_name,
        "relation_type": edge.relation_type,
        "discovery_method": edge.discovery_method,
        "confidence_score": edge.confidence_score,
        "evidence_source": edge.evidence_source,
        "evidence": edge.evidence,
        "transform_expression": edge.transform_expression,
        "notes": edge.notes,
        "is_verified": edge.is_verified,
        "is_active": edge.is_active,
    }

    next_source_asset_id = payload.source_asset_id or edge.source_asset_id
    next_target_asset_id = payload.target_asset_id or edge.target_asset_id
    next_source_column_name = payload.source_column_name or edge.source_column_name
    next_target_column_name = payload.target_column_name or edge.target_column_name
    next_relation_type = payload.relation_type or edge.relation_type
    _ensure_unique_edge(
        db,
        source_asset_id=next_source_asset_id,
        target_asset_id=next_target_asset_id,
        source_column_name=next_source_column_name,
        target_column_name=next_target_column_name,
        relation_type=next_relation_type,
        exclude_id=edge.id,
    )

    if payload.source_asset_id is not None:
        _get_visible_asset_or_404(db, payload.source_asset_id, user=user)
        edge.source_asset_id = payload.source_asset_id
    if payload.target_asset_id is not None:
        _get_visible_asset_or_404(db, payload.target_asset_id, user=user)
        edge.target_asset_id = payload.target_asset_id
    if payload.lineage_source_id is not None:
        edge.lineage_source_id = payload.lineage_source_id
    if payload.lineage_job_id is not None:
        edge.lineage_job_id = payload.lineage_job_id
    if payload.source_column_name is not None:
        edge.source_column_name = payload.source_column_name
    if payload.target_column_name is not None:
        edge.target_column_name = payload.target_column_name
    if payload.relation_type is not None:
        edge.relation_type = payload.relation_type
    if payload.discovery_method is not None:
        edge.discovery_method = payload.discovery_method
    if payload.evidence_source is not None:
        edge.evidence_source = payload.evidence_source
    if payload.evidence is not None:
        edge.evidence = payload.evidence
    if payload.confidence_score is not None:
        edge.confidence_score = payload.confidence_score
    if payload.transform_expression is not None:
        edge.transform_expression = payload.transform_expression
    if payload.notes is not None:
        edge.notes = payload.notes
    if payload.external_edge_key is not None:
        edge.external_edge_key = payload.external_edge_key
    if payload.is_verified is not None:
        edge.is_verified = payload.is_verified
    if payload.is_active is not None:
        edge.is_active = payload.is_active
    edge.updated_by_user_id = user.id
    db.flush()
    record_column_edge_version(db, edge, actor_user_id=user.id, force_version=True)

    after = {
        "source_asset_id": edge.source_asset_id,
        "target_asset_id": edge.target_asset_id,
        "source_column_name": edge.source_column_name,
        "target_column_name": edge.target_column_name,
        "relation_type": edge.relation_type,
        "discovery_method": edge.discovery_method,
        "confidence_score": edge.confidence_score,
        "evidence_source": edge.evidence_source,
        "evidence": edge.evidence,
        "transform_expression": edge.transform_expression,
        "notes": edge.notes,
        "is_verified": edge.is_verified,
        "is_active": edge.is_active,
    }
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.column_edge.update",
        entity_type="lineage_column_edge",
        entity_id=edge.id,
        message="Column lineage updated",
        changes={"before": before, "after": after},
    )
    db.commit()
    db.refresh(edge)
    return _serialize(edge)


def deactivate_manual_column_edge_with_audit(*, db: Session, edge: LineageColumnEdge, user: User) -> dict[str, bool]:
    edge.is_active = False
    edge.updated_by_user_id = user.id
    record_column_edge_version(db, edge, actor_user_id=user.id, force_version=True)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.column_edge.deactivate",
        entity_type="lineage_column_edge",
        entity_id=edge.id,
        message="Column lineage deactivated",
        changes={"is_active": False},
    )
    db.commit()
    return {"success": True}
