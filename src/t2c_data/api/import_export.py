from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, enforce_export_permission, redact_export_row, resolve_export_limit
from t2c_data.models.auth import User
from t2c_data.models.glossary import GlossaryTerm
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.tag import Tag
from t2c_data.schemas.import_export import ImportExportBundle, ImportResult
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync

router = APIRouter(prefix="/io", tags=["import-export"])


def _serialize_lineage_assets(db: Session) -> list[dict[str, Any]]:
    return [
        {
            "asset_key": asset.asset_key,
            "asset_name": asset.asset_name,
            "asset_type": asset.asset_type,
            "layer": asset.layer,
            "schema_name": asset.schema_name,
            "object_name": asset.object_name,
            "system_name": asset.system_name,
            "asset_origin": asset.asset_origin,
            "external_node_id": asset.external_node_id,
            "external_namespace": asset.external_namespace,
            "external_name": asset.external_name,
            "external_type": asset.external_type,
            "description": asset.description,
            "is_active": asset.is_active,
        }
        for asset in db.scalars(select(LineageAsset).order_by(LineageAsset.updated_at.desc(), LineageAsset.id.desc())).all()
    ]


def _serialize_lineage_relations(db: Session) -> list[dict[str, Any]]:
    relations = db.scalars(
        select(LineageRelation).order_by(LineageRelation.updated_at.desc(), LineageRelation.id.desc())
    ).all()
    return [
        {
            "source_asset_key": relation.source_asset.asset_key,
            "target_asset_key": relation.target_asset.asset_key,
            "relation_type": relation.relation_type,
            "process_name": relation.process_name,
            "process_type": relation.process_type,
            "dashboard_name": relation.dashboard_name,
            "notes": relation.notes,
            "discovery_method": relation.discovery_method,
            "confidence_score": relation.confidence_score,
            "is_active": relation.is_active,
        }
        for relation in relations
    ]


def _upsert_canonical_lineage_assets(db: Session, assets_payload: list[dict[str, Any]]) -> tuple[int, dict[str, int]]:
    imported_assets = 0
    asset_id_map: dict[str, int] = {}

    for asset_data in assets_payload:
        asset_key = str(asset_data.get("asset_key") or "").strip()
        if not asset_key:
            continue
        existing = db.scalar(select(LineageAsset).where(LineageAsset.asset_key == asset_key))
        if existing is not None:
            asset_id_map[asset_key] = existing.id
            continue
        asset = LineageAsset(
            asset_key=asset_key,
            asset_name=str(asset_data.get("asset_name") or asset_key),
            asset_type=str(asset_data.get("asset_type") or "table"),
            layer=str(asset_data.get("layer") or "definir"),
            schema_name=asset_data.get("schema_name"),
            object_name=asset_data.get("object_name"),
            system_name=asset_data.get("system_name"),
            asset_origin=str(asset_data.get("asset_origin") or "manual"),
            external_node_id=asset_data.get("external_node_id"),
            external_namespace=asset_data.get("external_namespace"),
            external_name=asset_data.get("external_name"),
            external_type=asset_data.get("external_type"),
            description=asset_data.get("description"),
            is_active=bool(asset_data.get("is_active", True)),
        )
        db.add(asset)
        db.flush()
        asset_id_map[asset_key] = asset.id
        imported_assets += 1

    return imported_assets, asset_id_map


def _import_canonical_lineage_relations(
    db: Session,
    relations_payload: list[dict[str, Any]],
    *,
    asset_id_map: dict[str, int],
    warnings: list[str],
) -> int:
    imported_relations = 0
    for relation_data in relations_payload:
        source_asset_key = str(relation_data.get("source_asset_key") or "").strip()
        target_asset_key = str(relation_data.get("target_asset_key") or "").strip()
        source_asset_id = asset_id_map.get(source_asset_key)
        target_asset_id = asset_id_map.get(target_asset_key)
        if source_asset_id is None or target_asset_id is None:
            warnings.append(
                f"Relação ignorada por ativo ausente no bundle: {source_asset_key or '?'} -> {target_asset_key or '?'}."
            )
            continue
        exists = db.scalar(
            select(LineageRelation).where(
                LineageRelation.source_asset_id == source_asset_id,
                LineageRelation.target_asset_id == target_asset_id,
                LineageRelation.relation_type == str(relation_data.get("relation_type") or "transformation"),
                LineageRelation.process_name == relation_data.get("process_name"),
                LineageRelation.process_type == relation_data.get("process_type"),
                LineageRelation.dashboard_name == relation_data.get("dashboard_name"),
                LineageRelation.notes == relation_data.get("notes"),
                LineageRelation.discovery_method == str(relation_data.get("discovery_method") or "manual"),
            )
        )
        if exists is not None:
            continue
        db.add(
            LineageRelation(
                source_asset_id=source_asset_id,
                target_asset_id=target_asset_id,
                relation_type=str(relation_data.get("relation_type") or "transformation"),
                process_name=relation_data.get("process_name"),
                process_type=relation_data.get("process_type"),
                dashboard_name=relation_data.get("dashboard_name"),
                notes=relation_data.get("notes"),
                discovery_method=str(relation_data.get("discovery_method") or "manual"),
                confidence_score=int(relation_data.get("confidence_score") or 100),
                is_active=bool(relation_data.get("is_active", True)),
            )
        )
        imported_relations += 1
    return imported_relations


@router.get("/export", response_model=ImportExportBundle)
def export_bundle(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ImportExportBundle:
    """Legacy governance bundle export.

    The governance bundle remains available for tags and glossary, but lineage
    now serializes the canonical `lineage_assets` and `lineage_relations`
    structures. Legacy lineage tables are no longer exported here.
    """
    enforce_export_permission(current_user, "integrations.export")
    export_limit = resolve_export_limit(source_module="io", entity_type="bundle")
    tags, tags_truncated = enforce_export_limit(
        [
            redact_export_row({"name": t.name, "color": t.color, "description": t.description})
            for t in db.scalars(select(Tag)).all()
        ],
        limit=export_limit,
    )
    terms, terms_truncated = enforce_export_limit(
        [
            redact_export_row({"name": t.name, "definition": t.definition, "steward": t.steward})
            for t in db.scalars(select(GlossaryTerm)).all()
        ],
        limit=export_limit,
    )
    lineage_assets, assets_truncated = enforce_export_limit(
        [redact_export_row(item) for item in _serialize_lineage_assets(db)],
        limit=export_limit,
    )
    lineage_relations, relations_truncated = enforce_export_limit(
        [redact_export_row(item) for item in _serialize_lineage_relations(db)],
        limit=export_limit,
    )

    bundle = ImportExportBundle(
        exported_at=datetime.now(timezone.utc),
        data={
            "tags": tags,
            "glossary_terms": terms,
            "lineage_assets": lineage_assets,
            "lineage_relations": lineage_relations,
        },
    )
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="io.export",
        entity_type="bundle",
        source_module="io",
        row_count=len(tags) + len(terms) + len(lineage_assets) + len(lineage_relations),
        filters={
            "counts": {
                "tags": len(tags),
                "glossary_terms": len(terms),
                "lineage_assets": len(lineage_assets),
                "lineage_relations": len(lineage_relations),
            },
            "truncated": {
                "tags": tags_truncated,
                "glossary_terms": terms_truncated,
                "lineage_assets": assets_truncated,
                "lineage_relations": relations_truncated,
            },
        },
        limit=export_limit,
        truncated=any((tags_truncated, terms_truncated, assets_truncated, relations_truncated)),
        export_format="json",
        permission_name="integrations.export",
    )
    return bundle


@router.post("/import", response_model=ImportResult)
def import_bundle(
    payload: ImportExportBundle,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> ImportResult:
    """Legacy governance bundle import.

    The governance bundle remains available for tags and glossary. Lineage now
    imports only the canonical `lineage_assets` and `lineage_relations`
    structures. Legacy lineage sections are ignored with warnings so older
    payloads do not fail abruptly during the transition.
    """
    imported_tags = 0
    imported_terms = 0
    imported_assets = 0
    imported_relations = 0
    ignored_legacy_processes = len(payload.data.get("lineage_processes", []))
    ignored_legacy_edges = len(payload.data.get("lineage_edges", []))
    warnings: list[str] = []

    for tag_data in payload.data.get("tags", []):
        exists = db.scalar(select(Tag).where(Tag.name == tag_data["name"]))
        if exists:
            continue
        db.add(Tag(name=tag_data["name"], color=tag_data.get("color"), description=tag_data.get("description")))
        imported_tags += 1

    for term_data in payload.data.get("glossary_terms", []):
        exists = db.scalar(select(GlossaryTerm).where(GlossaryTerm.name == term_data["name"]))
        if exists:
            continue
        db.add(
            GlossaryTerm(
                name=term_data["name"],
                definition=term_data["definition"],
                steward=term_data.get("steward"),
            )
        )
        imported_terms += 1

    db.flush()

    imported_assets, asset_id_map = _upsert_canonical_lineage_assets(
        db,
        payload.data.get("lineage_assets", []),
    )
    imported_relations = _import_canonical_lineage_relations(
        db,
        payload.data.get("lineage_relations", []),
        asset_id_map=asset_id_map,
        warnings=warnings,
    )

    if ignored_legacy_processes or ignored_legacy_edges:
        warnings.append(
            "O bundle recebeu seções legadas de linhagem (`lineage_processes`/`lineage_edges`), "
            "que não são mais importadas por `/api/v1/io/import`. Use `/api/v1/lineage/import/*` "
            "ou gere o bundle novamente no formato canônico."
        )

    db.commit()
    result = ImportResult(
        imported_tags=imported_tags,
        imported_terms=imported_terms,
        imported_lineage_assets=imported_assets,
        imported_lineage_relations=imported_relations,
        imported_lineage_processes=0,
        imported_lineage_edges=0,
        ignored_legacy_lineage_processes=ignored_legacy_processes,
        ignored_legacy_lineage_edges=ignored_legacy_edges,
        warnings=warnings,
    )
    write_audit_log_sync(
        db,
        action="io.import",
        entity_type="bundle",
        metadata={
            "counts": result.model_dump(),
            "payload_counts": {
                "tags": len(payload.data.get("tags", [])),
                "glossary_terms": len(payload.data.get("glossary_terms", [])),
                "lineage_assets": len(payload.data.get("lineage_assets", [])),
                "lineage_relations": len(payload.data.get("lineage_relations", [])),
                "lineage_processes": len(payload.data.get("lineage_processes", [])),
                "lineage_edges": len(payload.data.get("lineage_edges", [])),
            },
            "warnings": warnings,
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return result
