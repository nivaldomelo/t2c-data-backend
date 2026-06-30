from __future__ import annotations

from fastapi import Request

from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, resolve_export_limit
from t2c_data.features.lineage.spreadsheet import (
    LineageSpreadsheetError,
    build_lineage_workbook,
    commit_lineage_import,
)
from t2c_data.services.audit import write_audit_log_sync


def run_lineage_import_commit(
    *,
    db,
    content: bytes,
    mode: str,
    filename: str | None,
    audit_kwargs: dict,
) -> dict:
    try:
        result = commit_lineage_import(db, content, mode=mode)
    except LineageSpreadsheetError:
        db.rollback()
        raise

    write_audit_log_sync(
        db,
        action="lineage.import",
        entity_type="lineage_relation",
        metadata={
            "filename": filename,
            "mode": mode,
            "assets_found": result["assets_found"],
            "processed_assets": result["processed_assets"],
            "assets_created": result["assets_created"],
            "created_assets": result["created_assets"],
            "assets_updated": result["assets_updated"],
            "updated_assets": result["updated_assets"],
            "edges_found": result["edges_found"],
            "processed_relations": result["processed_relations"],
            "edges_created": result["edges_created"],
            "created_relations": result["created_relations"],
            "edges_updated": result["edges_updated"],
            "updated_relations": result["updated_relations"],
            "created_dashboards": result["created_dashboards"],
            "warnings": len(result["warnings"]),
            "errors": len(result["errors"]),
        },
        **audit_kwargs,
    )
    db.commit()
    return result


def run_lineage_export(*, db, request: Request, current_user) -> bytes:
    export_limit = resolve_export_limit(source_module="lineage", entity_type="lineage_relation")
    workbook, metadata = build_lineage_workbook(db, limit=export_limit)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="lineage.export",
        entity_type="lineage_relation",
        source_module="lineage",
        row_count=int(metadata.get("assets_exported", 0)) + int(metadata.get("relations_exported", 0)),
        filters={
            "assets_exported": metadata.get("assets_exported", 0),
            "assets_truncated": metadata.get("assets_truncated", False),
            "relations_exported": metadata.get("relations_exported", 0),
            "relations_truncated": metadata.get("relations_truncated", False),
        },
        limit=export_limit,
        truncated=bool(metadata.get("assets_truncated") or metadata.get("relations_truncated")),
        export_format="xlsx",
        permission_name="lineage:export",
    )
    return workbook
