from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from t2c_data.features.lineage.spreadsheet_commit_support import load_existing_assets, load_existing_relations
from t2c_data.features.lineage.spreadsheet_parser import no_relations_warning, parse_lineage_workbook
from t2c_data.features.lineage.spreadsheet_preview_support import (
    summarize_asset_preview,
    summarize_relation_preview,
)

logger = logging.getLogger(__name__)


def preview_lineage_import(session: Session, content: bytes) -> dict[str, object]:
    parsed = parse_lineage_workbook(content)
    asset_defs: dict[str, dict[str, object]] = parsed["assets"]  # type: ignore[assignment]
    relation_defs: list[dict[str, object]] = parsed["relations"]  # type: ignore[assignment]
    warnings: list[dict[str, object]] = parsed["warnings"]  # type: ignore[assignment]
    errors: list[dict[str, object]] = parsed["errors"]  # type: ignore[assignment]

    existing_assets = load_existing_assets(session)
    existing_relations = load_existing_relations(session)

    new_assets, updated_assets = summarize_asset_preview(
        asset_defs,
        existing_assets=existing_assets,
    )
    new_relations, updated_relations, ignored_rows = summarize_relation_preview(
        relation_defs,
        asset_defs=asset_defs,
        existing_assets=existing_assets,
        existing_relations=existing_relations,
        warnings=warnings,
    )

    if not relation_defs:
        warnings.append(no_relations_warning())

    logger.info(
        "lineage spreadsheet preview assets_found=%s assets_created=%s assets_updated=%s edges_found=%s edges_created=%s edges_updated=%s warnings=%s errors=%s ignored_rows=%s",
        len(asset_defs),
        new_assets,
        updated_assets,
        len(relation_defs),
        new_relations,
        updated_relations,
        len(warnings),
        len(errors),
        ignored_rows,
    )

    return {
        "mode": "merge",
        "summary": {
            "assets_found": len(asset_defs),
            "total_assets_identified": len(asset_defs),
            "assets_created": new_assets,
            "total_new_assets": new_assets,
            "assets_updated": updated_assets,
            "edges_found": len(relation_defs),
            "total_relations_identified": len(relation_defs),
            "edges_created": new_relations,
            "total_new_relations": new_relations,
            "edges_updated": updated_relations,
            "total_updated_relations": updated_relations,
            "ignored_rows": ignored_rows,
            "warnings_count": len(warnings),
            "errors_count": len(errors),
        },
        "assets_preview": list(asset_defs.values())[:50],
        "relations_preview": relation_defs[:100],
        "warnings": warnings,
        "errors": errors,
    }


__all__ = ["preview_lineage_import"]
