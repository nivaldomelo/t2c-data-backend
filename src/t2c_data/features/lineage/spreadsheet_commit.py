from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from t2c_data.features.lineage.spreadsheet_parser import (
    LineageSpreadsheetError,
    no_relations_warning,
    parse_lineage_workbook,
)
from t2c_data.features.lineage.spreadsheet_commit_support import (
    load_existing_assets,
    load_existing_relations,
    upsert_assets,
    upsert_relations,
)

logger = logging.getLogger(__name__)


def commit_lineage_import(session: Session, content: bytes, *, mode: str = "merge") -> dict[str, object]:
    if mode != "merge":
        raise LineageSpreadsheetError("Somente o modo Merge está disponível neste MVP.")

    parsed = parse_lineage_workbook(content)
    asset_defs: dict[str, dict[str, object]] = parsed["assets"]  # type: ignore[assignment]
    relation_defs: list[dict[str, object]] = parsed["relations"]  # type: ignore[assignment]
    warnings: list[dict[str, object]] = parsed["warnings"]  # type: ignore[assignment]
    errors: list[dict[str, object]] = parsed["errors"]  # type: ignore[assignment]

    existing_assets = load_existing_assets(session)
    created_assets, updated_assets = upsert_assets(
        session,
        asset_defs,
        existing_assets=existing_assets,
    )

    existing_relations = load_existing_relations(session)
    created_relations, updated_relations, created_dashboard_assets = upsert_relations(
        session,
        relation_defs,
        existing_assets=existing_assets,
        existing_relations=existing_relations,
        warnings=warnings,
    )

    if not relation_defs:
        warnings.append(no_relations_warning())
    elif created_relations == 0 and updated_relations == 0:
        warnings.append(
            {
                "sheet": "workbook",
                "row_number": 0,
                "message": "Import completed with warnings: lineage rows were detected, but none were persisted. Review asset_key values and relation activity flags in the spreadsheet.",
            }
        )

    logger.info(
        "lineage spreadsheet commit mode=%s assets_found=%s assets_created=%s assets_updated=%s edges_found=%s edges_created=%s edges_updated=%s warnings=%s errors=%s",
        mode,
        len(asset_defs),
        created_assets,
        updated_assets,
        len(relation_defs),
        created_relations,
        updated_relations,
        len(warnings),
        len(errors),
    )

    session.commit()
    return {
        "mode": "merge",
        "assets_found": len(asset_defs),
        "processed_assets": len(asset_defs),
        "assets_created": created_assets,
        "created_assets": created_assets,
        "assets_updated": updated_assets,
        "updated_assets": updated_assets,
        "edges_found": len(relation_defs),
        "processed_relations": len(relation_defs),
        "edges_created": created_relations,
        "created_relations": created_relations,
        "edges_updated": updated_relations,
        "updated_relations": updated_relations,
        "created_dashboards": created_dashboard_assets,
        "warnings": warnings,
        "errors": errors,
    }


__all__ = ["commit_lineage_import"]
