from __future__ import annotations

from t2c_data.models.lineage import LineageAsset, LineageRelation


def summarize_asset_preview(
    asset_defs: dict[str, dict[str, object]],
    *,
    existing_assets: dict[str, LineageAsset],
) -> tuple[int, int]:
    new_assets = 0
    updated_assets = 0
    for asset_key in asset_defs:
        if asset_key in existing_assets:
            updated_assets += 1
        else:
            new_assets += 1
    return new_assets, updated_assets


def summarize_relation_preview(
    relation_defs: list[dict[str, object]],
    *,
    asset_defs: dict[str, dict[str, object]],
    existing_assets: dict[str, LineageAsset],
    existing_relations: dict[tuple[str, str, str], LineageRelation],
    warnings: list[dict[str, object]],
) -> tuple[int, int, int]:
    new_relations = 0
    updated_relations = 0
    ignored_rows = 0

    for relation in relation_defs:
        source_key = relation["source_asset_key"]
        target_key = relation["target_asset_key"]
        if source_key not in asset_defs and source_key not in existing_assets:
            warnings.append(
                {
                    "sheet": relation["sheet"],
                    "row_number": relation["row_number"],
                    "message": f"Source asset não encontrado: {source_key}",
                }
            )
            ignored_rows += 1
            continue
        if target_key not in asset_defs and target_key not in existing_assets:
            warnings.append(
                {
                    "sheet": relation["sheet"],
                    "row_number": relation["row_number"],
                    "message": f"Target asset não encontrado: {target_key}",
                }
            )
            ignored_rows += 1
            continue
        key = (str(source_key), str(target_key), str(relation["relation_type"]))
        if key in existing_relations:
            updated_relations += 1
        else:
            new_relations += 1

    return new_relations, updated_relations, ignored_rows


__all__ = [
    "summarize_asset_preview",
    "summarize_relation_preview",
]
