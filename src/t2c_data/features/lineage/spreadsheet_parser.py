from __future__ import annotations

from t2c_data.features.lineage.spreadsheet_parser_support import (
    LINEAGE_ASSET_HEADERS,
    LINEAGE_IMPORT_HEADERS,
    LINEAGE_MAPPING_HEADERS,
    LineageSpreadsheetError,
    ParsedAssetRow,
    ParsedMappingRow,
    ParsedRelationRow,
    display_name_from_parts,
    excel_table_name,
    external_source_asset_key,
    iter_rows_from_workbook,
    no_relations_warning,
    normalize_asset_type,
    normalize_bool,
    normalize_layer,
    normalize_relation_type,
    normalize_text,
    split_multi_value,
)


def parse_lineage_workbook(content: bytes) -> dict[str, object]:
    assets: dict[str, ParsedAssetRow] = {}
    mappings: list[ParsedMappingRow] = []
    explicit_relations: list[ParsedRelationRow] = []
    warnings: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []

    for row_number, values in iter_rows_from_workbook(content, "2_ATIVOS", LINEAGE_ASSET_HEADERS):
        asset_key = normalize_text(values[0])
        if not asset_key:
            errors.append({"sheet": "2_ATIVOS", "row_number": row_number, "message": "asset_key é obrigatório."})
            continue
        assets[asset_key] = ParsedAssetRow(
            row_number=row_number,
            asset_key=asset_key,
            layer=normalize_layer(normalize_text(values[1])),
            asset_type=normalize_asset_type(normalize_text(values[2])),
            system_name=normalize_text(values[3]),
            schema_name=normalize_text(values[4]),
            object_name=normalize_text(values[5]),
            display_name=normalize_text(values[6]) or display_name_from_parts(normalize_text(values[4]), normalize_text(values[5]), asset_key),
            owner=normalize_text(values[7]),
            certified=normalize_bool(values[8], default=False) if normalize_text(values[8]) is not None else None,
            is_active=normalize_bool(values[9], default=True),
            description=normalize_text(values[10]),
            notes=normalize_text(values[11]),
        )

    for row_number, values in iter_rows_from_workbook(content, "3_MAPEAMENTO_NEGOCIO", LINEAGE_MAPPING_HEADERS):
        asset_key = normalize_text(values[4]) or ""
        if not asset_key:
            errors.append({"sheet": "3_MAPEAMENTO_NEGOCIO", "row_number": row_number, "message": "asset_key é obrigatório."})
            continue
        mappings.append(
            ParsedMappingRow(
                row_number=row_number,
                layer=normalize_text(values[0]),
                asset_type=normalize_text(values[1]),
                schema_name=normalize_text(values[2]),
                object_name=normalize_text(values[3]),
                asset_key=asset_key,
                source_kind=normalize_text(values[5]),
                source_name=normalize_text(values[6]),
                upstream_asset_keys=split_multi_value(normalize_text(values[7])),
                process_name=normalize_text(values[8]),
                process_type=normalize_text(values[9]),
                dashboards=split_multi_value(normalize_text(values[10])),
                notes=normalize_text(values[11]),
            )
        )

    for row_number, values in iter_rows_from_workbook(content, "4_LINHAGEM_IMPORTACAO", LINEAGE_IMPORT_HEADERS):
        source_asset_key = normalize_text(values[0])
        target_asset_key = normalize_text(values[1])
        if not source_asset_key or not target_asset_key:
            errors.append({"sheet": "4_LINHAGEM_IMPORTACAO", "row_number": row_number, "message": "source_asset_key e target_asset_key são obrigatórios."})
            continue
        explicit_relations.append(
            ParsedRelationRow(
                row_number=row_number,
                source_asset_key=source_asset_key,
                target_asset_key=target_asset_key,
                relation_type=normalize_relation_type(normalize_text(values[2])),
                process_name=normalize_text(values[3]),
                process_type=normalize_text(values[4]),
                notes=normalize_text(values[5]),
                is_active=normalize_bool(values[6], default=True),
            )
        )

    asset_defs: dict[str, dict[str, object]] = {}
    relation_defs: list[dict[str, object]] = []
    relation_index_by_key: dict[tuple[str, str, str], int] = {}
    seen_relation_keys: set[tuple[str, str, str]] = set()

    def ensure_asset_def(asset_key: str, data: dict[str, object]) -> None:
        existing = asset_defs.get(asset_key)
        if existing:
            for key, value in data.items():
                if value not in (None, "", []):
                    existing[key] = value
            return
        asset_defs[asset_key] = {"asset_key": asset_key, **data}

    def upsert_relation_def(data: dict[str, object]) -> None:
        key = (str(data["source_asset_key"]), str(data["target_asset_key"]), str(data["relation_type"]))
        existing_index = relation_index_by_key.get(key)
        if existing_index is None:
            relation_defs.append(data)
            relation_index_by_key[key] = len(relation_defs) - 1
            seen_relation_keys.add(key)
            return
        relation_defs[existing_index] = data

    for item in assets.values():
        ensure_asset_def(
            item.asset_key,
            {
                "asset_name": item.display_name,
                "asset_type": item.asset_type,
                "layer": item.layer,
                "schema_name": item.schema_name,
                "object_name": item.object_name,
                "system_name": item.system_name,
                "description": item.description,
                "notes": item.notes,
                "is_active": item.is_active,
            },
        )

    for item in mappings:
        ensure_asset_def(
            item.asset_key,
            {
                "asset_name": display_name_from_parts(item.schema_name, item.object_name, item.asset_key),
                "asset_type": normalize_asset_type(item.asset_type),
                "layer": normalize_layer(item.layer),
                "schema_name": item.schema_name,
                "object_name": item.object_name,
                "system_name": None,
                "description": None,
                "notes": item.notes,
                "is_active": True,
            },
        )
        if item.source_kind and item.source_name:
            source_key = external_source_asset_key(item.source_kind, item.source_name)
            ensure_asset_def(
                source_key,
                {
                    "asset_name": item.source_name,
                    "asset_type": "source",
                    "layer": "source",
                    "schema_name": None,
                    "object_name": item.source_name,
                    "system_name": item.source_kind,
                    "description": f"External source: {item.source_kind}",
                    "notes": None,
                    "is_active": True,
                },
            )
            key = (source_key, item.asset_key, "ingestion")
            if key not in seen_relation_keys:
                upsert_relation_def(
                    {
                        "sheet": "3_MAPEAMENTO_NEGOCIO",
                        "row_number": item.row_number,
                        "source_asset_key": source_key,
                        "target_asset_key": item.asset_key,
                        "relation_type": "ingestion",
                        "process_name": item.process_name,
                        "process_type": item.process_type,
                        "notes": item.notes,
                        "is_active": True,
                    }
                )

        for upstream_key in item.upstream_asset_keys:
            key = (upstream_key, item.asset_key, "transformation")
            if key in seen_relation_keys:
                continue
            upsert_relation_def(
                {
                    "sheet": "3_MAPEAMENTO_NEGOCIO",
                    "row_number": item.row_number,
                    "source_asset_key": upstream_key,
                    "target_asset_key": item.asset_key,
                    "relation_type": "transformation",
                    "process_name": item.process_name,
                    "process_type": item.process_type,
                    "notes": item.notes,
                    "is_active": True,
                }
            )

        for dashboard_name in item.dashboards:
            dashboard_key = dashboard_name if dashboard_name.lower().startswith("dashboard.") else f"dashboard.{dashboard_name.strip().lower().replace(' ', '_')}"
            ensure_asset_def(
                dashboard_key,
                {
                    "asset_name": dashboard_name,
                    "asset_type": "dashboard",
                    "layer": "dashboard",
                    "schema_name": None,
                    "object_name": dashboard_name,
                    "system_name": "BI",
                    "description": None,
                    "notes": None,
                    "is_active": True,
                },
            )
            key = (item.asset_key, dashboard_key, "consumption")
            if key in seen_relation_keys:
                continue
            upsert_relation_def(
                {
                    "sheet": "3_MAPEAMENTO_NEGOCIO",
                    "row_number": item.row_number,
                    "source_asset_key": item.asset_key,
                    "target_asset_key": dashboard_key,
                    "relation_type": "consumption",
                    "process_name": item.process_name,
                    "process_type": item.process_type,
                    "notes": item.notes,
                    "dashboard_name": dashboard_name,
                    "is_active": True,
                }
            )

    for item in explicit_relations:
        key = (item.source_asset_key, item.target_asset_key, item.relation_type)
        if key in seen_relation_keys:
            warnings.append({"sheet": "4_LINHAGEM_IMPORTACAO", "row_number": item.row_number, "message": "Relação explícita substituiu a versão gerada automaticamente."})
        upsert_relation_def(
            {
                "sheet": "4_LINHAGEM_IMPORTACAO",
                "row_number": item.row_number,
                "source_asset_key": item.source_asset_key,
                "target_asset_key": item.target_asset_key,
                "relation_type": item.relation_type,
                "process_name": item.process_name,
                "process_type": item.process_type,
                "notes": item.notes,
                "is_active": item.is_active,
            }
        )

    return {
        "assets": asset_defs,
        "relations": relation_defs,
        "warnings": warnings,
        "errors": errors,
    }


__all__ = [
    "LINEAGE_ASSET_HEADERS",
    "LINEAGE_IMPORT_HEADERS",
    "LINEAGE_MAPPING_HEADERS",
    "LineageSpreadsheetError",
    "excel_table_name",
    "no_relations_warning",
    "parse_lineage_workbook",
]
