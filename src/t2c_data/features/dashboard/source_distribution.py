from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.dashboard.support import TableProfile, engine_label
from t2c_data.models.catalog import DataSource


def build_source_distribution_summary(
    session: Session,
    tables: list[TableProfile],
    *,
    datasources: list[DataSource] | None = None,
) -> dict[str, object]:
    if datasources is None:
        datasources = session.scalars(select(DataSource).order_by(DataSource.name.asc())).all()

    tables_by_datasource: dict[int, list[TableProfile]] = defaultdict(list)
    schemas_by_datasource: dict[int, set[int]] = defaultdict(set)
    for table in tables:
        tables_by_datasource[table.datasource_id].append(table)
        schemas_by_datasource[table.datasource_id].add(table.schema_id)

    items: list[dict[str, object]] = []
    total_schemas = 0
    total_tables = 0
    served_tables = 0
    certified_tables = 0

    for datasource in datasources:
        source_tables = tables_by_datasource.get(datasource.id, [])
        schema_count = len(schemas_by_datasource.get(datasource.id, set()))
        table_count = len(source_tables)
        source_served = sum(1 for table in source_tables if table.eligible_for_certification)
        source_certified = sum(1 for table in source_tables if str(getattr(table, "certification_status", "")).lower() == "certified")
        source_pending = max(table_count - source_certified, 0)

        total_schemas += schema_count
        total_tables += table_count
        served_tables += source_served
        certified_tables += source_certified

        if not datasource.is_active:
            status_key = "inactive"
            status_label = "Fonte inativa"
            status_tone = "neutral"
        elif table_count <= 0:
            status_key = "awaiting_inventory"
            status_label = "Fonte monitorada, aguardando inventário"
            status_tone = "warning"
        elif source_certified >= table_count:
            status_key = "certified"
            status_label = "Inventário concluído"
            status_tone = "success"
        elif source_served > 0:
            status_key = "in_progress"
            status_label = "Inventário em andamento"
            status_tone = "accent"
        else:
            status_key = "pending_coverage"
            status_label = "Inventário pendente de cobertura"
            status_tone = "warning"

        items.append(
            {
                "datasource_id": datasource.id,
                "datasource_name": datasource.name,
                "engine": datasource.db_type,
                "engine_label": engine_label(datasource.db_type),
                "database_name": datasource.database,
                "schema_count": schema_count,
                "table_count": table_count,
                "served_tables": source_served,
                "certified_tables": source_certified,
                "pending_tables": source_pending,
                "is_active": datasource.is_active,
                "status_key": status_key,
                "status_label": status_label,
                "status_tone": status_tone,
            }
        )

    items.sort(key=lambda item: (-int(item["table_count"]), -int(item["schema_count"]), str(item["datasource_name"]).lower()))

    return {
        "total_sources": len(datasources),
        "total_schemas": total_schemas,
        "total_tables": total_tables,
        "served_tables": served_tables,
        "certified_tables": certified_tables,
        "pending_tables": max(total_tables - certified_tables, 0),
        "items": items,
    }
