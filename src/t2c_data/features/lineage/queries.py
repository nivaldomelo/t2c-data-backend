from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.relation_queries import (
    list_asset_candidates,
    list_assets,
    list_relations,
    list_relations_out,
)
from t2c_data.features.lineage.summary_builder import get_asset_summary, get_table_summary
from t2c_data.features.lineage.visibility import asset_visible_to_user
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageAsset
from t2c_data.schemas.lineage import (
    DownstreamSpec,
    LineageSpecLookupOut,
    LineageSpecOut,
    ProcessSpec,
    SourceSpec,
)


def get_lineage_spec_for_table(db: Session, table_id: int, *, current_user: User | None = None) -> LineageSpecOut:
    summary = get_table_summary(db, table_id, current_user=current_user)
    process = summary.related_processes[0] if summary.related_processes else None
    return LineageSpecOut(
        table_id=table_id,
        upstreams=[
            SourceSpec(
                type=(item.system_name or "external").lower(),
                name=item.asset_name,
                datasource_id=item.datasource_id,
                database=item.system_name,
                schema=item.schema_name,
                object=item.object_name,
            )
            for item in summary.upstream
        ],
        process=ProcessSpec(type=process.process_type or "manual", name=process.process_name, meta=None)
        if process
        else None,
        downstreams=[
            DownstreamSpec(type="dashboard" if item.asset_type in {"dashboard", "question"} else "table", name=item.asset_name, url=None)
            for item in summary.downstream
        ],
        notes=summary.notes[0] if summary.notes else None,
        updated_at=None,
    )



def get_lineage_spec_lookup_by_fqn(db: Session, table_fqn: str, *, current_user: User | None = None) -> LineageSpecLookupOut:
    raw = table_fqn.strip()
    if not raw:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="table_fqn is required")
    parts = [part.strip() for part in raw.split(".") if part.strip()]
    if len(parts) not in {2, 3}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Use schema.table ou database.schema.table")

    stmt = (
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    )
    if len(parts) == 2:
        schema_name, table_name = parts
        stmt = stmt.where(Schema.name == schema_name, TableEntity.name == table_name)
    else:
        database_name, schema_name, table_name = parts
        stmt = stmt.where(Database.name == database_name, Schema.name == schema_name, TableEntity.name == table_name)
    row = db.execute(stmt.limit(1)).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    table, schema, database, datasource = row
    if current_user is not None:
        asset = db.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == table.id))
        if asset is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
        if not asset_visible_to_user(db, current_user, asset):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return LineageSpecLookupOut(
        table_id=table.id,
        table_fqn=f"{schema.name}.{table.name}",
        table_name=table.name,
        table_type=table.table_type,
        schema_name=schema.name,
        database_name=database.name,
        db_type=datasource.db_type,
        spec=get_lineage_spec_for_table(db, table.id, current_user=current_user),
    )


__all__ = [
    "get_asset_summary",
    "get_lineage_spec_for_table",
    "get_lineage_spec_lookup_by_fqn",
    "get_table_summary",
    "list_asset_candidates",
    "list_assets",
    "list_relations",
    "list_relations_out",
]
