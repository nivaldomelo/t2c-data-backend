from __future__ import annotations

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.tag import TagAssignment
from t2c_data.schemas.tag import TagLinkedTablePreview


def linked_tables_stmt(
    tag_ids: list[int] | None = None,
) -> Select[tuple[int, int, str, str, str, str, str | None]]:
    stmt = (
        select(
            TagAssignment.tag_id,
            TableEntity.id,
            TableEntity.name,
            Schema.name,
            Database.name,
            DataSource.name,
            TableEntity.description_manual,
        )
        .join(TableEntity, TableEntity.id == TagAssignment.entity_id)
        .join(Schema, Schema.id == TableEntity.schema_id)
        .join(Database, Database.id == Schema.database_id)
        .join(DataSource, DataSource.id == Database.datasource_id)
        .where(TagAssignment.entity_type == "table")
        .order_by(Schema.name, TableEntity.name)
    )
    if tag_ids:
        stmt = stmt.where(TagAssignment.tag_id.in_(tag_ids))
    return stmt


def preview_linked_tables(session: Session, tag_ids: list[int], *, limit_per_tag: int = 3) -> dict[int, list[TagLinkedTablePreview]]:
    if not tag_ids:
        return {}

    previews: dict[int, list[TagLinkedTablePreview]] = {tag_id: [] for tag_id in tag_ids}
    for tag_id, table_id, table_name, schema_name, database_name, datasource_name, description in session.execute(
        linked_tables_stmt(tag_ids)
    ):
        current = previews.setdefault(tag_id, [])
        if len(current) >= limit_per_tag:
            continue
        current.append(
            TagLinkedTablePreview(
                id=table_id,
                name=table_name,
                schema_name=schema_name,
                database_name=database_name,
                datasource_name=datasource_name,
                description=description,
            )
        )
    return previews


def all_linked_tables(session: Session, tag_id: int) -> list[TagLinkedTablePreview]:
    tables: list[TagLinkedTablePreview] = []
    for _, table_id, table_name, schema_name, database_name, datasource_name, description in session.execute(
        linked_tables_stmt([tag_id])
    ):
        tables.append(
            TagLinkedTablePreview(
                id=table_id,
                name=table_name,
                schema_name=schema_name,
                database_name=database_name,
                datasource_name=datasource_name,
                description=description,
            )
        )
    return tables
