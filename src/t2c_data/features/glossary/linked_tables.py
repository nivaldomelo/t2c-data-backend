from __future__ import annotations

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment
from t2c_data.schemas.glossary import GlossaryLinkedTablePreview


def linked_tables_stmt(
    term_ids: list[int] | None = None,
) -> Select[tuple[int, int, str, str, str, str, str | None]]:
    stmt = (
        select(
            GlossaryAssignment.term_id,
            TableEntity.id,
            TableEntity.name,
            Schema.name,
            Database.name,
            DataSource.name,
            TableEntity.description_manual,
        )
        .join(TableEntity, TableEntity.id == GlossaryAssignment.entity_id)
        .join(Schema, Schema.id == TableEntity.schema_id)
        .join(Database, Database.id == Schema.database_id)
        .join(DataSource, DataSource.id == Database.datasource_id)
        .where(GlossaryAssignment.entity_type == "table")
        .order_by(Schema.name, TableEntity.name)
    )
    if term_ids:
        stmt = stmt.where(GlossaryAssignment.term_id.in_(term_ids))
    return stmt


def preview_linked_tables(
    session: Session, term_ids: list[int], *, limit_per_term: int = 3
) -> dict[int, list[GlossaryLinkedTablePreview]]:
    if not term_ids:
        return {}
    previews: dict[int, list[GlossaryLinkedTablePreview]] = {term_id: [] for term_id in term_ids}
    for term_id, table_id, table_name, schema_name, database_name, datasource_name, description in session.execute(
        linked_tables_stmt(term_ids)
    ):
        current = previews.setdefault(term_id, [])
        if len(current) >= limit_per_term:
            continue
        current.append(
            GlossaryLinkedTablePreview(
                id=table_id,
                name=table_name,
                schema_name=schema_name,
                database_name=database_name,
                datasource_name=datasource_name,
                description=description,
            )
        )
    return previews


def all_linked_tables(session: Session, term_id: int) -> list[GlossaryLinkedTablePreview]:
    tables: list[GlossaryLinkedTablePreview] = []
    for _, table_id, table_name, schema_name, database_name, datasource_name, description in session.execute(
        linked_tables_stmt([term_id])
    ):
        tables.append(
            GlossaryLinkedTablePreview(
                id=table_id,
                name=table_name,
                schema_name=schema_name,
                database_name=database_name,
                datasource_name=datasource_name,
                description=description,
            )
        )
    return tables
