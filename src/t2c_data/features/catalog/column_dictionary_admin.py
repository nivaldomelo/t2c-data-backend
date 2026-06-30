from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from fastapi import HTTPException, status
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.audit import AuditFieldChange
from t2c_data.features.access_control import can_view_table
from t2c_data.features.export_security import enforce_export_limit
from t2c_data.features.catalog.column_dictionary_workbook import (
    DICTIONARY_AUDIT_FIELDS,
    _build_existing_lookup,
    _build_existing_table_lookup,
    _resolve_existing_column,
    _resolve_existing_table,
    _summarize_catalog_gaps,
    ParsedColumnDictionaryRow,
    build_column_dictionary_workbook,
    import_column_dictionary_from_workbook,
    parse_column_dictionary_workbook,
)
from t2c_data.features.tags.api_support import load_entity_tag_contexts
from t2c_data.features.tags.intelligence import purge_tag_intelligence_for_entity_ids, reprocess_table_tag_intelligence
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.services.audit import log_field_changes, write_audit_log_sync
from t2c_data.schemas.column_dictionary import (
    ColumnDictionaryBulkUpdateIn,
    ColumnDictionaryBulkUpdateOut,
    ColumnDictionaryDetailOut,
    ColumnDictionaryFilterOptionsOut,
    ColumnDictionaryGapTableOut,
    ColumnDictionaryImportError,
    ColumnDictionaryCatalogGapTableOut,
    ColumnDictionaryImportPreviewOut,
    ColumnDictionaryImportPreviewRowOut,
    ColumnDictionaryItemOut,
    ColumnDictionaryPageOut,
    ColumnDictionarySummaryOut,
    ColumnDictionaryUpdateIn,
)


@dataclass(frozen=True)
class ColumnDictionaryFilters:
    datasource_name: str | None = None
    q: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    data_type: str | None = None
    is_primary_key: bool | None = None
    is_nullable: bool | None = None
    has_description: bool | None = None
    has_comment: bool | None = None
    has_existing_comment: bool | None = None
    sort_by: str = "schema"
    sort_dir: str = "asc"


CLEARED_DICTIONARY_FIELDS = (
    "external_id",
    "slug",
    "description_source",
    "description_manual",
    "existing_comment",
    "dictionary_description",
    "dictionary_comment",
)

CLEARED_TABLE_FIELDS = (
    "description_source",
    "description_manual",
)

CLEARED_SCHEMA_FIELDS = (
    "description_source",
    "description_manual",
)

CLEARED_DATABASE_FIELDS = (
    "description_source",
    "description_manual",
)


def _column_documentation_flags(column: ColumnEntity) -> tuple[bool, bool, bool]:
    has_description = bool((column.dictionary_description or column.description_manual or column.description_source or "").strip())
    has_comment = bool((column.dictionary_comment or "").strip())
    has_existing_comment = bool((column.existing_comment or "").strip())
    return has_description, has_comment, has_existing_comment


def _documentation_status(column: ColumnEntity) -> tuple[str, str, int, bool, bool, bool]:
    has_description, has_comment, has_existing_comment = _column_documentation_flags(column)
    if has_description and has_comment:
        return "complete", "Documentada", 100, has_description, has_comment, has_existing_comment
    if has_description or has_comment or has_existing_comment:
        return "partial", "Parcial", 50, has_description, has_comment, has_existing_comment
    return "pending", "Pendente", 0, has_description, has_comment, has_existing_comment


def _column_payload(
    column: ColumnEntity,
    *,
    datasource_name: str,
    schema_name: str,
    table_name: str,
    database_name: str | None = None,
    table_description_source: str | None = None,
    table_description_manual: str | None = None,
    schema_description_source: str | None = None,
    schema_description_manual: str | None = None,
    table_owner: str | None = None,
    table_lifecycle_status: str | None = None,
    tags: list | None = None,
) -> dict:
    documentation_status, documentation_status_label, documentation_pct, has_description, has_comment, has_existing_comment = _documentation_status(column)
    payload = {
        "id": column.id,
        "external_id": column.external_id,
        "slug": column.slug,
        "datasource_name": datasource_name,
        "schema_name": schema_name,
        "table_name": table_name,
        "table_id": column.table_id,
        "ordinal_position": column.ordinal_position,
        "name": column.name,
        "data_type": column.data_type,
        "udt_name": column.udt_name,
        "character_maximum_length": column.character_maximum_length,
        "numeric_precision": column.numeric_precision,
        "numeric_scale": column.numeric_scale,
        "is_nullable": column.is_nullable,
        "column_default": column.column_default,
        "existing_comment": column.existing_comment,
        "is_primary_key": column.is_primary_key,
        "description_source": column.description_source,
        "description_manual": column.description_manual,
        "dictionary_description": column.dictionary_description,
        "dictionary_comment": column.dictionary_comment,
        "documentation_status": documentation_status,
        "documentation_status_label": documentation_status_label,
        "documentation_pct": documentation_pct,
        "has_description": has_description,
        "has_comment": has_comment,
        "has_existing_comment": has_existing_comment,
        "created_at": column.created_at,
        "updated_at": column.updated_at,
    }
    if database_name is not None:
        payload["database_name"] = database_name
    if table_description_source is not None:
        payload["table_description_source"] = table_description_source
    if table_description_manual is not None:
        payload["table_description_manual"] = table_description_manual
    if schema_description_source is not None:
        payload["schema_description_source"] = schema_description_source
    if schema_description_manual is not None:
        payload["schema_description_manual"] = schema_description_manual
    if table_owner is not None:
        payload["table_owner"] = table_owner
    if table_lifecycle_status is not None:
        payload["table_lifecycle_status"] = table_lifecycle_status
    if tags is not None:
        payload["tags"] = tags
    return payload


def _base_stmt():
    return (
        select(
            ColumnEntity,
            DataSource.name.label("datasource_name"),
            Schema.name.label("schema_name"),
            TableEntity.name.label("table_name"),
            Database.name.label("database_name"),
            Schema.description_source.label("schema_description_source"),
            Schema.description_manual.label("schema_description_manual"),
            TableEntity.description_source.label("table_description_source"),
            TableEntity.description_manual.label("table_description_manual"),
            TableEntity.owner.label("table_owner"),
            TableEntity.lifecycle_status.label("table_lifecycle_status"),
        )
        .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    )


def _apply_filters(stmt, filters: ColumnDictionaryFilters):
    if filters.datasource_name:
        stmt = stmt.where(func.lower(DataSource.name) == filters.datasource_name.strip().lower())
    if filters.q:
        pattern = f"%{filters.q.strip()}%"
        stmt = stmt.where(
            or_(
                DataSource.name.ilike(pattern),
                ColumnEntity.name.ilike(pattern),
                ColumnEntity.slug.ilike(pattern),
                Schema.name.ilike(pattern),
                TableEntity.name.ilike(pattern),
                ColumnEntity.data_type.ilike(pattern),
                ColumnEntity.udt_name.ilike(pattern),
                ColumnEntity.dictionary_description.ilike(pattern),
                ColumnEntity.dictionary_comment.ilike(pattern),
                ColumnEntity.existing_comment.ilike(pattern),
            )
        )
    if filters.schema_name:
        stmt = stmt.where(func.lower(Schema.name) == filters.schema_name.strip().lower())
    if filters.table_name:
        stmt = stmt.where(func.lower(TableEntity.name) == filters.table_name.strip().lower())
    if filters.data_type:
        stmt = stmt.where(func.lower(ColumnEntity.data_type) == filters.data_type.strip().lower())
    if filters.is_primary_key is not None:
        stmt = stmt.where(ColumnEntity.is_primary_key.is_(filters.is_primary_key))
    if filters.is_nullable is not None:
        stmt = stmt.where(ColumnEntity.is_nullable.is_(filters.is_nullable))
    if filters.has_description is not None:
        description_expr = or_(
            ColumnEntity.dictionary_description.is_not(None),
            ColumnEntity.description_manual.is_not(None),
            ColumnEntity.description_source.is_not(None),
        )
        stmt = stmt.where(description_expr if filters.has_description else ~description_expr)
    if filters.has_comment is not None:
        stmt = stmt.where(ColumnEntity.dictionary_comment.is_not(None) if filters.has_comment else ColumnEntity.dictionary_comment.is_(None))
    if filters.has_existing_comment is not None:
        stmt = stmt.where(ColumnEntity.existing_comment.is_not(None) if filters.has_existing_comment else ColumnEntity.existing_comment.is_(None))
    return stmt


def _apply_sort(stmt, filters: ColumnDictionaryFilters):
    sort_map = {
        "schema": Schema.name,
        "table": TableEntity.name,
        "column": ColumnEntity.name,
        "ordinal_position": ColumnEntity.ordinal_position,
        "data_type": ColumnEntity.data_type,
        "updated_at": ColumnEntity.updated_at,
        "documentation": ColumnEntity.dictionary_description,
    }
    sort_expr = sort_map.get(filters.sort_by, Schema.name)
    if filters.sort_dir.lower() == "desc":
        return stmt.order_by(sort_expr.desc(), TableEntity.name.asc(), ColumnEntity.ordinal_position.asc(), ColumnEntity.id.asc())
    return stmt.order_by(sort_expr.asc(), TableEntity.name.asc(), ColumnEntity.ordinal_position.asc(), ColumnEntity.id.asc())


def _load_rows(session: Session, filters: ColumnDictionaryFilters):
    stmt = _apply_sort(_apply_filters(_base_stmt(), filters), filters)
    return session.execute(stmt).all()


def _build_item_payload(row) -> dict:
    column, datasource_name, schema_name, table_name, database_name, schema_description_source, schema_description_manual, table_description_source, table_description_manual, table_owner, table_lifecycle_status = row
    return _column_payload(
        column,
        datasource_name=datasource_name,
        schema_name=schema_name,
        table_name=table_name,
        database_name=database_name,
        table_description_source=table_description_source,
        table_description_manual=table_description_manual,
        schema_description_source=schema_description_source,
        schema_description_manual=schema_description_manual,
        table_owner=table_owner,
        table_lifecycle_status=table_lifecycle_status,
    )


def _attach_column_tags(session: Session, payloads: list[dict]) -> list[dict]:
    column_ids = [int(payload["id"]) for payload in payloads]
    tags_by_column_id = load_entity_tag_contexts(session, entity_type="column", entity_ids=column_ids)
    for payload in payloads:
        payload["tags"] = tags_by_column_id.get(int(payload["id"]), [])
    return payloads


def _visible_table_ids(session: Session, current_user=None) -> set[int]:
    if current_user is None:
        return {
            int(table_id)
            for table_id in session.scalars(select(TableEntity.id)).all()
        }
    tables = session.scalars(
        select(TableEntity)
        .options(
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
    ).all()
    return {int(table.id) for table in tables if can_view_table(current_user, table)}


def _load_filter_options(session: Session, _filters: ColumnDictionaryFilters, current_user=None) -> ColumnDictionaryFilterOptionsOut:
    visible_table_ids = _visible_table_ids(session, current_user)
    if not visible_table_ids:
        return ColumnDictionaryFilterOptionsOut()

    stmt = (
        select(
            DataSource.name.label("datasource_name"),
            Schema.name.label("schema_name"),
            TableEntity.name.label("table_name"),
            ColumnEntity.data_type.label("data_type"),
        )
        .select_from(ColumnEntity)
        .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(ColumnEntity.table_id.in_(visible_table_ids))
    )
    rows = session.execute(stmt).all()
    datasources = sorted({row.datasource_name for row in rows if row.datasource_name})
    schema_names = sorted({row.schema_name for row in rows if row.schema_name})
    table_names = sorted({row.table_name for row in rows if row.table_name})
    data_types = sorted({row.data_type for row in rows if row.data_type})
    return ColumnDictionaryFilterOptionsOut(
        datasources=datasources,
        schemas=schema_names,
        tables=table_names,
        data_types=data_types,
    )


def _build_summary(rows: Iterable[tuple]) -> ColumnDictionarySummaryOut:
    groups: dict[tuple[str, str], dict[str, int]] = {}
    total_columns = 0
    documented_columns = 0
    comment_columns = 0
    existing_comment_columns = 0
    for row in rows:
        column, _datasource_name, schema_name, table_name, *_ = row
        total_columns += 1
        has_description, has_comment, has_existing_comment = _column_documentation_flags(column)
        documented_columns += int(has_description and has_comment)
        comment_columns += int(has_comment)
        existing_comment_columns += int(has_existing_comment)
        entry = groups.setdefault((schema_name, table_name), {"total": 0, "documented": 0})
        entry["total"] += 1
        entry["documented"] += int(has_description and has_comment)

    gap_tables: list[ColumnDictionaryGapTableOut] = []
    for (schema_name, table_name), values in groups.items():
        total = values["total"]
        documented = values["documented"]
        pending = max(total - documented, 0)
        documented_pct = round((documented / total) * 100) if total else 0
        gap_tables.append(
            ColumnDictionaryGapTableOut(
                schema_name=schema_name,
                table_name=table_name,
                total_columns=total,
                documented_columns=documented,
                pending_columns=pending,
                documented_pct=documented_pct,
            )
        )

    gap_tables.sort(key=lambda item: (item.documented_pct, -item.pending_columns, item.schema_name.lower(), item.table_name.lower()))
    total_tables = len(groups)
    total_schemas = len({schema_name for schema_name, _ in groups})
    pending_columns = max(total_columns - documented_columns, 0)
    return ColumnDictionarySummaryOut(
        total_columns=total_columns,
        total_tables=total_tables,
        total_schemas=total_schemas,
        documented_columns=documented_columns,
        documented_pct=round((documented_columns / total_columns) * 100) if total_columns else 0,
        comment_columns=comment_columns,
        comment_pct=round((comment_columns / total_columns) * 100) if total_columns else 0,
        existing_comment_columns=existing_comment_columns,
        existing_comment_pct=round((existing_comment_columns / total_columns) * 100) if total_columns else 0,
        pending_columns=pending_columns,
        top_gap_tables=gap_tables[:5],
    )


def list_column_dictionary(
    session: Session,
    *,
    filters: ColumnDictionaryFilters,
    page: int,
    page_size: int,
    current_user=None,
) -> ColumnDictionaryPageOut:
    visible_table_ids = _visible_table_ids(session, current_user)
    if not visible_table_ids:
        filters_out = _load_filter_options(session, filters, current_user)
        return ColumnDictionaryPageOut(total=0, page=page, page_size=page_size, items=[], filters=filters_out)

    base_stmt = _apply_filters(_base_stmt(), filters)
    base_stmt = base_stmt.where(TableEntity.id.in_(visible_table_ids))
    total = session.scalar(select(func.count()).select_from(base_stmt.subquery())) or 0
    item_payloads = [_build_item_payload(row) for row in session.execute(
        _apply_sort(base_stmt, filters)
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()]
    items = [
        ColumnDictionaryItemOut.model_validate(payload)
        for payload in _attach_column_tags(session, item_payloads)
    ]
    filters_out = _load_filter_options(session, filters, current_user)
    return ColumnDictionaryPageOut(total=int(total), page=page, page_size=page_size, items=items, filters=filters_out)


def get_column_dictionary_summary(
    session: Session,
    *,
    filters: ColumnDictionaryFilters,
    current_user=None,
) -> ColumnDictionarySummaryOut:
    visible_table_ids = _visible_table_ids(session, current_user)
    if not visible_table_ids:
        return _build_summary([])
    rows = [row for row in _load_rows(session, filters) if row[0].table_id in visible_table_ids]
    return _build_summary(rows)


def get_column_dictionary_detail(session: Session, column_id: int, current_user=None) -> ColumnDictionaryDetailOut:
    row = session.execute(
        _base_stmt().where(ColumnEntity.id == column_id).limit(1)
    ).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
    if current_user is not None:
        column, *_ = row
        table = session.get(TableEntity, column.table_id)
        if not table or not can_view_table(current_user, table):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
    item = _build_item_payload(row)
    item = _attach_column_tags(session, [item])[0]
    return ColumnDictionaryDetailOut.model_validate(item)


def _apply_column_updates(column: ColumnEntity, payload: ColumnDictionaryUpdateIn) -> list[AuditFieldChange]:
    before = {field_name: getattr(column, field_name) for field_name in DICTIONARY_AUDIT_FIELDS}
    updates = payload.model_dump(exclude_unset=True)
    for key, value in updates.items():
        setattr(column, key, value)
    after = {field_name: getattr(column, field_name) for field_name in DICTIONARY_AUDIT_FIELDS}
    return [
        AuditFieldChange(field_name=field_name, before=before[field_name], after=after[field_name])
        for field_name in DICTIONARY_AUDIT_FIELDS
        if before[field_name] != after[field_name]
    ]


def update_column_dictionary_item(
    session: Session,
    *,
    column_id: int,
    payload: ColumnDictionaryUpdateIn,
    actor_user_id: int | None = None,
    audit_kwargs: dict | None = None,
    current_user=None,
) -> ColumnDictionaryDetailOut:
    column = session.get(ColumnEntity, column_id)
    if not column:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
    if current_user is not None:
        table = session.get(TableEntity, column.table_id)
        if not table or not can_view_table(current_user, table):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")

    changes = _apply_column_updates(column, payload)
    if changes:
        log_field_changes(
            session,
            action="column_dictionary.update",
            entity_type="column",
            entity_id=column.id,
            parent_entity_type="table",
            parent_entity_id=column.table_id,
            changes=changes,
            source_module="catalog.dictionary_admin",
            metadata={"message": "Manual dictionary metadata updated"},
            audit_kwargs=audit_kwargs,
            actor_user_id=actor_user_id,
        )
        reprocess_table_tag_intelligence(
            session,
            table_id=column.table_id,
            actor_user_id=actor_user_id,
            audit_kwargs=audit_kwargs,
            source_module="catalog.dictionary_admin",
            metadata={"origin": "column_dictionary.update", "column_id": column.id},
        )
    session.commit()
    return get_column_dictionary_detail(session, column_id, current_user=current_user)


def clear_column_dictionary_item(
    session: Session,
    *,
    column_id: int,
    actor_user_id: int | None = None,
    audit_kwargs: dict | None = None,
    current_user=None,
) -> ColumnDictionaryDetailOut:
    column = session.get(ColumnEntity, column_id)
    if not column:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
    if current_user is not None:
        table = session.get(TableEntity, column.table_id)
        if not table or not can_view_table(current_user, table):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")

    before = {field_name: getattr(column, field_name) for field_name in CLEARED_DICTIONARY_FIELDS}
    changes: list[AuditFieldChange] = []
    for field_name in CLEARED_DICTIONARY_FIELDS:
        if getattr(column, field_name) is not None:
            setattr(column, field_name, None)
            changes.append(AuditFieldChange(field_name=field_name, before=before[field_name], after=None))

    if changes:
        log_field_changes(
            session,
            action="column_dictionary.clear",
            entity_type="column",
            entity_id=column.id,
            parent_entity_type="table",
            parent_entity_id=column.table_id,
            changes=changes,
            source_module="catalog.dictionary_admin",
            metadata={"message": "Dictionary curation cleared"},
            audit_kwargs=audit_kwargs,
            actor_user_id=actor_user_id,
        )
        reprocess_table_tag_intelligence(
            session,
            table_id=column.table_id,
            actor_user_id=actor_user_id,
            audit_kwargs=audit_kwargs,
            source_module="catalog.dictionary_admin",
            metadata={"origin": "column_dictionary.clear", "column_id": column.id},
        )
    session.commit()
    return get_column_dictionary_detail(session, column_id, current_user=current_user)


def reset_column_dictionary_curation(
    session: Session,
    *,
    actor_user_id: int | None = None,
    audit_kwargs: dict | None = None,
    current_user=None,
) -> int:
    columns = session.scalars(select(ColumnEntity)).all()
    deleted_columns = len(columns)
    touched_table_ids = sorted({int(column.table_id) for column in columns})
    column_ids = [int(column.id) for column in columns]
    if column_ids:
        purge_tag_intelligence_for_entity_ids(session, entity_type="column", entity_ids=column_ids, delete_history=True)
    for column in columns:
        session.delete(column)

    payload = dict(audit_kwargs or {})
    if actor_user_id is not None and payload.get("user_id") is None:
        payload["user_id"] = actor_user_id
    write_audit_log_sync(
        session,
        action="column_dictionary.reset_all",
        entity_type="column_dictionary",
        entity_id="all",
        source_module="catalog.dictionary_admin",
        metadata={
            "deleted_columns": deleted_columns,
            "message": "Dictionary rows deleted",
        },
        **payload,
    )
    for table_id in touched_table_ids:
        reprocess_table_tag_intelligence(
            session,
            table_id=table_id,
            actor_user_id=actor_user_id,
            audit_kwargs=audit_kwargs,
            source_module="catalog.dictionary_admin",
            metadata={"origin": "column_dictionary.reset_all"},
        )
    session.commit()
    return deleted_columns


def bulk_update_column_dictionary(
    session: Session,
    *,
    payload: ColumnDictionaryBulkUpdateIn,
    actor_user_id: int | None = None,
    audit_kwargs: dict | None = None,
    current_user=None,
) -> ColumnDictionaryBulkUpdateOut:
    if not payload.column_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Selecione ao menos uma coluna.")
    if not payload.model_dump(exclude={"column_ids"}, exclude_unset=True):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Informe ao menos um campo para atualização.")

    columns = session.scalars(select(ColumnEntity).where(ColumnEntity.id.in_(payload.column_ids))).all()
    if current_user is not None:
        allowed_columns = []
        for column in columns:
            table = session.get(TableEntity, column.table_id)
            if table and can_view_table(current_user, table):
                allowed_columns.append(column)
        if len(allowed_columns) != len(columns):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
        columns = allowed_columns
    found_ids = {column.id for column in columns}
    not_found = sorted(set(payload.column_ids) - found_ids)
    updated = 0
    touched_table_ids: set[int] = set()
    for column in columns:
        update_values = payload.model_dump(exclude={"column_ids"}, exclude_unset=True)
        changes = _apply_column_updates(column, ColumnDictionaryUpdateIn(**update_values))
        if changes:
            updated += 1
            touched_table_ids.add(int(column.table_id))
            log_field_changes(
                session,
                action="column_dictionary.bulk_update",
                entity_type="column",
                entity_id=column.id,
                parent_entity_type="table",
                parent_entity_id=column.table_id,
                changes=changes,
                source_module="catalog.dictionary_admin",
                metadata={"message": "Batch dictionary metadata updated"},
                audit_kwargs=audit_kwargs,
                actor_user_id=actor_user_id,
            )
    for table_id in sorted(touched_table_ids):
        reprocess_table_tag_intelligence(
            session,
            table_id=table_id,
            actor_user_id=actor_user_id,
            audit_kwargs=audit_kwargs,
            source_module="catalog.dictionary_admin",
            metadata={"origin": "column_dictionary.bulk_update", "column_ids": payload.column_ids},
        )
    session.commit()
    return ColumnDictionaryBulkUpdateOut(matched=len(payload.column_ids), updated=updated, not_found=not_found)


def _build_existing_map(session: Session, parsed_rows: list[ParsedColumnDictionaryRow]) -> dict[tuple[str, str, str], ColumnEntity]:
    lookup_keys = {(row.schema_name.lower(), row.table_name.lower(), row.column_name.lower()) for row in parsed_rows}
    if not lookup_keys:
        return {}
    schemas = sorted({key[0] for key in lookup_keys})
    tables = sorted({key[1] for key in lookup_keys})
    columns = sorted({key[2] for key in lookup_keys})
    existing_rows = session.execute(
        select(
            ColumnEntity,
            func.lower(Schema.name).label("schema_name"),
            func.lower(TableEntity.name).label("table_name"),
            func.lower(ColumnEntity.name).label("column_name"),
        )
        .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .where(
            func.lower(Schema.name).in_(schemas),
            func.lower(TableEntity.name).in_(tables),
            func.lower(ColumnEntity.name).in_(columns),
        )
    ).all()
    return {
        (schema_name, table_name, column_name): column
        for column, schema_name, table_name, column_name in existing_rows
    }


def preview_column_dictionary_import(session: Session, content: bytes) -> ColumnDictionaryImportPreviewOut:
    parsed_rows, parsing_errors = parse_column_dictionary_workbook(content)
    errors = list(parsing_errors)
    preview_rows: list[ColumnDictionaryImportPreviewRowOut] = []
    inserted = 0
    updated = 0
    matched = 0
    ignored = 0
    rejected = len(parsing_errors)

    existing_lookup = _build_existing_lookup(session, parsed_rows)
    existing_table_lookup = _build_existing_table_lookup(session, parsed_rows)
    gap_summary = _summarize_catalog_gaps(parsed_rows, existing_lookup, existing_table_lookup)
    seen_keys: set[tuple[str, str, str]] = set()

    for row in parsed_rows:
        key = (row.schema_name.lower(), row.table_name.lower(), row.column_name.lower())
        if key in seen_keys:
            rejected += 1
            errors.append(
                ColumnDictionaryImportError(
                    row_number=row.row_number,
                    slug=row.slug,
                    message="Chave duplicada na planilha para Schema + Tabela + Nome_Coluna.",
                )
            )
            preview_rows.append(
                ColumnDictionaryImportPreviewRowOut(
                    row_number=row.row_number,
                    status="rejeitada",
                    schema_name=row.schema_name,
                    table_name=row.table_name,
                    column_name=row.column_name,
                    slug=row.slug,
                    message="Chave duplicada na planilha para Schema + Tabela + Nome_Coluna.",
                )
            )
            continue
        seen_keys.add(key)

        column, match_source, _ = _resolve_existing_column(row, existing_lookup)
        if column is None:
            table, table_match_source = _resolve_existing_table(row, existing_table_lookup)
            if table is None:
                rejected += 1
                message = (
                    "Tabela não encontrada no catálogo técnico atual. "
                    "Execute a sincronização do datasource antes de importar."
                )
                errors.append(ColumnDictionaryImportError(row_number=row.row_number, slug=row.slug, message=message))
                preview_rows.append(
                    ColumnDictionaryImportPreviewRowOut(
                        row_number=row.row_number,
                        status="rejeitada",
                        schema_name=row.schema_name,
                        table_name=row.table_name,
                        column_name=row.column_name,
                        slug=row.slug,
                        match_source=None,
                        message=message,
                    )
                )
                continue
            match_source = table_match_source
            if row.ordinal_position is None or not row.data_type:
                rejected += 1
                message = "Posicao_Coluna e Tipo_de_Dado são obrigatórios para criar nova coluna."
                errors.append(ColumnDictionaryImportError(row_number=row.row_number, slug=row.slug, message=message))
                preview_rows.append(
                    ColumnDictionaryImportPreviewRowOut(
                        row_number=row.row_number,
                        status="rejeitada",
                        schema_name=row.schema_name,
                        table_name=row.table_name,
                        column_name=row.column_name,
                        slug=row.slug,
                        match_source=match_source,
                        message=message,
                    )
                )
                continue
            matched += 1
            inserted += 1
            preview_rows.append(
                ColumnDictionaryImportPreviewRowOut(
                    row_number=row.row_number,
                    status="inserida",
                    schema_name=row.schema_name,
                    table_name=row.table_name,
                    column_name=row.column_name,
                    slug=row.slug,
                    match_source=match_source,
                    message=f"Nova coluna será criada na tabela existente via {match_source}.",
                )
            )
            continue

        matched += 1

        before = {field_name: getattr(column, field_name) for field_name in DICTIONARY_AUDIT_FIELDS}
        after = {
            "external_id": row.external_id,
            "slug": row.slug,
            "udt_name": row.udt_name,
            "character_maximum_length": row.character_maximum_length,
            "numeric_precision": row.numeric_precision,
            "numeric_scale": row.numeric_scale,
            "column_default": row.column_default,
            "existing_comment": row.existing_comment,
            "dictionary_description": row.dictionary_description,
            "dictionary_comment": row.dictionary_comment,
        }
        changed = any(before[field_name] != after[field_name] for field_name in DICTIONARY_AUDIT_FIELDS)
        if not changed:
            ignored += 1
            preview_rows.append(
                ColumnDictionaryImportPreviewRowOut(
                    row_number=row.row_number,
                    status="sem_alteracoes",
                    schema_name=row.schema_name,
                    table_name=row.table_name,
                    column_name=row.column_name,
                    slug=row.slug,
                    match_source=match_source,
                    message=f"A linha não altera metadados do dicionário. Match por {match_source}.",
                )
            )
            continue

        updated += 1
        status_label = "atualizada"
        message = f"Linha altera metadados já existentes via {match_source}."
        preview_rows.append(
            ColumnDictionaryImportPreviewRowOut(
                row_number=row.row_number,
                status=status_label,
                schema_name=row.schema_name,
                table_name=row.table_name,
                column_name=row.column_name,
                slug=row.slug,
                match_source=match_source,
                message=message,
            )
        )

    return ColumnDictionaryImportPreviewOut(
        processed=len(parsed_rows) + len(parsing_errors),
        matched=matched,
        inserted=inserted,
        updated=updated,
        ignored=ignored,
        rejected=rejected,
        duplicate_rows=gap_summary.duplicate_rows,
        missing_catalog_rows=gap_summary.missing_catalog_rows,
        catalog_sync_required=gap_summary.missing_catalog_rows > 0,
        missing_catalog_schemas=gap_summary.missing_catalog_schemas,
        missing_catalog_tables=[
            ColumnDictionaryCatalogGapTableOut(schema_name=schema_name, table_name=table_name, rows_count=rows_count)
            for schema_name, table_name, rows_count in gap_summary.missing_catalog_tables
        ],
        rows=preview_rows,
        errors=errors,
    )


def export_column_dictionary_rows(
    session: Session,
    *,
    filters: ColumnDictionaryFilters,
    current_user=None,
    limit: int | None = None,
) -> tuple[bytes, int, bool]:
    rows = _load_rows(session, filters)
    visible_table_ids = _visible_table_ids(session, current_user)
    if visible_table_ids:
        rows = [row for row in rows if row[0].table_id in visible_table_ids]
    items = [
        (row[0], row[2], row[3])
        for row in rows
    ]
    bounded_items, truncated = enforce_export_limit(items, limit=limit or 5000)
    return build_column_dictionary_workbook(bounded_items, include_readme=True), len(bounded_items), truncated


def template_column_dictionary_workbook() -> bytes:
    return build_column_dictionary_workbook([], include_readme=True)


def import_column_dictionary_from_file(
    session: Session,
    content: bytes,
    *,
    audit_kwargs: dict | None = None,
    actor_user_id: int | None = None,
    source_module: str = "catalog.dictionary_admin",
    metadata: dict | None = None,
):
    return import_column_dictionary_from_workbook(
        session,
        content,
        audit_kwargs=audit_kwargs,
        actor_user_id=actor_user_id,
        source_module=source_module,
        metadata=metadata,
    )


__all__ = [
    "ColumnDictionaryFilters",
    "bulk_update_column_dictionary",
    "clear_column_dictionary_item",
    "export_column_dictionary_rows",
    "get_column_dictionary_detail",
    "get_column_dictionary_summary",
    "import_column_dictionary_from_file",
    "list_column_dictionary",
    "preview_column_dictionary_import",
    "template_column_dictionary_workbook",
    "reset_column_dictionary_curation",
    "update_column_dictionary_item",
]
