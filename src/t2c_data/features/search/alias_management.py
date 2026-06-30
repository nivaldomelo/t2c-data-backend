from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Integer, String, func, literal, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from t2c_data.features.search.global_search import normalize_search_text
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.search import ColumnSearchAlias, TableSearchAlias

ALIAS_ENTITY_TYPES = {"table", "column"}
ALIAS_LABEL_KINDS = {"friendly_name", "alias", "synonym"}


@dataclass
class SearchAliasFilters:
    entity_type: str | None = None
    label_kind: str | None = None
    datasource_id: int | None = None
    database_id: int | None = None
    schema_id: int | None = None
    table_id: int | None = None
    column_id: int | None = None
    query: str | None = None
    limit: int = 100
    offset: int = 0


def validate_alias_payload(*, entity_type: str, label_kind: str, label: str, table_id: int | None, column_id: int | None) -> tuple[str, str, str]:
    normalized_entity_type = entity_type.strip().lower()
    normalized_label_kind = label_kind.strip().lower()
    trimmed_label = label.strip()

    if normalized_entity_type not in ALIAS_ENTITY_TYPES:
        raise ValueError("entity_type deve ser 'table' ou 'column'.")
    if normalized_label_kind not in ALIAS_LABEL_KINDS:
        raise ValueError("label_kind deve ser 'friendly_name', 'alias' ou 'synonym'.")
    if len(trimmed_label) < 2:
        raise ValueError("Informe um alias com pelo menos 2 caracteres.")
    if normalized_entity_type == "table" and not table_id:
        raise ValueError("table_id é obrigatório para aliases de tabela.")
    if normalized_entity_type == "column" and not column_id:
        raise ValueError("column_id é obrigatório para aliases de coluna.")
    if normalized_entity_type == "table" and column_id:
        raise ValueError("column_id não deve ser enviado para aliases de tabela.")
    if normalized_entity_type == "column" and table_id:
        raise ValueError("table_id não deve ser enviado para aliases de coluna.")

    return normalized_entity_type, normalized_label_kind, trimmed_label


def _option_rows(rows: list[tuple[int, str]]) -> list[dict[str, str]]:
    seen: set[int] = set()
    items: list[dict[str, str]] = []
    for row_id, row_label in rows:
        if row_id in seen:
            continue
        seen.add(row_id)
        items.append({"value": str(row_id), "label": row_label})
    return items


def get_alias_filters(session: Session) -> dict[str, object]:
    datasource_rows = session.execute(
        select(DataSource.id, DataSource.name).order_by(DataSource.name)
    ).all()
    database_rows = session.execute(
        select(Database.id, Database.name, DataSource.name)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .order_by(DataSource.name, Database.name)
    ).all()
    schema_rows = session.execute(
        select(Schema.id, Schema.name, Database.name, DataSource.name)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .order_by(DataSource.name, Database.name, Schema.name)
    ).all()
    table_rows = session.execute(
        select(TableEntity.id, TableEntity.name, Schema.name, Database.name, DataSource.name)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .order_by(DataSource.name, Database.name, Schema.name, TableEntity.name)
    ).all()
    column_rows = session.execute(
        select(ColumnEntity.id, ColumnEntity.name, TableEntity.name, Schema.name, Database.name, DataSource.name)
        .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .order_by(DataSource.name, Database.name, Schema.name, TableEntity.name, ColumnEntity.ordinal_position)
    ).all()
    return {
        "datasources": _option_rows([(int(row[0]), str(row[1])) for row in datasource_rows]),
        "databases": _option_rows([(int(row[0]), f"{row[2]} · {row[1]}") for row in database_rows]),
        "schemas": _option_rows([(int(row[0]), f"{row[3]} · {row[2]} · {row[1]}") for row in schema_rows]),
        "tables": _option_rows([(int(row[0]), f"{row[4]} · {row[3]} · {row[2]} · {row[1]}") for row in table_rows]),
        "columns": _option_rows([(int(row[0]), f"{row[5]} · {row[4]} · {row[3]} · {row[2]} · {row[1]}") for row in column_rows]),
        "label_kinds": [
            {"value": "friendly_name", "label": "Nome amigável"},
            {"value": "alias", "label": "Alias"},
            {"value": "synonym", "label": "Sinônimo"},
        ],
        "entity_types": [
            {"value": "table", "label": "Tabela"},
            {"value": "column", "label": "Coluna"},
        ],
    }


def list_aliases(session: Session, filters: SearchAliasFilters) -> dict[str, object]:
    table_query = (
        select(
            TableSearchAlias.id.label("id"),
            literal("table", type_=String()).label("entity_type"),
            TableSearchAlias.label_kind.label("label_kind"),
            TableSearchAlias.label.label("label"),
            TableSearchAlias.normalized_label.label("normalized_label"),
            DataSource.id.label("datasource_id"),
            DataSource.name.label("datasource_name"),
            Database.id.label("database_id"),
            Database.name.label("database_name"),
            Schema.id.label("schema_id"),
            Schema.name.label("schema_name"),
            TableEntity.id.label("table_id"),
            TableEntity.name.label("table_name"),
            literal(None, type_=Integer()).label("column_id"),
            literal(None, type_=String()).label("column_name"),
        )
        .join(TableEntity, TableSearchAlias.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    )
    column_query = (
        select(
            ColumnSearchAlias.id.label("id"),
            literal("column", type_=String()).label("entity_type"),
            ColumnSearchAlias.label_kind.label("label_kind"),
            ColumnSearchAlias.label.label("label"),
            ColumnSearchAlias.normalized_label.label("normalized_label"),
            DataSource.id.label("datasource_id"),
            DataSource.name.label("datasource_name"),
            Database.id.label("database_id"),
            Database.name.label("database_name"),
            Schema.id.label("schema_id"),
            Schema.name.label("schema_name"),
            TableEntity.id.label("table_id"),
            TableEntity.name.label("table_name"),
            ColumnEntity.id.label("column_id"),
            ColumnEntity.name.label("column_name"),
        )
        .join(ColumnEntity, ColumnSearchAlias.column_id == ColumnEntity.id)
        .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    )

    if filters.entity_type == "table":
        union_query = table_query
    elif filters.entity_type == "column":
        union_query = column_query
    else:
        union_query = table_query.union_all(column_query)

    subquery = union_query.subquery()
    conditions = []
    if filters.label_kind:
        conditions.append(subquery.c.label_kind == filters.label_kind)
    if filters.datasource_id:
        conditions.append(subquery.c.datasource_id == filters.datasource_id)
    if filters.database_id:
        conditions.append(subquery.c.database_id == filters.database_id)
    if filters.schema_id:
        conditions.append(subquery.c.schema_id == filters.schema_id)
    if filters.table_id:
        conditions.append(subquery.c.table_id == filters.table_id)
    if filters.column_id:
        conditions.append(subquery.c.column_id == filters.column_id)
    if filters.query and filters.query.strip():
        normalized = normalize_search_text(filters.query)
        like = f"%{normalized}%"
        conditions.append(
            or_(
                func.lower(subquery.c.label).like(f"%{filters.query.strip().lower()}%"),
                subquery.c.normalized_label.like(like),
                func.lower(subquery.c.table_name).like(f"%{filters.query.strip().lower()}%"),
                func.lower(func.coalesce(subquery.c.column_name, "")).like(f"%{filters.query.strip().lower()}%"),
            )
        )

    count_stmt = select(func.count()).select_from(subquery)
    items_stmt = select(subquery)
    for condition in conditions:
        count_stmt = count_stmt.where(condition)
        items_stmt = items_stmt.where(condition)
    items_stmt = items_stmt.order_by(
        subquery.c.datasource_name,
        subquery.c.database_name,
        subquery.c.schema_name,
        subquery.c.table_name,
        subquery.c.column_name,
        subquery.c.label_kind,
        subquery.c.label,
    ).offset(filters.offset).limit(filters.limit)

    total = int(session.scalar(count_stmt) or 0)
    rows = session.execute(items_stmt).all()
    return {
        "total": total,
        "items": [
            {
                "id": int(row.id),
                "entity_type": str(row.entity_type),
                "label_kind": str(row.label_kind),
                "label": str(row.label),
                "normalized_label": str(row.normalized_label),
                "datasource_id": int(row.datasource_id) if row.datasource_id is not None else None,
                "datasource_name": row.datasource_name,
                "database_id": int(row.database_id) if row.database_id is not None else None,
                "database_name": row.database_name,
                "schema_id": int(row.schema_id) if row.schema_id is not None else None,
                "schema_name": row.schema_name,
                "table_id": int(row.table_id) if row.table_id is not None else None,
                "table_name": row.table_name,
                "column_id": int(row.column_id) if row.column_id is not None else None,
                "column_name": row.column_name,
            }
            for row in rows
        ],
    }


def get_alias_detail(session: Session, *, entity_type: str, alias_id: int) -> dict[str, object]:
    payload = list_aliases(
        session,
        SearchAliasFilters(
            entity_type=entity_type,
            limit=500,
            offset=0,
        ),
    )
    for item in payload["items"]:
        if int(item["id"]) == alias_id:
            return item
    raise LookupError("Alias não encontrado.")


def _get_table_or_404(session: Session, table_id: int) -> TableEntity:
    table = session.get(TableEntity, table_id)
    if not table:
        raise LookupError("Tabela não encontrada para vincular o alias.")
    return table


def _get_column_or_404(session: Session, column_id: int) -> ColumnEntity:
    column = session.get(ColumnEntity, column_id)
    if not column:
        raise LookupError("Coluna não encontrada para vincular o alias.")
    return column


def create_alias(session: Session, *, entity_type: str, label_kind: str, label: str, table_id: int | None, column_id: int | None):
    entity_type, label_kind, label = validate_alias_payload(
        entity_type=entity_type,
        label_kind=label_kind,
        label=label,
        table_id=table_id,
        column_id=column_id,
    )
    if entity_type == "table":
        _get_table_or_404(session, int(table_id))
        item = TableSearchAlias(
            table_id=int(table_id),
            label_kind=label_kind,
            label=label,
            normalized_label=normalize_search_text(label),
        )
    else:
        _get_column_or_404(session, int(column_id))
        item = ColumnSearchAlias(
            column_id=int(column_id),
            label_kind=label_kind,
            label=label,
            normalized_label=normalize_search_text(label),
        )
    session.add(item)
    try:
        session.flush()
    except IntegrityError as exc:
        raise ValueError("Já existe um alias idêntico para essa entidade.") from exc
    return item


def get_alias_or_404(session: Session, *, entity_type: str, alias_id: int):
    if entity_type == "table":
        item = session.get(TableSearchAlias, alias_id)
    elif entity_type == "column":
        item = session.get(ColumnSearchAlias, alias_id)
    else:
        raise LookupError("Tipo de alias inválido.")
    if not item:
        raise LookupError("Alias não encontrado.")
    return item


def update_alias(session: Session, *, entity_type: str, alias_id: int, label_kind: str, label: str):
    item = get_alias_or_404(session, entity_type=entity_type, alias_id=alias_id)
    _, normalized_label_kind, trimmed_label = validate_alias_payload(
        entity_type=entity_type,
        label_kind=label_kind,
        label=label,
        table_id=getattr(item, "table_id", None),
        column_id=getattr(item, "column_id", None),
    )
    item.label_kind = normalized_label_kind
    item.label = trimmed_label
    item.normalized_label = normalize_search_text(trimmed_label)
    try:
        session.flush()
    except IntegrityError as exc:
        raise ValueError("Já existe um alias idêntico para essa entidade.") from exc
    return item


def delete_alias(session: Session, *, entity_type: str, alias_id: int) -> None:
    item = get_alias_or_404(session, entity_type=entity_type, alias_id=alias_id)
    session.delete(item)
    session.flush()
