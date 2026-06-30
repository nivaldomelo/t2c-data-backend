from __future__ import annotations

from typing import Any

from alembic import op
from sqlalchemy import Column, inspect


def column_exists(bind: Any, table_name: str, column_name: str, schema: str | None = None) -> bool:
    inspector = inspect(bind)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def table_exists(bind: Any, table_name: str, schema: str | None = None) -> bool:
    inspector = inspect(bind)
    return bool(inspector.has_table(table_name, schema=schema))


def index_exists(bind: Any, table_name: str, index_name: str, schema: str | None = None) -> bool:
    inspector = inspect(bind)
    indexes = inspector.get_indexes(table_name, schema=schema)
    return any(index.get("name") == index_name for index in indexes)


def safe_add_column(bind: Any, table_name: str, column: Column, *, schema: str | None = None) -> bool:
    if column_exists(bind, table_name, column.name, schema=schema):
        return False
    op.add_column(table_name, column, schema=schema)
    return True


def safe_create_table(bind: Any, table_name: str, *columns: Column, schema: str | None = None, **kwargs: Any) -> bool:
    if table_exists(bind, table_name, schema=schema):
        return False
    op.create_table(table_name, *columns, schema=schema, **kwargs)
    return True


def safe_create_index(
    bind: Any,
    index_name: str,
    table_name: str,
    columns: list[str] | tuple[str, ...],
    *,
    schema: str | None = None,
    unique: bool = False,
    **kwargs: Any,
) -> bool:
    if index_exists(bind, table_name, index_name, schema=schema):
        return False
    op.create_index(index_name, table_name, list(columns), schema=schema, unique=unique, **kwargs)
    return True
