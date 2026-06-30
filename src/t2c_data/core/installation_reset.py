from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from sqlalchemy import inspect
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.seed import ensure_installation_seed

DEFAULT_PRESERVED_TABLES = {"alembic_version"}


@dataclass(slots=True)
class InstallationResetReport:
    schema_name: str
    dialect: str
    preserved_tables: list[str] = field(default_factory=list)
    truncated_tables: list[str] = field(default_factory=list)
    seed_applied: bool = False
    bootstrap_admin_email: str = ""


def list_schema_tables(connection: Connection, schema_name: str) -> list[str]:
    inspector = inspect(connection)
    if connection.dialect.name == "sqlite":
        tables = inspector.get_table_names()
    else:
        tables = inspector.get_table_names(schema=schema_name)
    return sorted({table_name for table_name in tables if not table_name.startswith("sqlite_")})


def truncate_schema_tables(
    connection: Connection,
    schema_name: str,
    *,
    preserved_tables: Iterable[str] = DEFAULT_PRESERVED_TABLES,
) -> list[str]:
    preserved = {table_name for table_name in preserved_tables}
    table_names = [name for name in list_schema_tables(connection, schema_name) if name not in preserved]
    if not table_names:
        return []

    if connection.dialect.name == "sqlite":
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            for table_name in table_names:
                connection.exec_driver_sql(f'DELETE FROM "{table_name}"')
        finally:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        return table_names

    quoted_tables = ", ".join(f'"{schema_name}"."{table_name}"' for table_name in table_names)
    connection.exec_driver_sql(f"TRUNCATE TABLE {quoted_tables} RESTART IDENTITY CASCADE")
    return table_names


def reset_installation_state(
    session: Session,
    *,
    schema_name: str | None = None,
    preserved_tables: Iterable[str] = DEFAULT_PRESERVED_TABLES,
    include_viewer_seed: bool = False,
) -> InstallationResetReport:
    effective_schema = schema_name or settings.db_schema
    connection = session.connection()
    preserved = sorted({table_name for table_name in preserved_tables})
    truncated_tables = truncate_schema_tables(connection, effective_schema, preserved_tables=preserved)
    ensure_installation_seed(session, create_viewer=include_viewer_seed, commit=False)
    session.flush()
    return InstallationResetReport(
        schema_name=effective_schema,
        dialect=connection.dialect.name,
        preserved_tables=preserved,
        truncated_tables=truncated_tables,
        seed_applied=True,
        bootstrap_admin_email=settings.bootstrap_admin_email,
    )
