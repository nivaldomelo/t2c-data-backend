from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from sqlalchemy import func, inspect, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.models.auth import User

DEFAULT_PRESERVED_TABLES = {
    "alembic_version",
    "access_groups",
    "permissions",
    "platform_api_keys",
    "role_permissions",
    "roles",
    "user_access_groups",
    "user_role",
    "user_sessions",
    "users",
}

REQUIRED_PRESERVED_TABLES = {
    "alembic_version",
    "permissions",
    "role_permissions",
    "roles",
    "user_role",
    "users",
}


@dataclass(slots=True)
class PlatformDataResetPlan:
    schema_name: str
    dialect: str
    available_tables: list[str] = field(default_factory=list)
    preserved_tables: list[str] = field(default_factory=list)
    missing_preserved_tables: list[str] = field(default_factory=list)
    truncated_tables: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PlatformDataResetValidation:
    users_total: int
    active_users_total: int
    admin_exists: bool
    preserved_counts: dict[str, int] = field(default_factory=dict)
    truncated_non_empty: list[str] = field(default_factory=list)


def _qualified_table_name(schema_name: str | None, table_name: str, dialect_name: str) -> str:
    if dialect_name == "sqlite":
        return f'"{table_name}"'
    if schema_name:
        return f'"{schema_name}"."{table_name}"'
    return f'"{table_name}"'


def list_schema_tables(connection: Connection, schema_name: str | None) -> list[str]:
    inspector = inspect(connection)
    if connection.dialect.name == "sqlite":
        tables = inspector.get_table_names()
    else:
        tables = inspector.get_table_names(schema=schema_name)
    return sorted({table_name for table_name in tables if not table_name.startswith("sqlite_")})


def build_reset_plan(
    connection: Connection,
    schema_name: str | None,
    *,
    preserved_tables: Iterable[str] = DEFAULT_PRESERVED_TABLES,
) -> PlatformDataResetPlan:
    available_tables = list_schema_tables(connection, schema_name)
    available_set = set(available_tables)
    preserve_set = {table_name for table_name in preserved_tables}
    preserved = sorted(table_name for table_name in available_tables if table_name in preserve_set)
    missing = sorted(table_name for table_name in preserve_set if table_name not in available_set)
    truncated = sorted(table_name for table_name in available_tables if table_name not in preserve_set)
    return PlatformDataResetPlan(
        schema_name=schema_name or settings.db_schema,
        dialect=connection.dialect.name,
        available_tables=available_tables,
        preserved_tables=preserved,
        missing_preserved_tables=missing,
        truncated_tables=truncated,
    )


def truncate_tables(
    connection: Connection,
    schema_name: str | None,
    table_names: Iterable[str],
) -> list[str]:
    names = sorted({table_name for table_name in table_names})
    if not names:
        return []

    if connection.dialect.name == "sqlite":
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            for table_name in names:
                connection.exec_driver_sql(f"DELETE FROM {_qualified_table_name(None, table_name, 'sqlite')}")
        finally:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        return names

    quoted_tables = ", ".join(_qualified_table_name(schema_name, table_name, connection.dialect.name) for table_name in names)
    connection.exec_driver_sql(f"TRUNCATE TABLE {quoted_tables} RESTART IDENTITY CASCADE")
    return names


def count_table_rows(connection: Connection, schema_name: str | None, table_name: str) -> int:
    qualified = _qualified_table_name(schema_name, table_name, connection.dialect.name)
    return int(connection.execute(text(f"SELECT COUNT(*) FROM {qualified}")).scalar_one())


def build_table_row_counts(connection: Connection, schema_name: str | None, table_names: Iterable[str]) -> dict[str, int]:
    return {table_name: count_table_rows(connection, schema_name, table_name) for table_name in table_names}


def validate_reset_plan(plan: PlatformDataResetPlan) -> None:
    missing_required = [table_name for table_name in REQUIRED_PRESERVED_TABLES if table_name in plan.missing_preserved_tables]
    if missing_required:
        missing_list = ", ".join(missing_required)
        raise RuntimeError(
            "Missing required authentication tables in schema "
            f"{plan.schema_name!r}: {missing_list}. "
            "Reset aborted to avoid breaking login."
        )


def validate_post_reset_state(
    session: Session,
    schema_name: str,
    truncated_tables: Iterable[str],
    *,
    admin_email: str | None = None,
) -> PlatformDataResetValidation:
    available_tables = set(list_schema_tables(session.connection(), schema_name))
    users_total = int(session.scalar(select(func.count(User.id))) or 0)
    active_users_total = int(session.scalar(select(func.count(User.id)).where(User.is_active.is_(True))) or 0)
    target_admin_email = (admin_email or settings.bootstrap_admin_email).strip().lower()
    admin_exists = (
        session.scalar(
            select(func.count(User.id)).where(
                func.lower(User.email) == target_admin_email,
                User.is_active.is_(True),
            )
        )
        or 0
    ) > 0

    preserved_tables = [
        "users",
        "roles",
        "permissions",
        "user_role",
        "role_permissions",
        "user_sessions",
        "access_groups",
        "user_access_groups",
        "platform_api_keys",
        "alembic_version",
    ]
    preserved_counts = {
        table_name: count_table_rows(session.connection(), schema_name, table_name)
        for table_name in preserved_tables
        if table_name in available_tables
    }
    truncated_non_empty = [
        table_name
        for table_name in truncated_tables
        if table_name in available_tables and count_table_rows(session.connection(), schema_name, table_name) > 0
    ]

    if users_total < 1:
        raise RuntimeError("Reset validation failed: no users remain after truncation.")
    if not admin_exists:
        raise RuntimeError(
            "Reset validation failed: bootstrap admin user is missing or inactive "
            f"({target_admin_email})."
        )
    if truncated_non_empty:
        non_empty = ", ".join(truncated_non_empty)
        raise RuntimeError(f"Reset validation failed: truncated tables still contain rows: {non_empty}.")

    return PlatformDataResetValidation(
        users_total=users_total,
        active_users_total=active_users_total,
        admin_exists=admin_exists,
        preserved_counts=preserved_counts,
        truncated_non_empty=truncated_non_empty,
    )
