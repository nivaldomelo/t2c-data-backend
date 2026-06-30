from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from t2c_data.core.config import settings
from t2c_data.models import Base  # noqa: F401

# Alembic Config object
config = context.config

# Configure Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set DB URL from app settings
config.set_main_option("sqlalchemy.url", settings.database_url)

# Target metadata for 'autogenerate'
target_metadata = Base.metadata


def _include_object(object_, name, type_, reflected, compare_to):
    desired_schema = settings.db_schema or "public"
    schema = None

    if type_ == "table":
        schema = getattr(object_, "schema", None)
    elif type_ in {"index", "unique_constraint", "foreign_key_constraint", "primary_key_constraint"}:
        table = getattr(object_, "table", None)
        schema = getattr(table, "schema", None) if table is not None else None

    if schema is None:
        schema = desired_schema

    return schema == desired_schema


def _include_name(name, type_, parent_names):
    desired_schema = settings.db_schema or "public"
    if type_ == "schema":
        return name == desired_schema
    return True


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    In offline mode we emit SQL without connecting to the database. To make the generated
    script usable on a fresh cluster, we also emit the target schema creation before the
    migration statements.
    """
    url = config.get_main_option("sqlalchemy.url")
    desired_schema = settings.db_schema or "public"

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        include_schemas=True,
        include_name=_include_name,
        include_object=_include_object,
        version_table_schema=desired_schema,
        compare_type=True,
        # compare_server_default=True,  # opcional
    )

    with context.begin_transaction():
        context.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{desired_schema}"'))
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    Here we can execute SQL to ensure the schema exists and set the search_path so that
    migrations that create tables without explicit schema will land in desired_schema.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    desired_schema = settings.db_schema or "public"

    with connectable.connect() as connection:
        # Ensure schema exists
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{desired_schema}"'))
        connection.execute(text(f'SET search_path TO "{desired_schema}", public'))
        connection.commit()

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_name=_include_name,
            include_object=_include_object,
            version_table_schema=desired_schema,
            compare_type=True,
            # compare_server_default=True,  # opcional
        )

        with context.begin_transaction():
            context.run_migrations()

        connection.commit()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
