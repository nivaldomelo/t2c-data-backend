"""add table volume snapshot metadata

Revision ID: e2f1a3b4c5d6
Revises: c7e8f9a0b1c2
Create Date: 2026-05-19 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "e2f1a3b4c5d6"
down_revision = "c7e8f9a0b1c2"
branch_labels = None
depends_on = None


SCHEMA = "controle"
TABLE = "table_row_count_snapshots"


def _column_names(bind) -> set[str]:
    inspector = inspect(bind)
    if not inspector.has_table(TABLE, schema=SCHEMA):
        return set()
    return {column["name"] for column in inspector.get_columns(TABLE, schema=SCHEMA)}


def _index_names(bind) -> set[str]:
    inspector = inspect(bind)
    if not inspector.has_table(TABLE, schema=SCHEMA):
        return set()
    return {index["name"] for index in inspector.get_indexes(TABLE, schema=SCHEMA)}


def _add_column_if_missing(bind, column_name: str, column: sa.Column) -> None:
    if column_name in _column_names(bind):
        return
    op.add_column(TABLE, column, schema=SCHEMA)


def _create_index_if_missing(bind, name: str, columns: list[str]) -> None:
    if name in _index_names(bind):
        return
    op.create_index(name, TABLE, columns, schema=SCHEMA)


def upgrade() -> None:
    bind = op.get_bind()
    if not inspect(bind).has_table(TABLE, schema=SCHEMA):
        op.create_table(
            TABLE,
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column("table_id", sa.BigInteger(), nullable=False),
            sa.Column("datasource_id", sa.BigInteger(), nullable=True),
            sa.Column("schema_id", sa.BigInteger(), nullable=True),
            sa.Column("connection_name", sa.String(length=255), nullable=True),
            sa.Column("database_name", sa.String(length=255), nullable=True),
            sa.Column("schema_name", sa.String(length=255), nullable=True),
            sa.Column("table_name", sa.String(length=255), nullable=True),
            sa.Column("fqn", sa.String(length=1000), nullable=True),
            sa.Column("row_count", sa.BigInteger(), nullable=True),
            sa.Column("measurement_type", sa.String(length=40), nullable=False, server_default="exact"),
            sa.Column("measurement_source", sa.String(length=40), nullable=False, server_default="unknown"),
            sa.Column("status", sa.String(length=20), nullable=False, server_default="success"),
            sa.Column("measured_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("duration_ms", sa.Integer(), nullable=True),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("collection_method", sa.String(length=40), nullable=True),
            sa.Column("collection_status", sa.String(length=20), nullable=True),
            sa.Column("snapshot_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("snapshot_date", sa.Date(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            schema=SCHEMA,
        )
    else:
        _add_column_if_missing(bind, "datasource_id", sa.Column("datasource_id", sa.BigInteger(), nullable=True))
        _add_column_if_missing(bind, "schema_id", sa.Column("schema_id", sa.BigInteger(), nullable=True))
        _add_column_if_missing(bind, "connection_name", sa.Column("connection_name", sa.String(length=255), nullable=True))
        _add_column_if_missing(bind, "database_name", sa.Column("database_name", sa.String(length=255), nullable=True))
        _add_column_if_missing(bind, "schema_name", sa.Column("schema_name", sa.String(length=255), nullable=True))
        _add_column_if_missing(bind, "table_name", sa.Column("table_name", sa.String(length=255), nullable=True))
        _add_column_if_missing(bind, "fqn", sa.Column("fqn", sa.String(length=1000), nullable=True))
        _add_column_if_missing(bind, "measurement_type", sa.Column("measurement_type", sa.String(length=40), nullable=False, server_default="exact"))
        _add_column_if_missing(bind, "measurement_source", sa.Column("measurement_source", sa.String(length=40), nullable=False, server_default="unknown"))
        _add_column_if_missing(bind, "status", sa.Column("status", sa.String(length=20), nullable=False, server_default="success"))
        _add_column_if_missing(bind, "measured_at", sa.Column("measured_at", sa.DateTime(timezone=True), nullable=True))
        _add_column_if_missing(bind, "duration_ms", sa.Column("duration_ms", sa.Integer(), nullable=True))
        _add_column_if_missing(bind, "error_message", sa.Column("error_message", sa.Text(), nullable=True))
        _add_column_if_missing(bind, "created_at", sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")))

    _create_index_if_missing(bind, "ix_table_row_count_snapshots_table_measured_at", ["table_id", "measured_at"])
    _create_index_if_missing(bind, "ix_table_row_count_snapshots_status", ["status"])
    _create_index_if_missing(bind, "ix_table_row_count_snapshots_datasource_schema_table", ["datasource_id", "schema_name", "table_name"])


def downgrade() -> None:
    bind = op.get_bind()
    if inspect(bind).has_table(TABLE, schema=SCHEMA):
        existing_indexes = _index_names(bind)
        for name in (
            "ix_table_row_count_snapshots_datasource_schema_table",
            "ix_table_row_count_snapshots_status",
            "ix_table_row_count_snapshots_table_measured_at",
        ):
            if name in existing_indexes:
                op.drop_index(name, table_name=TABLE, schema=SCHEMA)

        existing_columns = _column_names(bind)
        for column_name in (
            "created_at",
            "error_message",
            "duration_ms",
            "measured_at",
            "status",
            "measurement_source",
            "measurement_type",
            "fqn",
            "table_name",
            "schema_name",
            "database_name",
            "connection_name",
            "schema_id",
            "datasource_id",
        ):
            if column_name in existing_columns:
                op.drop_column(TABLE, column_name, schema=SCHEMA)
