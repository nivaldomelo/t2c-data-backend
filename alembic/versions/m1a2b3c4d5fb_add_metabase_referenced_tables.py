"""add metabase_objects.referenced_tables_json

Revision ID: m1a2b3c4d5fb
Revises: l1a2b3c4d5fa
Create Date: 2026-06-26

Stores the tables each Metabase artifact (question/dashboard) uses, already
resolved to ``schema.table`` names. Structured (MBQL) queries reference numeric
Metabase ``source-table`` ids; these are resolved against the database metadata
at sync time so the listing can show real table names instead of opaque ids.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "m1a2b3c4d5fb"
down_revision = "l1a2b3c4d5fa"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_column(table_name: str, column_name: str) -> bool:
    try:
        return any(col["name"] == column_name for col in _inspector().get_columns(table_name, schema=SCHEMA))
    except Exception:
        return False


def upgrade() -> None:
    if not _has_column("metabase_objects", "referenced_tables_json"):
        op.add_column(
            "metabase_objects",
            sa.Column("referenced_tables_json", sa.JSON(), nullable=True),
            schema=SCHEMA,
        )


def downgrade() -> None:
    if _has_column("metabase_objects", "referenced_tables_json"):
        op.drop_column("metabase_objects", "referenced_tables_json", schema=SCHEMA)
