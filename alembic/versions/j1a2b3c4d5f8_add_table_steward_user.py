"""add steward_user_id to tables

Revision ID: j1a2b3c4d5f8
Revises: i1a2b3c4d5f7
Create Date: 2026-06-24

Adds a per-asset steward (FK to users), parallel to the data owner, so each table
can have a steward responsible for curation. Nullable; ON DELETE SET NULL.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "j1a2b3c4d5f8"
down_revision = "i1a2b3c4d5f7"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _inspector():
    return sa.inspect(op.get_bind())


def _has_column(table_name: str, column_name: str) -> bool:
    return any(column["name"] == column_name for column in _inspector().get_columns(table_name, schema=SCHEMA))


def _has_index(table_name: str, index_name: str) -> bool:
    return any(index["name"] == index_name for index in _inspector().get_indexes(table_name, schema=SCHEMA))


def upgrade() -> None:
    if not _has_column("tables", "steward_user_id"):
        op.add_column(
            "tables",
            sa.Column(
                "steward_user_id",
                sa.Integer(),
                sa.ForeignKey(f"{SCHEMA}.users.id", ondelete="SET NULL"),
                nullable=True,
            ),
            schema=SCHEMA,
        )
    if not _has_index("tables", "ix_tables_steward_user_id"):
        op.create_index("ix_tables_steward_user_id", "tables", ["steward_user_id"], schema=SCHEMA)


def downgrade() -> None:
    if _has_index("tables", "ix_tables_steward_user_id"):
        op.drop_index("ix_tables_steward_user_id", table_name="tables", schema=SCHEMA)
    if _has_column("tables", "steward_user_id"):
        op.drop_column("tables", "steward_user_id", schema=SCHEMA)
