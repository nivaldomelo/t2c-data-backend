"""add scale indexes for operational list views

Revision ID: d2e3f4a5b6c7
Revises: ac1b2c3d4e5f
Create Date: 2026-05-15 00:20:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings

revision: str = "d2e3f4a5b6c7"
down_revision: Union[str, Sequence[str], None] = "ac1b2c3d4e5f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_index_names(bind, schema: str, table_name: str) -> set[str]:
    inspector = sa.inspect(bind)
    try:
        indexes = inspector.get_indexes(table_name, schema=schema)
    except Exception:  # noqa: BLE001
        return set()
    return {str(index["name"]) for index in indexes if index.get("name")}


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str], *, schema: str) -> None:
    bind = op.get_bind()
    existing_indexes = _existing_index_names(bind, schema, table_name)
    if index_name in existing_indexes:
        return
    op.create_index(index_name, table_name, columns, unique=False, schema=schema)


def upgrade() -> None:
    schema = settings.db_schema
    _create_index_if_missing("ix_data_owners_name", "data_owners", ["name"], schema=schema)
    _create_index_if_missing("ix_data_owners_is_active", "data_owners", ["is_active"], schema=schema)
    _create_index_if_missing("ix_tables_data_owner_id", "tables", ["data_owner_id"], schema=schema)
    _create_index_if_missing("ix_backup_executions_scope_started_at", "backup_executions", ["scope", "started_at"], schema=schema)
    _create_index_if_missing("ix_backup_executions_status_started_at", "backup_executions", ["status", "started_at"], schema=schema)
    _create_index_if_missing("ix_operational_failure_events_occurred_at", "operational_failure_events", ["occurred_at"], schema=schema)
    _create_index_if_missing(
        "ix_operational_failure_events_source_occurred_at",
        "operational_failure_events",
        ["source", "occurred_at"],
        schema=schema,
    )


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_operational_failure_events_source_occurred_at", table_name="operational_failure_events", schema=schema)
    op.drop_index("ix_operational_failure_events_occurred_at", table_name="operational_failure_events", schema=schema)
    op.drop_index("ix_backup_executions_status_started_at", table_name="backup_executions", schema=schema)
    op.drop_index("ix_backup_executions_scope_started_at", table_name="backup_executions", schema=schema)
    op.drop_index("ix_tables_data_owner_id", table_name="tables", schema=schema)
    op.drop_index("ix_data_owners_is_active", table_name="data_owners", schema=schema)
    op.drop_index("ix_data_owners_name", table_name="data_owners", schema=schema)
