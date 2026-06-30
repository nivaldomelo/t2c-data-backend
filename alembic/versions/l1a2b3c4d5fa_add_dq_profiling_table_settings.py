"""add dq profiling table settings (per-table start date + watermark override)

Revision ID: l1a2b3c4d5fa
Revises: k1a2b3c4d5f9
Create Date: 2026-06-25

For large tables, the first profiling can start from a configured date (floor)
instead of a full read; subsequent runs continue as delta. Also allows overriding
the auto-detected date/time (watermark) column per table.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "l1a2b3c4d5fa"
down_revision = "k1a2b3c4d5f9"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return _inspector().has_table(table_name, schema=SCHEMA)


def _has_index(table_name: str, index_name: str) -> bool:
    try:
        return any(index["name"] == index_name for index in _inspector().get_indexes(table_name, schema=SCHEMA))
    except Exception:
        return False


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    if not _has_index(table_name, index_name):
        op.create_index(index_name, table_name, columns, schema=SCHEMA)


def upgrade() -> None:
    if not _has_table("dq_profiling_table_settings"):
        op.create_table(
            "dq_profiling_table_settings",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
            sa.Column("start_date", sa.DateTime(timezone=True), nullable=True),
            sa.Column("watermark_column", sa.String(length=255), nullable=True),
            sa.Column("updated_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint("table_id", name="uq_dq_profiling_table_settings_table"),
            schema=SCHEMA,
        )
    _create_index_if_missing("ix_dq_profiling_table_settings_table_id", "dq_profiling_table_settings", ["table_id"])
    _create_index_if_missing(
        "ix_dq_profiling_table_settings_updated_by_user_id",
        "dq_profiling_table_settings",
        ["updated_by_user_id"],
    )


def downgrade() -> None:
    for index_name in [
        "ix_dq_profiling_table_settings_updated_by_user_id",
        "ix_dq_profiling_table_settings_table_id",
    ]:
        try:
            if _has_index("dq_profiling_table_settings", index_name):
                op.drop_index(index_name, table_name="dq_profiling_table_settings", schema=SCHEMA)
        except Exception:
            pass
    if _has_table("dq_profiling_table_settings"):
        op.drop_table("dq_profiling_table_settings", schema=SCHEMA)
