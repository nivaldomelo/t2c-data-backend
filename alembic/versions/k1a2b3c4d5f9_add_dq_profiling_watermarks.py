"""add dq profiling watermarks (incremental full-then-delta profiling)

Revision ID: k1a2b3c4d5f9
Revises: j1a2b3c4d5f8
Create Date: 2026-06-25

Records each profiling execution per table so the engine can run the first
profiling as FULL and subsequent ones as a DELTA over (window_start, window_end].
The watermark only advances on success; failed runs keep their window for retry.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "k1a2b3c4d5f9"
down_revision = "j1a2b3c4d5f8"
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
    if not _has_table("dq_profiling_watermarks"):
        op.create_table(
            "dq_profiling_watermarks",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
            sa.Column("datasource_id", sa.Integer(), sa.ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True),
            sa.Column("dq_run_id", sa.Integer(), sa.ForeignKey("dq_runs.id", ondelete="SET NULL"), nullable=True),
            sa.Column("job_id", sa.Integer(), sa.ForeignKey("dq_job_runs.id", ondelete="SET NULL"), nullable=True),
            sa.Column("mode", sa.String(length=10), nullable=False, server_default=sa.text("'full'")),
            sa.Column("watermark_column", sa.String(length=255), nullable=True),
            sa.Column("window_start", sa.DateTime(timezone=True), nullable=True),
            sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
            sa.Column("rows_processed", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("status", sa.String(length=20), nullable=False, server_default=sa.text("'running'")),
            sa.Column("note", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            schema=SCHEMA,
        )
    _create_index_if_missing("ix_dq_profiling_watermarks_table_id", "dq_profiling_watermarks", ["table_id"])
    _create_index_if_missing("ix_dq_profiling_watermarks_datasource_id", "dq_profiling_watermarks", ["datasource_id"])
    _create_index_if_missing("ix_dq_profiling_watermarks_dq_run_id", "dq_profiling_watermarks", ["dq_run_id"])
    _create_index_if_missing("ix_dq_profiling_watermarks_job_id", "dq_profiling_watermarks", ["job_id"])
    _create_index_if_missing("ix_dq_profiling_watermarks_status", "dq_profiling_watermarks", ["status"])
    _create_index_if_missing(
        "ix_dq_profiling_watermarks_table_status_end",
        "dq_profiling_watermarks",
        ["table_id", "status", "window_end"],
    )


def downgrade() -> None:
    for index_name in [
        "ix_dq_profiling_watermarks_table_status_end",
        "ix_dq_profiling_watermarks_status",
        "ix_dq_profiling_watermarks_job_id",
        "ix_dq_profiling_watermarks_dq_run_id",
        "ix_dq_profiling_watermarks_datasource_id",
        "ix_dq_profiling_watermarks_table_id",
    ]:
        try:
            if _has_index("dq_profiling_watermarks", index_name):
                op.drop_index(index_name, table_name="dq_profiling_watermarks", schema=SCHEMA)
        except Exception:
            pass
    if _has_table("dq_profiling_watermarks"):
        op.drop_table("dq_profiling_watermarks", schema=SCHEMA)
