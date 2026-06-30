"""add lineage versioning and confidence fields

Revision ID: h1a2b3c4d5f6
Revises: g1a2b3c4d5e6
Create Date: 2026-05-28 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.exc import NoSuchTableError


revision = "h1a2b3c4d5f6"
down_revision = "g1a2b3c4d5e6"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return _inspector().has_table(table_name, schema=SCHEMA)


def _has_column(table_name: str, column_name: str) -> bool:
    inspector = _inspector()
    try:
        return any(column["name"] == column_name for column in inspector.get_columns(table_name, schema=SCHEMA))
    except NoSuchTableError:
        return False


def _has_index(table_name: str, index_name: str) -> bool:
    inspector = _inspector()
    try:
        return any(index["name"] == index_name for index in inspector.get_indexes(table_name, schema=SCHEMA))
    except NoSuchTableError:
        return False


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if not _has_column(table_name, column.name):
        op.add_column(table_name, column, schema=SCHEMA)


def _drop_column_if_exists(table_name: str, column_name: str) -> None:
    if _has_column(table_name, column_name):
        op.drop_column(table_name, column_name, schema=SCHEMA)


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    if not _has_index(table_name, index_name):
        op.create_index(index_name, table_name, columns, schema=SCHEMA)


def _drop_index_if_exists(index_name: str, table_name: str) -> None:
    if _has_index(table_name, index_name):
        op.drop_index(index_name, table_name=table_name, schema=SCHEMA)


def upgrade() -> None:
    _add_column_if_missing("lineage_relations", sa.Column("evidence", sa.Text(), nullable=True))
    _add_column_if_missing(
        "lineage_relations",
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
    )
    _add_column_if_missing(
        "lineage_relations",
        sa.Column("is_verified", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    _add_column_if_missing("lineage_relations", sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True))
    _create_index_if_missing("ix_lineage_relations_version", "lineage_relations", ["version"])
    _create_index_if_missing("ix_lineage_relations_is_verified", "lineage_relations", ["is_verified"])
    _create_index_if_missing("ix_lineage_relations_last_seen_at", "lineage_relations", ["last_seen_at"])

    _add_column_if_missing("lineage_column_edges", sa.Column("evidence", sa.Text(), nullable=True))
    _add_column_if_missing(
        "lineage_column_edges",
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
    )
    _add_column_if_missing(
        "lineage_column_edges",
        sa.Column("is_verified", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    _add_column_if_missing("lineage_column_edges", sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True))
    _add_column_if_missing("lineage_column_edges", sa.Column("created_by_user_id", sa.Integer(), nullable=True))
    _add_column_if_missing("lineage_column_edges", sa.Column("updated_by_user_id", sa.Integer(), nullable=True))
    _create_index_if_missing("ix_lineage_column_edges_version", "lineage_column_edges", ["version"])
    _create_index_if_missing("ix_lineage_column_edges_is_verified", "lineage_column_edges", ["is_verified"])
    _create_index_if_missing("ix_lineage_column_edges_last_seen_at", "lineage_column_edges", ["last_seen_at"])

    if not _has_table("lineage_relation_versions"):
        op.create_table(
            "lineage_relation_versions",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("lineage_relation_id", sa.Integer(), sa.ForeignKey("lineage_relations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("version_number", sa.Integer(), nullable=False),
            sa.Column("source_asset_id", sa.Integer(), sa.ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False),
            sa.Column("target_asset_id", sa.Integer(), sa.ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False),
            sa.Column("relation_type", sa.String(length=30), nullable=False),
            sa.Column("process_name", sa.String(length=255), nullable=True),
            sa.Column("process_type", sa.String(length=50), nullable=True),
            sa.Column("dashboard_name", sa.String(length=255), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("evidence", sa.Text(), nullable=True),
            sa.Column("discovery_method", sa.String(length=30), nullable=False),
            sa.Column("confidence_score", sa.Integer(), nullable=False, server_default=sa.text("100")),
            sa.Column("is_verified", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("external_edge_key", sa.String(length=500), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
            sa.Column("snapshot_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.Column("recorded_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint("lineage_relation_id", "version_number", name="uq_lineage_relation_versions_relation_version"),
            schema=SCHEMA,
        )
    _create_index_if_missing(
        "ix_lineage_relation_versions_lineage_relation_id",
        "lineage_relation_versions",
        ["lineage_relation_id"],
    )
    _create_index_if_missing(
        "ix_lineage_relation_versions_version_number",
        "lineage_relation_versions",
        ["version_number"],
    )

    if not _has_table("lineage_column_edge_versions"):
        op.create_table(
            "lineage_column_edge_versions",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("lineage_column_edge_id", sa.Integer(), sa.ForeignKey("lineage_column_edges.id", ondelete="CASCADE"), nullable=False),
            sa.Column("version_number", sa.Integer(), nullable=False),
            sa.Column("lineage_source_id", sa.Integer(), sa.ForeignKey("lineage_source_configs.id", ondelete="SET NULL"), nullable=True),
            sa.Column("lineage_job_id", sa.Integer(), sa.ForeignKey("lineage_jobs.id", ondelete="SET NULL"), nullable=True),
            sa.Column("source_asset_id", sa.Integer(), sa.ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False),
            sa.Column("target_asset_id", sa.Integer(), sa.ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False),
            sa.Column("source_column_name", sa.String(length=255), nullable=False),
            sa.Column("target_column_name", sa.String(length=255), nullable=False),
            sa.Column("relation_type", sa.String(length=30), nullable=False),
            sa.Column("discovery_method", sa.String(length=30), nullable=False),
            sa.Column("confidence_score", sa.Integer(), nullable=False, server_default=sa.text("100")),
            sa.Column("evidence_source", sa.String(length=40), nullable=True),
            sa.Column("evidence", sa.Text(), nullable=True),
            sa.Column("transform_expression", sa.Text(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("external_edge_key", sa.String(length=500), nullable=True),
            sa.Column("is_verified", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
            sa.Column("snapshot_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.Column("recorded_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint("lineage_column_edge_id", "version_number", name="uq_lineage_column_edge_versions_edge_version"),
            schema=SCHEMA,
        )
    _create_index_if_missing(
        "ix_lineage_column_edge_versions_lineage_column_edge_id",
        "lineage_column_edge_versions",
        ["lineage_column_edge_id"],
    )
    _create_index_if_missing(
        "ix_lineage_column_edge_versions_version_number",
        "lineage_column_edge_versions",
        ["version_number"],
    )


def downgrade() -> None:
    _drop_index_if_exists("ix_lineage_column_edge_versions_version_number", "lineage_column_edge_versions")
    _drop_index_if_exists("ix_lineage_column_edge_versions_lineage_column_edge_id", "lineage_column_edge_versions")
    if _has_table("lineage_column_edge_versions"):
        op.drop_table("lineage_column_edge_versions", schema=SCHEMA)

    _drop_index_if_exists("ix_lineage_relation_versions_version_number", "lineage_relation_versions")
    _drop_index_if_exists("ix_lineage_relation_versions_lineage_relation_id", "lineage_relation_versions")
    if _has_table("lineage_relation_versions"):
        op.drop_table("lineage_relation_versions", schema=SCHEMA)

    _drop_index_if_exists("ix_lineage_column_edges_last_seen_at", "lineage_column_edges")
    _drop_index_if_exists("ix_lineage_column_edges_is_verified", "lineage_column_edges")
    _drop_index_if_exists("ix_lineage_column_edges_version", "lineage_column_edges")
    _drop_column_if_exists("lineage_column_edges", "updated_by_user_id")
    _drop_column_if_exists("lineage_column_edges", "created_by_user_id")
    _drop_column_if_exists("lineage_column_edges", "last_seen_at")
    _drop_column_if_exists("lineage_column_edges", "is_verified")
    _drop_column_if_exists("lineage_column_edges", "version")
    _drop_column_if_exists("lineage_column_edges", "evidence")

    _drop_index_if_exists("ix_lineage_relations_last_seen_at", "lineage_relations")
    _drop_index_if_exists("ix_lineage_relations_is_verified", "lineage_relations")
    _drop_index_if_exists("ix_lineage_relations_version", "lineage_relations")
    _drop_column_if_exists("lineage_relations", "last_seen_at")
    _drop_column_if_exists("lineage_relations", "is_verified")
    _drop_column_if_exists("lineage_relations", "version")
    _drop_column_if_exists("lineage_relations", "evidence")
