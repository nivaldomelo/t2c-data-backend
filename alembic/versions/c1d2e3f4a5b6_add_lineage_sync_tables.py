"""add lineage sync tables

Revision ID: c1d2e3f4a5b6
Revises: ab39c8d4e2f1
Create Date: 2026-03-24 10:10:00.000000
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "c1d2e3f4a5b6"
down_revision: str | Sequence[str] | None = "ab39c8d4e2f1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lineage_source_configs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("source_type", sa.String(length=30), nullable=False),
        sa.Column("base_url", sa.String(length=500), nullable=False),
        sa.Column("default_namespace", sa.String(length=255), nullable=True),
        sa.Column("auth_type", sa.String(length=30), nullable=True),
        sa.Column("auth_username", sa.String(length=255), nullable=True),
        sa.Column("auth_secret", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("last_sync_at", sa.String(length=40), nullable=True),
        sa.Column("last_sync_status", sa.String(length=30), nullable=True),
        sa.Column("last_sync_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_lineage_source_configs_name"),
        schema="t2c_data",
    )
    op.create_index("ix_lineage_source_configs_enabled", "lineage_source_configs", ["enabled"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_source_configs_source_type", "lineage_source_configs", ["source_type"], unique=False, schema="t2c_data")

    op.create_table(
        "lineage_jobs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lineage_source_id", sa.Integer(), nullable=False),
        sa.Column("namespace", sa.String(length=255), nullable=False),
        sa.Column("job_name", sa.String(length=500), nullable=False),
        sa.Column("display_name", sa.String(length=500), nullable=False),
        sa.Column("job_type", sa.String(length=80), nullable=True),
        sa.Column("location", sa.String(length=500), nullable=True),
        sa.Column("latest_run_id", sa.String(length=255), nullable=True),
        sa.Column("latest_run_status", sa.String(length=80), nullable=True),
        sa.Column("latest_run_at", sa.String(length=40), nullable=True),
        sa.Column("raw_json", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["lineage_source_id"], ["t2c_data.lineage_source_configs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lineage_source_id", "namespace", "job_name", name="uq_lineage_jobs_source_namespace_job"),
        schema="t2c_data",
    )
    op.create_index("ix_lineage_jobs_is_active", "lineage_jobs", ["is_active"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_jobs_job_name", "lineage_jobs", ["job_name"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_jobs_lineage_source_id", "lineage_jobs", ["lineage_source_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_jobs_namespace", "lineage_jobs", ["namespace"], unique=False, schema="t2c_data")

    op.create_table(
        "lineage_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("lineage_job_id", sa.Integer(), nullable=False),
        sa.Column("external_run_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=80), nullable=True),
        sa.Column("started_at", sa.String(length=40), nullable=True),
        sa.Column("ended_at", sa.String(length=40), nullable=True),
        sa.Column("nominal_start_time", sa.String(length=40), nullable=True),
        sa.Column("raw_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["lineage_job_id"], ["t2c_data.lineage_jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lineage_job_id", "external_run_id", name="uq_lineage_runs_job_external_run"),
        schema="t2c_data",
    )
    op.create_index("ix_lineage_runs_external_run_id", "lineage_runs", ["external_run_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_runs_lineage_job_id", "lineage_runs", ["lineage_job_id"], unique=False, schema="t2c_data")

    op.add_column("lineage_assets", sa.Column("lineage_source_id", sa.Integer(), nullable=True), schema="t2c_data")
    op.add_column("lineage_assets", sa.Column("asset_origin", sa.String(length=30), nullable=False, server_default="manual"), schema="t2c_data")
    op.add_column("lineage_assets", sa.Column("external_node_id", sa.String(length=255), nullable=True), schema="t2c_data")
    op.add_column("lineage_assets", sa.Column("external_namespace", sa.String(length=255), nullable=True), schema="t2c_data")
    op.add_column("lineage_assets", sa.Column("external_name", sa.String(length=500), nullable=True), schema="t2c_data")
    op.add_column("lineage_assets", sa.Column("external_type", sa.String(length=30), nullable=True), schema="t2c_data")
    op.add_column("lineage_assets", sa.Column("aliases_text", sa.Text(), nullable=True), schema="t2c_data")
    op.create_foreign_key(
        "fk_lineage_assets_lineage_source_id",
        "lineage_assets",
        "lineage_source_configs",
        ["lineage_source_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )
    op.create_index("ix_lineage_assets_asset_origin", "lineage_assets", ["asset_origin"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_external_node_id", "lineage_assets", ["external_node_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_external_namespace", "lineage_assets", ["external_namespace"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_assets_lineage_source_id", "lineage_assets", ["lineage_source_id"], unique=False, schema="t2c_data")

    op.add_column("lineage_relations", sa.Column("lineage_source_id", sa.Integer(), nullable=True), schema="t2c_data")
    op.add_column("lineage_relations", sa.Column("lineage_job_id", sa.Integer(), nullable=True), schema="t2c_data")
    op.add_column("lineage_relations", sa.Column("external_edge_key", sa.String(length=500), nullable=True), schema="t2c_data")
    op.create_foreign_key(
        "fk_lineage_relations_lineage_source_id",
        "lineage_relations",
        "lineage_source_configs",
        ["lineage_source_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_lineage_relations_lineage_job_id",
        "lineage_relations",
        "lineage_jobs",
        ["lineage_job_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )
    op.create_index("ix_lineage_relations_external_edge_key", "lineage_relations", ["external_edge_key"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_relations_lineage_job_id", "lineage_relations", ["lineage_job_id"], unique=False, schema="t2c_data")
    op.create_index("ix_lineage_relations_lineage_source_id", "lineage_relations", ["lineage_source_id"], unique=False, schema="t2c_data")


def downgrade() -> None:
    op.drop_index("ix_lineage_relations_lineage_source_id", table_name="lineage_relations", schema="t2c_data")
    op.drop_index("ix_lineage_relations_lineage_job_id", table_name="lineage_relations", schema="t2c_data")
    op.drop_index("ix_lineage_relations_external_edge_key", table_name="lineage_relations", schema="t2c_data")
    op.drop_constraint("fk_lineage_relations_lineage_job_id", "lineage_relations", schema="t2c_data", type_="foreignkey")
    op.drop_constraint("fk_lineage_relations_lineage_source_id", "lineage_relations", schema="t2c_data", type_="foreignkey")
    op.drop_column("lineage_relations", "external_edge_key", schema="t2c_data")
    op.drop_column("lineage_relations", "lineage_job_id", schema="t2c_data")
    op.drop_column("lineage_relations", "lineage_source_id", schema="t2c_data")

    op.drop_index("ix_lineage_assets_lineage_source_id", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_external_namespace", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_external_node_id", table_name="lineage_assets", schema="t2c_data")
    op.drop_index("ix_lineage_assets_asset_origin", table_name="lineage_assets", schema="t2c_data")
    op.drop_constraint("fk_lineage_assets_lineage_source_id", "lineage_assets", schema="t2c_data", type_="foreignkey")
    op.drop_column("lineage_assets", "aliases_text", schema="t2c_data")
    op.drop_column("lineage_assets", "external_type", schema="t2c_data")
    op.drop_column("lineage_assets", "external_name", schema="t2c_data")
    op.drop_column("lineage_assets", "external_namespace", schema="t2c_data")
    op.drop_column("lineage_assets", "external_node_id", schema="t2c_data")
    op.drop_column("lineage_assets", "asset_origin", schema="t2c_data")
    op.drop_column("lineage_assets", "lineage_source_id", schema="t2c_data")

    op.drop_index("ix_lineage_runs_lineage_job_id", table_name="lineage_runs", schema="t2c_data")
    op.drop_index("ix_lineage_runs_external_run_id", table_name="lineage_runs", schema="t2c_data")
    op.drop_table("lineage_runs", schema="t2c_data")

    op.drop_index("ix_lineage_jobs_namespace", table_name="lineage_jobs", schema="t2c_data")
    op.drop_index("ix_lineage_jobs_lineage_source_id", table_name="lineage_jobs", schema="t2c_data")
    op.drop_index("ix_lineage_jobs_job_name", table_name="lineage_jobs", schema="t2c_data")
    op.drop_index("ix_lineage_jobs_is_active", table_name="lineage_jobs", schema="t2c_data")
    op.drop_table("lineage_jobs", schema="t2c_data")

    op.drop_index("ix_lineage_source_configs_source_type", table_name="lineage_source_configs", schema="t2c_data")
    op.drop_index("ix_lineage_source_configs_enabled", table_name="lineage_source_configs", schema="t2c_data")
    op.drop_table("lineage_source_configs", schema="t2c_data")
