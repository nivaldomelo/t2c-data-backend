"""add governance change management

Revision ID: c9d8e7f6a5b4
Revises: a0b1c2d3e4f5
Create Date: 2026-04-30 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "c9d8e7f6a5b4"
down_revision = "a0b1c2d3e4f5"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "asset_slas",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("asset_type", sa.String(length=40), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.Column("asset_name", sa.String(length=255), nullable=True),
        sa.Column("asset_fqn", sa.String(length=1000), nullable=True),
        sa.Column("sla_kind", sa.String(length=40), nullable=False, server_default="freshness"),
        sa.Column("sla_hours", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        sa.Column("source_kind", sa.String(length=40), nullable=False, server_default="manual"),
        sa.Column("source_ref", sa.String(length=255), nullable=True),
        sa.Column("context_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("reviewed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_type", "asset_id", "sla_kind", name="uq_asset_slas_asset_kind"),
        sa.ForeignKeyConstraint(["table_id"], [f"{SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["column_id"], [f"{SCHEMA}.columns.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["reviewed_by_user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        schema=SCHEMA,
    )
    op.create_index("ix_asset_slas_asset_type", "asset_slas", ["asset_type"], unique=False, schema=SCHEMA)
    op.create_index("ix_asset_slas_asset_id", "asset_slas", ["asset_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_asset_slas_status", "asset_slas", ["status"], unique=False, schema=SCHEMA)
    op.create_index("ix_asset_slas_table_id", "asset_slas", ["table_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_asset_slas_column_id", "asset_slas", ["column_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_asset_slas_source_kind", "asset_slas", ["source_kind"], unique=False, schema=SCHEMA)

    op.create_table(
        "metadata_change_requests",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("request_key", sa.String(length=255), nullable=False),
        sa.Column("asset_type", sa.String(length=40), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.Column("asset_name", sa.String(length=255), nullable=True),
        sa.Column("asset_fqn", sa.String(length=1000), nullable=True),
        sa.Column("change_kind", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="draft"),
        sa.Column("title", sa.String(length=240), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("requested_by_user_id", sa.Integer(), nullable=True),
        sa.Column("reviewed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("approved_by_user_id", sa.Integer(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("applied_by_user_id", sa.Integer(), nullable=True),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rejected_by_user_id", sa.Integer(), nullable=True),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("policy_rule_key", sa.String(length=120), nullable=True),
        sa.Column("recommendation_id", sa.Integer(), nullable=True),
        sa.Column("current_value_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("proposed_value_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("context_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("apply_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("request_key", name="uq_metadata_change_requests_request_key"),
        sa.ForeignKeyConstraint(["table_id"], [f"{SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["column_id"], [f"{SCHEMA}.columns.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["requested_by_user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["reviewed_by_user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["approved_by_user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["applied_by_user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["rejected_by_user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["recommendation_id"], [f"{SCHEMA}.governance_recommendations.id"], ondelete="SET NULL"),
        schema=SCHEMA,
    )
    op.create_index("ix_metadata_change_requests_status", "metadata_change_requests", ["status"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_metadata_change_requests_asset",
        "metadata_change_requests",
        ["asset_type", "asset_id"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index("ix_metadata_change_requests_table_id", "metadata_change_requests", ["table_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_metadata_change_requests_column_id", "metadata_change_requests", ["column_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_metadata_change_requests_change_kind", "metadata_change_requests", ["change_kind"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_metadata_change_requests_policy_rule_key",
        "metadata_change_requests",
        ["policy_rule_key"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_metadata_change_requests_recommendation_id",
        "metadata_change_requests",
        ["recommendation_id"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "metadata_change_request_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("metadata_change_request_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=30), nullable=False),
        sa.Column("previous_status", sa.String(length=20), nullable=True),
        sa.Column("next_status", sa.String(length=20), nullable=True),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["metadata_change_request_id"],
            [f"{SCHEMA}.metadata_change_requests.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["actor_user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_metadata_change_request_events_request_id",
        "metadata_change_request_events",
        ["metadata_change_request_id"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_metadata_change_request_events_event_type",
        "metadata_change_request_events",
        ["event_type"],
        unique=False,
        schema=SCHEMA,
    )

    op.add_column("columns", sa.Column("data_owner_id", sa.Integer(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("owner_reviewed_by_user_id", sa.Integer(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("owner_reviewed_at", sa.DateTime(timezone=True), nullable=True), schema=SCHEMA)
    op.create_foreign_key(
        "fk_columns_data_owner_id_data_owners",
        "columns",
        "data_owners",
        ["data_owner_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_columns_owner_reviewed_by_user_id_users",
        "columns",
        "users",
        ["owner_reviewed_by_user_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_columns_owner_reviewed_by_user_id_users", "columns", schema=SCHEMA, type_="foreignkey")
    op.drop_constraint("fk_columns_data_owner_id_data_owners", "columns", schema=SCHEMA, type_="foreignkey")
    op.drop_column("columns", "owner_reviewed_at", schema=SCHEMA)
    op.drop_column("columns", "owner_reviewed_by_user_id", schema=SCHEMA)
    op.drop_column("columns", "data_owner_id", schema=SCHEMA)

    op.drop_index("ix_metadata_change_request_events_event_type", table_name="metadata_change_request_events", schema=SCHEMA)
    op.drop_index("ix_metadata_change_request_events_request_id", table_name="metadata_change_request_events", schema=SCHEMA)
    op.drop_table("metadata_change_request_events", schema=SCHEMA)

    op.drop_index("ix_metadata_change_requests_recommendation_id", table_name="metadata_change_requests", schema=SCHEMA)
    op.drop_index("ix_metadata_change_requests_policy_rule_key", table_name="metadata_change_requests", schema=SCHEMA)
    op.drop_index("ix_metadata_change_requests_change_kind", table_name="metadata_change_requests", schema=SCHEMA)
    op.drop_index("ix_metadata_change_requests_column_id", table_name="metadata_change_requests", schema=SCHEMA)
    op.drop_index("ix_metadata_change_requests_table_id", table_name="metadata_change_requests", schema=SCHEMA)
    op.drop_index("ix_metadata_change_requests_asset", table_name="metadata_change_requests", schema=SCHEMA)
    op.drop_index("ix_metadata_change_requests_status", table_name="metadata_change_requests", schema=SCHEMA)
    op.drop_table("metadata_change_requests", schema=SCHEMA)

    op.drop_index("ix_asset_slas_source_kind", table_name="asset_slas", schema=SCHEMA)
    op.drop_index("ix_asset_slas_column_id", table_name="asset_slas", schema=SCHEMA)
    op.drop_index("ix_asset_slas_table_id", table_name="asset_slas", schema=SCHEMA)
    op.drop_index("ix_asset_slas_status", table_name="asset_slas", schema=SCHEMA)
    op.drop_index("ix_asset_slas_asset_id", table_name="asset_slas", schema=SCHEMA)
    op.drop_index("ix_asset_slas_asset_type", table_name="asset_slas", schema=SCHEMA)
    op.drop_table("asset_slas", schema=SCHEMA)
