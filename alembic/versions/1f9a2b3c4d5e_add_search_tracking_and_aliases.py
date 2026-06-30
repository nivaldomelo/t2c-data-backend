"""add search tracking and aliases

Revision ID: 1f9a2b3c4d5e
Revises: d4b9e6a1c2f0
Create Date: 2026-03-27 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "1f9a2b3c4d5e"
down_revision = "d4b9e6a1c2f0"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "table_search_aliases",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("label_kind", sa.String(length=30), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=False),
        sa.Column("normalized_label", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["table_id"], [f"{SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("table_id", "label_kind", "label", name="uq_table_search_alias_label"),
        schema=SCHEMA,
    )
    op.create_index("ix_table_search_aliases_table_id", "table_search_aliases", ["table_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_table_search_aliases_table_kind", "table_search_aliases", ["table_id", "label_kind"], unique=False, schema=SCHEMA)
    op.create_index("ix_table_search_aliases_normalized", "table_search_aliases", ["normalized_label"], unique=False, schema=SCHEMA)

    op.create_table(
        "column_search_aliases",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("column_id", sa.Integer(), nullable=False),
        sa.Column("label_kind", sa.String(length=30), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=False),
        sa.Column("normalized_label", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["column_id"], [f"{SCHEMA}.columns.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("column_id", "label_kind", "label", name="uq_column_search_alias_label"),
        schema=SCHEMA,
    )
    op.create_index("ix_column_search_aliases_column_id", "column_search_aliases", ["column_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_column_search_aliases_column_kind", "column_search_aliases", ["column_id", "label_kind"], unique=False, schema=SCHEMA)
    op.create_index("ix_column_search_aliases_normalized", "column_search_aliases", ["normalized_label"], unique=False, schema=SCHEMA)

    op.create_table(
        "search_query_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("raw_query", sa.String(length=255), nullable=False),
        sa.Column("normalized_query", sa.String(length=255), nullable=False),
        sa.Column("search_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("last_searched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "normalized_query", name="uq_search_query_history_user_query"),
        schema=SCHEMA,
    )
    op.create_index("ix_search_query_history_user_id", "search_query_history", ["user_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_search_query_history_user_recent", "search_query_history", ["user_id", "last_searched_at"], unique=False, schema=SCHEMA)
    op.create_index("ix_search_query_history_normalized", "search_query_history", ["normalized_query"], unique=False, schema=SCHEMA)

    op.create_table(
        "search_result_clicks",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("query_text", sa.String(length=255), nullable=True),
        sa.Column("normalized_query", sa.String(length=255), nullable=True),
        sa.Column("target_url", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )
    op.create_index("ix_search_result_clicks_user_id", "search_result_clicks", ["user_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_search_result_clicks_entity", "search_result_clicks", ["entity_type", "entity_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_search_result_clicks_user_created", "search_result_clicks", ["user_id", "created_at"], unique=False, schema=SCHEMA)
    op.create_index("ix_search_result_clicks_query", "search_result_clicks", ["normalized_query"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_search_result_clicks_query", table_name="search_result_clicks", schema=SCHEMA)
    op.drop_index("ix_search_result_clicks_user_created", table_name="search_result_clicks", schema=SCHEMA)
    op.drop_index("ix_search_result_clicks_entity", table_name="search_result_clicks", schema=SCHEMA)
    op.drop_index("ix_search_result_clicks_user_id", table_name="search_result_clicks", schema=SCHEMA)
    op.drop_table("search_result_clicks", schema=SCHEMA)

    op.drop_index("ix_search_query_history_normalized", table_name="search_query_history", schema=SCHEMA)
    op.drop_index("ix_search_query_history_user_recent", table_name="search_query_history", schema=SCHEMA)
    op.drop_index("ix_search_query_history_user_id", table_name="search_query_history", schema=SCHEMA)
    op.drop_table("search_query_history", schema=SCHEMA)

    op.drop_index("ix_column_search_aliases_normalized", table_name="column_search_aliases", schema=SCHEMA)
    op.drop_index("ix_column_search_aliases_column_kind", table_name="column_search_aliases", schema=SCHEMA)
    op.drop_index("ix_column_search_aliases_column_id", table_name="column_search_aliases", schema=SCHEMA)
    op.drop_table("column_search_aliases", schema=SCHEMA)

    op.drop_index("ix_table_search_aliases_normalized", table_name="table_search_aliases", schema=SCHEMA)
    op.drop_index("ix_table_search_aliases_table_kind", table_name="table_search_aliases", schema=SCHEMA)
    op.drop_index("ix_table_search_aliases_table_id", table_name="table_search_aliases", schema=SCHEMA)
    op.drop_table("table_search_aliases", schema=SCHEMA)
