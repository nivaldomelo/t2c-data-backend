"""add dashboard read model activity fields

Revision ID: f4a5b6c7d8e9
Revises: e2f3a4b5c6d7
Create Date: 2026-04-11 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "f4a5b6c7d8e9"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("dashboard_asset_read_model", schema=schema):
        return
    op.add_column(
        "dashboard_asset_read_model",
        sa.Column("search_clicks_30d", sa.Integer(), nullable=False, server_default="0"),
        schema=schema,
    )
    op.add_column(
        "dashboard_asset_read_model",
        sa.Column("active_dq_rules_count", sa.Integer(), nullable=False, server_default="0"),
        schema=schema,
    )
    op.add_column(
        "dashboard_asset_read_model",
        sa.Column("recent_dq_failure_runs_30d", sa.Integer(), nullable=False, server_default="0"),
        schema=schema,
    )
    op.alter_column("dashboard_asset_read_model", "search_clicks_30d", server_default=None, schema=schema)
    op.alter_column("dashboard_asset_read_model", "active_dq_rules_count", server_default=None, schema=schema)
    op.alter_column("dashboard_asset_read_model", "recent_dq_failure_runs_30d", server_default=None, schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("dashboard_asset_read_model", schema=schema):
        return
    op.drop_column("dashboard_asset_read_model", "recent_dq_failure_runs_30d", schema=schema)
    op.drop_column("dashboard_asset_read_model", "active_dq_rules_count", schema=schema)
    op.drop_column("dashboard_asset_read_model", "search_clicks_30d", schema=schema)
