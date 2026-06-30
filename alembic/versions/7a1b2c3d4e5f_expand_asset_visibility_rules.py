"""expand asset visibility rules

Revision ID: 7a1b2c3d4e5f
Revises: 6f0a1b2c3d4e
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "7a1b2c3d4e5f"
down_revision = "6f0a1b2c3d4e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.add_column(
        "asset_visibility_rules",
        sa.Column("rule_scope", sa.String(length=30), nullable=False, server_default="asset"),
        schema=schema,
    )
    op.add_column(
        "asset_visibility_rules",
        sa.Column("match_value", sa.String(length=255), nullable=True),
        schema=schema,
    )
    op.add_column(
        "asset_visibility_rules",
        sa.Column("mask_sensitive_fields", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema=schema,
    )
    op.alter_column("asset_visibility_rules", "entity_id", existing_type=sa.Integer(), nullable=True, schema=schema)
    op.create_index("ix_asset_visibility_rules_rule_scope", "asset_visibility_rules", ["rule_scope"], schema=schema)
    op.create_index("ix_asset_visibility_rules_match_value", "asset_visibility_rules", ["match_value"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_asset_visibility_rules_match_value", table_name="asset_visibility_rules", schema=schema)
    op.drop_index("ix_asset_visibility_rules_rule_scope", table_name="asset_visibility_rules", schema=schema)
    op.alter_column("asset_visibility_rules", "entity_id", existing_type=sa.Integer(), nullable=False, schema=schema)
    op.drop_column("asset_visibility_rules", "mask_sensitive_fields", schema=schema)
    op.drop_column("asset_visibility_rules", "match_value", schema=schema)
    op.drop_column("asset_visibility_rules", "rule_scope", schema=schema)
