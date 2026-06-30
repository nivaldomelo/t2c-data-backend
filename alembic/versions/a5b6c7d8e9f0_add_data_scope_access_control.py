"""add data scope access control

Revision ID: a5b6c7d8e9f0
Revises: a4b5c6d7e8f9
Create Date: 2026-04-08 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a5b6c7d8e9f0"
down_revision = "a4b5c6d7e8f9"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_table(
        "access_groups",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("name", name="uq_access_groups_name"),
        schema=SCHEMA,
    )

    op.create_table(
        "user_access_groups",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["group_id"], [f"{SCHEMA}.access_groups.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "group_id"),
        schema=SCHEMA,
    )

    op.create_table(
        "data_access_grants",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.Column("effect", sa.String(length=10), nullable=False),
        sa.Column("datasource_id", sa.Integer(), nullable=True),
        sa.Column("schema_id", sa.Integer(), nullable=True),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("note", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint(
            "(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN group_id IS NOT NULL THEN 1 ELSE 0 END) = 1",
            name="ck_data_access_grants_principal",
        ),
        sa.CheckConstraint(
            "(CASE WHEN datasource_id IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN schema_id IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN table_id IS NOT NULL THEN 1 ELSE 0 END) = 1",
            name="ck_data_access_grants_scope",
        ),
        sa.CheckConstraint("effect IN ('allow', 'deny')", name="ck_data_access_grants_effect"),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["group_id"], [f"{SCHEMA}.access_groups.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["datasource_id"], [f"{SCHEMA}.data_sources.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["schema_id"], [f"{SCHEMA}.schemas.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], [f"{SCHEMA}.tables.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "user_id",
            "group_id",
            "datasource_id",
            "schema_id",
            "table_id",
            "effect",
            name="uq_data_access_grants_unique",
        ),
        schema=SCHEMA,
    )

    op.create_index("ix_t2c_data_access_grants_user_id", "data_access_grants", ["user_id"], schema=SCHEMA)
    op.create_index("ix_t2c_data_access_grants_group_id", "data_access_grants", ["group_id"], schema=SCHEMA)
    op.create_index("ix_t2c_data_access_grants_datasource_id", "data_access_grants", ["datasource_id"], schema=SCHEMA)
    op.create_index("ix_t2c_data_access_grants_schema_id", "data_access_grants", ["schema_id"], schema=SCHEMA)
    op.create_index("ix_t2c_data_access_grants_table_id", "data_access_grants", ["table_id"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_t2c_data_access_grants_table_id", table_name="data_access_grants", schema=SCHEMA)
    op.drop_index("ix_t2c_data_access_grants_schema_id", table_name="data_access_grants", schema=SCHEMA)
    op.drop_index("ix_t2c_data_access_grants_datasource_id", table_name="data_access_grants", schema=SCHEMA)
    op.drop_index("ix_t2c_data_access_grants_group_id", table_name="data_access_grants", schema=SCHEMA)
    op.drop_index("ix_t2c_data_access_grants_user_id", table_name="data_access_grants", schema=SCHEMA)
    op.drop_table("data_access_grants", schema=SCHEMA)
    op.drop_table("user_access_groups", schema=SCHEMA)
    op.drop_table("access_groups", schema=SCHEMA)

