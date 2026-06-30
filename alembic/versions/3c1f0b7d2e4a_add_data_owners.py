"""add data owners

Revision ID: 3c1f0b7d2e4a
Revises: 7f3c9d1a4b22
Create Date: 2026-03-17 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "3c1f0b7d2e4a"
down_revision = "7f3c9d1a4b22"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_owners",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("area", sa.String(length=160), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email", name="uq_data_owners_email"),
        schema="t2c_data",
    )
    op.add_column("tables", sa.Column("data_owner_id", sa.Integer(), nullable=True), schema="t2c_data")
    op.create_foreign_key(
        "fk_tables_data_owner_id_data_owners",
        "tables",
        "data_owners",
        ["data_owner_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )
    op.create_index("ix_tables_data_owner_id", "tables", ["data_owner_id"], schema="t2c_data")


def downgrade() -> None:
    op.drop_index("ix_tables_data_owner_id", table_name="tables", schema="t2c_data")
    op.drop_constraint("fk_tables_data_owner_id_data_owners", "tables", schema="t2c_data", type_="foreignkey")
    op.drop_column("tables", "data_owner_id", schema="t2c_data")
    op.drop_table("data_owners", schema="t2c_data")
