"""add schema scope support to dq_runs

Revision ID: 7f3c9d1a4b22
Revises: 2f1c4be8aa10
Create Date: 2026-02-24 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7f3c9d1a4b22"
down_revision = "2f1c4be8aa10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("dq_runs", "datasource_id", schema="t2c_data", existing_type=sa.Integer(), nullable=True)
    op.alter_column("dq_runs", "table_id", schema="t2c_data", existing_type=sa.Integer(), nullable=True)

    op.add_column("dq_runs", sa.Column("scope", sa.String(length=20), nullable=True), schema="t2c_data")
    op.add_column("dq_runs", sa.Column("schema_name", sa.String(length=255), nullable=True), schema="t2c_data")
    op.add_column("dq_runs", sa.Column("parent_run_id", sa.BigInteger(), nullable=True), schema="t2c_data")

    op.execute("UPDATE t2c_data.dq_runs SET scope = 'table' WHERE scope IS NULL")
    op.alter_column(
        "dq_runs",
        "scope",
        schema="t2c_data",
        existing_type=sa.String(length=20),
        nullable=False,
        server_default="table",
    )

    op.create_index("ix_t2c_data_dq_runs_scope", "dq_runs", ["scope"], unique=False, schema="t2c_data")
    op.create_index("ix_t2c_data_dq_runs_schema_name", "dq_runs", ["schema_name"], unique=False, schema="t2c_data")
    op.create_index("ix_t2c_data_dq_runs_parent_run_id", "dq_runs", ["parent_run_id"], unique=False, schema="t2c_data")
    op.create_foreign_key(
        "fk_t2c_data_dq_runs_parent_run_id",
        "dq_runs",
        "dq_runs",
        ["parent_run_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint("fk_t2c_data_dq_runs_parent_run_id", "dq_runs", schema="t2c_data", type_="foreignkey")
    op.drop_index("ix_t2c_data_dq_runs_parent_run_id", table_name="dq_runs", schema="t2c_data")
    op.drop_index("ix_t2c_data_dq_runs_schema_name", table_name="dq_runs", schema="t2c_data")
    op.drop_index("ix_t2c_data_dq_runs_scope", table_name="dq_runs", schema="t2c_data")
    op.drop_column("dq_runs", "parent_run_id", schema="t2c_data")
    op.drop_column("dq_runs", "schema_name", schema="t2c_data")
    op.drop_column("dq_runs", "scope", schema="t2c_data")
    op.alter_column("dq_runs", "table_id", schema="t2c_data", existing_type=sa.Integer(), nullable=False)
    op.alter_column("dq_runs", "datasource_id", schema="t2c_data", existing_type=sa.Integer(), nullable=False)

