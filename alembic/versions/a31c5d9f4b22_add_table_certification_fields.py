"""add table certification fields

Revision ID: a31c5d9f4b22
Revises: 9b7e3c2d1f44
Create Date: 2026-03-21 15:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a31c5d9f4b22"
down_revision = "9b7e3c2d1f44"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column("tables", sa.Column("certification_status", sa.String(length=40), nullable=False, server_default="not_assessed"), schema=SCHEMA)
    op.add_column("tables", sa.Column("certification_criticality", sa.String(length=20), nullable=True), schema=SCHEMA)
    op.add_column("tables", sa.Column("certification_badges", sa.JSON(), nullable=True), schema=SCHEMA)
    op.add_column("tables", sa.Column("certification_notes", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("tables", sa.Column("certification_decided_by_user_id", sa.Integer(), nullable=True), schema=SCHEMA)
    op.add_column("tables", sa.Column("certification_decided_at", sa.DateTime(timezone=True), nullable=True), schema=SCHEMA)
    op.add_column("tables", sa.Column("certification_review_at", sa.DateTime(timezone=True), nullable=True), schema=SCHEMA)
    op.create_foreign_key(
        "fk_tables_certification_decided_by_user_id_users",
        "tables",
        "users",
        ["certification_decided_by_user_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )
    op.create_index("ix_tables_certification_status", "tables", ["certification_status"], schema=SCHEMA)
    op.create_index("ix_tables_certification_criticality", "tables", ["certification_criticality"], schema=SCHEMA)
    op.create_index("ix_tables_certification_decided_by_user_id", "tables", ["certification_decided_by_user_id"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_tables_certification_decided_by_user_id", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_certification_criticality", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_certification_status", table_name="tables", schema=SCHEMA)
    op.drop_constraint("fk_tables_certification_decided_by_user_id_users", "tables", schema=SCHEMA, type_="foreignkey")
    op.drop_column("tables", "certification_review_at", schema=SCHEMA)
    op.drop_column("tables", "certification_decided_at", schema=SCHEMA)
    op.drop_column("tables", "certification_decided_by_user_id", schema=SCHEMA)
    op.drop_column("tables", "certification_notes", schema=SCHEMA)
    op.drop_column("tables", "certification_badges", schema=SCHEMA)
    op.drop_column("tables", "certification_criticality", schema=SCHEMA)
    op.drop_column("tables", "certification_status", schema=SCHEMA)
