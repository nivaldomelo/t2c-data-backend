"""add lineage column edge details

Revision ID: d4e5f6a7b8c9
Revises: 0b1c2d3e4f5a
Create Date: 2026-04-09 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "d4e5f6a7b8c9"
down_revision = "0b1c2d3e4f5a"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column("lineage_column_edges", sa.Column("evidence_source", sa.String(length=40), nullable=True), schema=SCHEMA)
    op.add_column("lineage_column_edges", sa.Column("transform_expression", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("lineage_column_edges", sa.Column("notes", sa.Text(), nullable=True), schema=SCHEMA)
    op.create_index("ix_lineage_column_edges_evidence_source", "lineage_column_edges", ["evidence_source"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_lineage_column_edges_evidence_source", table_name="lineage_column_edges", schema=SCHEMA)
    op.drop_column("lineage_column_edges", "notes", schema=SCHEMA)
    op.drop_column("lineage_column_edges", "transform_expression", schema=SCHEMA)
    op.drop_column("lineage_column_edges", "evidence_source", schema=SCHEMA)
