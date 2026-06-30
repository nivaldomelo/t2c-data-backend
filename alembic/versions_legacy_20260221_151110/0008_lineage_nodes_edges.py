"""add lineage nodes and edges tables

Revision ID: 0008_lineage_nodes_edges
Revises: 0007_glossary_description
Create Date: 2026-02-20 13:10:00

"""

from alembic import op
import sqlalchemy as sa


revision = "0008_lineage_nodes_edges"
down_revision = "0007_glossary_description"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "lineage_nodes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("lineage_table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("kind", sa.String(length=30), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=False),
        sa.Column("datasource_id", sa.Integer(), sa.ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="SET NULL"), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_lineage_nodes_lineage_table_id", "lineage_nodes", ["lineage_table_id"])
    op.create_index("ix_lineage_nodes_table_id", "lineage_nodes", ["table_id"])

    op.create_table(
        "lineage_graph_edges",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("lineage_table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("from_node_id", sa.Integer(), sa.ForeignKey("lineage_nodes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("to_node_id", sa.Integer(), sa.ForeignKey("lineage_nodes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("edge_type", sa.String(length=30), nullable=False),
        sa.Column("transform", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_lineage_graph_edges_lineage_table_id", "lineage_graph_edges", ["lineage_table_id"])


def downgrade() -> None:
    op.drop_index("ix_lineage_graph_edges_lineage_table_id", table_name="lineage_graph_edges")
    op.drop_table("lineage_graph_edges")
    op.drop_index("ix_lineage_nodes_table_id", table_name="lineage_nodes")
    op.drop_index("ix_lineage_nodes_lineage_table_id", table_name="lineage_nodes")
    op.drop_table("lineage_nodes")
