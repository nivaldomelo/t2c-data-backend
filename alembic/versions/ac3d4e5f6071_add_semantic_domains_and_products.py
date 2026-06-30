"""add semantic domains and data products

Revision ID: ac3d4e5f6071
Revises: ab2c3d4e5f60
Create Date: 2026-04-17 18:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "ac3d4e5f6071"
down_revision = "ab2c3d4e5f60"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    op.create_table(
        "semantic_domains",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.String(length=160), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("owner", sa.String(length=160), nullable=True),
        sa.Column("steward", sa.String(length=160), nullable=True),
        sa.Column("criticality", sa.String(length=30), nullable=True),
        sa.Column("maturity_status", sa.String(length=40), server_default="emerging", nullable=False),
        sa.Column("quality_score", sa.Integer(), nullable=True),
        sa.Column("governance_score", sa.Integer(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_semantic_domains_slug"),
        sa.UniqueConstraint("name", name="uq_semantic_domains_name"),
        schema=schema,
    )
    op.create_index("ix_semantic_domains_criticality", "semantic_domains", ["criticality"], schema=schema)
    op.create_index("ix_semantic_domains_maturity_status", "semantic_domains", ["maturity_status"], schema=schema)

    op.create_table(
        "semantic_data_products",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.String(length=160), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("owner", sa.String(length=160), nullable=True),
        sa.Column("steward", sa.String(length=160), nullable=True),
        sa.Column("consumers", sa.JSON(), nullable=True),
        sa.Column("sla_text", sa.Text(), nullable=True),
        sa.Column("contract_text", sa.Text(), nullable=True),
        sa.Column("maturity_status", sa.String(length=40), server_default="emerging", nullable=False),
        sa.Column("quality_score", sa.Integer(), nullable=True),
        sa.Column("governance_score", sa.Integer(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], [f"{schema}.semantic_domains.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("domain_id", "slug", name="uq_semantic_data_products_domain_slug"),
        sa.UniqueConstraint("domain_id", "name", name="uq_semantic_data_products_domain_name"),
        schema=schema,
    )
    op.create_index("ix_semantic_data_products_domain_id", "semantic_data_products", ["domain_id"], schema=schema)
    op.create_index("ix_semantic_data_products_maturity_status", "semantic_data_products", ["maturity_status"], schema=schema)

    op.create_table(
        "semantic_links",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=True),
        sa.Column("product_id", sa.Integer(), nullable=True),
        sa.Column("relation_kind", sa.String(length=60), nullable=False),
        sa.Column("entity_kind", sa.String(length=60), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=True),
        sa.Column("entity_label", sa.String(length=255), nullable=False),
        sa.Column("entity_href", sa.String(length=500), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_primary", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "(domain_id IS NOT NULL AND product_id IS NULL) OR (domain_id IS NULL AND product_id IS NOT NULL)",
            name="ck_semantic_links_scope",
        ),
        sa.ForeignKeyConstraint(["domain_id"], [f"{schema}.semantic_domains.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["product_id"], [f"{schema}.semantic_data_products.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_semantic_links_domain_id", "semantic_links", ["domain_id"], schema=schema)
    op.create_index("ix_semantic_links_product_id", "semantic_links", ["product_id"], schema=schema)
    op.create_index("ix_semantic_links_entity", "semantic_links", ["entity_kind", "entity_id"], schema=schema)


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index("ix_semantic_links_entity", table_name="semantic_links", schema=schema)
    op.drop_index("ix_semantic_links_product_id", table_name="semantic_links", schema=schema)
    op.drop_index("ix_semantic_links_domain_id", table_name="semantic_links", schema=schema)
    op.drop_table("semantic_links", schema=schema)

    op.drop_index("ix_semantic_data_products_maturity_status", table_name="semantic_data_products", schema=schema)
    op.drop_index("ix_semantic_data_products_domain_id", table_name="semantic_data_products", schema=schema)
    op.drop_table("semantic_data_products", schema=schema)

    op.drop_index("ix_semantic_domains_maturity_status", table_name="semantic_domains", schema=schema)
    op.drop_index("ix_semantic_domains_criticality", table_name="semantic_domains", schema=schema)
    op.drop_table("semantic_domains", schema=schema)
