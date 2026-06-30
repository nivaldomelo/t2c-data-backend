"""add column dictionary fields

Revision ID: 9b7e3c2d1f44
Revises: 8a2d4f1c6b77
Create Date: 2026-03-19 12:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "9b7e3c2d1f44"
down_revision = "8a2d4f1c6b77"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column("columns", sa.Column("external_id", sa.String(length=64), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("slug", sa.String(length=255), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("udt_name", sa.String(length=255), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("character_maximum_length", sa.Integer(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("numeric_precision", sa.Integer(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("numeric_scale", sa.Integer(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("column_default", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("existing_comment", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("dictionary_description", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("columns", sa.Column("dictionary_comment", sa.Text(), nullable=True), schema=SCHEMA)
    op.create_index("ix_columns_slug", "columns", ["slug"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_columns_slug", table_name="columns", schema=SCHEMA)
    op.drop_column("columns", "dictionary_comment", schema=SCHEMA)
    op.drop_column("columns", "dictionary_description", schema=SCHEMA)
    op.drop_column("columns", "existing_comment", schema=SCHEMA)
    op.drop_column("columns", "column_default", schema=SCHEMA)
    op.drop_column("columns", "numeric_scale", schema=SCHEMA)
    op.drop_column("columns", "numeric_precision", schema=SCHEMA)
    op.drop_column("columns", "character_maximum_length", schema=SCHEMA)
    op.drop_column("columns", "udt_name", schema=SCHEMA)
    op.drop_column("columns", "slug", schema=SCHEMA)
    op.drop_column("columns", "external_id", schema=SCHEMA)
