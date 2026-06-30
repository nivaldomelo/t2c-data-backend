"""add table privacy access fields

Revision ID: f7c1d2e3a4b5
Revises: e6d4a1b9c2f3
Create Date: 2026-03-22 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "f7c1d2e3a4b5"
down_revision = "e6d4a1b9c2f3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tables", sa.Column("sensitivity_level", sa.String(length=30), nullable=True), schema="t2c_data")
    op.add_column("tables", sa.Column("has_personal_data", sa.Boolean(), nullable=False, server_default=sa.text("false")), schema="t2c_data")
    op.add_column("tables", sa.Column("has_sensitive_personal_data", sa.Boolean(), nullable=False, server_default=sa.text("false")), schema="t2c_data")
    op.add_column("tables", sa.Column("legal_basis", sa.String(length=50), nullable=True), schema="t2c_data")
    op.add_column("tables", sa.Column("retention_policy", sa.String(length=255), nullable=True), schema="t2c_data")
    op.add_column("tables", sa.Column("is_masked", sa.Boolean(), nullable=False, server_default=sa.text("false")), schema="t2c_data")
    op.add_column("tables", sa.Column("external_sharing", sa.Boolean(), nullable=False, server_default=sa.text("false")), schema="t2c_data")
    op.add_column("tables", sa.Column("access_scope", sa.String(length=30), nullable=True), schema="t2c_data")
    op.add_column("tables", sa.Column("access_roles", sa.JSON(), nullable=True), schema="t2c_data")
    op.add_column("tables", sa.Column("privacy_notes", sa.Text(), nullable=True), schema="t2c_data")
    op.add_column("tables", sa.Column("privacy_reviewed_by_user_id", sa.Integer(), nullable=True), schema="t2c_data")
    op.add_column("tables", sa.Column("privacy_reviewed_at", sa.DateTime(timezone=True), nullable=True), schema="t2c_data")
    op.create_foreign_key(
        "fk_tables_privacy_reviewed_by_user_id_users",
        "tables",
        "users",
        ["privacy_reviewed_by_user_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )
    op.alter_column("tables", "has_personal_data", server_default=None, schema="t2c_data")
    op.alter_column("tables", "has_sensitive_personal_data", server_default=None, schema="t2c_data")
    op.alter_column("tables", "is_masked", server_default=None, schema="t2c_data")
    op.alter_column("tables", "external_sharing", server_default=None, schema="t2c_data")


def downgrade() -> None:
    op.drop_constraint("fk_tables_privacy_reviewed_by_user_id_users", "tables", schema="t2c_data", type_="foreignkey")
    op.drop_column("tables", "privacy_reviewed_at", schema="t2c_data")
    op.drop_column("tables", "privacy_reviewed_by_user_id", schema="t2c_data")
    op.drop_column("tables", "privacy_notes", schema="t2c_data")
    op.drop_column("tables", "access_roles", schema="t2c_data")
    op.drop_column("tables", "access_scope", schema="t2c_data")
    op.drop_column("tables", "external_sharing", schema="t2c_data")
    op.drop_column("tables", "is_masked", schema="t2c_data")
    op.drop_column("tables", "retention_policy", schema="t2c_data")
    op.drop_column("tables", "legal_basis", schema="t2c_data")
    op.drop_column("tables", "has_sensitive_personal_data", schema="t2c_data")
    op.drop_column("tables", "has_personal_data", schema="t2c_data")
    op.drop_column("tables", "sensitivity_level", schema="t2c_data")
