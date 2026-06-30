"""add permissions rbac

Revision ID: 7c5bf86829b1
Revises: 55da547ae3ae
Create Date: 2026-02-21 17:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "7c5bf86829b1"
down_revision = "55da547ae3ae"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "permissions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_permissions_name"),
        schema="t2c_data",
    )
    op.create_table(
        "role_permissions",
        sa.Column("role_id", sa.Integer(), nullable=False),
        sa.Column("permission_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["permission_id"], ["t2c_data.permissions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["role_id"], ["t2c_data.roles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("role_id", "permission_id"),
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_table("role_permissions", schema="t2c_data")
    op.drop_table("permissions", schema="t2c_data")
