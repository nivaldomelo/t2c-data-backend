"""add sensitive flags to audit log

Revision ID: 3c4d5e6f7a8b
Revises: 2d6b7c8a9e10
Create Date: 2026-03-27 02:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "3c4d5e6f7a8b"
down_revision = "2d6b7c8a9e10"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "audit_log",
        sa.Column("is_sensitive_change", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema=SCHEMA,
    )
    op.add_column("audit_log", sa.Column("sensitive_category", sa.Text(), nullable=True), schema=SCHEMA)
    op.create_index("ix_audit_log_is_sensitive_change", "audit_log", ["is_sensitive_change"], unique=False, schema=SCHEMA)
    op.create_index("ix_audit_log_sensitive_category", "audit_log", ["sensitive_category"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_audit_log_sensitive_category", table_name="audit_log", schema=SCHEMA)
    op.drop_index("ix_audit_log_is_sensitive_change", table_name="audit_log", schema=SCHEMA)
    op.drop_column("audit_log", "sensitive_category", schema=SCHEMA)
    op.drop_column("audit_log", "is_sensitive_change", schema=SCHEMA)
