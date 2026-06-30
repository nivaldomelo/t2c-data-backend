"""expand audit log for history tracking

Revision ID: 2d6b7c8a9e10
Revises: 1f9a2b3c4d5e
Create Date: 2026-03-27 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "2d6b7c8a9e10"
down_revision = "1f9a2b3c4d5e"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column("audit_log", sa.Column("actor_name", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("audit_log", sa.Column("parent_entity_type", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("audit_log", sa.Column("parent_entity_id", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("audit_log", sa.Column("change_set_id", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("audit_log", sa.Column("change_type", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("audit_log", sa.Column("field_name", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column("audit_log", sa.Column("source_module", sa.Text(), nullable=True), schema=SCHEMA)

    op.create_index("ix_audit_log_change_set_id", "audit_log", ["change_set_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_audit_log_change_type", "audit_log", ["change_type"], unique=False, schema=SCHEMA)
    op.create_index("ix_audit_log_field_name", "audit_log", ["field_name"], unique=False, schema=SCHEMA)
    op.create_index("ix_audit_log_source_module", "audit_log", ["source_module"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_audit_log_source_module", table_name="audit_log", schema=SCHEMA)
    op.drop_index("ix_audit_log_field_name", table_name="audit_log", schema=SCHEMA)
    op.drop_index("ix_audit_log_change_type", table_name="audit_log", schema=SCHEMA)
    op.drop_index("ix_audit_log_change_set_id", table_name="audit_log", schema=SCHEMA)

    op.drop_column("audit_log", "source_module", schema=SCHEMA)
    op.drop_column("audit_log", "field_name", schema=SCHEMA)
    op.drop_column("audit_log", "change_type", schema=SCHEMA)
    op.drop_column("audit_log", "change_set_id", schema=SCHEMA)
    op.drop_column("audit_log", "parent_entity_id", schema=SCHEMA)
    op.drop_column("audit_log", "parent_entity_type", schema=SCHEMA)
    op.drop_column("audit_log", "actor_name", schema=SCHEMA)
