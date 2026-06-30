"""create_audit_log_table

Revision ID: 2f1c4be8aa10
Revises: 6c0464c9c46a
Create Date: 2026-02-24 23:59:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "2f1c4be8aa10"
down_revision = "6c0464c9c46a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("user_id", sa.BigInteger(), nullable=True),
        sa.Column("user_email", sa.Text(), nullable=True),
        sa.Column("ip", postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=True),
        sa.Column("entity_id", sa.Text(), nullable=True),
        sa.Column("route", sa.Text(), nullable=True),
        sa.Column("method", sa.Text(), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.Column("before_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("after_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        schema="t2c_data",
    )
    op.create_index("ix_t2c_data_audit_log_created_at", "audit_log", ["created_at"], unique=False, schema="t2c_data")
    op.create_index("ix_t2c_data_audit_log_user_id", "audit_log", ["user_id"], unique=False, schema="t2c_data")
    op.create_index(
        "ix_t2c_data_audit_log_entity_ref",
        "audit_log",
        ["entity_type", "entity_id"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_t2c_data_audit_log_entity_ref", table_name="audit_log", schema="t2c_data")
    op.drop_index("ix_t2c_data_audit_log_user_id", table_name="audit_log", schema="t2c_data")
    op.drop_index("ix_t2c_data_audit_log_created_at", table_name="audit_log", schema="t2c_data")
    op.drop_table("audit_log", schema="t2c_data")

