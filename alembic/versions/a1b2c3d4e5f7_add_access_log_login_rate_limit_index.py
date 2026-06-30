"""add audit log index for login rate limiting

Revision ID: a1b2c3d4e5f7
Revises: ff1a2b3c4d5f
Create Date: 2026-04-15 00:00:00.000000
"""

from alembic import op

from t2c_data.core.config import settings


revision = "a1b2c3d4e5f7"
down_revision = "ff1a2b3c4d5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = settings.db_schema
    op.create_index(
        "ix_audit_log_action_user_email_created_at",
        "audit_log",
        ["action", "user_email", "created_at"],
        schema=schema,
    )


def downgrade() -> None:
    schema = settings.db_schema
    op.drop_index(
        "ix_audit_log_action_user_email_created_at",
        table_name="audit_log",
        schema=schema,
    )
