"""add dq rule notification recipient user

Revision ID: b4c6d7e8f9a0
Revises: f8a9b0c1d2e3
Create Date: 2026-04-06 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b4c6d7e8f9a0"
down_revision: Union[str, Sequence[str], None] = "f8a9b0c1d2e3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "dq_rules",
        sa.Column("notification_recipient_user_id", sa.Integer(), nullable=True),
        schema="t2c_data",
    )
    op.create_foreign_key(
        "fk_dq_rules_notification_recipient_user_id",
        "dq_rules",
        "users",
        ["notification_recipient_user_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_t2c_data_dq_rules_notification_recipient_user_id",
        "dq_rules",
        ["notification_recipient_user_id"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_t2c_data_dq_rules_notification_recipient_user_id",
        table_name="dq_rules",
        schema="t2c_data",
    )
    op.drop_constraint(
        "fk_dq_rules_notification_recipient_user_id",
        "dq_rules",
        schema="t2c_data",
        type_="foreignkey",
    )
    op.drop_column("dq_rules", "notification_recipient_user_id", schema="t2c_data")
