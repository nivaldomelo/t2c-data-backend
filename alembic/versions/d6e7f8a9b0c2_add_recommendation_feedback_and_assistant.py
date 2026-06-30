"""add recommendation feedback and assistant fields

Revision ID: d6e7f8a9b0c2
Revises: c1d2e3f4a501, c2a3b4d5e6f7
Create Date: 2026-04-10 16:45:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d6e7f8a9b0c2"
down_revision: Union[str, Sequence[str], None] = ("c1d2e3f4a501", "c2a3b4d5e6f7")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("governance_recommendations", sa.Column("feedback_rating", sa.String(length=20), nullable=True), schema="t2c_data")
    op.add_column("governance_recommendations", sa.Column("feedback_note", sa.Text(), nullable=True), schema="t2c_data")
    op.add_column("governance_recommendations", sa.Column("feedback_updated_at", sa.DateTime(timezone=True), nullable=True), schema="t2c_data")
    op.add_column(
        "governance_recommendations",
        sa.Column("feedback_updated_by_user_id", sa.Integer(), nullable=True),
        schema="t2c_data",
    )
    op.create_foreign_key(
        "fk_governance_recommendations_feedback_updated_by_user_id_users",
        "governance_recommendations",
        "users",
        ["feedback_updated_by_user_id"],
        ["id"],
        source_schema="t2c_data",
        referent_schema="t2c_data",
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_governance_recommendations_feedback_updated_by_user_id_users",
        "governance_recommendations",
        schema="t2c_data",
        type_="foreignkey",
    )
    op.drop_column("governance_recommendations", "feedback_updated_by_user_id", schema="t2c_data")
    op.drop_column("governance_recommendations", "feedback_updated_at", schema="t2c_data")
    op.drop_column("governance_recommendations", "feedback_note", schema="t2c_data")
    op.drop_column("governance_recommendations", "feedback_rating", schema="t2c_data")
