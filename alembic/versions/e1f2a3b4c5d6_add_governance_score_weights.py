"""add governance score weights

Revision ID: e1f2a3b4c5d6
Revises: b2c3d4e5f6a
Create Date: 2026-03-29 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


DEFAULT_GOVERNANCE_SCORE_WEIGHTS = (
    '{"certification": 10, "certification_review": 5, "column_description_complete": 12, '
    '"dq_score": 15, "glossary_terms": 8, "incident_health": 10, "owner_defined": 10, '
    '"owner_review": 7, "privacy_review": 5, "table_description_complete": 10, "tags_applied": 8}'
)


def upgrade() -> None:
    op.add_column(
        "governance_settings",
        sa.Column("governance_score_weights", sa.Text(), nullable=True),
        schema="t2c_data",
    )
    op.execute(
        sa.text(
            """
            UPDATE t2c_data.governance_settings
            SET governance_score_weights = :weights
            WHERE governance_score_weights IS NULL
            """
        ).bindparams(weights=DEFAULT_GOVERNANCE_SCORE_WEIGHTS)
    )


def downgrade() -> None:
    op.drop_column("governance_settings", "governance_score_weights", schema="t2c_data")
