"""merge alembic heads

Revision ID: e9f0a1b2c3d4
Revises: b4c6d7e8f9a0, e7f8a9b0c1d2
Create Date: 2026-04-06 00:00:00.000000
"""

from typing import Sequence, Union


revision: str = "e9f0a1b2c3d4"
down_revision: Union[str, Sequence[str], None] = (
    "b4c6d7e8f9a0",
    "e7f8a9b0c1d2",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
