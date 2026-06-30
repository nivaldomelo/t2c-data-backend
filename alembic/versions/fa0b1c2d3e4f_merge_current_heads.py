"""merge current alembic heads

Revision ID: fa0b1c2d3e4f
Revises: 1f2e3d4c5b6c, d4e5f6a7b8c9
Create Date: 2026-04-10 12:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "fa0b1c2d3e4f"
down_revision: Union[str, Sequence[str], None] = (
    "1f2e3d4c5b6c",
    "d4e5f6a7b8c9",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
