"""merge integration health and rate limit heads

Revision ID: d0e1f2a3b4c5
Revises: a1b2c3d4e5f7, c7d8e9f0a1b2
Create Date: 2026-04-15 19:10:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "d0e1f2a3b4c5"
down_revision: Union[str, Sequence[str], None] = (
    "a1b2c3d4e5f7",
    "c7d8e9f0a1b2",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
