"""merge data contracts and webhook attempts heads

Revision ID: f9a0b1c2d3e4
Revises: d6e7f8a9b0c1, e2f3a4b5c6d7
Create Date: 2026-04-13 19:20:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "f9a0b1c2d3e4"
down_revision: Union[str, Sequence[str], None] = ("d6e7f8a9b0c1", "e2f3a4b5c6d7")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
