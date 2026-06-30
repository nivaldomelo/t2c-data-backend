"""merge dq observability and contract heads

Revision ID: 5d4c3b2a1f0e
Revises: 9a7b6c5d4e3f, f9a0b1c2d3e4
Create Date: 2026-04-13 22:10:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "5d4c3b2a1f0e"
down_revision: Union[str, Sequence[str], None] = ("9a7b6c5d4e3f", "f9a0b1c2d3e4")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
