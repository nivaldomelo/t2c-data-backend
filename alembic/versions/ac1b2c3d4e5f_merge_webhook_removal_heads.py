"""merge webhook removal heads

Revision ID: ac1b2c3d4e5f
Revises: ab1b2c3d4e5f, ff3a4b5c6d7e
Create Date: 2026-05-14 00:00:01.000000
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "ac1b2c3d4e5f"
down_revision: Union[str, Sequence[str], None] = (
    "ab1b2c3d4e5f",
    "ff3a4b5c6d7e",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

