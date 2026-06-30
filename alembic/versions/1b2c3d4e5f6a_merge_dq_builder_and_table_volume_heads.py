"""merge dq builder and table volume heads

Revision ID: 1b2c3d4e5f6a
Revises: 0a9b8c7d6e5f, e2f1a3b4c5d6
Create Date: 2026-05-25 00:10:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "1b2c3d4e5f6a"
down_revision: Union[str, Sequence[str], None] = ("0a9b8c7d6e5f", "e2f1a3b4c5d6")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
