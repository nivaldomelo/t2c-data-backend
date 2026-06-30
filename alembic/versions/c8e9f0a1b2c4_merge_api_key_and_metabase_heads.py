"""merge api key and metabase heads

Revision ID: c8e9f0a1b2c4
Revises: aa1b2c3d4e5f, b7c8d9e0f2a3
Create Date: 2026-04-16 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union


revision: str = "c8e9f0a1b2c4"
down_revision: Union[str, Sequence[str], None] = (
    "aa1b2c3d4e5f",
    "b7c8d9e0f2a3",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
