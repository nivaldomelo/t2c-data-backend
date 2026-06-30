"""merge dq scheduler and data lake heads

Revision ID: f2a3b4c5d6e7
Revises: a9b1c2d3e4f5, e10f12a3b4c5
Create Date: 2026-04-19 22:20:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

revision: str = "f2a3b4c5d6e7"
down_revision: Union[str, Sequence[str], None] = (
    "a9b1c2d3e4f5",
    "e10f12a3b4c5",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
