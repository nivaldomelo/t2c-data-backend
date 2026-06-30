"""merge dq observability and timeline heads

Revision ID: 7e6d5c4b3a29
Revises: 5d4c3b2a1f0e, b1d2c3e4f5a6
Create Date: 2026-04-13 12:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

revision: str = "7e6d5c4b3a29"
down_revision: Union[str, Sequence[str], None] = ("5d4c3b2a1f0e", "b1d2c3e4f5a6")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
