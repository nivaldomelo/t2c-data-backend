"""merge user sessions and scale indexes heads

Revision ID: f5e6d7c8b9a0
Revises: c1d2e3f4a5c8, d2e3f4a5b6c7
Create Date: 2026-05-15 00:45:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

revision: str = "f5e6d7c8b9a0"
down_revision: Union[str, Sequence[str], None] = ("c1d2e3f4a5c8", "d2e3f4a5b6c7")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
