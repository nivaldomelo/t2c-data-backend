"""merge governance active and trust snapshot heads

Revision ID: ff1a2b3c4d5f
Revises: fb1c2d3e4f50, fe1a2b3c4d5e
Create Date: 2026-04-10 14:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

revision: str = "ff1a2b3c4d5f"
down_revision: Union[str, Sequence[str], None] = ("fb1c2d3e4f50", "fe1a2b3c4d5e")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
