"""merge privacy review and cockpit heads

Revision ID: ff3a4b5c6d7e
Revises: a8c7e6d5f4b3, ff2a3b4c5d6e
Create Date: 2026-05-13 17:15:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

revision: str = "ff3a4b5c6d7e"
down_revision: Union[str, Sequence[str], None] = ("a8c7e6d5f4b3", "ff2a3b4c5d6e")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
