"""merge current alembic heads into a single head

Revision ID: c6d7e8f9a1b2
Revises: 1f2e3d4c5b6c, a1b2c3d4e5f6, ab39c8d4e2f1, c2a3b4d5e6f7, c8d1e2f3a4b5, d4e5f6a7b8c9, d5e6f7a8b9c0, d6e7f8a9b0c1, e7f8a9b0c1d2, f4a5b6c7d8e9, b5c6d7e8f9a1
Create Date: 2026-04-14 13:00:00.000000
"""

from __future__ import annotations


revision = "c6d7e8f9a1b2"
down_revision = (
    "1f2e3d4c5b6c",
    "a1b2c3d4e5f6",
    "ab39c8d4e2f1",
    "c2a3b4d5e6f7",
    "c8d1e2f3a4b5",
    "d4e5f6a7b8c9",
    "d5e6f7a8b9c0",
    "d6e7f8a9b0c1",
    "e7f8a9b0c1d2",
    "f4a5b6c7d8e9",
    "b5c6d7e8f9a1",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
