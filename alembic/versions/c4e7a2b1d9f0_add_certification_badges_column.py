"""add certification badges column

Revision ID: c4e7a2b1d9f0
Revises: a31c5d9f4b22
Create Date: 2026-03-21 17:45:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "c4e7a2b1d9f0"
down_revision = "a31c5d9f4b22"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.execute(f'ALTER TABLE "{SCHEMA}"."tables" ADD COLUMN IF NOT EXISTS certification_badges JSON')


def downgrade() -> None:
    op.execute(f'ALTER TABLE "{SCHEMA}"."tables" DROP COLUMN IF EXISTS certification_badges')
