"""add reason_code to integration health history

Revision ID: a6b7c8d9e0f2
Revises: d0e1f2a3b4c5
Create Date: 2026-04-15 21:10:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from t2c_data.core.alembic_safe import column_exists, safe_add_column

revision = "a6b7c8d9e0f2"
down_revision = "d0e1f2a3b4c5"
branch_labels = None
depends_on = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    bind = op.get_bind()
    if not column_exists(bind, "integration_health", "reason_code", schema=SCHEMA):
        safe_add_column(bind, "integration_health", sa.Column("reason_code", sa.String(length=80), nullable=True), schema=SCHEMA)
    if not column_exists(bind, "integration_health_history", "reason_code", schema=SCHEMA):
        safe_add_column(
            bind,
            "integration_health_history",
            sa.Column("reason_code", sa.String(length=80), nullable=True),
            schema=SCHEMA,
        )


def downgrade() -> None:
    op.drop_column("integration_health_history", "reason_code", schema=SCHEMA)
    op.drop_column("integration_health", "reason_code", schema=SCHEMA)
