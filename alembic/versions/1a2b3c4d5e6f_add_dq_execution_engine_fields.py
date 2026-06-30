"""add dq execution engine fields

Revision ID: 1a2b3c4d5e6f
Revises: f1a2b3c4d5e6
Create Date: 2026-04-07 00:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "1a2b3c4d5e6f"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = "d1e2f3a4b5c6"


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE t2c_data.dq_rules
        ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(20) NOT NULL DEFAULT 'python'
        """
    )
    op.execute(
        """
        UPDATE t2c_data.dq_rules
        SET execution_engine = COALESCE(NULLIF(TRIM(execution_engine), ''), 'python')
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_execution_engine
        ON t2c_data.dq_rules (execution_engine)
        """
    )
    op.execute(
        """
        ALTER TABLE t2c_data.dq_profiling_schedules
        ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(20) NOT NULL DEFAULT 'spark'
        """
    )
    op.execute(
        """
        UPDATE t2c_data.dq_profiling_schedules
        SET execution_engine = COALESCE(NULLIF(TRIM(execution_engine), ''), 'spark')
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_execution_engine
        ON t2c_data.dq_profiling_schedules (execution_engine)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS t2c_data.ix_t2c_data_dq_profiling_schedules_execution_engine")
    op.execute("DROP INDEX IF EXISTS t2c_data.ix_t2c_data_dq_rules_execution_engine")
    op.execute("ALTER TABLE t2c_data.dq_profiling_schedules DROP COLUMN IF EXISTS execution_engine")
    op.execute("ALTER TABLE t2c_data.dq_rules DROP COLUMN IF EXISTS execution_engine")
