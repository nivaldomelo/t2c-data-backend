"""ensure dq scheduler support

Revision ID: f1a2b3c4d5e6
Revises: a9b1c2d3e4f5
Create Date: 2026-04-07 00:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "a9b1c2d3e4f5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_enabled BOOLEAN NOT NULL DEFAULT true
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_every_minutes INTEGER
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_last_run_at TIMESTAMP WITH TIME ZONE
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.dq_scheduler_status (
                id INTEGER PRIMARY KEY,
                scheduler_name VARCHAR(80) NOT NULL DEFAULT 'dq_rules',
                mode VARCHAR(20) NOT NULL DEFAULT 'embedded',
                is_enabled BOOLEAN NOT NULL DEFAULT true,
                last_started_at VARCHAR(64),
                last_heartbeat_at VARCHAR(64),
                last_success_at VARCHAR(64),
                last_failure_at VARCHAR(64),
                last_error TEXT,
                last_run_summary_json JSON,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
            )
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            INSERT INTO {SCHEMA}.dq_scheduler_status (id, scheduler_name, mode, is_enabled)
            VALUES (1, 'dq_rules', 'embedded', true)
            ON CONFLICT (id) DO NOTHING
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_enabled
            ON {SCHEMA}.dq_rules (schedule_enabled)
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_every_minutes
            ON {SCHEMA}.dq_rules (schedule_every_minutes)
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_last_run_at
            ON {SCHEMA}.dq_rules (schedule_last_run_at)
            """
        )
    )


def downgrade() -> None:
    op.execute(sa.text(f"DROP TABLE IF EXISTS {SCHEMA}.dq_scheduler_status"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_last_run_at"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_every_minutes"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_enabled"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_last_run_at"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_every_minutes"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_enabled"))
