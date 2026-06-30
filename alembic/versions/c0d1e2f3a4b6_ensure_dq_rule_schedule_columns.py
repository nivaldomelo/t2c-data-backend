"""ensure dq rule schedule columns

Revision ID: c0d1e2f3a4b6
Revises: a2b3c4d5e6f7
Create Date: 2026-04-07 02:10:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c0d1e2f3a4b6"
down_revision: Union[str, Sequence[str], None] = "a2b3c4d5e6f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS notification_recipient_user_id INTEGER
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_mode VARCHAR(20) NOT NULL DEFAULT 'manual'
            """
        )
    )
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
            ADD COLUMN IF NOT EXISTS schedule_time VARCHAR(5)
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_day_of_week INTEGER
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_day_of_month INTEGER
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_anchor_date TIMESTAMP WITH TIME ZONE
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
            UPDATE {SCHEMA}.dq_rules
            SET schedule_mode = CASE
                WHEN schedule_mode IS NULL OR schedule_mode = '' THEN
                    CASE
                        WHEN schedule_enabled IS FALSE THEN 'manual'
                        WHEN schedule_every_minutes IS NOT NULL THEN 'interval'
                        ELSE 'daily'
                    END
                ELSE schedule_mode
            END
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.dq_rules
            SET schedule_enabled = true
            WHERE schedule_enabled IS NULL
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.dq_rules
            SET schedule_every_minutes = 60
            WHERE schedule_mode = 'interval' AND schedule_every_minutes IS NULL
            """
        )
    )

    op.execute(
        sa.text(
            f"""
            CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_mode
            ON {SCHEMA}.dq_rules (schedule_mode)
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
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_last_run_at"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_every_minutes"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_enabled"))
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_mode"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_last_run_at"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_anchor_date"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_day_of_month"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_day_of_week"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_time"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_every_minutes"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_enabled"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_mode"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS notification_recipient_user_id"))
