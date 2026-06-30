"""add friendly dq schedule modes

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-04-07 01:15:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a2b3c4d5e6f7"
down_revision: Union[str, Sequence[str], None] = "f1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "t2c_data"


def upgrade() -> None:
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {SCHEMA}.dq_rules
            ADD COLUMN IF NOT EXISTS schedule_mode VARCHAR(20)
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
            UPDATE {SCHEMA}.dq_rules
            SET schedule_mode = CASE
                WHEN schedule_enabled IS FALSE THEN 'manual'
                ELSE 'interval'
            END
            WHERE schedule_mode IS NULL OR schedule_mode = ''
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.dq_rules
            SET schedule_mode = 'manual'
            WHERE schedule_enabled IS FALSE
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


def downgrade() -> None:
    op.execute(sa.text(f"DROP INDEX IF EXISTS {SCHEMA}.ix_t2c_data_dq_rules_schedule_mode"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_anchor_date"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_day_of_month"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_day_of_week"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_time"))
    op.execute(sa.text(f"ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS schedule_mode"))
