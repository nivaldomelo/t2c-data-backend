"""harden scheduler modes and drop dq sql text

Revision ID: 6b7c8d9e0f1a
Revises: 5a6b7c8d9e0f
Create Date: 2026-05-26 12:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "6b7c8d9e0f1a"
down_revision: Union[str, Sequence[str], None] = "5a6b7c8d9e0f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "t2c_data"
SCHEDULER_STATUS_TABLES = (
    "dq_scheduler_status",
    "dq_profiling_scheduler_status",
    "datasource_scan_scheduler_status",
    "platform_scheduler_status",
    "data_lake_scan_scheduler_status",
)


def _table_exists_sql(table_name: str) -> str:
    return f"to_regclass('{SCHEMA}.{table_name}') IS NOT NULL"


def upgrade() -> None:
    for table_name in SCHEDULER_STATUS_TABLES:
        op.execute(
            sa.text(
                f"""
                DO $$
                BEGIN
                  IF {_table_exists_sql(table_name)} THEN
                    UPDATE {SCHEMA}.{table_name}
                    SET mode = 'worker'
                    WHERE mode = 'embedded';
                    ALTER TABLE {SCHEMA}.{table_name}
                    ALTER COLUMN mode SET DEFAULT 'worker';
                  END IF;
                END $$;
                """
            )
        )

    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
              IF {_table_exists_sql("dq_rules")} THEN
                UPDATE {SCHEMA}.dq_rules
                SET is_active = false,
                    archived = true,
                    archived_reason = COALESCE(archived_reason, 'legacy_sql_rule_removed'),
                    archived_at = COALESCE(archived_at, NOW())
                WHERE rule_definition_json IS NULL
                   OR legacy_rule_type IN ('custom_sql', 'raw_sql', 'sql_expression');

                ALTER TABLE {SCHEMA}.dq_rules DROP COLUMN IF EXISTS sql_text;
              END IF;
            END $$;
            """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
              IF {_table_exists_sql("dq_rules")} THEN
                ALTER TABLE {SCHEMA}.dq_rules
                ADD COLUMN IF NOT EXISTS sql_text TEXT;
              END IF;
            END $$;
            """
        )
    )
    for table_name in SCHEDULER_STATUS_TABLES:
        op.execute(
            sa.text(
                f"""
                DO $$
                BEGIN
                  IF {_table_exists_sql(table_name)} THEN
                    ALTER TABLE {SCHEMA}.{table_name}
                    ALTER COLUMN mode SET DEFAULT 'embedded';
                  END IF;
                END $$;
                """
            )
        )
