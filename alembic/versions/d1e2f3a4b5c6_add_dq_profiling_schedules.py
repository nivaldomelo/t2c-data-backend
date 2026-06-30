"""add dq profiling schedules

Revision ID: d1e2f3a4b5c6
Revises: c0d1e2f3a4b6
Create Date: 2026-04-07 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d1e2f3a4b5c6"
down_revision = "c0d1e2f3a4b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.dq_profiling_schedules (
                id SERIAL NOT NULL,
                scope VARCHAR(20) NOT NULL DEFAULT 'table',
                table_id INTEGER NULL REFERENCES t2c_data.tables (id) ON DELETE CASCADE,
                datasource_id INTEGER NULL REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE,
                schema_name VARCHAR(255),
                schedule_mode VARCHAR(20) NOT NULL DEFAULT 'manual',
                schedule_enabled BOOLEAN NOT NULL DEFAULT true,
                schedule_every_minutes INTEGER,
                schedule_time VARCHAR(5),
                schedule_day_of_week INTEGER,
                schedule_day_of_month INTEGER,
                schedule_anchor_date TIMESTAMP WITH TIME ZONE,
                schedule_last_run_at TIMESTAMP WITH TIME ZONE,
                schedule_last_started_at TIMESTAMP WITH TIME ZONE,
                schedule_last_finished_at TIMESTAMP WITH TIME ZONE,
                schedule_last_status VARCHAR(20),
                schedule_last_error TEXT,
                schedule_next_run_at TIMESTAMP WITH TIME ZONE,
                schema_limit INTEGER,
                schema_concurrency INTEGER,
                schema_sample_fraction FLOAT,
                schema_include_tables_json JSON,
                schema_exclude_tables_json JSON,
                schema_columns_json JSON,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                PRIMARY KEY (id)
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.dq_profiling_schedule_recipients (
                schedule_id INTEGER NOT NULL REFERENCES t2c_data.dq_profiling_schedules (id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES t2c_data.users (id) ON DELETE CASCADE,
                PRIMARY KEY (schedule_id, user_id)
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.dq_profiling_scheduler_status (
                id SERIAL NOT NULL,
                scheduler_name VARCHAR(80) NOT NULL DEFAULT 'dq_profiling',
                mode VARCHAR(20) NOT NULL DEFAULT 'embedded',
                is_enabled BOOLEAN NOT NULL DEFAULT true,
                last_started_at VARCHAR(64),
                last_heartbeat_at VARCHAR(64),
                last_success_at VARCHAR(64),
                last_failure_at VARCHAR(64),
                last_error TEXT,
                last_run_summary_json JSON,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                PRIMARY KEY (id)
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE t2c_data.dq_runs
            ADD COLUMN IF NOT EXISTS profiling_schedule_id INTEGER NULL REFERENCES t2c_data.dq_profiling_schedules (id) ON DELETE SET NULL
            """
        )
    )
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_runs_profiling_schedule_id ON t2c_data.dq_runs (profiling_schedule_id)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_schedules_scope ON t2c_data.dq_profiling_schedules (scope)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_schedules_table_id ON t2c_data.dq_profiling_schedules (table_id)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_schedules_datasource_id ON t2c_data.dq_profiling_schedules (datasource_id)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_schedules_schema_name ON t2c_data.dq_profiling_schedules (schema_name)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_schedules_schedule_enabled ON t2c_data.dq_profiling_schedules (schedule_enabled)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_schedules_schedule_mode ON t2c_data.dq_profiling_schedules (schedule_mode)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_schedules_schedule_next_run_at ON t2c_data.dq_profiling_schedules (schedule_next_run_at)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_dq_profiling_scheduler_status_scheduler_name ON t2c_data.dq_profiling_scheduler_status (scheduler_name)"))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_scheduler_status_scheduler_name"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_schedules_schedule_next_run_at"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_schedules_schedule_mode"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_schedules_schedule_enabled"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_schedules_schema_name"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_schedules_datasource_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_schedules_table_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_profiling_schedules_scope"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_dq_runs_profiling_schedule_id"))
    op.execute(sa.text("ALTER TABLE t2c_data.dq_runs DROP COLUMN IF EXISTS profiling_schedule_id"))
    op.execute(sa.text("DROP TABLE IF EXISTS t2c_data.dq_profiling_scheduler_status"))
    op.execute(sa.text("DROP TABLE IF EXISTS t2c_data.dq_profiling_schedule_recipients"))
    op.execute(sa.text("DROP TABLE IF EXISTS t2c_data.dq_profiling_schedules"))
