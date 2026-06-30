"""add datasource scan scheduler

Revision ID: a3b4c5d6e7f8
Revises: f0a1b2c3d4e5
Create Date: 2026-04-07 00:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a3b4c5d6e7f8"
down_revision = "f0a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.datasource_scan_schedules (
                id SERIAL NOT NULL,
                datasource_id INTEGER NOT NULL REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE,
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
                schedule_summary TEXT,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                PRIMARY KEY (id),
                CONSTRAINT uq_datasource_scan_schedules_datasource UNIQUE (datasource_id)
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.datasource_scan_schedule_recipients (
                schedule_id INTEGER NOT NULL REFERENCES t2c_data.datasource_scan_schedules (id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES t2c_data.users (id) ON DELETE CASCADE,
                PRIMARY KEY (schedule_id, user_id)
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.datasource_scan_scheduler_status (
                id SERIAL NOT NULL,
                scheduler_name VARCHAR(80) NOT NULL DEFAULT 'datasource_scan',
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
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_datasource_scan_schedules_enabled ON t2c_data.datasource_scan_schedules (schedule_enabled)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_datasource_scan_schedules_mode ON t2c_data.datasource_scan_schedules (schedule_mode)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_datasource_scan_schedules_next_run ON t2c_data.datasource_scan_schedules (schedule_next_run_at)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_datasource_scan_schedules_datasource_id ON t2c_data.datasource_scan_schedules (datasource_id)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_datasource_scan_scheduler_status_name ON t2c_data.datasource_scan_scheduler_status (scheduler_name)"))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_datasource_scan_scheduler_status_name"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_datasource_scan_schedules_datasource_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_datasource_scan_schedules_next_run"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_datasource_scan_schedules_mode"))
    op.execute(sa.text("DROP INDEX IF EXISTS t2c_data.ix_datasource_scan_schedules_enabled"))
    op.execute(sa.text("DROP TABLE IF EXISTS t2c_data.datasource_scan_scheduler_status"))
    op.execute(sa.text("DROP TABLE IF EXISTS t2c_data.datasource_scan_schedule_recipients"))
    op.execute(sa.text("DROP TABLE IF EXISTS t2c_data.datasource_scan_schedules"))
