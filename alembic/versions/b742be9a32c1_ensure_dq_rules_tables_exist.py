"""ensure dq rules tables exist

Revision ID: b742be9a32c1
Revises: 9f2a6d1d77aa
Create Date: 2026-02-22 19:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "b742be9a32c1"
down_revision = "9f2a6d1d77aa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "t2c_data"')

    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.dq_rules (
              id SERIAL PRIMARY KEY,
              table_id INTEGER NULL REFERENCES t2c_data.tables(id) ON DELETE SET NULL,
              table_fqn VARCHAR(500) NOT NULL,
              name VARCHAR(255) NOT NULL,
              description TEXT NULL,
              rule_type VARCHAR(50) NOT NULL DEFAULT 'row_violation',
              severity VARCHAR(20) NOT NULL DEFAULT 'medium',
              sql_text TEXT NOT NULL,
              is_active BOOLEAN NOT NULL DEFAULT TRUE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    )

    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS t2c_data.dq_rule_runs (
              id SERIAL PRIMARY KEY,
              rule_id INTEGER NOT NULL,
              status VARCHAR(20) NOT NULL DEFAULT 'pass',
              violations_count BIGINT NOT NULL DEFAULT 0,
              sample_rows_json JSON NULL,
              error_message TEXT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    )

    op.execute('CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_table_id ON t2c_data.dq_rules (table_id)')
    op.execute('CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_table_fqn ON t2c_data.dq_rules (table_fqn)')
    op.execute('CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_is_active ON t2c_data.dq_rules (is_active)')
    op.execute('CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rule_runs_rule_id ON t2c_data.dq_rule_runs (rule_id)')

    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'fk_dq_rule_runs_rule_id_dq_rules'
              ) THEN
                ALTER TABLE t2c_data.dq_rule_runs
                  ADD CONSTRAINT fk_dq_rule_runs_rule_id_dq_rules
                  FOREIGN KEY (rule_id)
                  REFERENCES t2c_data.dq_rules(id)
                  ON DELETE CASCADE;
              END IF;
            END
            $$;
            """
        )
    )


def downgrade() -> None:
    op.execute('DROP TABLE IF EXISTS t2c_data.dq_rule_runs')
    op.execute('DROP TABLE IF EXISTS t2c_data.dq_rules')
