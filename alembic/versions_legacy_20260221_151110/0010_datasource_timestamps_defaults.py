"""ensure datasource timestamps have server defaults

Revision ID: 0010_datasource_timestamps_defaults
Revises: 0009_move_app_tables_to_t2c_data
Create Date: 2026-02-21 18:50:00.000000
"""

from alembic import op


revision = "0010_datasource_timestamps_defaults"
down_revision = "0009_move_app_tables_to_t2c_data"
branch_labels = None
depends_on = None


def _set_defaults(schema_name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = 'data_sources'
              AND column_name = 'created_at'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.data_sources ALTER COLUMN created_at SET DEFAULT now()';
          END IF;

          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = 'data_sources'
              AND column_name = 'updated_at'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.data_sources ALTER COLUMN updated_at SET DEFAULT now()';
          END IF;
        END $$;
        """
    )


def _drop_defaults(schema_name: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = 'data_sources'
              AND column_name = 'created_at'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.data_sources ALTER COLUMN created_at DROP DEFAULT';
          END IF;

          IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = 'data_sources'
              AND column_name = 'updated_at'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.data_sources ALTER COLUMN updated_at DROP DEFAULT';
          END IF;
        END $$;
        """
    )


def upgrade() -> None:
    _set_defaults("t2c_data")
    _set_defaults("public")


def downgrade() -> None:
    _drop_defaults("t2c_data")
    _drop_defaults("public")
