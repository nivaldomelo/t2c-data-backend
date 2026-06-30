"""enforce datasource foreign keys with ON DELETE CASCADE

Revision ID: 0011_datasource_fk_cascades
Revises: 0010_datasource_timestamps_defaults
Create Date: 2026-02-21 20:10:00.000000
"""

from alembic import op


revision = "0011_datasource_fk_cascades"
down_revision = "0010_datasource_timestamps_defaults"
branch_labels = None
depends_on = None


def _replace_fk(schema_name: str, table_name: str, constraint_name: str, delete_rule: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
              AND table_name = '{table_name}'
          ) THEN
            EXECUTE 'ALTER TABLE {schema_name}.{table_name} DROP CONSTRAINT IF EXISTS {constraint_name}';
            EXECUTE '
              ALTER TABLE {schema_name}.{table_name}
              ADD CONSTRAINT {constraint_name}
              FOREIGN KEY (datasource_id)
              REFERENCES {schema_name}.data_sources(id)
              ON DELETE {delete_rule}
            ';
          END IF;
        END $$;
        """
    )


def upgrade() -> None:
    for schema_name in ("t2c_data", "public"):
        _replace_fk(schema_name, "databases", "databases_datasource_id_fkey", "CASCADE")
        _replace_fk(schema_name, "scan_runs", "scan_runs_datasource_id_fkey", "CASCADE")
        _replace_fk(schema_name, "lineage_nodes", "lineage_nodes_datasource_id_fkey", "CASCADE")


def downgrade() -> None:
    for schema_name in ("t2c_data", "public"):
        _replace_fk(schema_name, "databases", "databases_datasource_id_fkey", "CASCADE")
        _replace_fk(schema_name, "scan_runs", "scan_runs_datasource_id_fkey", "CASCADE")
        _replace_fk(schema_name, "lineage_nodes", "lineage_nodes_datasource_id_fkey", "SET NULL")
