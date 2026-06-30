"""fix datasource type to db_type rename

Revision ID: 0003_fix_db_type_rename
Revises: 0002_ds_conn_fields
Create Date: 2026-02-20 00:00:01

"""

from alembic import op
import sqlalchemy as sa


revision = "0003_fix_db_type_rename"
down_revision = "0002_ds_conn_fields"
branch_labels = None
depends_on = None


def _columns() -> set[str]:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return {col["name"] for col in inspector.get_columns("data_sources")}


def upgrade() -> None:
    cols = _columns()

    if "type" in cols and "db_type" not in cols:
        op.alter_column("data_sources", "type", new_column_name="db_type")
        cols = _columns()

    if "db_type" not in cols:
        op.add_column("data_sources", sa.Column("db_type", sa.String(length=20), nullable=True))
        cols = _columns()

    if "type" in cols:
        op.execute(
            """
            UPDATE data_sources
            SET db_type = type
            WHERE db_type IS NULL OR db_type = ''
            """
        )
        op.drop_column("data_sources", "type")

    op.execute(
        """
        UPDATE data_sources
        SET db_type = 'postgres'
        WHERE db_type IS NULL OR db_type = ''
        """
    )
    op.alter_column("data_sources", "db_type", existing_type=sa.String(length=20), nullable=False)


def downgrade() -> None:
    cols = _columns()

    if "type" not in cols:
        op.add_column("data_sources", sa.Column("type", sa.String(length=30), nullable=True))

    if "db_type" in cols:
        op.execute(
            """
            UPDATE data_sources
            SET type = db_type
            WHERE type IS NULL OR type = ''
            """
        )
