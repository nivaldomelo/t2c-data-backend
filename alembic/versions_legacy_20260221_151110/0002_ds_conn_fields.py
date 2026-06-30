"""replace datasource uri with structured connection fields

Revision ID: 0002_ds_conn_fields
Revises: 0001_initial_schema
Create Date: 2026-02-20 00:00:00

"""

from alembic import op
import sqlalchemy as sa


revision = "0002_ds_conn_fields"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("data_sources", sa.Column("db_type", sa.String(length=20), nullable=True))
    op.add_column("data_sources", sa.Column("host", sa.String(length=255), nullable=True))
    op.add_column("data_sources", sa.Column("port", sa.Integer(), nullable=True))
    op.add_column("data_sources", sa.Column("database", sa.String(length=255), nullable=True))
    op.add_column("data_sources", sa.Column("username", sa.String(length=255), nullable=True))
    op.add_column("data_sources", sa.Column("password", sa.Text(), nullable=True))
    op.add_column("data_sources", sa.Column("include_schemas", sa.JSON(), nullable=True))
    op.add_column("data_sources", sa.Column("exclude_schemas", sa.JSON(), nullable=True))

    op.execute(
        """
        UPDATE data_sources
        SET
          db_type = 'postgres',
          host = 'localhost',
          port = 5432,
          database = 'postgres',
          username = 'postgres',
          password = 'change-me',
          include_schemas = '[]'
        WHERE db_type IS NULL
        """
    )

    op.alter_column("data_sources", "db_type", nullable=False)
    op.alter_column("data_sources", "host", nullable=False)
    op.alter_column("data_sources", "port", nullable=False)
    op.alter_column("data_sources", "database", nullable=False)
    op.alter_column("data_sources", "username", nullable=False)
    op.alter_column("data_sources", "password", nullable=False)
    op.alter_column("data_sources", "include_schemas", nullable=False)

    op.drop_column("data_sources", "type")
    op.drop_column("data_sources", "connection_uri")


def downgrade() -> None:
    op.add_column("data_sources", sa.Column("type", sa.String(length=30), nullable=True))
    op.add_column("data_sources", sa.Column("connection_uri", sa.Text(), nullable=True))

    op.execute(
        """
        UPDATE data_sources
        SET
          type = db_type,
          connection_uri = 'postgresql://***:***@' || host || ':' || port || '/' || database
        WHERE type IS NULL
        """
    )

    op.alter_column("data_sources", "type", nullable=False)
    op.alter_column("data_sources", "connection_uri", nullable=False)

    op.drop_column("data_sources", "exclude_schemas")
    op.drop_column("data_sources", "include_schemas")
    op.drop_column("data_sources", "password")
    op.drop_column("data_sources", "username")
    op.drop_column("data_sources", "database")
    op.drop_column("data_sources", "port")
    op.drop_column("data_sources", "host")
    op.drop_column("data_sources", "db_type")
