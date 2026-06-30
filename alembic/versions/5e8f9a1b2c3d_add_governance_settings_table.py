"""add governance settings table

Revision ID: 5e8f9a1b2c3d
Revises: 4d7e8f9a1b2c
Create Date: 2026-03-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "5e8f9a1b2c3d"
down_revision = "4d7e8f9a1b2c"
branch_labels = None
depends_on = None


def _sync_pk_sequence(schema: str, table_name: str, column_name: str = "id") -> None:
    op.execute(
        sa.text(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{schema}.{table_name}', '{column_name}'),
                GREATEST((SELECT COALESCE(MAX({column_name}), 0) FROM {schema}.{table_name}), 1),
                (SELECT COALESCE(MAX({column_name}), 0) > 0 FROM {schema}.{table_name})
            )
            """
        )
    )


def upgrade() -> None:
    op.create_table(
        "governance_settings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("owner_review_interval_days", sa.Integer(), server_default="90", nullable=False),
        sa.Column("privacy_review_interval_days", sa.Integer(), server_default="180", nullable=False),
        sa.Column("sensitive_privacy_review_interval_days", sa.Integer(), server_default="90", nullable=False),
        sa.Column("certification_review_interval_days", sa.Integer(), server_default="180", nullable=False),
        sa.Column("certification_review_sla_days", sa.Integer(), server_default="7", nullable=False),
        sa.Column("certification_revalidation_window_days", sa.Integer(), server_default="30", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=settings.db_schema,
    )
    op.execute(
        sa.text(
            f"""
            INSERT INTO {settings.db_schema}.governance_settings (
                id,
                owner_review_interval_days,
                privacy_review_interval_days,
                sensitive_privacy_review_interval_days,
                certification_review_interval_days,
                certification_review_sla_days,
                certification_revalidation_window_days
            )
            VALUES (1, 90, 180, 90, 180, 7, 30)
            ON CONFLICT (id) DO NOTHING
            """
        )
    )
    _sync_pk_sequence(settings.db_schema, "governance_settings")


def downgrade() -> None:
    op.drop_table("governance_settings", schema=settings.db_schema)
