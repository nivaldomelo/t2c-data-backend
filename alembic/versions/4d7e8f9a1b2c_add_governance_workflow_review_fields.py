"""add governance workflow review fields

Revision ID: 4d7e8f9a1b2c
Revises: 3c4d5e6f7a8b
Create Date: 2026-03-28 11:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "4d7e8f9a1b2c"
down_revision = "3c4d5e6f7a8b"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.add_column(
        "tables",
        sa.Column("certification_submitted_by_user_id", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "tables",
        sa.Column("certification_submitted_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "tables",
        sa.Column("certification_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "tables",
        sa.Column("owner_reviewed_by_user_id", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "tables",
        sa.Column("owner_reviewed_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.execute(sa.text(f"UPDATE {SCHEMA}.tables SET certification_status = 'not_eligible' WHERE certification_status = 'not_assessed'"))
    op.create_foreign_key(
        "fk_tables_certification_submitted_by_user_id",
        "tables",
        "users",
        ["certification_submitted_by_user_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_tables_owner_reviewed_by_user_id",
        "tables",
        "users",
        ["owner_reviewed_by_user_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )
    op.create_index("ix_tables_owner_reviewed_at", "tables", ["owner_reviewed_at"], unique=False, schema=SCHEMA)
    op.create_index("ix_tables_certification_review_at", "tables", ["certification_review_at"], unique=False, schema=SCHEMA)
    op.create_index("ix_tables_certification_expires_at", "tables", ["certification_expires_at"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_tables_certification_expires_at", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_certification_review_at", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_owner_reviewed_at", table_name="tables", schema=SCHEMA)
    op.drop_constraint("fk_tables_owner_reviewed_by_user_id", "tables", schema=SCHEMA, type_="foreignkey")
    op.drop_constraint("fk_tables_certification_submitted_by_user_id", "tables", schema=SCHEMA, type_="foreignkey")
    op.drop_column("tables", "owner_reviewed_at", schema=SCHEMA)
    op.drop_column("tables", "owner_reviewed_by_user_id", schema=SCHEMA)
    op.drop_column("tables", "certification_expires_at", schema=SCHEMA)
    op.drop_column("tables", "certification_submitted_at", schema=SCHEMA)
    op.drop_column("tables", "certification_submitted_by_user_id", schema=SCHEMA)
    op.execute(sa.text(f"UPDATE {SCHEMA}.tables SET certification_status = 'not_assessed' WHERE certification_status = 'not_eligible'"))
