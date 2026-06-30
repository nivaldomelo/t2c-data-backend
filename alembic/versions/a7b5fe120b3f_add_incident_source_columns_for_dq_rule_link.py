"""add incident source columns for dq rule link

Revision ID: a7b5fe120b3f
Revises: f2c7b0de91aa
Create Date: 2026-02-22 22:05:00.000000
"""

from alembic import op


revision = "a7b5fe120b3f"
down_revision = "f2c7b0de91aa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "t2c_ops"')

    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_type VARCHAR(50)")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_ref_id INTEGER")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS evidence_json JSONB")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS occurrences INTEGER")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ")

    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN source_type TYPE VARCHAR(50)")
    op.execute("UPDATE t2c_ops.incidents SET occurrences = 1 WHERE occurrences IS NULL")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET DEFAULT 1")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET NOT NULL")

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_incidents_source_ref "
        "ON t2c_ops.incidents (source_type, source_ref_id)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS t2c_ops.ix_incidents_source_ref")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN source_type TYPE VARCHAR(30)")
