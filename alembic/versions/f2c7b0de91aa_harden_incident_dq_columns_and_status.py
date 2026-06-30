"""harden incident dq columns and status

Revision ID: f2c7b0de91aa
Revises: d1a8f9c77b21
Create Date: 2026-02-22 21:20:00.000000
"""

from alembic import op


revision = "f2c7b0de91aa"
down_revision = "d1a8f9c77b21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "t2c_ops"')

    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_type VARCHAR(30)")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_ref_id INTEGER")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS evidence_json JSONB")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS occurrences INTEGER")
    op.execute("ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ")

    op.execute("UPDATE t2c_ops.incidents SET status = 'investigating' WHERE status = 'in_progress'")
    op.execute("UPDATE t2c_ops.incidents SET occurrences = 1 WHERE occurrences IS NULL")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET DEFAULT 1")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET NOT NULL")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN status SET DEFAULT 'open'")

    # Keep status values aligned with Python enum literals.
    op.execute("ALTER TABLE t2c_ops.incidents DROP CONSTRAINT IF EXISTS incident_status")
    op.execute(
        "ALTER TABLE t2c_ops.incidents "
        "ADD CONSTRAINT incident_status CHECK (status IN ('open','investigating','mitigated','resolved','closed'))"
    )

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_incidents_source_ref "
        "ON t2c_ops.incidents (source_type, source_ref_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_incidents_source_ref_status "
        "ON t2c_ops.incidents (source_type, source_ref_id, status)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_incidents_status_detected_at "
        "ON t2c_ops.incidents (status, detected_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS t2c_ops.ix_incidents_status_detected_at")
    op.execute("DROP INDEX IF EXISTS t2c_ops.ix_incidents_source_ref")
    # Keep ix_incidents_source_ref_status because previous revision also created it.

    op.execute("ALTER TABLE t2c_ops.incidents DROP CONSTRAINT IF EXISTS incident_status")
    op.execute(
        "ALTER TABLE t2c_ops.incidents "
        "ADD CONSTRAINT incident_status CHECK (status IN ('open','investigating','mitigated','resolved','closed'))"
    )

