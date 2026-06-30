"""add incident source fields for dq

Revision ID: d1a8f9c77b21
Revises: b742be9a32c1
Create Date: 2026-02-22 19:40:00.000000
"""

from alembic import op


revision = "d1a8f9c77b21"
down_revision = "b742be9a32c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "t2c_ops"')

    op.execute('ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_type VARCHAR(30)')
    op.execute('ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_ref_id INTEGER')
    op.execute('ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS evidence_json JSONB')
    op.execute('ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS occurrences INTEGER')

    op.execute('UPDATE t2c_ops.incidents SET occurrences = 1 WHERE occurrences IS NULL')
    op.execute('ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET DEFAULT 1')
    op.execute('ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET NOT NULL')

    op.execute(
        'CREATE INDEX IF NOT EXISTS ix_incidents_source_ref_status '
        'ON t2c_ops.incidents (source_type, source_ref_id, status)'
    )


def downgrade() -> None:
    op.execute('DROP INDEX IF EXISTS t2c_ops.ix_incidents_source_ref_status')

    with op.batch_alter_table('incidents', schema='t2c_ops') as batch_op:
        batch_op.drop_column('occurrences')
        batch_op.drop_column('evidence_json')
        batch_op.drop_column('source_ref_id')
        batch_op.drop_column('source_type')
