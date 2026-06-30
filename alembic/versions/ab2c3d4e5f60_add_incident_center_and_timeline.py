"""add incident center and timeline

Revision ID: ab2c3d4e5f60
Revises: c8e9f0a1b2c4
Create Date: 2026-04-17 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "ab2c3d4e5f60"
down_revision = "c8e9f0a1b2c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "t2c_ops"')

    for column_sql in (
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS triaged_at TIMESTAMPTZ",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS mitigated_at TIMESTAMPTZ",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS reopened_at TIMESTAMPTZ",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS sla_due_at TIMESTAMPTZ",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS technical_origin_json JSONB",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS related_links_json JSONB",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS impact_json JSONB",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS mitigation_json JSONB",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS postmortem_json JSONB",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS root_cause TEXT",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS impact_summary TEXT",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS mitigation_summary TEXT",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS postmortem_summary TEXT",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS domain_name VARCHAR(255)",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS owner_team VARCHAR(255)",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS squad_name VARCHAR(255)",
        "ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS recurrence_count INTEGER",
    ):
        op.execute(column_sql)

    op.execute("UPDATE t2c_ops.incidents SET recurrence_count = COALESCE(recurrence_count, 0)")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN recurrence_count SET DEFAULT 0")
    op.execute("ALTER TABLE t2c_ops.incidents ALTER COLUMN recurrence_count SET NOT NULL")

    op.execute("ALTER TABLE t2c_ops.incidents DROP CONSTRAINT IF EXISTS incident_status")
    op.execute(
        "ALTER TABLE t2c_ops.incidents "
        "ADD CONSTRAINT incident_status CHECK (status IN ('open','investigating','mitigated','resolved','closed','reopened','recurring'))"
    )

    op.create_table(
        "incident_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("incident_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=60), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("status_from", sa.String(length=40), nullable=True),
        sa.Column("status_to", sa.String(length=40), nullable=True),
        sa.Column("evidence_json", sa.JSON(), nullable=True),
        sa.Column("actor_user_id", sa.Integer(), nullable=True),
        sa.Column("actor_name", sa.String(length=255), nullable=True),
        sa.Column("actor_email", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["actor_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["incident_id"], ["t2c_ops.incidents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_ops",
    )
    op.create_index("ix_incident_events_incident_created", "incident_events", ["incident_id", "created_at"], schema="t2c_ops")
    op.create_index("ix_incident_events_event_type", "incident_events", ["event_type"], schema="t2c_ops")
    op.execute('CREATE INDEX IF NOT EXISTS ix_incidents_domain_name ON t2c_ops.incidents (domain_name)')
    op.execute('CREATE INDEX IF NOT EXISTS ix_incidents_owner_team ON t2c_ops.incidents (owner_team)')
    op.execute('CREATE INDEX IF NOT EXISTS ix_incidents_sla_due_at ON t2c_ops.incidents (sla_due_at)')


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS t2c_ops.ix_incidents_sla_due_at")
    op.execute("DROP INDEX IF EXISTS t2c_ops.ix_incidents_owner_team")
    op.execute("DROP INDEX IF EXISTS t2c_ops.ix_incidents_domain_name")
    op.drop_index("ix_incident_events_event_type", table_name="incident_events", schema="t2c_ops")
    op.drop_index("ix_incident_events_incident_created", table_name="incident_events", schema="t2c_ops")
    op.drop_table("incident_events", schema="t2c_ops")

    op.execute("ALTER TABLE t2c_ops.incidents DROP CONSTRAINT IF EXISTS incident_status")
    op.execute(
        "ALTER TABLE t2c_ops.incidents "
        "ADD CONSTRAINT incident_status CHECK (status IN ('open','investigating','mitigated','resolved','closed'))"
    )

    for column_sql in (
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS recurrence_count",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS squad_name",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS owner_team",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS domain_name",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS postmortem_summary",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS mitigation_summary",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS impact_summary",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS root_cause",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS postmortem_json",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS mitigation_json",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS impact_json",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS related_links_json",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS technical_origin_json",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS sla_due_at",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS reopened_at",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS closed_at",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS resolved_at",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS mitigated_at",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS triaged_at",
        "ALTER TABLE t2c_ops.incidents DROP COLUMN IF EXISTS acknowledged_at",
    ):
        op.execute(column_sql)
