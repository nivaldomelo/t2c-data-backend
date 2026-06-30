"""add incidents table in t2c_ops

Revision ID: 5e0cb6f8c1a2
Revises: e0a4d2c3b901
Create Date: 2026-02-21 21:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5e0cb6f8c1a2"
down_revision = "e0a4d2c3b901"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "t2c_ops"')

    op.create_table(
        "incidents",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "entity_type",
            sa.Enum("table", "airflow_dag", name="incident_entity_type", native_enum=False),
            nullable=False,
        ),
        sa.Column("table_fqn", sa.String(length=500), nullable=True),
        sa.Column("airflow_dag_id", sa.String(length=255), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "open",
                "investigating",
                "mitigated",
                "resolved",
                "closed",
                name="incident_status",
                native_enum=False,
            ),
            server_default="open",
            nullable=False,
        ),
        sa.Column(
            "severity",
            sa.Enum("sev1", "sev2", "sev3", "sev4", name="incident_severity", native_enum=False),
            server_default="sev3",
            nullable=False,
        ),
        sa.Column("owner_user_id", sa.Integer(), nullable=True),
        sa.Column("reporter_user_id", sa.Integer(), nullable=True),
        sa.Column("tags", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["owner_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["reporter_user_id"], ["t2c_data.users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        schema="t2c_ops",
    )

    op.create_index("ix_incidents_status", "incidents", ["status"], schema="t2c_ops")
    op.create_index("ix_incidents_severity", "incidents", ["severity"], schema="t2c_ops")
    op.create_index("ix_incidents_entity_type", "incidents", ["entity_type"], schema="t2c_ops")
    op.create_index("ix_incidents_detected_at", "incidents", ["detected_at"], schema="t2c_ops")
    op.create_index("ix_incidents_owner_user_id", "incidents", ["owner_user_id"], schema="t2c_ops")
    op.create_index("ix_incidents_table_fqn", "incidents", ["table_fqn"], schema="t2c_ops")
    op.create_index("ix_incidents_airflow_dag_id", "incidents", ["airflow_dag_id"], schema="t2c_ops")


def downgrade() -> None:
    op.drop_index("ix_incidents_airflow_dag_id", table_name="incidents", schema="t2c_ops")
    op.drop_index("ix_incidents_table_fqn", table_name="incidents", schema="t2c_ops")
    op.drop_index("ix_incidents_owner_user_id", table_name="incidents", schema="t2c_ops")
    op.drop_index("ix_incidents_detected_at", table_name="incidents", schema="t2c_ops")
    op.drop_index("ix_incidents_entity_type", table_name="incidents", schema="t2c_ops")
    op.drop_index("ix_incidents_severity", table_name="incidents", schema="t2c_ops")
    op.drop_index("ix_incidents_status", table_name="incidents", schema="t2c_ops")
    op.drop_table("incidents", schema="t2c_ops")
