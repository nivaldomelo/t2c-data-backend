"""add governance trust snapshots

Revision ID: fe1a2b3c4d5e
Revises: fa0b1c2d3e4f
Create Date: 2026-04-10 13:10:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "fe1a2b3c4d5e"
down_revision: Union[str, Sequence[str], None] = "fa0b1c2d3e4f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "governance_trust_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "datasource_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.data_sources.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("owner_name", sa.String(length=255), nullable=True),
        sa.Column("domain_label", sa.String(length=255), nullable=True),
        sa.Column("score", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(length=40), nullable=False),
        sa.Column("tone", sa.String(length=20), nullable=False),
        sa.Column("readiness_score", sa.Integer(), nullable=False),
        sa.Column("governance_score", sa.Integer(), nullable=False),
        sa.Column("operational_score", sa.Integer(), nullable=False),
        sa.Column("dq_score", sa.Float(), nullable=True),
        sa.Column("open_incidents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("critical_open_incidents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("active_dq_violation", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("recent_dq_failure_runs_30d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("trust_context_json", sa.JSON(), nullable=True),
        sa.Column("bucket_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_unique_constraint(
        "uq_governance_trust_snapshot_table_bucket",
        "governance_trust_snapshots",
        ["table_id", "bucket_date"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_trust_snapshots_bucket_date",
        "governance_trust_snapshots",
        ["bucket_date"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_trust_snapshots_table_bucket",
        "governance_trust_snapshots",
        ["table_id", "bucket_date"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_governance_trust_snapshots_score",
        "governance_trust_snapshots",
        ["score"],
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_governance_trust_snapshots_score", table_name="governance_trust_snapshots", schema="t2c_data")
    op.drop_index("ix_governance_trust_snapshots_table_bucket", table_name="governance_trust_snapshots", schema="t2c_data")
    op.drop_index("ix_governance_trust_snapshots_bucket_date", table_name="governance_trust_snapshots", schema="t2c_data")
    op.drop_constraint(
        "uq_governance_trust_snapshot_table_bucket",
        "governance_trust_snapshots",
        schema="t2c_data",
        type_="unique",
    )
    op.drop_table("governance_trust_snapshots", schema="t2c_data")
