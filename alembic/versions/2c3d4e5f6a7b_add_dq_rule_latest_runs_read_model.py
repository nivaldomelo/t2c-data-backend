"""add dq rule latest runs read model

Revision ID: 2c3d4e5f6a7b
Revises: 1b2c3d4e5f6a
Create Date: 2026-05-25 16:10:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session


revision: str = "2c3d4e5f6a7b"
down_revision: Union[str, Sequence[str], None] = "1b2c3d4e5f6a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "dq_rule_latest_runs",
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("latest_rule_run_id", sa.Integer(), nullable=True),
        sa.Column("latest_job_run_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["latest_job_run_id"], ["t2c_data.dq_job_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["latest_rule_run_id"], ["t2c_data.dq_rule_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["rule_id"], ["t2c_data.dq_rules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["table_id"], ["tables.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("rule_id"),
        schema="t2c_data",
    )
    op.create_index(
        "ix_dq_rule_latest_runs_table_id",
        "dq_rule_latest_runs",
        ["table_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_dq_rule_latest_runs_table_rule",
        "dq_rule_latest_runs",
        ["table_id", "rule_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_dq_rule_latest_runs_latest_rule_run",
        "dq_rule_latest_runs",
        ["latest_rule_run_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_dq_rule_latest_runs_latest_job_run",
        "dq_rule_latest_runs",
        ["latest_job_run_id"],
        unique=False,
        schema="t2c_data",
    )

    bind = op.get_bind()
    session = Session(bind=bind)
    try:
        from t2c_data.features.data_quality.latest_runs import backfill_latest_rule_runs

        backfill_latest_rule_runs(session)
        session.commit()
    finally:
        session.close()


def downgrade() -> None:
    op.drop_index("ix_dq_rule_latest_runs_latest_job_run", table_name="dq_rule_latest_runs", schema="t2c_data")
    op.drop_index("ix_dq_rule_latest_runs_latest_rule_run", table_name="dq_rule_latest_runs", schema="t2c_data")
    op.drop_index("ix_dq_rule_latest_runs_table_rule", table_name="dq_rule_latest_runs", schema="t2c_data")
    op.drop_index("ix_dq_rule_latest_runs_table_id", table_name="dq_rule_latest_runs", schema="t2c_data")
    op.drop_table("dq_rule_latest_runs", schema="t2c_data")
