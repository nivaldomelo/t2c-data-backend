"""add dq observability historical artifacts

Revision ID: 9a7b6c5d4e3f
Revises: fa0b1c2d3e4f
Create Date: 2026-04-13 20:45:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "9a7b6c5d4e3f"
down_revision: Union[str, Sequence[str], None] = "fa0b1c2d3e4f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "dq_observability_baselines",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(), sa.ForeignKey("t2c_data.dq_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("column_id", sa.Integer(), sa.ForeignKey("t2c_data.columns.id", ondelete="SET NULL"), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("metric_key", sa.String(length=120), nullable=False),
        sa.Column("metric_scope", sa.String(length=30), nullable=False, server_default="table"),
        sa.Column("current_value", sa.Float(), nullable=True),
        sa.Column("baseline_value", sa.Float(), nullable=True),
        sa.Column("mean_value", sa.Float(), nullable=True),
        sa.Column("median_value", sa.Float(), nullable=True),
        sa.Column("min_value", sa.Float(), nullable=True),
        sa.Column("max_value", sa.Float(), nullable=True),
        sa.Column("tolerance_abs", sa.Float(), nullable=True),
        sa.Column("tolerance_pct", sa.Float(), nullable=True),
        sa.Column("window_size", sa.Integer(), nullable=False, server_default="14"),
        sa.Column("calculated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("run_id", "metric_key", "column_name", name="uq_dq_observability_baseline_run_metric"),
        schema="t2c_data",
    )
    op.create_index("ix_dq_observability_baselines_run_id", "dq_observability_baselines", ["run_id"], schema="t2c_data")
    op.create_index("ix_dq_observability_baselines_table_id", "dq_observability_baselines", ["table_id"], schema="t2c_data")
    op.create_index("ix_dq_observability_baselines_column_id", "dq_observability_baselines", ["column_id"], schema="t2c_data")
    op.create_index("ix_dq_observability_baselines_metric_key", "dq_observability_baselines", ["metric_key"], schema="t2c_data")
    op.create_index(
        "ix_dq_observability_baselines_table_metric",
        "dq_observability_baselines",
        ["table_id", "metric_key"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_dq_observability_baselines_table_column_metric",
        "dq_observability_baselines",
        ["table_id", "column_name", "metric_key"],
        schema="t2c_data",
    )

    op.create_table(
        "dq_observability_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(), sa.ForeignKey("t2c_data.dq_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("column_id", sa.Integer(), sa.ForeignKey("t2c_data.columns.id", ondelete="SET NULL"), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("metric_key", sa.String(length=120), nullable=False),
        sa.Column("dimension_key", sa.String(length=80), nullable=True),
        sa.Column("event_type", sa.String(length=30), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="open"),
        sa.Column("severity", sa.String(length=20), nullable=False, server_default="warning"),
        sa.Column("observed_value", sa.Float(), nullable=True),
        sa.Column("expected_value", sa.Float(), nullable=True),
        sa.Column("baseline_value", sa.Float(), nullable=True),
        sa.Column("delta_value", sa.Float(), nullable=True),
        sa.Column("delta_pct", sa.Float(), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_index("ix_dq_observability_events_run_id", "dq_observability_events", ["run_id"], schema="t2c_data")
    op.create_index("ix_dq_observability_events_table_id", "dq_observability_events", ["table_id"], schema="t2c_data")
    op.create_index("ix_dq_observability_events_column_id", "dq_observability_events", ["column_id"], schema="t2c_data")
    op.create_index("ix_dq_observability_events_metric_key", "dq_observability_events", ["metric_key"], schema="t2c_data")
    op.create_index("ix_dq_observability_events_dimension_key", "dq_observability_events", ["dimension_key"], schema="t2c_data")
    op.create_index("ix_dq_observability_events_status", "dq_observability_events", ["status"], schema="t2c_data")
    op.create_index("ix_dq_observability_events_type", "dq_observability_events", ["event_type"], schema="t2c_data")
    op.create_index(
        "ix_dq_observability_events_table_metric",
        "dq_observability_events",
        ["table_id", "metric_key"],
        schema="t2c_data",
    )
    op.create_index(
        "ix_dq_observability_events_table_detected",
        "dq_observability_events",
        ["table_id", "detected_at"],
        schema="t2c_data",
    )

    op.create_table(
        "dq_evidence_samples",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("dq_run_id", sa.Integer(), sa.ForeignKey("t2c_data.dq_runs.id", ondelete="CASCADE"), nullable=True),
        sa.Column("rule_run_id", sa.Integer(), nullable=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("t2c_data.tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("column_id", sa.Integer(), sa.ForeignKey("t2c_data.columns.id", ondelete="SET NULL"), nullable=True),
        sa.Column("rule_id", sa.Integer(), nullable=True),
        sa.Column("evidence_type", sa.String(length=40), nullable=False),
        sa.Column("origin", sa.String(length=40), nullable=False, server_default="dq"),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="masked"),
        sa.Column("sample_size", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("affected_rows_count", sa.Integer(), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("sample_rows_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("masked_fields_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_index("ix_dq_evidence_samples_dq_run", "dq_evidence_samples", ["dq_run_id"], schema="t2c_data")
    op.create_index("ix_dq_evidence_samples_rule_run", "dq_evidence_samples", ["rule_run_id"], schema="t2c_data")
    op.create_index("ix_dq_evidence_samples_table_id", "dq_evidence_samples", ["table_id"], schema="t2c_data")
    op.create_index("ix_dq_evidence_samples_column_id", "dq_evidence_samples", ["column_id"], schema="t2c_data")
    op.create_index("ix_dq_evidence_samples_rule", "dq_evidence_samples", ["rule_id"], schema="t2c_data")
    op.create_index("ix_dq_evidence_samples_type", "dq_evidence_samples", ["evidence_type"], schema="t2c_data")
    op.create_index("ix_dq_evidence_samples_table_created", "dq_evidence_samples", ["table_id", "created_at"], schema="t2c_data")


def downgrade() -> None:
    op.drop_index("ix_dq_evidence_samples_table_created", table_name="dq_evidence_samples", schema="t2c_data")
    op.drop_index("ix_dq_evidence_samples_type", table_name="dq_evidence_samples", schema="t2c_data")
    op.drop_index("ix_dq_evidence_samples_rule", table_name="dq_evidence_samples", schema="t2c_data")
    op.drop_index("ix_dq_evidence_samples_column_id", table_name="dq_evidence_samples", schema="t2c_data")
    op.drop_index("ix_dq_evidence_samples_table_id", table_name="dq_evidence_samples", schema="t2c_data")
    op.drop_index("ix_dq_evidence_samples_rule_run", table_name="dq_evidence_samples", schema="t2c_data")
    op.drop_index("ix_dq_evidence_samples_dq_run", table_name="dq_evidence_samples", schema="t2c_data")
    op.drop_table("dq_evidence_samples", schema="t2c_data")

    op.drop_index("ix_dq_observability_events_table_detected", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_table_metric", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_type", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_status", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_dimension_key", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_metric_key", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_column_id", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_table_id", table_name="dq_observability_events", schema="t2c_data")
    op.drop_index("ix_dq_observability_events_run_id", table_name="dq_observability_events", schema="t2c_data")
    op.drop_table("dq_observability_events", schema="t2c_data")

    op.drop_index("ix_dq_observability_baselines_table_column_metric", table_name="dq_observability_baselines", schema="t2c_data")
    op.drop_index("ix_dq_observability_baselines_table_metric", table_name="dq_observability_baselines", schema="t2c_data")
    op.drop_index("ix_dq_observability_baselines_metric_key", table_name="dq_observability_baselines", schema="t2c_data")
    op.drop_index("ix_dq_observability_baselines_column_id", table_name="dq_observability_baselines", schema="t2c_data")
    op.drop_index("ix_dq_observability_baselines_table_id", table_name="dq_observability_baselines", schema="t2c_data")
    op.drop_index("ix_dq_observability_baselines_run_id", table_name="dq_observability_baselines", schema="t2c_data")
    op.drop_table("dq_observability_baselines", schema="t2c_data")
