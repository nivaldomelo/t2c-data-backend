"""add dq profiling intelligence tables

Revision ID: i1a2b3c4d5f7
Revises: h1a2b3c4d5f6
Create Date: 2026-05-28 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "i1a2b3c4d5f7"
down_revision = "h1a2b3c4d5f6"
branch_labels = None
depends_on = None

SCHEMA = "t2c_data"


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return _inspector().has_table(table_name, schema=SCHEMA)


def _has_index(table_name: str, index_name: str) -> bool:
    try:
        return any(index["name"] == index_name for index in _inspector().get_indexes(table_name, schema=SCHEMA))
    except Exception:
        return False


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    if not _has_index(table_name, index_name):
        op.create_index(index_name, table_name, columns, schema=SCHEMA)


def upgrade() -> None:
    if not _has_table("dq_profile_runs"):
        op.create_table(
            "dq_profile_runs",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("dq_run_id", sa.Integer(), sa.ForeignKey("dq_runs.id", ondelete="SET NULL"), nullable=True),
            sa.Column("job_id", sa.Integer(), sa.ForeignKey("dq_job_runs.id", ondelete="SET NULL"), nullable=True),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
            sa.Column("datasource_id", sa.Integer(), sa.ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True),
            sa.Column("schema_name", sa.String(length=255), nullable=False),
            sa.Column("table_name", sa.String(length=200), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False, server_default=sa.text("'success'")),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("duration_seconds", sa.BigInteger(), nullable=True),
            sa.Column("row_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("column_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("sampled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("sample_ratio", sa.Float(), nullable=True),
            sa.Column("execution_engine", sa.String(length=20), nullable=False, server_default=sa.text("'spark'")),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
            sa.Column("trigger_type", sa.String(length=20), nullable=False, server_default=sa.text("'system'")),
            sa.Column("profile_summary_json", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint("dq_run_id", name="uq_dq_profile_runs_dq_run_id"),
            schema=SCHEMA,
        )
    _create_index_if_missing("ix_dq_profile_runs_table_status_started", "dq_profile_runs", ["table_id", "status", "started_at"])
    _create_index_if_missing(
        "ix_dq_profile_runs_datasource_status_started",
        "dq_profile_runs",
        ["datasource_id", "status", "started_at"],
    )
    _create_index_if_missing("ix_dq_profile_runs_trigger_type", "dq_profile_runs", ["trigger_type"])

    if not _has_table("dq_profile_table_metrics"):
        op.create_table(
            "dq_profile_table_metrics",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("profile_run_id", sa.Integer(), sa.ForeignKey("t2c_data.dq_profile_runs.id", ondelete="CASCADE"), nullable=False),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
            sa.Column("row_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("column_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("duplicate_rows_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("duplicate_business_key_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("schema_hash", sa.String(length=64), nullable=True),
            sa.Column("schema_drift_detected", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("freshness_seconds", sa.BigInteger(), nullable=True),
            sa.Column("volume_change_ratio", sa.Float(), nullable=True),
            sa.Column("quality_score", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("observed_score", sa.Float(), nullable=True),
            sa.Column("formal_score", sa.Float(), nullable=True),
            sa.Column("coverage_score", sa.Float(), nullable=True),
            sa.Column("score_breakdown_json", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint("profile_run_id", "table_id", name="uq_dq_profile_table_metrics_run_table"),
            schema=SCHEMA,
        )
    _create_index_if_missing("ix_dq_profile_table_metrics_table_id", "dq_profile_table_metrics", ["table_id"])
    _create_index_if_missing("ix_dq_profile_table_metrics_quality_score", "dq_profile_table_metrics", ["quality_score"])

    if not _has_table("dq_profile_column_metrics"):
        op.create_table(
            "dq_profile_column_metrics",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("profile_run_id", sa.Integer(), sa.ForeignKey("t2c_data.dq_profile_runs.id", ondelete="CASCADE"), nullable=False),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
            sa.Column("column_id", sa.Integer(), sa.ForeignKey("columns.id", ondelete="SET NULL"), nullable=True),
            sa.Column("column_name", sa.String(length=255), nullable=False),
            sa.Column("data_type", sa.String(length=255), nullable=False),
            sa.Column("inferred_type", sa.String(length=80), nullable=True),
            sa.Column("expected_type", sa.String(length=80), nullable=True),
            sa.Column("type_mismatch", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("null_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("null_ratio", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("fill_ratio", sa.Float(), nullable=False, server_default=sa.text("100")),
            sa.Column("distinct_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("distinct_ratio", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("cardinality_level", sa.String(length=20), nullable=True),
            sa.Column("min_value_masked", sa.Text(), nullable=True),
            sa.Column("max_value_masked", sa.Text(), nullable=True),
            sa.Column("mean_value", sa.Float(), nullable=True),
            sa.Column("median_value", sa.Float(), nullable=True),
            sa.Column("stddev_value", sa.Float(), nullable=True),
            sa.Column("top_values_json_masked", sa.JSON(), nullable=True),
            sa.Column("pattern_type", sa.String(length=80), nullable=True),
            sa.Column("pattern_confidence", sa.Float(), nullable=True),
            sa.Column("outlier_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("duplicate_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
            sa.Column("sensitive_guess", sa.String(length=80), nullable=True),
            sa.Column("examples_masked_json", sa.JSON(), nullable=True),
            sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint("profile_run_id", "table_id", "column_name", name="uq_dq_profile_column_metrics_unique"),
            schema=SCHEMA,
        )
    _create_index_if_missing("ix_dq_profile_column_metrics_table_id", "dq_profile_column_metrics", ["table_id"])
    _create_index_if_missing("ix_dq_profile_column_metrics_column_id", "dq_profile_column_metrics", ["column_id"])
    _create_index_if_missing("ix_dq_profile_column_metrics_pattern_type", "dq_profile_column_metrics", ["pattern_type"])

    if not _has_table("dq_rule_suggestions"):
        op.create_table(
            "dq_rule_suggestions",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("profile_run_id", sa.Integer(), sa.ForeignKey("t2c_data.dq_profile_runs.id", ondelete="CASCADE"), nullable=False),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("tables.id", ondelete="CASCADE"), nullable=False),
            sa.Column("column_id", sa.Integer(), sa.ForeignKey("columns.id", ondelete="SET NULL"), nullable=True),
            sa.Column("column_name", sa.String(length=255), nullable=True),
            sa.Column("dimension", sa.String(length=40), nullable=False),
            sa.Column("suggested_rule_type", sa.String(length=80), nullable=False),
            sa.Column("rule_definition_json", sa.JSON(), nullable=True),
            sa.Column("confidence_score", sa.Float(), nullable=False, server_default=sa.text("0")),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("status", sa.String(length=20), nullable=False, server_default=sa.text("'suggested'")),
            sa.Column("reviewed_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
            sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint(
                "profile_run_id",
                "column_name",
                "suggested_rule_type",
                "dimension",
                name="uq_dq_rule_suggestions_unique",
            ),
            schema=SCHEMA,
        )
    _create_index_if_missing("ix_dq_rule_suggestions_table_id", "dq_rule_suggestions", ["table_id"])
    _create_index_if_missing("ix_dq_rule_suggestions_status", "dq_rule_suggestions", ["status"])
    _create_index_if_missing("ix_dq_rule_suggestions_dimension", "dq_rule_suggestions", ["dimension"])

    if not _has_table("dq_score_weight_profiles"):
        op.create_table(
            "dq_score_weight_profiles",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("name", sa.String(length=120), nullable=False),
            sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("applies_to_domain", sa.String(length=80), nullable=True),
            sa.Column("applies_to_criticality", sa.String(length=40), nullable=True),
            sa.Column("weights_json", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
            sa.UniqueConstraint("name", name="uq_dq_score_weight_profiles_name"),
            schema=SCHEMA,
        )
    _create_index_if_missing("ix_dq_score_weight_profiles_is_default", "dq_score_weight_profiles", ["is_default"])
    _create_index_if_missing("ix_dq_score_weight_profiles_domain", "dq_score_weight_profiles", ["applies_to_domain"])
    _create_index_if_missing("ix_dq_score_weight_profiles_criticality", "dq_score_weight_profiles", ["applies_to_criticality"])


def downgrade() -> None:
    for index_name, table_name in [
        ("ix_dq_score_weight_profiles_criticality", "dq_score_weight_profiles"),
        ("ix_dq_score_weight_profiles_domain", "dq_score_weight_profiles"),
        ("ix_dq_score_weight_profiles_is_default", "dq_score_weight_profiles"),
        ("ix_dq_rule_suggestions_dimension", "dq_rule_suggestions"),
        ("ix_dq_rule_suggestions_status", "dq_rule_suggestions"),
        ("ix_dq_rule_suggestions_table_id", "dq_rule_suggestions"),
        ("ix_dq_profile_column_metrics_pattern_type", "dq_profile_column_metrics"),
        ("ix_dq_profile_column_metrics_column_id", "dq_profile_column_metrics"),
        ("ix_dq_profile_column_metrics_table_id", "dq_profile_column_metrics"),
        ("ix_dq_profile_table_metrics_quality_score", "dq_profile_table_metrics"),
        ("ix_dq_profile_table_metrics_table_id", "dq_profile_table_metrics"),
        ("ix_dq_profile_runs_trigger_type", "dq_profile_runs"),
        ("ix_dq_profile_runs_datasource_status_started", "dq_profile_runs"),
        ("ix_dq_profile_runs_table_status_started", "dq_profile_runs"),
    ]:
        try:
            if _has_index(table_name, index_name):
                op.drop_index(index_name, table_name=table_name, schema=SCHEMA)
        except Exception:
            pass

    for table_name in [
        "dq_score_weight_profiles",
        "dq_rule_suggestions",
        "dq_profile_column_metrics",
        "dq_profile_table_metrics",
        "dq_profile_runs",
    ]:
        if _has_table(table_name):
            op.drop_table(table_name, schema=SCHEMA)

