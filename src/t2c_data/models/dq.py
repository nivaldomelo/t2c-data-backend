from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, JSON, String, Table, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.auth import User
from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


dq_rule_notification_recipients = Table(
    "dq_rule_notification_recipients",
    Base.metadata,
    Column("rule_id", ForeignKey("t2c_data.dq_rules.id", ondelete="CASCADE"), primary_key=True),
    Column("user_id", ForeignKey("t2c_data.users.id", ondelete="CASCADE"), primary_key=True),
    schema="t2c_data",
)

dq_profiling_schedule_recipients = Table(
    "dq_profiling_schedule_recipients",
    Base.metadata,
    Column("schedule_id", ForeignKey("t2c_data.dq_profiling_schedules.id", ondelete="CASCADE"), primary_key=True),
    Column("user_id", ForeignKey("t2c_data.users.id", ondelete="CASCADE"), primary_key=True),
    schema="t2c_data",
)


class DQRun(TimestampMixin, Base):
    __tablename__ = "dq_runs"
    __table_args__ = (
        Index("ix_dq_runs_status_created", "status", "created_at"),
        Index("ix_dq_runs_table_status_created", "table_id", "status", "created_at"),
        Index("ix_dq_runs_datasource_status_created", "datasource_id", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=True, index=True
    )
    profiling_schedule_id: Mapped[int | None] = mapped_column(
        ForeignKey("t2c_data.dq_profiling_schedules.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=True, index=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False, default="table", index=True)
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    parent_run_id: Mapped[int | None] = mapped_column(ForeignKey("dq_runs.id", ondelete="CASCADE"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="success")
    execution_engine: Mapped[str] = mapped_column(String(20), nullable=False, default="spark")
    spark_app_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    log_tail: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text)
    profile_payload_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    profiling_schedule: Mapped["DQProfilingSchedule | None"] = relationship(
        "DQProfilingSchedule",
        back_populates="runs",
    )

    table_metrics: Mapped[list[DQTableMetric]] = relationship(
        "DQTableMetric",
        back_populates="run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    parent_run: Mapped[DQRun | None] = relationship(
        "DQRun",
        remote_side="DQRun.id",
        back_populates="child_runs",
    )
    child_runs: Mapped[list[DQRun]] = relationship(
        "DQRun",
        back_populates="parent_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DQTableMetric(TimestampMixin, Base):
    __tablename__ = "dq_table_metrics"
    __table_args__ = (UniqueConstraint("run_id", "table_id", name="uq_dq_table_metrics_run_table"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("dq_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    column_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completeness_pct_avg: Mapped[float] = mapped_column(Float, nullable=False, default=100.0)
    dq_score: Mapped[float] = mapped_column(Float, nullable=False, default=100.0)
    duplicates_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    failed_rules: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metrics_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    run: Mapped[DQRun] = relationship("DQRun", back_populates="table_metrics")
    column_metrics: Mapped[list[DQColumnMetric]] = relationship(
        "DQColumnMetric",
        back_populates="table_metric",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DQColumnMetric(TimestampMixin, Base):
    __tablename__ = "dq_column_metrics"
    __table_args__ = (UniqueConstraint("run_id", "table_metric_id", "column_name", name="uq_dq_column_metrics_unique"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("dq_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    table_metric_id: Mapped[int] = mapped_column(
        ForeignKey("dq_table_metrics.id", ondelete="CASCADE"), nullable=False, index=True
    )
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str] = mapped_column(String(255), nullable=False)
    null_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    distinct_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    null_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    min_value: Mapped[str | None] = mapped_column(Text)
    max_value: Mapped[str | None] = mapped_column(Text)

    table_metric: Mapped[DQTableMetric] = relationship("DQTableMetric", back_populates="column_metrics")


class DQProfileRun(TimestampMixin, Base):
    __tablename__ = "dq_profile_runs"
    __table_args__ = (
        Index("ix_dq_profile_runs_table_status_started", "table_id", "status", "started_at"),
        Index("ix_dq_profile_runs_datasource_status_started", "datasource_id", "status", "started_at"),
        Index("ix_dq_profile_runs_trigger_type", "trigger_type"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    dq_run_id: Mapped[int | None] = mapped_column(ForeignKey("dq_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("dq_job_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True, index=True)
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)
    table_name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="success", index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    duration_seconds: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    column_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sampled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sample_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    execution_engine: Mapped[str] = mapped_column(String(20), nullable=False, default="spark", index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    trigger_type: Mapped[str] = mapped_column(String(20), nullable=False, default="system", index=True)
    profile_summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    table: Mapped[TableEntity] = relationship("TableEntity")
    datasource: Mapped[DataSource | None] = relationship("DataSource")
    created_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[created_by_user_id])
    table_metrics: Mapped[list["DQProfileTableMetric"]] = relationship(
        "DQProfileTableMetric",
        back_populates="profile_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    column_metrics: Mapped[list["DQProfileColumnMetric"]] = relationship(
        "DQProfileColumnMetric",
        back_populates="profile_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    rule_suggestions: Mapped[list["DQRuleSuggestion"]] = relationship(
        "DQRuleSuggestion",
        back_populates="profile_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DQProfileTableMetric(TimestampMixin, Base):
    __tablename__ = "dq_profile_table_metrics"
    __table_args__ = (
        UniqueConstraint("profile_run_id", "table_id", name="uq_dq_profile_table_metrics_run_table"),
        Index("ix_dq_profile_table_metrics_table_id", "table_id"),
        Index("ix_dq_profile_table_metrics_quality_score", "quality_score"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    profile_run_id: Mapped[int] = mapped_column(ForeignKey("t2c_data.dq_profile_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    column_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_rows_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    duplicate_business_key_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    schema_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    schema_drift_detected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    freshness_seconds: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    volume_change_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    quality_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, index=True)
    observed_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    formal_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    coverage_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    score_breakdown_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    profile_run: Mapped[DQProfileRun] = relationship("DQProfileRun", back_populates="table_metrics")


class DQProfileColumnMetric(TimestampMixin, Base):
    __tablename__ = "dq_profile_column_metrics"
    __table_args__ = (
        UniqueConstraint("profile_run_id", "table_id", "column_name", name="uq_dq_profile_column_metrics_unique"),
        Index("ix_dq_profile_column_metrics_table_id", "table_id"),
        Index("ix_dq_profile_column_metrics_column_id", "column_id"),
        Index("ix_dq_profile_column_metrics_pattern_type", "pattern_type"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    profile_run_id: Mapped[int] = mapped_column(ForeignKey("t2c_data.dq_profile_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str] = mapped_column(String(255), nullable=False)
    inferred_type: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    expected_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    type_mismatch: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    null_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    null_ratio: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    fill_ratio: Mapped[float] = mapped_column(Float, nullable=False, default=100.0)
    distinct_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    distinct_ratio: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cardinality_level: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    min_value_masked: Mapped[str | None] = mapped_column(Text, nullable=True)
    max_value_masked: Mapped[str | None] = mapped_column(Text, nullable=True)
    mean_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    median_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    stddev_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    top_values_json_masked: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    pattern_type: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    pattern_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    outlier_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    duplicate_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    sensitive_guess: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    examples_masked_json: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    profile_run: Mapped[DQProfileRun] = relationship("DQProfileRun", back_populates="column_metrics")


class DQRuleSuggestion(TimestampMixin, Base):
    __tablename__ = "dq_rule_suggestions"
    __table_args__ = (
        UniqueConstraint("profile_run_id", "column_name", "suggested_rule_type", "dimension", name="uq_dq_rule_suggestions_unique"),
        Index("ix_dq_rule_suggestions_table_id", "table_id"),
        Index("ix_dq_rule_suggestions_status", "status"),
        Index("ix_dq_rule_suggestions_dimension", "dimension"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    profile_run_id: Mapped[int] = mapped_column(ForeignKey("t2c_data.dq_profile_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    dimension: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    suggested_rule_type: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    rule_definition_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="suggested", index=True)
    reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    profile_run: Mapped[DQProfileRun] = relationship("DQProfileRun", back_populates="rule_suggestions")
    reviewed_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[reviewed_by_user_id])
    created_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[created_by_user_id])


class DQScoreWeightProfile(TimestampMixin, Base):
    __tablename__ = "dq_score_weight_profiles"
    __table_args__ = (
        UniqueConstraint("name", name="uq_dq_score_weight_profiles_name"),
        Index("ix_dq_score_weight_profiles_is_default", "is_default"),
        Index("ix_dq_score_weight_profiles_domain", "applies_to_domain"),
        Index("ix_dq_score_weight_profiles_criticality", "applies_to_criticality"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false", index=True)
    applies_to_domain: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    applies_to_criticality: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    weights_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=False)


class DQObservabilityBaseline(TimestampMixin, Base):
    __tablename__ = "dq_observability_baselines"
    __table_args__ = (
        UniqueConstraint("run_id", "metric_key", "column_name", name="uq_dq_observability_baseline_run_metric"),
        Index("ix_dq_observability_baselines_table_metric", "table_id", "metric_key"),
        Index("ix_dq_observability_baselines_table_column_metric", "table_id", "column_name", "metric_key"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("dq_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    metric_key: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    metric_scope: Mapped[str] = mapped_column(String(30), nullable=False, default="table")
    current_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    baseline_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    mean_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    median_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    tolerance_abs: Mapped[float | None] = mapped_column(Float, nullable=True)
    tolerance_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    window_size: Mapped[int] = mapped_column(Integer, nullable=False, default=14)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=func.now())
    details_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DQObservabilityEvent(TimestampMixin, Base):
    __tablename__ = "dq_observability_events"
    __table_args__ = (
        Index("ix_dq_observability_events_table_metric", "table_id", "metric_key"),
        Index("ix_dq_observability_events_table_detected", "table_id", "detected_at"),
        Index("ix_dq_observability_events_status", "status"),
        Index("ix_dq_observability_events_type", "event_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("dq_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    metric_key: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    dimension_key: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="open", index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="warning", index=True)
    observed_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    expected_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    baseline_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    delta_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    delta_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=func.now(), index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    details_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DQEvidenceSample(TimestampMixin, Base):
    __tablename__ = "dq_evidence_samples"
    __table_args__ = (
        Index("ix_dq_evidence_samples_table_created", "table_id", "created_at"),
        Index("ix_dq_evidence_samples_dq_run", "dq_run_id"),
        Index("ix_dq_evidence_samples_rule_run", "rule_run_id"),
        Index("ix_dq_evidence_samples_rule", "rule_id"),
        Index("ix_dq_evidence_samples_type", "evidence_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    dq_run_id: Mapped[int | None] = mapped_column(ForeignKey("dq_runs.id", ondelete="CASCADE"), nullable=True, index=True)
    rule_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    rule_id: Mapped[int | None] = mapped_column(nullable=True, index=True)
    evidence_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    origin: Mapped[str] = mapped_column(String(40), nullable=False, default="dq")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="masked")
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    affected_rows_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    sample_rows_json: Mapped[list[dict] | None] = mapped_column(JSON, nullable=True)
    masked_fields_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    evidence_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DQRule(TimestampMixin, Base):
    __tablename__ = "dq_rules"
    __table_args__ = (
        Index("ix_dq_rules_table_archived_active", "table_id", "archived", "is_active"),
        Index("ix_dq_rules_table_severity", "table_id", "severity"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True, index=True)
    execution_engine: Mapped[str] = mapped_column(String(20), nullable=False, default="spark", server_default="spark", index=True)
    notification_recipient_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    schedule_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual", index=True)
    schedule_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true", index=True)
    schedule_every_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    schedule_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    schedule_day_of_week: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_day_of_month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_anchor_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    table_fqn: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    rule_type: Mapped[str] = mapped_column(String(50), nullable=False, default="column_validation")
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="medium")
    rule_builder_version: Mapped[int | None] = mapped_column(Integer, nullable=True, default=1)
    rule_definition_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    legacy_rule_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    archived: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false", index=True)
    archived_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False, index=True)
    notification_recipient_user: Mapped[User | None] = relationship("User", foreign_keys=[notification_recipient_user_id])
    notification_recipients: Mapped[list[User]] = relationship(
        "User",
        secondary=dq_rule_notification_recipients,
        lazy="selectin",
    )

    runs: Mapped[list[DQRuleRun]] = relationship(
        "DQRuleRun",
        back_populates="rule",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    latest_run_snapshot: Mapped["DQRuleLatestRun | None"] = relationship(
        "DQRuleLatestRun",
        back_populates="rule",
        cascade="all, delete-orphan",
        passive_deletes=True,
        uselist=False,
    )


class DQRuleRun(TimestampMixin, Base):
    __tablename__ = "dq_rule_runs"
    __table_args__ = (
        Index("ix_dq_rule_runs_rule_created", "rule_id", "created_at"),
        Index("ix_dq_rule_runs_rule_status_created", "rule_id", "status", "created_at"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("dq_rules.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pass")
    execution_engine: Mapped[str] = mapped_column(String(20), nullable=False, default="spark")
    violations_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    sample_rows_json: Mapped[list[dict] | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text)

    rule: Mapped[DQRule] = relationship("DQRule", back_populates="runs")


class DQJobRun(TimestampMixin, Base):
    __tablename__ = "dq_job_runs"
    __table_args__ = (
        Index("ix_dq_job_runs_table_created", "table_id", "created_at"),
        Index("ix_dq_job_runs_status_created", "status", "created_at"),
        Index("ix_dq_job_runs_datasource_created", "datasource_id", "created_at"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)  # profiling | rules
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued", index=True)
    execution_engine: Mapped[str] = mapped_column(String(20), nullable=False, default="spark", index=True)
    dq_run_id: Mapped[int | None] = mapped_column(ForeignKey("dq_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True, index=True)
    table_fqn: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True, index=True
    )
    requested_by_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    spark_app_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    spark_master_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    logs_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    command: Mapped[str | None] = mapped_column(Text, nullable=True)
    stdout_log: Mapped[str | None] = mapped_column(Text, nullable=True)
    stderr_log: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class DQRuleLatestRun(TimestampMixin, Base):
    __tablename__ = "dq_rule_latest_runs"
    __table_args__ = (
        Index("ix_dq_rule_latest_runs_table_rule", "table_id", "rule_id"),
        Index("ix_dq_rule_latest_runs_latest_rule_run", "latest_rule_run_id"),
        Index("ix_dq_rule_latest_runs_latest_job_run", "latest_job_run_id"),
        {"schema": "t2c_data"},
    )

    rule_id: Mapped[int] = mapped_column(ForeignKey("dq_rules.id", ondelete="CASCADE"), primary_key=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True, index=True)
    latest_rule_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("dq_rule_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    latest_job_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("dq_job_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    rule: Mapped[DQRule] = relationship("DQRule", back_populates="latest_run_snapshot")


class DQSchedulerStatus(TimestampMixin, Base):
    __tablename__ = "dq_scheduler_status"
    __table_args__ = {"schema": "t2c_data"}

    id: Mapped[int] = mapped_column(primary_key=True)
    scheduler_name: Mapped[str] = mapped_column(String(80), nullable=False, default="dq_rules", server_default="dq_rules")
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="worker", server_default="worker")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    last_started_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_heartbeat_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_success_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_failure_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_run_summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DQProfilingSchedulerStatus(TimestampMixin, Base):
    __tablename__ = "dq_profiling_scheduler_status"
    __table_args__ = {"schema": "t2c_data"}

    id: Mapped[int] = mapped_column(primary_key=True)
    scheduler_name: Mapped[str] = mapped_column(String(80), nullable=False, default="dq_profiling", server_default="dq_profiling")
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="worker", server_default="worker")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    last_started_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_heartbeat_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_success_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_failure_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_run_summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DQProfilingSchedule(TimestampMixin, Base):
    __tablename__ = "dq_profiling_schedules"
    __table_args__ = {"schema": "t2c_data"}

    id: Mapped[int] = mapped_column(primary_key=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False, default="table", server_default="table", index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=True, index=True)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=True, index=True)
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    table_ids_json: Mapped[list[int] | None] = mapped_column(JSON, nullable=True)
    execution_engine: Mapped[str] = mapped_column(String(20), nullable=False, default="spark", server_default="spark", index=True)
    schedule_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual", index=True)
    schedule_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true", index=True)
    schedule_every_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    schedule_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    schedule_timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    schedule_day_of_week: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_day_of_month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_anchor_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_status: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    schedule_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    schedule_next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    schema_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schema_concurrency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schema_sample_fraction: Mapped[float | None] = mapped_column(Float, nullable=True)
    schema_include_tables_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    schema_exclude_tables_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    schema_columns_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    table: Mapped[TableEntity | None] = relationship("TableEntity")
    datasource: Mapped[DataSource | None] = relationship("DataSource")
    notification_recipients: Mapped[list[User]] = relationship(
        "User",
        secondary=dq_profiling_schedule_recipients,
        lazy="selectin",
    )
    runs: Mapped[list[DQRun]] = relationship(
        "DQRun",
        back_populates="profiling_schedule",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DQProfilingWatermark(TimestampMixin, Base):
    """Per-execution log that drives incremental (full-then-delta) profiling.

    The first successful profiling of a table is recorded as ``full``; subsequent
    runs read only the rows whose watermark column falls in ``(window_start, window_end]``
    and are recorded as ``delta``. The watermark only advances on success, so a failed
    run never skips its window (no data loss). When no usable date/time column is found,
    the run stays ``full`` and ``watermark_column`` is null (reason kept in ``note``).
    """

    __tablename__ = "dq_profiling_watermarks"
    __table_args__ = (
        Index("ix_dq_profiling_watermarks_table_status_end", "table_id", "status", "window_end"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True, index=True)
    dq_run_id: Mapped[int | None] = mapped_column(ForeignKey("dq_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("dq_job_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    mode: Mapped[str] = mapped_column(String(10), nullable=False, default="full")
    watermark_column: Mapped[str | None] = mapped_column(String(255), nullable=True)
    window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    rows_processed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", index=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)


class DQProfilingTableSetting(TimestampMixin, Base):
    """Per-table profiling configuration for large tables.

    ``start_date`` is the floor for the FIRST profiling of a table: instead of a full
    read, the first run reads from ``start_date`` up to the execution time, and the
    following runs continue as delta (which naturally never goes below the floor).
    ``watermark_column`` lets the user override the auto-detected date/time column when
    the name does not follow convention.
    """

    __tablename__ = "dq_profiling_table_settings"
    __table_args__ = (
        UniqueConstraint("table_id", name="uq_dq_profiling_table_settings_table"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    start_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    watermark_column: Mapped[str | None] = mapped_column(String(255), nullable=True)
    updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
