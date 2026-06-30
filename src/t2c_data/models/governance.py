from __future__ import annotations

from datetime import date

from sqlalchemy import JSON, Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class GovernanceSettings(TimestampMixin, Base):
    __tablename__ = "governance_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    owner_review_interval_days: Mapped[int] = mapped_column(Integer, nullable=False, default=90, server_default="90")
    privacy_review_interval_days: Mapped[int] = mapped_column(Integer, nullable=False, default=180, server_default="180")
    sensitive_privacy_review_interval_days: Mapped[int] = mapped_column(Integer, nullable=False, default=90, server_default="90")
    certification_review_interval_days: Mapped[int] = mapped_column(Integer, nullable=False, default=180, server_default="180")
    certification_review_sla_days: Mapped[int] = mapped_column(Integer, nullable=False, default=7, server_default="7")
    certification_revalidation_window_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30, server_default="30")
    audit_log_retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=730, server_default="730")
    audit_log_archive_retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=2555, server_default="2555")
    access_log_retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30, server_default="30")
    access_log_archive_retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=365, server_default="365")
    platform_usage_event_retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=180, server_default="180")
    search_result_click_retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=180, server_default="180")
    legacy_api_cutoff_window_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30, server_default="30")
    legacy_api_disabled_modules: Mapped[str | None] = mapped_column(Text, nullable=True)
    legacy_api_force_enabled_modules: Mapped[str | None] = mapped_column(Text, nullable=True)
    stewardship_assignment_rules: Mapped[str | None] = mapped_column(Text, nullable=True)
    governance_policy_rules: Mapped[str | None] = mapped_column(Text, nullable=True)
    governance_score_weights: Mapped[str | None] = mapped_column(Text, nullable=True)
    trust_score_domain_adjustments: Mapped[str | None] = mapped_column(Text, nullable=True)
    trust_score_criticality_adjustments: Mapped[str | None] = mapped_column(Text, nullable=True)
    governance_notifications_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    governance_notification_repeat_days: Mapped[int] = mapped_column(Integer, nullable=False, default=7, server_default="7")
    governance_notification_critical_repeat_hours: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=24,
        server_default="24",
    )
    pipeline_failure_owner_sla_hours: Mapped[int] = mapped_column(Integer, nullable=False, default=24, server_default="24")
    platform_job_running_attention_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=120,
        server_default="120",
    )
    platform_job_running_critical_hours: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=24,
        server_default="24",
    )
    platform_job_next_expected_delay_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=60,
        server_default="60",
    )
    platform_recent_success_window_hours: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=72,
        server_default="72",
    )
    operational_high_volume_threshold_rows: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=100000,
        server_default="100000",
    )
    governance_high_usage_click_threshold: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=20,
        server_default="20",
    )
    dq_operational_failure_penalty_points: Mapped[int] = mapped_column(Integer, nullable=False, default=15, server_default="15")
    dq_operational_stale_penalty_points: Mapped[int] = mapped_column(Integer, nullable=False, default=8, server_default="8")
    dq_operational_recurrent_penalty_points: Mapped[int] = mapped_column(Integer, nullable=False, default=5, server_default="5")
    airflow_ui_base_url: Mapped[str | None] = mapped_column(Text, nullable=True)


class CertificationGoal(TimestampMixin, Base):
    __tablename__ = "certification_goals"
    __table_args__ = (
        Index("ix_certification_goals_status", "status"),
        Index("ix_certification_goals_period", "period_start", "period_end"),
        Index("ix_certification_goals_scope", "scope_type", "scope_value"),
        Index("ix_certification_goals_owner", "owner"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    target_certified_assets: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    target_eligible_assets: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    target_reviewed_assets: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    target_revalidated_assets: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    scope_type: Mapped[str] = mapped_column(String(40), nullable=False, default="global", server_default="global")
    scope_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    owner: Mapped[str | None] = mapped_column(String(160), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active", server_default="active")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class CertificationDecisionEvent(TimestampMixin, Base):
    __tablename__ = "certification_decision_events"
    __table_args__ = (
        Index("ix_certification_decision_events_asset_id", "asset_id"),
        Index("ix_certification_decision_events_decision_type", "decision_type"),
        Index("ix_certification_decision_events_decision_source", "decision_source"),
        Index("ix_certification_decision_events_created_at", "created_at"),
        Index("ix_certification_decision_events_goal_id", "goal_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    asset_name: Mapped[str] = mapped_column(String(255), nullable=False)
    database_name: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    previous_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    new_status: Mapped[str] = mapped_column(String(40), nullable=False)
    previous_readiness: Mapped[int | None] = mapped_column(Integer, nullable=True)
    new_readiness: Mapped[int | None] = mapped_column(Integer, nullable=True)
    decision_type: Mapped[str] = mapped_column(String(40), nullable=False)
    decision_source: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual")
    reviewer_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    reviewer: Mapped[str | None] = mapped_column(String(255), nullable=True)
    reviewer_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    observation: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    valid_until: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revalidation_due_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    goal_id: Mapped[int | None] = mapped_column(ForeignKey("certification_goals.id", ondelete="SET NULL"), nullable=True)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    asset = relationship("TableEntity")
    goal = relationship("CertificationGoal")
    reviewer_user = relationship("User", foreign_keys=[reviewer_user_id])


class PrivacyReviewEvent(TimestampMixin, Base):
    __tablename__ = "privacy_review_events"
    __table_args__ = (
        Index("ix_privacy_review_events_table_id", "table_id"),
        Index("ix_privacy_review_events_created_at", "created_at"),
        Index("ix_privacy_review_events_review_type", "review_type"),
        Index("ix_privacy_review_events_reviewer_user_id", "reviewer_user_id"),
        Index("ix_privacy_review_events_risk_after", "risk_after"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    database_name: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)

    previous_sensitivity_level: Mapped[str | None] = mapped_column(String(40), nullable=True)
    new_sensitivity_level: Mapped[str | None] = mapped_column(String(40), nullable=True)
    previous_has_personal_data: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    new_has_personal_data: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    previous_has_sensitive_personal_data: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    new_has_sensitive_personal_data: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    previous_legal_basis: Mapped[str | None] = mapped_column(String(50), nullable=True)
    new_legal_basis: Mapped[str | None] = mapped_column(String(50), nullable=True)
    previous_privacy_purpose: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_privacy_purpose: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_retention_policy: Mapped[str | None] = mapped_column(String(255), nullable=True)
    new_retention_policy: Mapped[str | None] = mapped_column(String(255), nullable=True)
    previous_access_scope: Mapped[str | None] = mapped_column(String(40), nullable=True)
    new_access_scope: Mapped[str | None] = mapped_column(String(40), nullable=True)
    previous_access_roles: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    new_access_roles: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    previous_is_masked: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    new_is_masked: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    previous_external_sharing: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    new_external_sharing: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    previous_privacy_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_privacy_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    review_type: Mapped[str] = mapped_column(String(40), nullable=False)
    review_source: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual")
    reviewer_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    reviewer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    reviewer_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    risk_before: Mapped[str | None] = mapped_column(String(20), nullable=True)
    risk_after: Mapped[str | None] = mapped_column(String(20), nullable=True)
    next_review_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    table = relationship("TableEntity")
    reviewer_user = relationship("User", foreign_keys=[reviewer_user_id])


class GovernanceNotification(TimestampMixin, Base):
    __tablename__ = "governance_notifications"
    __table_args__ = (
        Index("ix_governance_notifications_status", "status"),
        Index("ix_governance_notifications_severity", "severity"),
        Index("ix_governance_notifications_rule_key", "rule_key"),
        Index("ix_governance_notifications_table_id", "table_id"),
        Index("ix_governance_notifications_next_send_at", "next_send_at"),
        Index("ix_governance_notifications_active_status", "status", "next_send_at"),
        Index("ix_governance_notifications_dedupe_key", "dedupe_key", unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_key: Mapped[str] = mapped_column(String(80), nullable=False)
    channel: Mapped[str] = mapped_column(String(20), nullable=False, default="in_app", server_default="in_app")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active", server_default="active")
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", server_default="medium")
    origin: Mapped[str] = mapped_column(String(40), nullable=False, default="governance", server_default="governance")
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, default="table", server_default="table")
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True)
    data_owner_id: Mapped[int | None] = mapped_column(ForeignKey("data_owners.id", ondelete="SET NULL"), nullable=True)
    target_href: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    first_detected_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_detected_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_sent_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_send_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    send_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_delivery_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    last_delivery_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    table = relationship("TableEntity")
    data_owner = relationship("DataOwner")


class OperationalStabilitySnapshot(TimestampMixin, Base):
    __tablename__ = "operational_stability_snapshots"
    __table_args__ = (
        UniqueConstraint("table_id", "bucket_start_at", name="uq_operational_stability_table_bucket"),
        Index("ix_operational_stability_bucket", "bucket_start_at"),
        Index("ix_operational_stability_table_bucket", "table_id", "bucket_start_at"),
        Index("ix_operational_stability_dag", "dag_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True)
    schema_name: Mapped[str] = mapped_column(String(100), nullable=False)
    table_name: Mapped[str] = mapped_column(String(200), nullable=False)
    pipeline_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    dag_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    task_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    latest_status_label: Mapped[str | None] = mapped_column(String(60), nullable=True)
    last_success_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_execution_finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rows_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    window_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    success_rate_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0, server_default="0")
    failed_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recurrent_degradation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    currently_stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    bucket_start_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)

    table = relationship("TableEntity")
    datasource = relationship("DataSource")


class GovernanceScoreSnapshot(TimestampMixin, Base):
    __tablename__ = "governance_score_snapshots"
    __table_args__ = (
        UniqueConstraint("table_id", "bucket_date", name="uq_governance_score_snapshot_table_bucket"),
        Index("ix_governance_score_snapshots_bucket_date", "bucket_date"),
        Index("ix_governance_score_snapshots_table_bucket", "table_id", "bucket_date"),
        Index("ix_governance_score_snapshots_score", "score"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True)
    owner_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    domain_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    label: Mapped[str] = mapped_column(String(40), nullable=False)
    tone: Mapped[str] = mapped_column(String(20), nullable=False)
    dq_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    bucket_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)

    table = relationship("TableEntity")
    datasource = relationship("DataSource")


class GovernanceTrustSnapshot(TimestampMixin, Base):
    __tablename__ = "governance_trust_snapshots"
    __table_args__ = (
        UniqueConstraint("table_id", "bucket_date", name="uq_governance_trust_snapshot_table_bucket"),
        Index("ix_governance_trust_snapshots_bucket_date", "bucket_date"),
        Index("ix_governance_trust_snapshots_table_bucket", "table_id", "bucket_date"),
        Index("ix_governance_trust_snapshots_score", "score"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True)
    owner_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    domain_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    label: Mapped[str] = mapped_column(String(40), nullable=False)
    tone: Mapped[str] = mapped_column(String(20), nullable=False)
    readiness_score: Mapped[int] = mapped_column(Integer, nullable=False)
    governance_score: Mapped[int] = mapped_column(Integer, nullable=False)
    operational_score: Mapped[int] = mapped_column(Integer, nullable=False)
    dq_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    critical_open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    active_dq_violation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    recent_dq_failure_runs_30d: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    trust_context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    bucket_date: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)

    table = relationship("TableEntity")
    datasource = relationship("DataSource")


class GovernanceRecommendation(TimestampMixin, Base):
    __tablename__ = "governance_recommendations"
    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_governance_recommendations_dedupe_key"),
        Index("ix_governance_recommendations_status", "status"),
        Index("ix_governance_recommendations_severity", "severity"),
        Index("ix_governance_recommendations_priority", "priority"),
        Index("ix_governance_recommendations_table", "table_id"),
        Index("ix_governance_recommendations_column", "column_id"),
        Index("ix_governance_recommendations_domain", "domain_name"),
        Index("ix_governance_recommendations_due_at", "due_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False)
    recommendation_key: Mapped[str] = mapped_column(String(120), nullable=False)
    policy_rule_key: Mapped[str | None] = mapped_column(String(120), nullable=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"), nullable=True)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True)
    domain_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_kind: Mapped[str] = mapped_column(String(40), nullable=False, default="governance", server_default="governance")
    source_label: Mapped[str] = mapped_column(String(120), nullable=False, default="Governança", server_default="Governança")
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    detail: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    impact: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open", server_default="open")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    trust_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    risk_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    action_key: Mapped[str] = mapped_column(String(120), nullable=False)
    action_label: Mapped[str] = mapped_column(String(160), nullable=False)
    due_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    context_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    explanation_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    resolved_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    resolution_action: Mapped[str | None] = mapped_column(String(40), nullable=True)
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    feedback_rating: Mapped[str | None] = mapped_column(String(20), nullable=True)
    feedback_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    feedback_updated_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    feedback_updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    table = relationship("TableEntity")
    column = relationship("ColumnEntity")
    datasource = relationship("DataSource")
    resolved_by_user = relationship("User", foreign_keys=[resolved_by_user_id])
    feedback_updated_by_user = relationship("User", foreign_keys=[feedback_updated_by_user_id])


class AssetSla(TimestampMixin, Base):
    __tablename__ = "asset_slas"
    __table_args__ = (
        UniqueConstraint("asset_type", "asset_id", "sla_kind", name="uq_asset_slas_asset_kind"),
        Index("ix_asset_slas_asset_type", "asset_type"),
        Index("ix_asset_slas_asset_id", "asset_id"),
        Index("ix_asset_slas_status", "status"),
        Index("ix_asset_slas_table_id", "table_id"),
        Index("ix_asset_slas_column_id", "column_id"),
        Index("ix_asset_slas_source_kind", "source_kind"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_type: Mapped[str] = mapped_column(String(40), nullable=False)
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"), nullable=True)
    asset_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    asset_fqn: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    sla_kind: Mapped[str] = mapped_column(String(40), nullable=False, default="freshness", server_default="freshness")
    sla_hours: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active", server_default="active")
    source_kind: Mapped[str] = mapped_column(String(40), nullable=False, default="manual", server_default="manual")
    source_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    table = relationship("TableEntity")
    column = relationship("ColumnEntity")
    reviewed_by_user = relationship("User", foreign_keys=[reviewed_by_user_id])


class MetadataChangeRequest(TimestampMixin, Base):
    __tablename__ = "metadata_change_requests"
    __table_args__ = (
        UniqueConstraint("request_key", name="uq_metadata_change_requests_request_key"),
        Index("ix_metadata_change_requests_status", "status"),
        Index("ix_metadata_change_requests_asset", "asset_type", "asset_id"),
        Index("ix_metadata_change_requests_table_id", "table_id"),
        Index("ix_metadata_change_requests_column_id", "column_id"),
        Index("ix_metadata_change_requests_change_kind", "change_kind"),
        Index("ix_metadata_change_requests_policy_rule_key", "policy_rule_key"),
        Index("ix_metadata_change_requests_recommendation_id", "recommendation_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    request_key: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_type: Mapped[str] = mapped_column(String(40), nullable=False)
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"), nullable=True)
    asset_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    asset_fqn: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    change_kind: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="draft", server_default="draft")
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    requested_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    approved_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    approved_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    applied_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    applied_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rejected_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    rejected_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    policy_rule_key: Mapped[str | None] = mapped_column(String(120), nullable=True)
    recommendation_id: Mapped[int | None] = mapped_column(ForeignKey("governance_recommendations.id", ondelete="SET NULL"), nullable=True)
    current_value_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    proposed_value_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    context_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    apply_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    table = relationship("TableEntity")
    column = relationship("ColumnEntity")
    requested_by_user = relationship("User", foreign_keys=[requested_by_user_id])
    reviewed_by_user = relationship("User", foreign_keys=[reviewed_by_user_id])
    approved_by_user = relationship("User", foreign_keys=[approved_by_user_id])
    applied_by_user = relationship("User", foreign_keys=[applied_by_user_id])
    rejected_by_user = relationship("User", foreign_keys=[rejected_by_user_id])
    recommendation = relationship("GovernanceRecommendation")
    events: Mapped[list["MetadataChangeRequestEvent"]] = relationship(
        "MetadataChangeRequestEvent",
        back_populates="request",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="MetadataChangeRequestEvent.created_at",
    )


class MetadataChangeRequestEvent(TimestampMixin, Base):
    __tablename__ = "metadata_change_request_events"
    __table_args__ = (
        Index("ix_metadata_change_request_events_request_id", "metadata_change_request_id"),
        Index("ix_metadata_change_request_events_event_type", "event_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    metadata_change_request_id: Mapped[int] = mapped_column(
        ForeignKey("metadata_change_requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    previous_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    next_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    request = relationship("MetadataChangeRequest", back_populates="events")
    actor_user = relationship("User", foreign_keys=[actor_user_id])
