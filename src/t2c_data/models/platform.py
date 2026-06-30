from __future__ import annotations

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class SearchReadModel(TimestampMixin, Base):
    __tablename__ = "search_read_model"
    __table_args__ = (UniqueConstraint("entity_type", "entity_id", name="uq_search_read_model_entity"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    parent_table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True, index=True)
    category: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    subtitle: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    context_path: Mapped[str | None] = mapped_column(Text)
    target_url: Mapped[str] = mapped_column(String(1000), nullable=False)
    searchable_name: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    searchable_aliases: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    searchable_synonyms: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    searchable_descriptions: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    searchable_context: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    source_name: Mapped[str | None] = mapped_column(String(255))
    database_name: Mapped[str | None] = mapped_column(String(255))
    schema_name: Mapped[str | None] = mapped_column(String(255))
    owner_name: Mapped[str | None] = mapped_column(String(255))
    domain_name: Mapped[str | None] = mapped_column(String(255))
    classification: Mapped[str | None] = mapped_column(String(120))
    certified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    popularity_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DashboardAssetReadModel(TimestampMixin, Base):
    __tablename__ = "dashboard_asset_read_model"
    __table_args__ = (
        Index("ix_dashboard_asset_read_model_certification_status", "certification_status"),
        Index("ix_dashboard_asset_read_model_privacy_flags", "has_personal_data", "has_sensitive_personal_data"),
    )

    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), primary_key=True)
    datasource_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    database_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    schema_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    table_type: Mapped[str] = mapped_column(String(40), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)
    database_name: Mapped[str] = mapped_column(String(255), nullable=False)
    datasource_name: Mapped[str] = mapped_column(String(255), nullable=False)
    engine: Mapped[str] = mapped_column(String(40), nullable=False)
    owner_defined: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    description_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    dictionary_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    classification_defined: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tags_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    terms_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    search_clicks_30d: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_dq_rules_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    recent_dq_failure_runs_30d: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    certification_status: Mapped[str] = mapped_column(String(40), nullable=False)
    certification_criticality: Mapped[str | None] = mapped_column(String(40))
    certification_badges: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    certification_decided_at: Mapped[str | None] = mapped_column(String(64))
    certification_review_at: Mapped[str | None] = mapped_column(String(64))
    certification_expires_at: Mapped[str | None] = mapped_column(String(64))
    review_recent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    dq_score: Mapped[float | None] = mapped_column(nullable=True)
    completeness_pct_avg: Mapped[float | None] = mapped_column(nullable=True)
    freshness_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    critical_open_incidents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    owner_name: Mapped[str | None] = mapped_column(String(255))
    data_owner_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    domain_name: Mapped[str | None] = mapped_column(String(255))
    sensitivity_level: Mapped[str | None] = mapped_column(String(40))
    has_personal_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_sensitive_personal_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    owner_reviewed_at: Mapped[str | None] = mapped_column(String(64))
    privacy_reviewed_at: Mapped[str | None] = mapped_column(String(64))
    last_review_at: Mapped[str | None] = mapped_column(String(64))
    last_sync_at: Mapped[str | None] = mapped_column(String(64))
    last_updated_at: Mapped[str | None] = mapped_column(String(64))


class AssetVisibilityRule(TimestampMixin, Base):
    __tablename__ = "asset_visibility_rules"

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True, default="table")
    entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    rule_scope: Mapped[str] = mapped_column(String(30), nullable=False, default="asset", index=True)
    match_value: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    allowed_role: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    allowed_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    visibility_scope: Mapped[str] = mapped_column(String(20), nullable=False, default="full")
    mask_sensitive_fields: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reason: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    allowed_user = relationship("User")


class PlatformUsageEvent(TimestampMixin, Base):
    __tablename__ = "platform_usage_events"
    __table_args__ = (
        Index("ix_platform_usage_events_created_at", "created_at"),
        Index("ix_platform_usage_events_module_created", "module_name", "created_at"),
        Index("ix_platform_usage_events_event_created", "event_name", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    event_name: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    module_name: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    page_path: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    entity_type: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    target_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    user = relationship("User")


class IntegrationSyncJob(TimestampMixin, Base):
    __tablename__ = "integration_sync_jobs"
    __table_args__ = (
        Index("ix_integration_sync_jobs_job_key_started_at", "job_key", "started_at"),
        Index("ix_integration_sync_jobs_source_started_at", "source", "started_at"),
        Index("ix_integration_sync_jobs_status_started_at", "status", "started_at"),
        Index("ix_integration_sync_jobs_status_queued_at", "status", "queued_at"),
        Index("ix_integration_sync_jobs_target", "target_type", "target_id"),
        Index("ix_integration_sync_jobs_artifact_expires_at", "artifact_expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    job_type: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    target_type: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    target_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    target_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    trigger_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", server_default="running", index=True)
    queued_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_expected_run_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    records_processed: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    progress_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    requested_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    payload_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    result_summary_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    artifact_public_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    artifact_filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    artifact_content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    artifact_storage_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    artifact_available_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    artifact_expires_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    artifact_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    artifact_download_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    artifact_last_downloaded_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PlatformWorkerHeartbeat(TimestampMixin, Base):
    __tablename__ = "platform_worker_heartbeats"
    __table_args__ = (
        Index("ix_platform_worker_heartbeats_status_last_seen", "status", "last_seen_at"),
        Index("ix_platform_worker_heartbeats_hostname", "hostname"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    worker_id: Mapped[str] = mapped_column(String(160), nullable=False, unique=True, index=True)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="idle", server_default="idle", index=True)
    supported_job_types_json: Mapped[list | dict | None] = mapped_column(JSONB, nullable=True)
    active_job_source: Mapped[str | None] = mapped_column(String(40), nullable=True)
    active_job_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    active_job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_job_finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_job_status: Mapped[str | None] = mapped_column(String(20), nullable=True)


class AssetRowCountSnapshot(TimestampMixin, Base):
    __tablename__ = "asset_row_count_snapshots"
    __table_args__ = (
        Index("ix_asset_row_count_snapshots_asset_observed", "asset_type", "asset_id", "observed_at"),
        Index("ix_asset_row_count_snapshots_source_observed", "source", "observed_at"),
        Index("ix_asset_row_count_snapshots_integration_job", "integration_sync_job_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    asset_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    asset_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    asset_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    asset_fqn: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    source: Mapped[str] = mapped_column(String(40), nullable=False, default="s3", server_default="s3", index=True)
    observed_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    row_count_method: Mapped[str | None] = mapped_column(String(40), nullable=True)
    row_count_confidence: Mapped[str | None] = mapped_column(String(40), nullable=True)
    integration_sync_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("integration_sync_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    context_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)


class PlatformAutomationRule(TimestampMixin, Base):
    __tablename__ = "platform_automation_rules"
    __table_args__ = (
        Index("ix_platform_automation_rules_status", "status"),
        Index("ix_platform_automation_rules_action_key", "action_key"),
        Index("ix_platform_automation_rules_scope", "scope_kind", "scope_value"),
        Index("ix_platform_automation_rules_condition", "condition_kind", "condition_operator"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active", server_default="active")
    scope_kind: Mapped[str] = mapped_column(String(40), nullable=False, default="asset", server_default="asset")
    scope_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    condition_kind: Mapped[str] = mapped_column(String(60), nullable=False)
    condition_operator: Mapped[str] = mapped_column(String(20), nullable=False, default="gte", server_default="gte")
    threshold_value: Mapped[int | None] = mapped_column(Integer, nullable=True)
    window_days: Mapped[int] = mapped_column(Integer, nullable=False, default=7, server_default="7")
    action_key: Mapped[str] = mapped_column(String(120), nullable=False)
    action_target_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    execution_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="automatic", server_default="automatic")
    notify_owner: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    open_incident: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    schedule_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    last_evaluated_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_triggered_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_triggered_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    last_triggered_summary_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

    created_by_user = relationship("User")
    executions = relationship(
        "PlatformAutomationExecution",
        back_populates="rule",
        cascade="all, delete-orphan",
    )


class PlatformAutomationExecution(TimestampMixin, Base):
    __tablename__ = "platform_automation_executions"
    __table_args__ = (
        Index("ix_platform_automation_executions_created_at", "created_at"),
        Index("ix_platform_automation_executions_status", "status"),
        Index("ix_platform_automation_executions_action_key", "action_key"),
        Index("ix_platform_automation_executions_rule_id", "rule_id"),
        Index("ix_platform_automation_executions_scope", "scope_kind", "scope_value"),
        Index("ix_platform_automation_executions_entity", "entity_type", "entity_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    rule_id: Mapped[int | None] = mapped_column(
        ForeignKey("platform_automation_rules.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    action_key: Mapped[str] = mapped_column(String(120), nullable=False)
    action_label: Mapped[str] = mapped_column(String(200), nullable=False)
    execution_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="suggested", server_default="suggested")
    trigger_source: Mapped[str] = mapped_column(String(40), nullable=False, default="manual", server_default="manual")
    scope_kind: Mapped[str] = mapped_column(String(40), nullable=False, default="asset", server_default="asset")
    scope_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    entity_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True, index=True)
    datasource_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    domain_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    product_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    target_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    input_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    result_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    impact_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    executed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    rule = relationship("PlatformAutomationRule", back_populates="executions")
    created_by_user = relationship("User", foreign_keys=[created_by_user_id])
    executed_by_user = relationship("User", foreign_keys=[executed_by_user_id])
    table = relationship("TableEntity")


class PlatformDomainEvent(TimestampMixin, Base):
    __tablename__ = "platform_domain_events"
    __table_args__ = (
        Index("ix_platform_domain_events_event_key", "event_key"),
        Index("ix_platform_domain_events_category_created_at", "category", "created_at"),
        Index("ix_platform_domain_events_entity", "entity_type", "entity_id"),
        Index("ix_platform_domain_events_table_created_at", "table_id", "created_at"),
        Index("ix_platform_domain_events_column_created_at", "column_id", "created_at"),
        Index("ix_platform_domain_events_severity", "severity"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_key: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", server_default="medium")
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_module: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    source_action: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    entity_type: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    datasource_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    actor_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    actor_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    manual_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="unknown", server_default="unknown")
    correlation_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    payload_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

    actor_user = relationship("User")
    table = relationship("TableEntity")
    column = relationship("ColumnEntity")


class TimelineEpisodeAction(TimestampMixin, Base):
    __tablename__ = "timeline_episode_actions"
    __table_args__ = (
        Index("ix_timeline_episode_actions_episode_created", "episode_key", "created_at"),
        Index("ix_timeline_episode_actions_table_created", "table_id", "created_at"),
        Index("ix_timeline_episode_actions_action_type", "action_type"),
        Index("ix_timeline_episode_actions_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    episode_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=True, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"), nullable=True, index=True)
    action_type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open", server_default="open")
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    silent_until: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    actor_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    actor_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

    actor_user = relationship("User")
    table = relationship("TableEntity")
    column = relationship("ColumnEntity")


class PlatformSchedulerStatus(TimestampMixin, Base):
    __tablename__ = "platform_scheduler_status"

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    scheduler_name: Mapped[str] = mapped_column(String(80), nullable=False, default="platform_maintenance", server_default="platform_maintenance")
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="worker", server_default="worker")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    last_started_at: Mapped[str | None] = mapped_column(String(64))
    last_heartbeat_at: Mapped[str | None] = mapped_column(String(64))
    last_success_at: Mapped[str | None] = mapped_column(String(64))
    last_failure_at: Mapped[str | None] = mapped_column(String(64))
    last_error: Mapped[str | None] = mapped_column(Text)
    last_run_summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class RetentionCleanupRun(TimestampMixin, Base):
    __tablename__ = "retention_cleanup_runs"
    __table_args__ = (
        Index("ix_retention_cleanup_runs_status_created_at", "status", "created_at"),
        Index("ix_retention_cleanup_runs_trigger_source_created_at", "trigger_source", "created_at"),
        Index("ix_retention_cleanup_runs_started_at", "started_at"),
        {"schema": "t2c_data"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_name: Mapped[str] = mapped_column(String(80), nullable=False, default="retention_cleanup_job", server_default="retention_cleanup_job")
    trigger_source: Mapped[str] = mapped_column(String(40), nullable=False, default="scheduler", server_default="scheduler")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", server_default="running")
    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    retention_policy_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    summary_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class PlatformApiKey(TimestampMixin, Base):
    __tablename__ = "platform_api_keys"
    __table_args__ = (
        UniqueConstraint("public_id", name="uq_platform_api_keys_public_id"),
        Index("ix_platform_api_keys_status", "status"),
        Index("ix_platform_api_keys_expires_at", "expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    public_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="active", server_default="active")
    scopes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    environment: Mapped[str] = mapped_column(String(32), nullable=False, default="shared", server_default="shared")
    allowed_ips_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    token_hash: Mapped[str] = mapped_column(Text, nullable=False)
    token_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    expires_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_ip: Mapped[str | None] = mapped_column(String(80), nullable=True)
    last_used_user_agent: Mapped[str | None] = mapped_column(String(320), nullable=True)
    usage_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    created_by_user = relationship("User")


class DataLakeConnection(TimestampMixin, Base):
    __tablename__ = "data_lake_connections"
    __table_args__ = (UniqueConstraint("name", name="uq_data_lake_connections_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    bucket: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    region: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    prefix: Mapped[str | None] = mapped_column(String(500), nullable=True)
    auth_type: Mapped[str] = mapped_column(String(80), nullable=False, default="default_environment", server_default="default_environment")
    freshness_sla_hours_default: Mapped[int | None] = mapped_column(Integer, nullable=True)
    freshness_sla_hours_bronze: Mapped[int | None] = mapped_column(Integer, nullable=True)
    freshness_sla_hours_silver: Mapped[int | None] = mapped_column(Integer, nullable=True)
    freshness_sla_hours_gold: Mapped[int | None] = mapped_column(Integer, nullable=True)
    access_key_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    role_arn: Mapped[str | None] = mapped_column(String(500), nullable=True)
    _secret_payload: Mapped[str] = mapped_column("credentials_payload", Text, nullable=False, default="")
    last_test_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    last_test_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_test_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    created_by_user = relationship("User")

    scan_schedules = relationship(
        "DataLakeScanSchedule",
        back_populates="connection",
        cascade="all, delete-orphan",
    )

    @property
    def secret_values(self) -> dict[str, str]:
        from t2c_data.core.secret_store import decrypt_secret_mapping

        return decrypt_secret_mapping(self._secret_payload)

    def set_secret_values(self, values: dict[str, str] | None) -> None:
        from t2c_data.core.secret_store import encrypt_secret_mapping

        self._secret_payload = encrypt_secret_mapping(values or {})

    @property
    def aws_secret_access_key(self) -> str | None:
        return self.secret_values.get("aws_secret_access_key") or None

    @aws_secret_access_key.setter
    def aws_secret_access_key(self, value: str | None) -> None:
        current = self.secret_values
        if value:
            current["aws_secret_access_key"] = value
        else:
            current.pop("aws_secret_access_key", None)
        self.set_secret_values(current)

    @property
    def aws_session_token(self) -> str | None:
        return self.secret_values.get("aws_session_token") or None

    @aws_session_token.setter
    def aws_session_token(self, value: str | None) -> None:
        current = self.secret_values
        if value:
            current["aws_session_token"] = value
        else:
            current.pop("aws_session_token", None)
        self.set_secret_values(current)


class DataLakeInventoryScanRun(TimestampMixin, Base):
    __tablename__ = "data_lake_inventory_scan_runs"
    __table_args__ = (
        Index("ix_data_lake_inventory_scan_runs_connection_created", "connection_id", "created_at"),
        Index("ix_data_lake_inventory_scan_runs_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("data_lake_connections.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", server_default="running")
    scanned_layers_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    discovered_tables_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    discovered_parquet_files_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    trigger_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual")
    schedule_id: Mapped[int | None] = mapped_column(ForeignKey("data_lake_scan_schedules.id", ondelete="SET NULL"), nullable=True, index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scanned_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    connection = relationship("DataLakeConnection", back_populates="inventory_scan_runs")
    scanned_by_user = relationship("User")
    schedule = relationship("DataLakeScanSchedule", back_populates="scan_runs")
    tables = relationship("DataLakeInventoryTable", back_populates="scan_run")


class DataLakeInventoryTable(TimestampMixin, Base):
    __tablename__ = "data_lake_inventory_tables"
    __table_args__ = (
        UniqueConstraint("connection_id", "layer", "table_name", "path_base", name="uq_data_lake_inventory_tables_identity"),
        Index("ix_data_lake_inventory_tables_connection_layer", "connection_id", "layer"),
        Index("ix_data_lake_inventory_tables_status_scan", "status_scan"),
        Index("ix_data_lake_inventory_tables_last_scan", "data_last_scan_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("data_lake_connections.id", ondelete="CASCADE"), nullable=False, index=True)
    layer: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    path_base: Mapped[str] = mapped_column(String(1000), nullable=False)
    files_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    parquet_files_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    non_parquet_files_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    size_total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_modified_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    has_partitions: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    partition_pattern_detected: Mapped[str | None] = mapped_column(String(80), nullable=True)
    status_scan: Mapped[str] = mapped_column(String(20), nullable=False, default="unknown")
    data_last_scan_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    freshness_sla_hours_override: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_quality_evaluated_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    data_owner_id: Mapped[int | None] = mapped_column(ForeignKey("data_owners.id", ondelete="SET NULL"), nullable=True, index=True)
    domain_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    classification: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    criticality: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    is_monitored: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    governance_last_updated_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sample_parquet_files_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    scan_run_id: Mapped[int | None] = mapped_column(ForeignKey("data_lake_inventory_scan_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    connection = relationship("DataLakeConnection", back_populates="inventory_tables")
    scan_run = relationship("DataLakeInventoryScanRun", back_populates="tables")
    data_owner = relationship("DataOwner")


class DataLakeTableObservation(TimestampMixin, Base):
    __tablename__ = "data_lake_table_observations"
    __table_args__ = (
        Index("ix_data_lake_table_observations_table_created", "table_id", "created_at"),
        Index("ix_data_lake_table_observations_connection_created", "connection_id", "created_at"),
        Index("ix_data_lake_table_observations_source_created", "source_kind", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("data_lake_connections.id", ondelete="CASCADE"), nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("data_lake_inventory_tables.id", ondelete="CASCADE"), nullable=False, index=True)
    source_kind: Mapped[str] = mapped_column(String(20), nullable=False, default="detail", server_default="detail")
    observed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    freshness_status: Mapped[str] = mapped_column(String(20), nullable=False, default="unknown")
    freshness_age_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    freshness_sla_hours: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    row_count_method: Mapped[str | None] = mapped_column(String(40), nullable=True)
    row_count_confidence: Mapped[str | None] = mapped_column(String(40), nullable=True)
    size_total_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    schema_variants_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    null_columns_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    missing_columns_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unreadable_files_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    drift_detected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    signals_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    summary_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

    connection = relationship("DataLakeConnection")
    table = relationship("DataLakeInventoryTable")


class DataLakeScanSchedulerStatus(TimestampMixin, Base):
    __tablename__ = "data_lake_scan_scheduler_status"

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    scheduler_name: Mapped[str] = mapped_column(String(80), nullable=False, default="data_lake_scan", server_default="data_lake_scan")
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="worker", server_default="worker")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    last_started_at: Mapped[str | None] = mapped_column(String(64))
    last_heartbeat_at: Mapped[str | None] = mapped_column(String(64))
    last_success_at: Mapped[str | None] = mapped_column(String(64))
    last_failure_at: Mapped[str | None] = mapped_column(String(64))
    last_error: Mapped[str | None] = mapped_column(Text)
    last_run_summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class DataLakeScanSchedule(TimestampMixin, Base):
    __tablename__ = "data_lake_scan_schedules"
    __table_args__ = (UniqueConstraint("connection_id", name="uq_data_lake_scan_schedules_connection"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("data_lake_connections.id", ondelete="CASCADE"), nullable=False, index=True)
    schedule_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="manual", server_default="manual", index=True)
    schedule_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true", index=True)
    schedule_every_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    schedule_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    schedule_day_of_week: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_day_of_month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schedule_anchor_date: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_run_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_started_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_last_status: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    schedule_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    schedule_next_run_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    schedule_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    connection = relationship("DataLakeConnection", back_populates="scan_schedules")
    scan_runs = relationship("DataLakeInventoryScanRun", back_populates="schedule")
    created_by_user = relationship("User")


DataLakeConnection.inventory_scan_runs = relationship(  # type: ignore[attr-defined]
    "DataLakeInventoryScanRun",
    back_populates="connection",
    cascade="all, delete-orphan",
)
DataLakeConnection.inventory_tables = relationship(  # type: ignore[attr-defined]
    "DataLakeInventoryTable",
    back_populates="connection",
    cascade="all, delete-orphan",
)


class ApiRateLimitBucket(TimestampMixin, Base):
    __tablename__ = "api_rate_limit_buckets"
    __table_args__ = (
        UniqueConstraint(
            "api_key_id",
            "route_group",
            "window_seconds",
            "bucket_start",
            name="uq_api_rate_limit_bucket",
        ),
        Index("ix_api_rate_limit_bucket_route", "route_group", "bucket_start"),
        Index("ix_api_rate_limit_bucket_key", "api_key_id", "bucket_start"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    api_key_id: Mapped[int | None] = mapped_column(
        ForeignKey("platform_api_keys.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    route_group: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    window_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60, server_default="60")
    bucket_start: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    counter: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
