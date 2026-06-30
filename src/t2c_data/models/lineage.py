from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.core.secret_store import decrypt_secret_mapping, encrypt_secret_mapping
from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LineageProcess(TimestampMixin, Base):
    # Compatibility-only persistence for the first lineage implementation.
    # Keep during transition while external imports and legacy routes are deprecated.
    __tablename__ = "lineage_processes"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)

    edges: Mapped[list[LineageEdge]] = relationship(
        "LineageEdge",
        back_populates="process",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class LineageEdge(TimestampMixin, Base):
    # Compatibility-only persistence for the first lineage implementation.
    # Canonical lineage now lives in lineage_assets + lineage_relations.
    __tablename__ = "lineage_edges"

    id: Mapped[int] = mapped_column(primary_key=True)
    process_id: Mapped[int] = mapped_column(
        ForeignKey("lineage_processes.id", ondelete="CASCADE"), nullable=False
    )
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    from_entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    from_entity_id: Mapped[int] = mapped_column(nullable=False)
    to_entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    to_entity_id: Mapped[int] = mapped_column(nullable=False)

    process: Mapped[LineageProcess] = relationship("LineageProcess", back_populates="edges")


class LineageAsset(TimestampMixin, Base):
    # Canonical lineage asset model used by Explorer, lineage UI, sync and import/export flows.
    __tablename__ = "lineage_assets"
    __table_args__ = (
        UniqueConstraint("asset_key", name="uq_lineage_assets_asset_key"),
        UniqueConstraint("catalog_table_id", name="uq_lineage_assets_catalog_table_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("lineage_source_configs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    catalog_table_id: Mapped[int | None] = mapped_column(
        ForeignKey("tables.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    asset_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    asset_name: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    layer: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    schema_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    object_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    system_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    asset_origin: Mapped[str] = mapped_column(String(30), nullable=False, default="manual", index=True)
    external_node_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    external_namespace: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    external_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    external_type: Mapped[str | None] = mapped_column(String(30), nullable=True)
    aliases_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)

    lineage_source: Mapped["LineageSourceConfig | None"] = relationship("LineageSourceConfig", back_populates="assets")


class LineageRelation(TimestampMixin, Base):
    # Canonical lineage relation model used by manual editing, OpenLineage sync and graph summaries.
    __tablename__ = "lineage_relations"

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("lineage_source_configs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    lineage_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("lineage_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_asset_id: Mapped[int] = mapped_column(
        ForeignKey("lineage_assets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    target_asset_id: Mapped[int] = mapped_column(
        ForeignKey("lineage_assets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    relation_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    process_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    process_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    dashboard_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    discovery_method: Mapped[str] = mapped_column(String(30), nullable=False, default="manual")
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1", index=True)
    is_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false", index=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    external_edge_key: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    created_by_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    updated_by_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    source_asset: Mapped["LineageAsset"] = relationship("LineageAsset", foreign_keys=[source_asset_id])
    target_asset: Mapped["LineageAsset"] = relationship("LineageAsset", foreign_keys=[target_asset_id])
    lineage_source: Mapped["LineageSourceConfig | None"] = relationship("LineageSourceConfig", back_populates="relations")
    lineage_job: Mapped["LineageJob | None"] = relationship("LineageJob", back_populates="relations")
    versions: Mapped[list["LineageRelationVersion"]] = relationship(
        "LineageRelationVersion",
        back_populates="relation",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class LineageColumnEdge(TimestampMixin, Base):
    __tablename__ = "lineage_column_edges"
    __table_args__ = (
        UniqueConstraint(
            "source_asset_id",
            "target_asset_id",
            "source_column_name",
            "target_column_name",
            "relation_type",
            name="uq_lineage_column_edges_unique",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("lineage_source_configs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    lineage_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("lineage_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_asset_id: Mapped[int] = mapped_column(ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False, index=True)
    target_asset_id: Mapped[int] = mapped_column(ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False, index=True)
    source_column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    target_column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    relation_type: Mapped[str] = mapped_column(String(30), nullable=False, default="transformation", index=True)
    discovery_method: Mapped[str] = mapped_column(String(30), nullable=False, default="automatic")
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    evidence_source: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    transform_expression: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1", index=True)
    is_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false", index=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    external_edge_key: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    created_by_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    updated_by_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    lineage_source: Mapped["LineageSourceConfig | None"] = relationship("LineageSourceConfig", back_populates="column_edges")
    lineage_job: Mapped["LineageJob | None"] = relationship("LineageJob")
    source_asset: Mapped["LineageAsset"] = relationship("LineageAsset", foreign_keys=[source_asset_id])
    target_asset: Mapped["LineageAsset"] = relationship("LineageAsset", foreign_keys=[target_asset_id])
    versions: Mapped[list["LineageColumnEdgeVersion"]] = relationship(
        "LineageColumnEdgeVersion",
        back_populates="column_edge",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class LineageRelationVersion(TimestampMixin, Base):
    __tablename__ = "lineage_relation_versions"
    __table_args__ = (
        UniqueConstraint("lineage_relation_id", "version_number", name="uq_lineage_relation_versions_relation_version"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_relation_id: Mapped[int] = mapped_column(ForeignKey("lineage_relations.id", ondelete="CASCADE"), nullable=False, index=True)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    source_asset_id: Mapped[int] = mapped_column(ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False)
    target_asset_id: Mapped[int] = mapped_column(ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False)
    relation_type: Mapped[str] = mapped_column(String(30), nullable=False)
    process_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    process_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    dashboard_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    discovery_method: Mapped[str] = mapped_column(String(30), nullable=False)
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    is_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    external_edge_key: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    recorded_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    relation: Mapped["LineageRelation"] = relationship("LineageRelation", back_populates="versions")


class LineageColumnEdgeVersion(TimestampMixin, Base):
    __tablename__ = "lineage_column_edge_versions"
    __table_args__ = (
        UniqueConstraint("lineage_column_edge_id", "version_number", name="uq_lineage_column_edge_versions_edge_version"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_column_edge_id: Mapped[int] = mapped_column(ForeignKey("lineage_column_edges.id", ondelete="CASCADE"), nullable=False, index=True)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    lineage_source_id: Mapped[int | None] = mapped_column(ForeignKey("lineage_source_configs.id", ondelete="SET NULL"), nullable=True)
    lineage_job_id: Mapped[int | None] = mapped_column(ForeignKey("lineage_jobs.id", ondelete="SET NULL"), nullable=True)
    source_asset_id: Mapped[int] = mapped_column(ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False)
    target_asset_id: Mapped[int] = mapped_column(ForeignKey("lineage_assets.id", ondelete="CASCADE"), nullable=False)
    source_column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    target_column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    relation_type: Mapped[str] = mapped_column(String(30), nullable=False)
    discovery_method: Mapped[str] = mapped_column(String(30), nullable=False)
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    evidence_source: Mapped[str | None] = mapped_column(String(40), nullable=True)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    transform_expression: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    external_edge_key: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    recorded_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    column_edge: Mapped["LineageColumnEdge"] = relationship("LineageColumnEdge", back_populates="versions")


class LineageSourceConfig(TimestampMixin, Base):
    __tablename__ = "lineage_source_configs"
    __table_args__ = (
        UniqueConstraint("name", name="uq_lineage_source_configs_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    source_type: Mapped[str] = mapped_column(String(30), nullable=False, default="openlineage", index=True)
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    default_namespace: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    auth_type: Mapped[str | None] = mapped_column(String(30), nullable=True)
    auth_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    _secret_payload: Mapped[str] = mapped_column("auth_secret", Text, nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    last_sync_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    last_sync_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    last_sync_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    assets: Mapped[list["LineageAsset"]] = relationship("LineageAsset", back_populates="lineage_source")
    relations: Mapped[list["LineageRelation"]] = relationship("LineageRelation", back_populates="lineage_source")
    column_edges: Mapped[list["LineageColumnEdge"]] = relationship("LineageColumnEdge", back_populates="lineage_source")
    jobs: Mapped[list["LineageJob"]] = relationship("LineageJob", back_populates="lineage_source")
    events_raw: Mapped[list["LineageEventRaw"]] = relationship("LineageEventRaw", back_populates="lineage_source")
    checkpoints: Mapped[list["LineageSyncCheckpoint"]] = relationship(
        "LineageSyncCheckpoint",
        back_populates="lineage_source",
        cascade="all, delete-orphan",
    )

    @property
    def auth_secret(self) -> str | None:
        raw = self._secret_payload
        if not raw:
            return None
        payload = decrypt_secret_mapping(raw)
        return payload.get("auth_secret") or None

    @auth_secret.setter
    def auth_secret(self, value: str | None) -> None:
        self._secret_payload = encrypt_secret_mapping({"auth_secret": value}) if value else ""


class LineageJob(TimestampMixin, Base):
    __tablename__ = "lineage_jobs"
    __table_args__ = (
        UniqueConstraint("lineage_source_id", "namespace", "job_name", name="uq_lineage_jobs_source_namespace_job"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_source_id: Mapped[int] = mapped_column(
        ForeignKey("lineage_source_configs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    namespace: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    job_name: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    display_name: Mapped[str] = mapped_column(String(500), nullable=False)
    job_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    location: Mapped[str | None] = mapped_column(String(500), nullable=True)
    latest_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    latest_run_status: Mapped[str | None] = mapped_column(String(80), nullable=True)
    latest_run_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)

    lineage_source: Mapped["LineageSourceConfig"] = relationship("LineageSourceConfig", back_populates="jobs")
    relations: Mapped[list["LineageRelation"]] = relationship("LineageRelation", back_populates="lineage_job")
    runs: Mapped[list["LineageRun"]] = relationship("LineageRun", back_populates="job", cascade="all, delete-orphan")


class LineageRun(TimestampMixin, Base):
    __tablename__ = "lineage_runs"
    __table_args__ = (
        UniqueConstraint("lineage_job_id", "external_run_id", name="uq_lineage_runs_job_external_run"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_job_id: Mapped[int] = mapped_column(
        ForeignKey("lineage_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    external_run_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[str | None] = mapped_column(String(80), nullable=True)
    started_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    ended_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    nominal_start_time: Mapped[str | None] = mapped_column(String(40), nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["LineageJob"] = relationship("LineageJob", back_populates="runs")


class LineageEventRaw(TimestampMixin, Base):
    __tablename__ = "lineage_event_raw"
    __table_args__ = (
        UniqueConstraint("lineage_source_id", "event_key", name="uq_lineage_event_raw_source_event_key"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("lineage_source_configs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    event_key: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    event_type: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    producer: Mapped[str | None] = mapped_column(String(500), nullable=True)
    namespace: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    job_name: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    run_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True, index=True)
    schema_name: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    object_name: Mapped[str | None] = mapped_column(String(200), nullable=True, index=True)
    object_type: Mapped[str | None] = mapped_column(String(30), nullable=True)
    event_time: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    status: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_processed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)

    lineage_source: Mapped["LineageSourceConfig | None"] = relationship("LineageSourceConfig", back_populates="events_raw")


class LineageSyncCheckpoint(TimestampMixin, Base):
    __tablename__ = "lineage_sync_checkpoints"
    __table_args__ = (
        UniqueConstraint("lineage_source_id", "checkpoint_type", name="uq_lineage_sync_checkpoints_source_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_source_id: Mapped[int] = mapped_column(
        ForeignKey("lineage_source_configs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    checkpoint_type: Mapped[str] = mapped_column(String(40), nullable=False, default="openlineage")
    last_event_raw_id: Mapped[int | None] = mapped_column(ForeignKey("lineage_event_raw.id", ondelete="SET NULL"), nullable=True)
    last_processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    cursor_value: Mapped[str | None] = mapped_column(String(255), nullable=True)

    lineage_source: Mapped["LineageSourceConfig"] = relationship("LineageSourceConfig", back_populates="checkpoints")
