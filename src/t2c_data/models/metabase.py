from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.core.secret_store import decrypt_secret_mapping, encrypt_secret_mapping
from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class MetabaseInstance(TimestampMixin, Base):
    __tablename__ = "metabase_instances"
    __table_args__ = (UniqueConstraint("name", name="uq_metabase_instances_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    auth_type: Mapped[str | None] = mapped_column(String(30), nullable=True)
    auth_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    _secret_payload: Mapped[str] = mapped_column("auth_secret", Text, nullable=False, default="")
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    sync_dashboards: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sync_questions: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sync_collections: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sync_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    last_sync_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_sync_dashboards: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_sync_questions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_sync_collections: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_sync_links: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_sync_unresolved: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_sync_warnings: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    objects: Mapped[list["MetabaseObject"]] = relationship(
        "MetabaseObject",
        back_populates="instance",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    links: Mapped[list["MetabaseObjectLink"]] = relationship(
        "MetabaseObjectLink",
        back_populates="instance",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    sync_runs: Mapped[list["MetabaseSyncRun"]] = relationship(
        "MetabaseSyncRun",
        back_populates="instance",
        cascade="all, delete-orphan",
        passive_deletes=True,
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


class MetabaseObject(TimestampMixin, Base):
    __tablename__ = "metabase_objects"
    __table_args__ = (
        UniqueConstraint("instance_id", "object_type", "external_id", name="uq_metabase_objects_instance_type_external"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instance_id: Mapped[int] = mapped_column(
        ForeignKey("metabase_instances.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    external_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    object_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    collection_external_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    collection_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    database_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    archived: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    remote_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    dataset_query_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    referenced_tables_json: Mapped[list | None] = mapped_column(JSON, nullable=True)
    raw_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    instance: Mapped["MetabaseInstance"] = relationship("MetabaseInstance", back_populates="objects")
    links: Mapped[list["MetabaseObjectLink"]] = relationship(
        "MetabaseObjectLink",
        back_populates="object",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class MetabaseObjectLink(TimestampMixin, Base):
    __tablename__ = "metabase_object_links"
    __table_args__ = ()

    id: Mapped[int] = mapped_column(primary_key=True)
    instance_id: Mapped[int] = mapped_column(
        ForeignKey("metabase_instances.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    metabase_object_id: Mapped[int] = mapped_column(
        ForeignKey("metabase_objects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    column_id: Mapped[int | None] = mapped_column(ForeignKey("columns.id", ondelete="SET NULL"), nullable=True, index=True)
    match_method: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    confidence_level: Mapped[str] = mapped_column(String(20), nullable=False, default="partial", index=True)
    confidence_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_database_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_column_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)

    instance: Mapped["MetabaseInstance"] = relationship("MetabaseInstance", back_populates="links")
    object: Mapped["MetabaseObject"] = relationship("MetabaseObject", back_populates="links")


class MetabaseSyncRun(TimestampMixin, Base):
    __tablename__ = "metabase_sync_runs"
    __table_args__ = ()

    id: Mapped[int] = mapped_column(primary_key=True)
    instance_id: Mapped[int] = mapped_column(
        ForeignKey("metabase_instances.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dashboards_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    questions_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    collections_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    links_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unresolved_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    warnings_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    instance: Mapped["MetabaseInstance"] = relationship("MetabaseInstance", back_populates="sync_runs")


Index(
    "uq_metabase_object_links_object_table_column_method",
    MetabaseObjectLink.metabase_object_id,
    MetabaseObjectLink.table_id,
    func.coalesce(MetabaseObjectLink.column_id, -1),
    MetabaseObjectLink.match_method,
    unique=True,
)
