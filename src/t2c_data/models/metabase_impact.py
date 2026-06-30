from __future__ import annotations

from datetime import datetime

from sqlalchemy import MetaData
from sqlalchemy import Boolean, DateTime, Index, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from t2c_data.models.common import TimestampMixin


class ControlBase(DeclarativeBase):
    metadata = MetaData(schema="controle")


class MetabaseAsset(TimestampMixin, ControlBase):
    __tablename__ = "metabase_assets"
    __table_args__ = (
        UniqueConstraint("instance_id", "metabase_object_id", name="uq_metabase_assets_instance_object"),
        {"schema": "controle"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instance_id: Mapped[int] = mapped_column(nullable=False, index=True)
    metabase_object_id: Mapped[int] = mapped_column(nullable=False, index=True)
    metabase_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    asset_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    collection_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    collection_external_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    archived: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)


class MetabaseTableDependency(TimestampMixin, ControlBase):
    __tablename__ = "metabase_table_dependencies"
    __table_args__ = (
        UniqueConstraint("instance_id", "table_id", "metabase_asset_id", name="uq_metabase_table_dependencies_unique"),
        {"schema": "controle"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instance_id: Mapped[int] = mapped_column(nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(nullable=False, index=True)
    metabase_asset_id: Mapped[int] = mapped_column(nullable=False, index=True)
    dependency_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    confidence_level: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", index=True)
    break_risk_on_drop: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", index=True)
    break_risk_on_change: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", index=True)
    details_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)


class MetabaseFieldDependency(TimestampMixin, ControlBase):
    __tablename__ = "metabase_field_dependencies"
    __table_args__ = (
        UniqueConstraint(
            "instance_id",
            "table_id",
            "column_id",
            "field_name",
            "metabase_asset_id",
            name="uq_metabase_field_dependencies_unique",
        ),
        {"schema": "controle"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instance_id: Mapped[int] = mapped_column(nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(nullable=False, index=True)
    column_id: Mapped[int | None] = mapped_column(nullable=True, index=True)
    field_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    metabase_asset_id: Mapped[int] = mapped_column(nullable=False, index=True)
    dependency_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    confidence_level: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", index=True)
    break_risk_on_drop: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", index=True)
    break_risk_on_change: Mapped[str] = mapped_column(String(20), nullable=False, default="medium", index=True)
    details_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)


class MetabaseImpactSnapshot(TimestampMixin, ControlBase):
    __tablename__ = "metabase_impact_snapshots"
    __table_args__ = (
        Index("ix_metabase_impact_snapshots_instance_table_created", "instance_id", "table_id", "created_at"),
        {"schema": "controle"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    instance_id: Mapped[int] = mapped_column(nullable=False, index=True)
    table_id: Mapped[int] = mapped_column(nullable=False, index=True)
    dashboard_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    question_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    model_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    asset_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    break_risk_on_drop: Mapped[str] = mapped_column(String(20), nullable=False, default="none")
    break_risk_on_change: Mapped[str] = mapped_column(String(20), nullable=False, default="none")
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    summary_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
