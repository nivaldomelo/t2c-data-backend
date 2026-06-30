from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import JSON, ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin

if TYPE_CHECKING:
    from t2c_data.models.catalog import DataSource


class ScanRun(TimestampMixin, Base):
    __tablename__ = "scan_runs"
    __table_args__ = (
        Index("ix_scan_runs_datasource_created", "datasource_id", "created_at"),
        Index("ix_scan_runs_status_created", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    datasource_id: Mapped[int] = mapped_column(ForeignKey("data_sources.id", ondelete="CASCADE"))
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_by: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    summary: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    # ✅ adiciona este relacionamento (é o que está faltando)
    datasource: Mapped["DataSource"] = relationship("DataSource", back_populates="scan_runs")

    snapshots: Mapped[list[ScanSnapshot]] = relationship(
        "ScanSnapshot",
        back_populates="scan_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    diffs: Mapped[list[ScanDiff]] = relationship(
        "ScanDiff",
        back_populates="scan_run",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ScanSnapshot(TimestampMixin, Base):
    __tablename__ = "scan_snapshots"
    __table_args__ = (
        Index("ix_scan_snapshots_run_entity", "scan_run_id", "entity_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    scan_run_id: Mapped[int] = mapped_column(ForeignKey("scan_runs.id", ondelete="CASCADE"))
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_key: Mapped[str] = mapped_column(String(400), nullable=False, index=True)
    entity_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    scan_run: Mapped[ScanRun] = relationship("ScanRun", back_populates="snapshots")


class ScanDiff(TimestampMixin, Base):
    __tablename__ = "scan_diffs"
    __table_args__ = (
        Index("ix_scan_diffs_run_diff_type", "scan_run_id", "diff_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    scan_run_id: Mapped[int] = mapped_column(ForeignKey("scan_runs.id", ondelete="CASCADE"))
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_key: Mapped[str] = mapped_column(String(400), nullable=False, index=True)
    diff_type: Mapped[str] = mapped_column(String(20), nullable=False)
    old_hash: Mapped[str | None] = mapped_column(String(64))
    new_hash: Mapped[str | None] = mapped_column(String(64))
    details: Mapped[str | None] = mapped_column(Text)

    scan_run: Mapped[ScanRun] = relationship("ScanRun", back_populates="diffs")
