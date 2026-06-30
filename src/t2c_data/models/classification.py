from __future__ import annotations

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class ColumnClassification(TimestampMixin, Base):
    __tablename__ = "column_classifications"
    __table_args__ = (
        UniqueConstraint("column_id", name="uq_column_classifications_column"),
        Index("ix_column_classifications_taxonomy_key", "taxonomy_key"),
        Index("ix_column_classifications_review_status", "review_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    column_id: Mapped[int] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"), nullable=False, unique=True)
    taxonomy_key: Mapped[str] = mapped_column(String(80), nullable=False)
    taxonomy_label: Mapped[str] = mapped_column(String(120), nullable=False)
    taxonomy_group: Mapped[str] = mapped_column(String(40), nullable=False, default="operational", server_default="operational")
    review_status: Mapped[str] = mapped_column(String(30), nullable=False, default="approved", server_default="approved")
    source_kind: Mapped[str] = mapped_column(String(40), nullable=False, default="manual", server_default="manual")
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    is_personal_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_sensitive_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_financial_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_operational_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    evidence_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text)
    reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))

    column: Mapped["ColumnEntity"] = relationship("ColumnEntity", back_populates="classification")
    reviewed_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[reviewed_by_user_id])


class ColumnClassificationVersion(TimestampMixin, Base):
    __tablename__ = "column_classification_versions"
    __table_args__ = (
        Index("ix_column_classification_versions_column_id", "column_id"),
        Index("ix_column_classification_versions_decided_at", "decided_at"),
        Index("ix_column_classification_versions_status", "decision_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    column_id: Mapped[int] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"), nullable=False)
    column_classification_id: Mapped[int | None] = mapped_column(
        ForeignKey("column_classifications.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    decision_status: Mapped[str] = mapped_column(String(30), nullable=False)
    taxonomy_key: Mapped[str] = mapped_column(String(80), nullable=False)
    taxonomy_label: Mapped[str] = mapped_column(String(120), nullable=False)
    taxonomy_group: Mapped[str] = mapped_column(String(40), nullable=False, default="operational", server_default="operational")
    source_kind: Mapped[str] = mapped_column(String(40), nullable=False)
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    is_personal_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_sensitive_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_financial_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_operational_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    evidence_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text)
    decided_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    decided_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))

    column: Mapped["ColumnEntity"] = relationship("ColumnEntity", back_populates="classification_versions")
    classification: Mapped[ColumnClassification | None] = relationship("ColumnClassification")
    decided_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[decided_by_user_id])

