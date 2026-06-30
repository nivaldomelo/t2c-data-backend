from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime as SADateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class DataContract(TimestampMixin, Base):
    __tablename__ = "data_contracts"
    __table_args__ = (
        UniqueConstraint("table_id", "version", name="uq_data_contract_table_version"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="draft", server_default="draft")
    description: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
    owner_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    steward_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    published_at: Mapped[datetime | None] = mapped_column(SADateTime(timezone=True))
    freshness_hours: Mapped[int | None] = mapped_column(Integer)
    min_row_count: Mapped[int | None] = mapped_column(Integer)
    max_row_count: Mapped[int | None] = mapped_column(Integer)
    compatibility_rules_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    last_validation_status: Mapped[str | None] = mapped_column(String(30))
    last_validation_at: Mapped[datetime | None] = mapped_column(SADateTime(timezone=True))
    last_validation_issues: Mapped[int | None] = mapped_column(Integer)

    columns: Mapped[list["DataContractColumn"]] = relationship(
        "DataContractColumn",
        back_populates="contract",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    validations: Mapped[list["DataContractValidation"]] = relationship(
        "DataContractValidation",
        back_populates="contract",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DataContractColumn(TimestampMixin, Base):
    __tablename__ = "data_contract_columns"
    __table_args__ = (
        UniqueConstraint("contract_id", "column_name", name="uq_data_contract_column"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    contract_id: Mapped[int] = mapped_column(
        ForeignKey("data_contracts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    column_name: Mapped[str] = mapped_column(String(160), nullable=False)
    data_type: Mapped[str | None] = mapped_column(String(120))
    is_nullable: Mapped[bool | None] = mapped_column(Boolean)
    is_primary_key: Mapped[bool | None] = mapped_column(Boolean)
    is_required: Mapped[bool | None] = mapped_column(Boolean)
    ordinal_position: Mapped[int | None] = mapped_column(Integer)
    notes: Mapped[str | None] = mapped_column(Text)

    contract: Mapped[DataContract] = relationship("DataContract", back_populates="columns")


class DataContractValidation(TimestampMixin, Base):
    __tablename__ = "data_contract_validations"

    id: Mapped[int] = mapped_column(primary_key=True)
    contract_id: Mapped[int] = mapped_column(
        ForeignKey("data_contracts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    checked_at: Mapped[datetime] = mapped_column(SADateTime(timezone=True), nullable=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    issues_json: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    summary_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))

    contract: Mapped[DataContract] = relationship("DataContract", back_populates="validations")


__all__ = ["DataContract", "DataContractColumn", "DataContractValidation"]
