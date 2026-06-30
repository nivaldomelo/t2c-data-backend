from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class TableSearchAlias(TimestampMixin, Base):
    __tablename__ = "table_search_aliases"
    __table_args__ = (
        UniqueConstraint("table_id", "label_kind", "label", name="uq_table_search_alias_label"),
        Index("ix_table_search_aliases_table_kind", "table_id", "label_kind"),
        Index("ix_table_search_aliases_normalized", "normalized_label"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False, index=True)
    label_kind: Mapped[str] = mapped_column(String(30), nullable=False)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_label: Mapped[str] = mapped_column(String(255), nullable=False)

    table = relationship("TableEntity")


class ColumnSearchAlias(TimestampMixin, Base):
    __tablename__ = "column_search_aliases"
    __table_args__ = (
        UniqueConstraint("column_id", "label_kind", "label", name="uq_column_search_alias_label"),
        Index("ix_column_search_aliases_column_kind", "column_id", "label_kind"),
        Index("ix_column_search_aliases_normalized", "normalized_label"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    column_id: Mapped[int] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"), nullable=False, index=True)
    label_kind: Mapped[str] = mapped_column(String(30), nullable=False)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_label: Mapped[str] = mapped_column(String(255), nullable=False)

    column = relationship("ColumnEntity")


class SearchQueryHistory(TimestampMixin, Base):
    __tablename__ = "search_query_history"
    __table_args__ = (
        UniqueConstraint("user_id", "normalized_query", name="uq_search_query_history_user_query"),
        Index("ix_search_query_history_user_recent", "user_id", "last_searched_at"),
        Index("ix_search_query_history_normalized", "normalized_query"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    raw_query: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_query: Mapped[str] = mapped_column(String(255), nullable=False)
    search_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    last_searched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=func.now())

    user = relationship("User")


class SearchResultClick(TimestampMixin, Base):
    __tablename__ = "search_result_clicks"
    __table_args__ = (
        Index("ix_search_result_clicks_entity", "entity_type", "entity_id"),
        Index("ix_search_result_clicks_user_created", "user_id", "created_at"),
        Index("ix_search_result_clicks_query", "normalized_query"),
        Index("ix_search_result_clicks_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False)
    query_text: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_query: Mapped[str | None] = mapped_column(String(255), nullable=True)
    target_url: Mapped[str | None] = mapped_column(String(500), nullable=True)

    user = relationship("User")


class SearchFavoriteAsset(TimestampMixin, Base):
    __tablename__ = "search_favorite_assets"
    __table_args__ = (
        UniqueConstraint("user_id", "entity_type", "entity_id", name="uq_search_favorite_assets_user_entity"),
        Index("ix_search_favorite_assets_user_created", "user_id", "created_at"),
        Index("ix_search_favorite_assets_entity", "entity_type", "entity_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    target_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    category: Mapped[str | None] = mapped_column(String(80), nullable=True)
    subtitle: Mapped[str | None] = mapped_column(String(255), nullable=True)
    context_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    metadata_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)

    user = relationship("User")
