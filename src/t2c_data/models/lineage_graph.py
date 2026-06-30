from __future__ import annotations

from sqlalchemy import JSON, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class LineageNode(TimestampMixin, Base):
    # Compatibility-only graph table from the first table-centric lineage editor.
    # New graph reads are derived from lineage_assets + lineage_relations.
    __tablename__ = "lineage_nodes"

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=True
    )
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="SET NULL"), nullable=True)
    meta: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class LineageGraphEdge(TimestampMixin, Base):
    # Compatibility-only graph edge table from the first table-centric lineage editor.
    __tablename__ = "lineage_graph_edges"

    id: Mapped[int] = mapped_column(primary_key=True)
    lineage_table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"), nullable=False)
    from_node_id: Mapped[int] = mapped_column(ForeignKey("lineage_nodes.id", ondelete="CASCADE"), nullable=False)
    to_node_id: Mapped[int] = mapped_column(ForeignKey("lineage_nodes.id", ondelete="CASCADE"), nullable=False)
    edge_type: Mapped[str] = mapped_column(String(30), nullable=False, default="data_flow")
    transform: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
