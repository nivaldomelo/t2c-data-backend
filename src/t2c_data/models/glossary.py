from __future__ import annotations

from sqlalchemy import ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class GlossaryTerm(TimestampMixin, Base):
    __tablename__ = "glossary_terms"
    __table_args__ = (
        UniqueConstraint("name", name="uq_glossary_terms_name"),
        UniqueConstraint("slug", name="uq_glossary_terms_slug"),
        UniqueConstraint("external_id", name="uq_glossary_terms_external_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    external_id: Mapped[str | None] = mapped_column(String(40))
    slug: Mapped[str] = mapped_column(String(160), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    definition: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    steward: Mapped[str | None] = mapped_column(String(120))
    category: Mapped[str | None] = mapped_column(String(120))
    subcategory: Mapped[str | None] = mapped_column(String(120))
    example_of_use: Mapped[str | None] = mapped_column(Text)
    synonyms: Mapped[str | None] = mapped_column(Text)
    suggested_priority: Mapped[str | None] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="active", server_default="active")
    tag_labels: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)

    assignments: Mapped[list[GlossaryAssignment]] = relationship(
        "GlossaryAssignment",
        back_populates="term",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class GlossaryAssignment(TimestampMixin, Base):
    __tablename__ = "glossary_assignments"
    __table_args__ = (
        UniqueConstraint("term_id", "entity_type", "entity_id", name="uq_glossary_assignment_entity"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    term_id: Mapped[int] = mapped_column(
        ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False
    )
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[int] = mapped_column(nullable=False)

    term: Mapped[GlossaryTerm] = relationship("GlossaryTerm", back_populates="assignments")
