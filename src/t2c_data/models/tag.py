from __future__ import annotations

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class Tag(TimestampMixin, Base):
    __tablename__ = "tags"
    __table_args__ = (
        UniqueConstraint("name", name="uq_tags_name"),
        UniqueConstraint("slug", name="uq_tags_slug"),
        UniqueConstraint("external_id", name="uq_tags_external_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    external_id: Mapped[str | None] = mapped_column(String(40))
    slug: Mapped[str] = mapped_column(String(160), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    color: Mapped[str | None] = mapped_column(String(20))
    description: Mapped[str | None] = mapped_column(Text)
    group_name: Mapped[str | None] = mapped_column(String(120))
    subgroup_name: Mapped[str | None] = mapped_column(String(120))
    example_of_use: Mapped[str | None] = mapped_column(Text)
    tag_type: Mapped[str | None] = mapped_column(String(120))
    suggested_scope: Mapped[str | None] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="active", server_default="active")
    synonyms: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)

    assignments: Mapped[list[TagAssignment]] = relationship(
        "TagAssignment",
        back_populates="tag",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class TagAssignment(TimestampMixin, Base):
    __tablename__ = "tag_assignments"
    __table_args__ = (
        UniqueConstraint("tag_id", "entity_type", "entity_id", name="uq_tag_assignment_entity"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id", ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[int] = mapped_column(nullable=False)
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    inference_source: Mapped[str | None] = mapped_column(String(80))
    inference_reason: Mapped[str | None] = mapped_column(Text)
    evidence_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    applied_automatically: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    review_status: Mapped[str] = mapped_column(String(30), nullable=False, default="manual_applied", server_default="manual_applied")
    rule_key: Mapped[str | None] = mapped_column(String(120))
    rule_label: Mapped[str | None] = mapped_column(String(160))
    reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))

    tag: Mapped[Tag] = relationship("Tag", back_populates="assignments")
    reviewed_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[reviewed_by_user_id])


class TagAssignmentOverride(TimestampMixin, Base):
    __tablename__ = "tag_assignment_overrides"
    __table_args__ = (
        UniqueConstraint("tag_id", "entity_type", "entity_id", name="uq_tag_assignment_override_entity"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id", ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[int] = mapped_column(nullable=False)
    state: Mapped[str] = mapped_column(String(30), nullable=False, default="blocked", server_default="blocked")
    reason: Mapped[str | None] = mapped_column(Text)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))

    tag: Mapped[Tag] = relationship("Tag")


class TagIntelligenceEvent(TimestampMixin, Base):
    __tablename__ = "tag_intelligence_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id", ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[int | None] = mapped_column(
        ForeignKey("data_sources.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_type: Mapped[str] = mapped_column(String(40), nullable=False)
    entity_id: Mapped[int] = mapped_column(nullable=False)
    rule_key: Mapped[str | None] = mapped_column(String(120))
    rule_label: Mapped[str | None] = mapped_column(String(160))
    inference_source: Mapped[str | None] = mapped_column(String(80))
    inference_reason: Mapped[str | None] = mapped_column(Text)
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    applied_automatically: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    review_status: Mapped[str] = mapped_column(String(30), nullable=False, default="suggested", server_default="suggested")
    evidence: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))

    tag: Mapped[Tag] = relationship("Tag")


class TagAutomationRule(TimestampMixin, Base):
    __tablename__ = "tag_automation_rules"

    id: Mapped[int] = mapped_column(primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    scope: Mapped[str] = mapped_column(String(40), nullable=False, default="column", server_default="column")
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="active", server_default="active")
    action: Mapped[str] = mapped_column(String(30), nullable=False, default="apply", server_default="apply")
    category: Mapped[str | None] = mapped_column(String(60))
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    match_fields: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    keywords: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    aliases: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    regex_pattern: Mapped[str | None] = mapped_column(Text)
    min_confidence: Mapped[int] = mapped_column(Integer, nullable=False, default=90, server_default="90")
    notes: Mapped[str | None] = mapped_column(Text)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))

    tag: Mapped[Tag] = relationship("Tag")
