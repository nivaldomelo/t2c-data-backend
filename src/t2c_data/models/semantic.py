from __future__ import annotations

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class SemanticDomain(TimestampMixin, Base):
    __tablename__ = "semantic_domains"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_semantic_domains_slug"),
        UniqueConstraint("name", name="uq_semantic_domains_name"),
        Index("ix_semantic_domains_criticality", "criticality"),
        Index("ix_semantic_domains_maturity_status", "maturity_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(String(160), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner: Mapped[str | None] = mapped_column(String(160), nullable=True)
    steward: Mapped[str | None] = mapped_column(String(160), nullable=True)
    criticality: Mapped[str | None] = mapped_column(String(30), nullable=True)
    maturity_status: Mapped[str] = mapped_column(String(40), nullable=False, default="emerging", server_default="emerging")
    quality_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    governance_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")

    products: Mapped[list["SemanticDataProduct"]] = relationship(
        "SemanticDataProduct",
        back_populates="domain",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    links: Mapped[list["SemanticLink"]] = relationship(
        "SemanticLink",
        back_populates="domain",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class SemanticDataProduct(TimestampMixin, Base):
    __tablename__ = "semantic_data_products"
    __table_args__ = (
        UniqueConstraint("domain_id", "slug", name="uq_semantic_data_products_domain_slug"),
        UniqueConstraint("domain_id", "name", name="uq_semantic_data_products_domain_name"),
        Index("ix_semantic_data_products_domain_id", "domain_id"),
        Index("ix_semantic_data_products_maturity_status", "maturity_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    domain_id: Mapped[int] = mapped_column(
        ForeignKey("t2c_data.semantic_domains.id", ondelete="CASCADE"),
        nullable=False,
    )
    slug: Mapped[str] = mapped_column(String(160), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner: Mapped[str | None] = mapped_column(String(160), nullable=True)
    steward: Mapped[str | None] = mapped_column(String(160), nullable=True)
    consumers: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    sla_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    contract_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    maturity_status: Mapped[str] = mapped_column(String(40), nullable=False, default="emerging", server_default="emerging")
    quality_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    governance_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")

    domain: Mapped["SemanticDomain"] = relationship("SemanticDomain", back_populates="products")
    links: Mapped[list["SemanticLink"]] = relationship(
        "SemanticLink",
        back_populates="product",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class SemanticLink(TimestampMixin, Base):
    __tablename__ = "semantic_links"
    __table_args__ = (
        CheckConstraint(
            "(domain_id IS NOT NULL AND product_id IS NULL) OR (domain_id IS NULL AND product_id IS NOT NULL)",
            name="ck_semantic_links_scope",
        ),
        Index("ix_semantic_links_domain_id", "domain_id"),
        Index("ix_semantic_links_product_id", "product_id"),
        Index("ix_semantic_links_entity", "entity_kind", "entity_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    domain_id: Mapped[int | None] = mapped_column(
        ForeignKey("t2c_data.semantic_domains.id", ondelete="CASCADE"),
        nullable=True,
    )
    product_id: Mapped[int | None] = mapped_column(
        ForeignKey("t2c_data.semantic_data_products.id", ondelete="CASCADE"),
        nullable=True,
    )
    relation_kind: Mapped[str] = mapped_column(String(60), nullable=False)
    entity_kind: Mapped[str] = mapped_column(String(60), nullable=False)
    entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    entity_label: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_href: Mapped[str | None] = mapped_column(String(500), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    domain: Mapped["SemanticDomain | None"] = relationship("SemanticDomain", back_populates="links")
    product: Mapped["SemanticDataProduct | None"] = relationship("SemanticDataProduct", back_populates="links")
