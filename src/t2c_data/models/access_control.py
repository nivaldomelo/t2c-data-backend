from __future__ import annotations

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.auth import User, user_access_group
from t2c_data.models.base import Base
from t2c_data.models.catalog import DataSource, Schema, TableEntity
from t2c_data.models.common import TimestampMixin


class AccessGroup(TimestampMixin, Base):
    __tablename__ = "access_groups"
    __table_args__ = (UniqueConstraint("name", name="uq_access_groups_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    users: Mapped[list[User]] = relationship("User", secondary=user_access_group, back_populates="access_groups")
    grants: Mapped[list[DataAccessGrant]] = relationship(
        "DataAccessGrant",
        back_populates="group",
        cascade="all, delete-orphan",
    )


class DataAccessGrant(TimestampMixin, Base):
    __tablename__ = "data_access_grants"
    __table_args__ = (
        CheckConstraint(
            "(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN group_id IS NOT NULL THEN 1 ELSE 0 END) = 1",
            name="ck_data_access_grants_principal",
        ),
        CheckConstraint(
            "(CASE WHEN datasource_id IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN schema_id IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN table_id IS NOT NULL THEN 1 ELSE 0 END) = 1",
            name="ck_data_access_grants_scope",
        ),
        CheckConstraint("effect IN ('allow', 'deny')", name="ck_data_access_grants_effect"),
        UniqueConstraint(
            "user_id",
            "group_id",
            "datasource_id",
            "schema_id",
            "table_id",
            "effect",
            name="uq_data_access_grants_unique",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    group_id: Mapped[int | None] = mapped_column(ForeignKey("access_groups.id", ondelete="CASCADE"))
    effect: Mapped[str] = mapped_column(String(10), nullable=False, default="allow")
    datasource_id: Mapped[int | None] = mapped_column(ForeignKey("data_sources.id", ondelete="CASCADE"))
    schema_id: Mapped[int | None] = mapped_column(ForeignKey("schemas.id", ondelete="CASCADE"))
    table_id: Mapped[int | None] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"))
    note: Mapped[str | None] = mapped_column(String(255))

    user: Mapped[User | None] = relationship("User", back_populates="access_grants")
    group: Mapped[AccessGroup | None] = relationship("AccessGroup", back_populates="grants")
    datasource: Mapped[DataSource | None] = relationship("DataSource")
    schema: Mapped[Schema | None] = relationship("Schema")
    table: Mapped[TableEntity | None] = relationship("TableEntity")

    @property
    def scope_kind(self) -> str:
        if self.table_id is not None:
            return "object"
        if self.schema_id is not None:
            return "schema"
        if self.datasource_id is not None:
            return "datasource"
        return "unknown"

