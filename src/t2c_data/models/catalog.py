from __future__ import annotations

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.core.secret_store import decrypt_secret_mapping, encrypt_secret_mapping
from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class DataSource(TimestampMixin, Base):
    __tablename__ = "data_sources"
    __table_args__ = (UniqueConstraint("name", name="uq_data_sources_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    db_type: Mapped[str] = mapped_column(String(20), nullable=False, default="postgres")
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    database: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    environment: Mapped[str | None] = mapped_column(String(40), nullable=True, default="shared", server_default="shared", index=True)
    _secret_payload: Mapped[str] = mapped_column("password", Text, nullable=False, default="")
    connection_config: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    detected_schemas: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    include_schemas: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    exclude_schemas: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    databases: Mapped[list[Database]] = relationship(
        "Database",
        back_populates="datasource",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    scan_runs: Mapped[list[ScanRun]] = relationship(
        "ScanRun",
        back_populates="datasource",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    @property
    def secret_values(self) -> dict[str, str]:
        return decrypt_secret_mapping(self._secret_payload)

    def get_secret(self, key: str) -> str | None:
        value = self.secret_values.get(key)
        return value or None

    def set_secret_values(self, values: dict[str, str] | None) -> None:
        self._secret_payload = encrypt_secret_mapping(values or {})

    @property
    def password(self) -> str:
        return self.get_secret("password") or ""

    @password.setter
    def password(self, value: str) -> None:
        current = self.secret_values
        if value:
            current["password"] = value
        else:
            current.pop("password", None)
        self.set_secret_values(current)


class Database(TimestampMixin, Base):
    __tablename__ = "databases"
    __table_args__ = (UniqueConstraint("datasource_id", "name", name="uq_databases_datasource_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    datasource_id: Mapped[int] = mapped_column(ForeignKey("data_sources.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description_source: Mapped[str | None] = mapped_column(Text)
    description_manual: Mapped[str | None] = mapped_column(Text)
    owner: Mapped[str | None] = mapped_column(String(120))
    lifecycle_status: Mapped[str | None] = mapped_column(String(50))

    datasource: Mapped[DataSource] = relationship("DataSource", back_populates="databases")
    schemas: Mapped[list[Schema]] = relationship(
        "Schema",
        back_populates="database",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class Schema(TimestampMixin, Base):
    __tablename__ = "schemas"
    __table_args__ = (UniqueConstraint("database_id", "name", name="uq_schemas_database_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    database_id: Mapped[int] = mapped_column(ForeignKey("databases.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description_source: Mapped[str | None] = mapped_column(Text)
    description_manual: Mapped[str | None] = mapped_column(Text)
    owner: Mapped[str | None] = mapped_column(String(120))
    lifecycle_status: Mapped[str | None] = mapped_column(String(50))

    database: Mapped[Database] = relationship("Database", back_populates="schemas")
    tables: Mapped[list[TableEntity]] = relationship(
        "TableEntity",
        back_populates="schema",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DataOwner(TimestampMixin, Base):
    __tablename__ = "data_owners"
    __table_args__ = (
        Index("ix_data_owners_name", "name"),
        Index("ix_data_owners_is_active", "is_active"),
        UniqueConstraint("email", name="uq_data_owners_email"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    area: Mapped[str | None] = mapped_column(String(160))
    description: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    tables: Mapped[list[TableEntity]] = relationship("TableEntity", back_populates="data_owner")
    columns: Mapped[list["ColumnEntity"]] = relationship("ColumnEntity", back_populates="data_owner")


class TableEntity(TimestampMixin, Base):
    __tablename__ = "tables"
    __table_args__ = (
        UniqueConstraint("schema_id", "name", name="uq_tables_schema_name"),
        Index("ix_tables_data_owner_id", "data_owner_id"),
        Index("ix_tables_steward_user_id", "steward_user_id"),
        Index("ix_tables_schema_updated_at", "schema_id", "updated_at"),
        Index("ix_tables_certification_status_review_at", "certification_status", "certification_review_at"),
        Index("ix_tables_privacy_flags_reviewed_at", "has_personal_data", "has_sensitive_personal_data", "privacy_reviewed_at"),
        Index("ix_tables_access_scope_sensitivity", "access_scope", "sensitivity_level"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    schema_id: Mapped[int] = mapped_column(ForeignKey("schemas.id", ondelete="CASCADE"))
    data_owner_id: Mapped[int | None] = mapped_column(ForeignKey("data_owners.id", ondelete="SET NULL"))
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    table_type: Mapped[str] = mapped_column(String(20), nullable=False)
    description_source: Mapped[str | None] = mapped_column(Text)
    description_manual: Mapped[str | None] = mapped_column(Text)
    owner: Mapped[str | None] = mapped_column(String(120))
    owner_email: Mapped[str | None] = mapped_column(String(255))
    steward_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    lifecycle_status: Mapped[str | None] = mapped_column(String(50))
    certification_status: Mapped[str] = mapped_column(String(40), nullable=False, default="not_eligible")
    certification_criticality: Mapped[str | None] = mapped_column(String(20))
    certification_badges: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    certification_notes: Mapped[str | None] = mapped_column(Text)
    certification_submitted_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    certification_submitted_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    certification_decided_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    certification_decided_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    certification_review_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    certification_expires_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    owner_reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    owner_reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    sensitivity_level: Mapped[str | None] = mapped_column(String(30))
    has_personal_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_sensitive_personal_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    legal_basis: Mapped[str | None] = mapped_column(String(50))
    privacy_purpose: Mapped[str | None] = mapped_column(Text)
    retention_policy: Mapped[str | None] = mapped_column(String(255))
    is_masked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    external_sharing: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    access_scope: Mapped[str | None] = mapped_column(String(30))
    access_roles: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    privacy_notes: Mapped[str | None] = mapped_column(Text)
    privacy_reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    privacy_reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    schema_hash: Mapped[str | None] = mapped_column(String(64), index=True)

    schema: Mapped[Schema] = relationship("Schema", back_populates="tables")
    data_owner: Mapped[DataOwner | None] = relationship("DataOwner", back_populates="tables")
    certification_submitted_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[certification_submitted_by_user_id])
    certification_decided_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[certification_decided_by_user_id])
    owner_reviewed_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[owner_reviewed_by_user_id])
    privacy_reviewed_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[privacy_reviewed_by_user_id])
    steward: Mapped["User | None"] = relationship("User", foreign_keys=[steward_user_id])
    columns: Mapped[list[ColumnEntity]] = relationship(
        "ColumnEntity",
        back_populates="table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    @property
    def certification_submitted_by_user_name(self) -> str | None:
        if not self.certification_submitted_by_user:
            return None
        return self.certification_submitted_by_user.name or self.certification_submitted_by_user.full_name

    @property
    def certification_submitted_by_user_email(self) -> str | None:
        if not self.certification_submitted_by_user:
            return None
        return self.certification_submitted_by_user.email

    @property
    def certification_decided_by_user_name(self) -> str | None:
        if not self.certification_decided_by_user:
            return None
        return self.certification_decided_by_user.name or self.certification_decided_by_user.full_name

    @property
    def certification_decided_by_user_email(self) -> str | None:
        if not self.certification_decided_by_user:
            return None
        return self.certification_decided_by_user.email

    @property
    def privacy_reviewed_by_user_name(self) -> str | None:
        if not self.privacy_reviewed_by_user:
            return None
        return self.privacy_reviewed_by_user.name or self.privacy_reviewed_by_user.full_name

    @property
    def privacy_reviewed_by_user_email(self) -> str | None:
        if not self.privacy_reviewed_by_user:
            return None
        return self.privacy_reviewed_by_user.email

    @property
    def owner_reviewed_by_user_name(self) -> str | None:
        if not self.owner_reviewed_by_user:
            return None
        return self.owner_reviewed_by_user.name or self.owner_reviewed_by_user.full_name

    @property
    def owner_reviewed_by_user_email(self) -> str | None:
        if not self.owner_reviewed_by_user:
            return None
        return self.owner_reviewed_by_user.email


class ColumnEntity(TimestampMixin, Base):
    __tablename__ = "columns"
    __table_args__ = (UniqueConstraint("table_id", "name", name="uq_columns_table_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"))
    data_owner_id: Mapped[int | None] = mapped_column(ForeignKey("data_owners.id", ondelete="SET NULL"))
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    data_type: Mapped[str] = mapped_column(String(200), nullable=False)
    is_primary_key: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_nullable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ordinal_position: Mapped[int] = mapped_column(Integer, nullable=False)
    description_source: Mapped[str | None] = mapped_column(Text)
    description_manual: Mapped[str | None] = mapped_column(Text)
    external_id: Mapped[str | None] = mapped_column(String(64))
    slug: Mapped[str | None] = mapped_column(String(255), index=True)
    udt_name: Mapped[str | None] = mapped_column(String(255))
    character_maximum_length: Mapped[int | None] = mapped_column(Integer)
    numeric_precision: Mapped[int | None] = mapped_column(Integer)
    numeric_scale: Mapped[int | None] = mapped_column(Integer)
    column_default: Mapped[str | None] = mapped_column(Text)
    existing_comment: Mapped[str | None] = mapped_column(Text)
    dictionary_description: Mapped[str | None] = mapped_column(Text)
    dictionary_comment: Mapped[str | None] = mapped_column(Text)
    owner_reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    owner_reviewed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))

    table: Mapped[TableEntity] = relationship("TableEntity", back_populates="columns")
    data_owner: Mapped[DataOwner | None] = relationship("DataOwner", back_populates="columns")
    owner_reviewed_by_user: Mapped["User | None"] = relationship("User", foreign_keys=[owner_reviewed_by_user_id])
    classification: Mapped["ColumnClassification | None"] = relationship(
        "ColumnClassification",
        back_populates="column",
        uselist=False,
    )
    classification_versions: Mapped[list["ColumnClassificationVersion"]] = relationship(
        "ColumnClassificationVersion",
        back_populates="column",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    @property
    def owner_reviewed_by_user_name(self) -> str | None:
        if not self.owner_reviewed_by_user:
            return None
        return self.owner_reviewed_by_user.name or self.owner_reviewed_by_user.full_name

    @property
    def owner_reviewed_by_user_email(self) -> str | None:
        if not self.owner_reviewed_by_user:
            return None
        return self.owner_reviewed_by_user.email
