from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Index, JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from t2c_data.models.auth import User
from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin

incident_entity_type_enum = Enum(
    "table",
    "airflow_dag",
    name="incident_entity_type",
    native_enum=False,
    validate_strings=True,
)

incident_status_enum = Enum(
    "open",
    "investigating",
    "mitigated",
    "resolved",
    "closed",
    "reopened",
    "recurring",
    name="incident_status",
    native_enum=False,
    validate_strings=True,
)

incident_severity_enum = Enum(
    "sev1",
    "sev2",
    "sev3",
    "sev4",
    name="incident_severity",
    native_enum=False,
    validate_strings=True,
)


class Incident(TimestampMixin, Base):
    __tablename__ = "incidents"
    __table_args__ = (
        Index("ix_incidents_status", "status"),
        Index("ix_incidents_severity", "severity"),
        Index("ix_incidents_entity_type", "entity_type"),
        Index("ix_incidents_detected_at", "detected_at"),
        Index("ix_incidents_sla_due_at", "sla_due_at"),
        Index("ix_incidents_owner_user_id", "owner_user_id"),
        Index("ix_incidents_domain_name", "domain_name"),
        Index("ix_incidents_owner_team", "owner_team"),
        Index("ix_incidents_table_fqn", "table_fqn"),
        Index("ix_incidents_airflow_dag_id", "airflow_dag_id"),
        Index("ix_incidents_source_ref", "source_type", "source_ref_id"),
        Index("ix_incidents_source_ref_status", "source_type", "source_ref_id", "status"),
        Index("ix_incidents_status_detected_at", "status", "detected_at"),
        {"schema": "t2c_ops"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)

    entity_type: Mapped[str] = mapped_column(incident_entity_type_enum, nullable=False)
    table_fqn: Mapped[str | None] = mapped_column(String(500), nullable=True)
    airflow_dag_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=func.now())
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    triaged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    mitigated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reopened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sla_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[str] = mapped_column(incident_status_enum, nullable=False, default="open")
    severity: Mapped[str] = mapped_column(incident_severity_enum, nullable=False, default="sev3")
    source_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source_ref_id: Mapped[int | None] = mapped_column(nullable=True)
    evidence_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    technical_origin_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    related_links_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    impact_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    mitigation_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    postmortem_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    root_cause: Mapped[str | None] = mapped_column(Text, nullable=True)
    impact_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    mitigation_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    postmortem_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    domain_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    owner_team: Mapped[str | None] = mapped_column(String(255), nullable=True)
    squad_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    recurrence_count: Mapped[int] = mapped_column(nullable=False, default=0)
    occurrences: Mapped[int] = mapped_column(nullable=False, default=1)

    owner_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    reporter_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    tags: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    owner_user: Mapped[User | None] = relationship("User", foreign_keys=[owner_user_id])
    reporter_user: Mapped[User | None] = relationship("User", foreign_keys=[reporter_user_id])
    events: Mapped[list["IncidentEvent"]] = relationship(
        "IncidentEvent",
        back_populates="incident",
        cascade="all, delete-orphan",
        order_by=lambda: IncidentEvent.created_at.asc(),
    )


class IncidentEvent(TimestampMixin, Base):
    __tablename__ = "incident_events"
    __table_args__ = (
        Index("ix_incident_events_incident_created", "incident_id", "created_at"),
        Index("ix_incident_events_event_type", "event_type"),
        {"schema": "t2c_ops"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    incident_id: Mapped[int] = mapped_column(
        ForeignKey("t2c_ops.incidents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(String(60), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_from: Mapped[str | None] = mapped_column(String(40), nullable=True)
    status_to: Mapped[str | None] = mapped_column(String(40), nullable=True)
    evidence_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    actor_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    actor_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    actor_email: Mapped[str | None] = mapped_column(String(255), nullable=True)

    incident: Mapped[Incident] = relationship("Incident", back_populates="events")
    actor_user: Mapped[User | None] = relationship("User", foreign_keys=[actor_user_id])
