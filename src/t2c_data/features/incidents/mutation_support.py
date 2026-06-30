from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from t2c_data.models.auth import User
from t2c_data.models.incident import Incident
from t2c_data.schemas.incident import IncidentCreate, IncidentEventCreate, IncidentUpdate

_STATUS_TRANSITION_TIMESTAMP_FIELDS = {
    "investigating": "triaged_at",
    "mitigated": "mitigated_at",
    "resolved": "resolved_at",
    "closed": "closed_at",
    "reopened": "reopened_at",
    "recurring": "reopened_at",
}


def _now(value: datetime | None = None) -> datetime:
    if value is not None:
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def validate_incident_entity(payload: IncidentCreate | IncidentUpdate, fallback: Incident | None = None) -> None:
    entity_type = payload.entity_type if payload.entity_type is not None else (fallback.entity_type if fallback else None)
    table_fqn = payload.table_fqn if payload.table_fqn is not None else (fallback.table_fqn if fallback else None)
    airflow_dag_id = (
        payload.airflow_dag_id if payload.airflow_dag_id is not None else (fallback.airflow_dag_id if fallback else None)
    )

    if entity_type == "table" and not (table_fqn and table_fqn.strip()):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="table_fqn is required for table incidents")
    if entity_type == "airflow_dag" and not (airflow_dag_id and airflow_dag_id.strip()):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="airflow_dag_id is required for airflow_dag incidents",
        )


def validate_incident_user_refs(
    db: Session,
    *,
    reporter_user_id: int | None,
    owner_user_id: int | None,
) -> None:
    if reporter_user_id is not None and not db.get(User, reporter_user_id):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid reporter_user_id")
    if owner_user_id is not None and not db.get(User, owner_user_id):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid owner_user_id")


def create_incident_model(payload: IncidentCreate, *, reporter_user_id: int) -> Incident:
    detected_at = payload.detected_at if payload.detected_at.tzinfo is not None else payload.detected_at.replace(tzinfo=timezone.utc)
    incident = Incident(
        title=payload.title.strip(),
        description=payload.description,
        entity_type=payload.entity_type,
        table_fqn=payload.table_fqn.strip() if payload.table_fqn else None,
        airflow_dag_id=payload.airflow_dag_id.strip() if payload.airflow_dag_id else None,
        detected_at=detected_at,
        last_seen_at=payload.last_seen_at,
        acknowledged_at=payload.acknowledged_at,
        triaged_at=payload.triaged_at,
        mitigated_at=payload.mitigated_at,
        resolved_at=payload.resolved_at,
        closed_at=payload.closed_at,
        reopened_at=payload.reopened_at,
        sla_due_at=payload.sla_due_at,
        status=payload.status,
        severity=payload.severity,
        owner_user_id=payload.owner_user_id,
        reporter_user_id=reporter_user_id,
        tags=payload.tags,
        source_type=payload.source_type,
        source_ref_id=payload.source_ref_id,
        evidence_json=payload.evidence_json,
        technical_origin_json=payload.technical_origin_json,
        related_links_json=payload.related_links_json,
        impact_json=payload.impact_json,
        mitigation_json=payload.mitigation_json,
        postmortem_json=payload.postmortem_json,
        root_cause=payload.root_cause,
        impact_summary=payload.impact_summary,
        mitigation_summary=payload.mitigation_summary,
        postmortem_summary=payload.postmortem_summary,
        domain_name=payload.domain_name,
        owner_team=payload.owner_team,
        squad_name=payload.squad_name,
        recurrence_count=payload.recurrence_count or 0,
        occurrences=payload.occurrences or 1,
    )
    apply_incident_lifecycle_transition(incident, previous_status=None, next_status=payload.status, occurred_at=detected_at)
    return incident


def apply_incident_lifecycle_transition(
    incident: Incident,
    *,
    previous_status: str | None,
    next_status: str,
    occurred_at: datetime | None = None,
) -> None:
    timestamp = _now(occurred_at)
    if next_status in {"investigating", "mitigated", "resolved", "closed", "reopened", "recurring"}:
        if incident.acknowledged_at is None:
            incident.acknowledged_at = timestamp
    timestamp_field = _STATUS_TRANSITION_TIMESTAMP_FIELDS.get(next_status)
    if timestamp_field and getattr(incident, timestamp_field) is None:
        setattr(incident, timestamp_field, timestamp)
    if previous_status in {"resolved", "closed"} and next_status in {"open", "investigating", "reopened", "recurring"}:
        incident.reopened_at = timestamp
    if next_status == "reopened":
        incident.reopened_at = timestamp
    if next_status == "recurring":
        incident.recurrence_count = int(incident.recurrence_count or 0) + 1


def build_incident_event_update_map(payload: IncidentEventCreate) -> dict:
    updates = payload.model_dump(exclude_unset=True)
    for key, value in list(updates.items()):
        if isinstance(value, str) and key in {"title", "detail", "root_cause", "impact_summary", "mitigation_summary", "postmortem_summary"}:
            updates[key] = value.strip() or None
    return updates


def build_incident_update_map(payload: IncidentUpdate) -> dict:
    updates = payload.model_dump(exclude_unset=True)
    for key, value in list(updates.items()):
        if key in {"table_fqn", "airflow_dag_id"} and isinstance(value, str):
            updates[key] = value.strip() or None
        elif key == "title" and isinstance(value, str):
            updates[key] = value.strip()
        elif key in {"root_cause", "impact_summary", "mitigation_summary", "postmortem_summary"} and isinstance(value, str):
            updates[key] = value.strip() or None
    return updates


__all__ = [
    "apply_incident_lifecycle_transition",
    "build_incident_update_map",
    "build_incident_event_update_map",
    "create_incident_model",
    "validate_incident_entity",
    "validate_incident_user_refs",
]
