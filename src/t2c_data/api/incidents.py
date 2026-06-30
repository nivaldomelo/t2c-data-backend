from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import get_current_user
from t2c_data.features.incidents.api_support import (
    apply_incident_lifecycle_transition,
    build_incident_center_summary,
    build_incident_event_update_map,
    build_incident_filters,
    build_incident_summary,
    build_incident_update_map,
    can_edit_incident,
    create_incident_model,
    filter_incidents_for_user,
    get_incident_or_404,
    incident_query,
    _profile_map_for_incidents,
    build_incident_timeline,
    record_incident_event,
    serialize_incident_out,
    validate_incident_entity,
    validate_incident_user_refs,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.models.auth import User
from t2c_data.models.catalog import Schema, TableEntity
from t2c_data.models.incident import Incident
from t2c_data.schemas.incident import (
    IncidentCenterSummaryOut,
    IncidentCreate,
    IncidentEventCreate,
    IncidentEventOut,
    IncidentOut,
    IncidentSummaryOut,
    IncidentUpdate,
)
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync

router = APIRouter(prefix="/incidents", tags=["incidents"])

_ACTIVE_STATUSES = {"open", "investigating", "mitigated", "reopened", "recurring"}
_SLA_STATUSES = {"within_sla", "due_soon", "overdue"}


def _resolve_table_fqn(db: Session, table_id: int | None) -> str | None:
    if table_id is None:
        return None
    row = db.execute(
        select(Schema.name, TableEntity.name)
        .join(TableEntity, TableEntity.schema_id == Schema.id)
        .where(TableEntity.id == table_id)
    ).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return f"{row[0]}.{row[1]}"


def _incident_domain(item: Incident | IncidentOut) -> str:
    if isinstance(item, IncidentOut):
        if item.domain_name and item.domain_name.strip():
            return item.domain_name.strip()
        if item.asset_context and item.asset_context.domain_name:
            return item.asset_context.domain_name
        if item.entity_type == "airflow_dag":
            return "Integração / Airflow"
        return "Sem domínio"
    if item.domain_name and item.domain_name.strip():
        return item.domain_name.strip()
    if item.entity_type == "airflow_dag":
        return "Integração / Airflow"
    return "Sem domínio"


def _incident_owner_label(item: IncidentOut) -> str:
    if item.owner_team and item.owner_team.strip():
        return item.owner_team.strip()
    if item.squad_name and item.squad_name.strip():
        return item.squad_name.strip()
    if item.asset_context and item.asset_context.owner_name:
        return item.asset_context.owner_name
    if item.owner_user and (item.owner_user.name or item.owner_user.email):
        return item.owner_user.name or item.owner_user.email
    return "Sem responsável"


def _incident_sla_status(item: IncidentOut) -> str:
    if item.operational_sla is not None:
        return item.operational_sla.status
    due_at = item.sla_due_at
    if due_at is None:
        return "within_sla"
    now = datetime.now(timezone.utc)
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=timezone.utc)
    if due_at <= now:
        return "overdue"
    if due_at <= now + timedelta(hours=4):
        return "due_soon"
    return "within_sla"


def _incident_post_filter(item: IncidentOut, *, domain_name: str | None, owner_name: str | None, unassigned: bool | None, sla_status: str | None) -> bool:
    if domain_name and _incident_domain(item).strip().lower() != domain_name.strip().lower():
        return False
    if owner_name and _incident_owner_label(item).strip().lower() != owner_name.strip().lower():
        return False
    if unassigned is True and not (_incident_owner_label(item) == "Sem responsável"):
        return False
    if sla_status and _incident_sla_status(item) != sla_status:
        return False
    return True


def _incident_event_query(db: Session, incident_id: int) -> list[IncidentEventOut]:
    incident = get_incident_or_404(db, incident_id)
    return build_incident_timeline(db, incident)


def _incident_payload(db: Session, incident: Incident, *, current_user: User) -> IncidentOut:
    incidents, profile_map = filter_incidents_for_user(db, [incident], user=current_user)
    if not incidents:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Incident is not visible for this profile")
    timeline = build_incident_timeline(db, incidents[0])
    return serialize_incident_out(incidents[0], profile_map, timeline=timeline)


def _incident_event_detail_from_updates(updates: dict[str, Any]) -> str | None:
    notes: list[str] = []
    for key, label in (
        ("status", "status"),
        ("owner_user_id", "owner"),
        ("root_cause", "causa raiz"),
        ("impact_summary", "impacto"),
        ("mitigation_summary", "mitigação"),
        ("postmortem_summary", "postmortem"),
        ("evidence_json", "evidência"),
    ):
        if key in updates:
            notes.append(label)
    if not notes:
        return None
    return f"Atualização de {', '.join(notes)}."


@router.get("", response_model=PageOut[IncidentOut])
def list_incidents(
    status: list[str] | None = Query(default=None),
    severity: list[str] | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    reporter_id: int | None = Query(default=None),
    source_type: str | None = Query(default=None),
    source_ref_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    domain_name: str | None = Query(default=None),
    owner_name: str | None = Query(default=None),
    unassigned: bool | None = Query(default=None),
    sla_status: str | None = Query(default=None),
    q: str | None = Query(default=None),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> PageOut[IncidentOut]:
    query = incident_query()
    table_fqn = _resolve_table_fqn(db, table_id)
    filters = build_incident_filters(
        status,
        severity,
        entity_type,
        owner_id,
        reporter_id,
        source_type,
        source_ref_id,
        table_fqn,
        q,
        date_from,
        date_to,
    )

    if filters:
        query = query.where(and_(*filters))

    incidents = db.scalars(query.order_by(Incident.detected_at.desc(), Incident.id.desc())).all()
    incidents, profile_map = filter_incidents_for_user(db, incidents, user=current_user)
    payload = [serialize_incident_out(item, profile_map) for item in incidents]
    payload = [
        item
        for item in payload
        if _incident_post_filter(item, domain_name=domain_name, owner_name=owner_name, unassigned=unassigned, sla_status=sla_status)
    ]
    return paginate_items(payload, page=page, page_size=page_size)


@router.get("/summary", response_model=IncidentSummaryOut)
def incidents_summary(
    days: int = Query(default=30, ge=1, le=365),
    status: list[str] | None = Query(default=None),
    severity: list[str] | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    reporter_id: int | None = Query(default=None),
    source_type: str | None = Query(default=None),
    source_ref_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    q: str | None = Query(default=None),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IncidentSummaryOut:
    table_fqn = _resolve_table_fqn(db, table_id)
    return build_incident_summary(
        db,
        days=days,
        status=status,
        severity=severity,
        entity_type=entity_type,
        owner_id=owner_id,
        reporter_id=reporter_id,
        source_type=source_type,
        source_ref_id=source_ref_id,
        table_fqn=table_fqn,
        q=q,
        date_from=date_from,
        date_to=date_to,
        current_user=current_user,
    )


@router.get("/center", response_model=IncidentCenterSummaryOut)
def incidents_center(
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IncidentCenterSummaryOut:
    return build_incident_center_summary(db, days=days, current_user=current_user)


@router.post("", response_model=IncidentOut, status_code=status.HTTP_201_CREATED)
def create_incident(
    payload: IncidentCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IncidentOut:
    validate_incident_entity(payload)

    reporter_user_id = payload.reporter_user_id or current_user.id
    validate_incident_user_refs(db, reporter_user_id=reporter_user_id, owner_user_id=payload.owner_user_id)

    incident = create_incident_model(payload, reporter_user_id=reporter_user_id)
    db.add(incident)
    db.commit()
    db.refresh(incident)
    record_incident_event(
        db,
        incident=incident,
        event_type="created",
        title="Incidente criado",
        actor_user=current_user,
        detail=incident.description,
        status_to=incident.status,
        evidence_json=incident.evidence_json,
    )
    db.commit()
    write_audit_log_sync(
        db,
        action="incident.create",
        entity_type="incident",
        entity_id=incident.id,
        after=serialize_model(incident),
        **request_audit_kwargs(request, current_user),
    )
    db.commit()

    fresh = get_incident_or_404(db, incident.id)
    return _incident_payload(db, fresh, current_user=current_user)


@router.get("/{incident_id}", response_model=IncidentOut)
def get_incident(
    incident_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IncidentOut:
    incident = get_incident_or_404(db, incident_id)
    return _incident_payload(db, incident, current_user=current_user)


@router.get("/{incident_id}/events", response_model=list[IncidentEventOut])
def get_incident_events(
    incident_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[IncidentEventOut]:
    incident = get_incident_or_404(db, incident_id)
    incidents, _profile_map = filter_incidents_for_user(db, [incident], user=current_user)
    if not incidents:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Incident is not visible for this profile")
    return _incident_event_query(db, incident.id)


@router.post("/{incident_id}/events", response_model=IncidentEventOut, status_code=status.HTTP_201_CREATED)
def create_incident_event(
    incident_id: int,
    payload: IncidentEventCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IncidentEventOut:
    incident = get_incident_or_404(db, incident_id)
    if not can_edit_incident(current_user, incident):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permission")

    updates = build_incident_event_update_map(payload)
    if "status_to" in updates and updates["status_to"]:
        previous_status = incident.status
        incident.status = str(updates["status_to"])
        apply_incident_lifecycle_transition(
            incident,
            previous_status=previous_status,
            next_status=incident.status,
            occurred_at=datetime.now(timezone.utc),
        )
        incident.last_seen_at = datetime.now(timezone.utc)
    if "root_cause" in updates:
        incident.root_cause = updates["root_cause"]
        incident.last_seen_at = datetime.now(timezone.utc)
    if "impact_summary" in updates:
        incident.impact_summary = updates["impact_summary"]
        incident.last_seen_at = datetime.now(timezone.utc)
    if "mitigation_summary" in updates:
        incident.mitigation_summary = updates["mitigation_summary"]
        incident.last_seen_at = datetime.now(timezone.utc)
    if "postmortem_summary" in updates:
        incident.postmortem_summary = updates["postmortem_summary"]
        incident.last_seen_at = datetime.now(timezone.utc)
    if "evidence_json" in updates and updates["evidence_json"] is not None:
        incident.evidence_json = updates["evidence_json"]
        incident.last_seen_at = datetime.now(timezone.utc)
    db.add(incident)
    db.flush()
    event = record_incident_event(
        db,
        incident=incident,
        event_type=payload.event_type,
        title=payload.title,
        actor_user=current_user,
        detail=payload.detail,
        status_from=payload.status_from,
        status_to=payload.status_to,
        evidence_json=payload.evidence_json,
    )
    db.commit()
    write_audit_log_sync(
        db,
        action=f"incident.event.{payload.event_type}",
        entity_type="incident",
        entity_id=incident.id,
        after=serialize_model(event),
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return serialize_incident_event_out(event)


@router.put("/{incident_id}", response_model=IncidentOut)
def update_incident(
    incident_id: int,
    payload: IncidentUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> IncidentOut:
    incident = get_incident_or_404(db, incident_id)
    if not can_edit_incident(current_user, incident):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permission")
    before = serialize_model(incident)
    previous_status = incident.status

    validate_incident_entity(payload, fallback=incident)

    updates = build_incident_update_map(payload)
    validate_incident_user_refs(
        db,
        reporter_user_id=updates.get("reporter_user_id"),
        owner_user_id=updates.get("owner_user_id"),
    )

    for key, value in updates.items():
        setattr(incident, key, value)

    if "status" in updates and incident.status != previous_status:
        apply_incident_lifecycle_transition(
            incident,
            previous_status=previous_status,
            next_status=incident.status,
            occurred_at=datetime.now(timezone.utc),
        )
        incident.last_seen_at = incident.last_seen_at or datetime.now(timezone.utc)
    if any(key in updates for key in {"root_cause", "impact_summary", "mitigation_summary", "postmortem_summary", "evidence_json"}):
        incident.last_seen_at = incident.last_seen_at or datetime.now(timezone.utc)

    db.add(incident)
    db.commit()
    db.refresh(incident)

    if "status" in updates and incident.status != previous_status:
        record_incident_event(
            db,
            incident=incident,
            event_type="status_change",
            title="Status alterado",
            actor_user=current_user,
            detail=f"{previous_status} → {incident.status}",
            status_from=previous_status,
            status_to=incident.status,
        )
    if "owner_user_id" in updates and updates.get("owner_user_id") != before.get("owner_user_id"):
        record_incident_event(
            db,
            incident=incident,
            event_type="assignment",
            title="Responsável atualizado",
            actor_user=current_user,
            detail="Owner do incidente foi atualizado.",
            evidence_json={"owner_user_id": updates.get("owner_user_id")},
        )
    if any(key in updates for key in {"root_cause", "impact_summary", "mitigation_summary", "postmortem_summary", "evidence_json"}):
        record_incident_event(
            db,
            incident=incident,
            event_type="evidence",
            title="Evidência / postmortem atualizado",
            actor_user=current_user,
            detail=_incident_event_detail_from_updates(updates),
            evidence_json={key: updates.get(key) for key in ("root_cause", "impact_summary", "mitigation_summary", "postmortem_summary", "evidence_json") if key in updates},
        )
    db.commit()

    write_audit_log_sync(
        db,
        action="incident.update",
        entity_type="incident",
        entity_id=incident.id,
        before=before,
        after=serialize_model(incident),
        **request_audit_kwargs(request, current_user),
    )
    db.commit()

    fresh = get_incident_or_404(db, incident_id)
    return _incident_payload(db, fresh, current_user=current_user)


@router.delete("/{incident_id}", response_model=None, status_code=status.HTTP_204_NO_CONTENT)
def delete_incident(
    incident_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Response:
    incident = get_incident_or_404(db, incident_id)
    if not can_edit_incident(current_user, incident):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permission")
    before = serialize_model(incident)
    db.delete(incident)
    db.commit()
    write_audit_log_sync(
        db,
        action="incident.delete",
        entity_type="incident",
        entity_id=incident_id,
        before=before,
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
