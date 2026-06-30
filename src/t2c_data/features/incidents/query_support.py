from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, status
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.operational_context import build_asset_links, build_contextual_actions, incident_impact_payload, incident_origin_payload
from t2c_data.features.dashboard.executive_scoring import compute_priority_score, risk_label
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.privacy_access.policy import sensitivity_label
from t2c_data.features.platform.visibility import mask_incident_asset_context_payload, visibility_for_profiles
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.incident import Incident
from t2c_data.schemas.incident import IncidentAssetContextOut, IncidentEventOut, IncidentImpactOut, IncidentOriginOut, IncidentOut, IncidentSummaryOut, IncidentUserRefOut

SEVERITY_LABELS: dict[str, str] = {
    "sev1": "Crítico",
    "sev2": "Alto",
    "sev3": "Médio",
    "sev4": "Baixo",
}
SEVERITY_ALIASES: dict[str, str] = {
    "sev1": "sev1",
    "sev2": "sev2",
    "sev3": "sev3",
    "sev4": "sev4",
    "critical": "sev1",
    "high": "sev2",
    "medium": "sev3",
    "low": "sev4",
}

_OPERATIONAL_INCIDENT_SOURCES = {"platform_ops", "pipeline_failure", "pipeline_stale", "ingestion_ops"}
_OPERATIONAL_SLA_STATUS_LABELS = {
    "within_sla": "Dentro do SLA",
    "due_soon": "SLA próximo do vencimento",
    "overdue": "Fora do SLA",
}


def _is_admin(user: User) -> bool:
    return any(role.name == "admin" for role in user.roles)


def can_edit_incident(user: User, incident: Incident) -> bool:
    if _is_admin(user):
        return True
    return incident.owner_user_id == user.id or incident.reporter_user_id == user.id


def _user_ref(user: User | None) -> IncidentUserRefOut | None:
    if not user:
        return None
    return IncidentUserRefOut(id=user.id, name=user.name or user.full_name, email=user.email)


def _incident_fqn_variants(table_fqn: str | None) -> set[str]:
    if not table_fqn:
        return set()
    normalized = table_fqn.strip()
    if not normalized:
        return set()
    variants = {normalized}
    parts = [part for part in normalized.split(".") if part]
    if len(parts) >= 2:
        variants.add(".".join(parts[-2:]))
    return variants


def _profile_map_for_incidents(db: Session, incidents: list[Incident], *, current_user: User | None = None) -> dict[str, object]:
    incident_fqns = sorted(
        {
            candidate
            for incident in incidents
            if incident.entity_type == "table"
            for candidate in _incident_fqn_variants(incident.table_fqn)
        }
    )
    if not incident_fqns:
        return {}
    now = datetime.now(timezone.utc)
    profiles = load_table_profiles(db, now, incident_fqns=incident_fqns, current_user=current_user)
    profile_map: dict[str, object] = {}
    for profile in profiles:
        profile_map[profile.incident_lookup_key] = profile
        profile_map[profile.table_fqn] = profile
    return profile_map


def filter_incidents_for_user(db: Session, incidents: list[Incident], *, user: User | None) -> tuple[list[Incident], dict[str, object]]:
    profile_map = _profile_map_for_incidents(db, incidents, current_user=user)
    decisions = visibility_for_profiles(db, list(profile_map.values()), user=user)
    for profile in profile_map.values():
        decision = decisions.get(profile.table_id)
        setattr(profile, "masked_sensitive_fields", bool(decision and decision.masked))
    filtered = []
    for incident in incidents:
        if incident.entity_type != "table" or not incident.table_fqn:
            filtered.append(incident)
            continue
        profile = profile_map.get(incident.table_fqn)
        if profile is None:
            for candidate in _incident_fqn_variants(incident.table_fqn):
                profile = profile_map.get(candidate)
                if profile is not None:
                    break
        if profile is None:
            continue
        decision = decisions.get(profile.table_id)
        if decision is None or decision.visible:
            filtered.append(incident)
    return filtered, profile_map


def _incident_asset_context(incident: Incident, profile, *, masked: bool = False) -> IncidentAssetContextOut | None:
    if profile is None:
        return None
    links = build_asset_links(
        table_id=profile.table_id,
        datasource_id=profile.datasource_id,
        database_id=profile.database_id,
        schema_id=profile.schema_id,
        data_owner_id=profile.data_owner_id,
    )
    score, _factors = compute_priority_score(
        profile,
        recent_incident_count=profile.open_incidents,
        recent_occurrences=profile.open_incidents,
    )
    payload = IncidentAssetContextOut(
        table_id=profile.table_id,
        table_name=profile.table_name,
        table_fqn=profile.table_fqn,
        datasource_name=profile.datasource_name,
        database_name=profile.database_name,
        schema_name=profile.schema_name,
        domain_name=profile.domain_name or "Sem dados suficientes",
        owner_name=profile.owner_name or "Não definido",
        owner_defined=profile.owner_defined,
        data_owner_id=profile.data_owner_id,
        criticality_score=score,
        criticality_label=risk_label(score),
        sensitivity_level=profile.sensitivity_level,
        sensitivity_label=sensitivity_label(profile.sensitivity_level),
        dq_score=round(profile.dq_score, 1) if profile.dq_score is not None else None,
        certification_status=profile.certification_status,
        open_incidents=profile.open_incidents,
        critical_open_incidents=profile.critical_open_incidents,
        links=links,
        actions=build_contextual_actions(profile, links),
    )
    if masked:
        return IncidentAssetContextOut(**mask_incident_asset_context_payload(payload.model_dump()))
    return payload


def _incident_operational_sla(incident: Incident):
    source = (incident.source_type or "").strip().lower()
    evidence = incident.evidence_json if isinstance(incident.evidence_json, dict) else {}
    if source not in _OPERATIONAL_INCIDENT_SOURCES and evidence.get("origin") not in {"explorer_ingestion", "dq_operational_context", "ops_cockpit"}:
        return None

    now = datetime.now(timezone.utc)
    raw_issue_type = str(evidence.get("operational_issue_type") or "failure").strip().lower()
    issue_type = raw_issue_type if raw_issue_type in {"failure", "stale", "degraded"} else "failure"
    issue_label = {
        "failure": "Falha operacional",
        "stale": "Sem sucesso recente",
        "degraded": "Pipeline degradado",
    }[issue_type]

    detected_at = incident.detected_at
    if detected_at.tzinfo is None:
        detected_at = detected_at.replace(tzinfo=timezone.utc)

    raw_due_at = evidence.get("operational_sla_due_at")
    due_at = None
    if isinstance(raw_due_at, str):
        try:
            due_at = datetime.fromisoformat(raw_due_at)
        except ValueError:
            due_at = None
    elif isinstance(raw_due_at, datetime):
        due_at = raw_due_at
    if due_at is not None and due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=timezone.utc)

    sla_hours = None
    raw_sla_hours = evidence.get("operational_sla_hours")
    try:
        sla_hours = int(raw_sla_hours) if raw_sla_hours is not None else None
    except (TypeError, ValueError):
        sla_hours = None
    if due_at is None and sla_hours is not None:
        due_at = detected_at + timedelta(hours=sla_hours)

    remaining_seconds = (due_at - now).total_seconds() if due_at is not None else None
    if due_at is None:
        status = "within_sla"
    elif remaining_seconds is not None and remaining_seconds <= 0:
        status = "overdue"
    elif remaining_seconds is not None and remaining_seconds <= 4 * 3600:
        status = "due_soon"
    else:
        status = "within_sla"

    aging_hours = max(int((now - detected_at).total_seconds() // 3600), 0)
    recurrent = bool(evidence.get("recurrent_degradation")) or int(incident.occurrences or 1) > 1

    from t2c_data.schemas.incident import IncidentOperationalSLAOut

    return IncidentOperationalSLAOut(
        issue_type=issue_type,
        issue_label=issue_label,
        detected_at=detected_at,
        due_at=due_at,
        aging_hours=aging_hours,
        sla_hours=sla_hours,
        status=status,
        status_label=_OPERATIONAL_SLA_STATUS_LABELS[status],
        recurrent=recurrent,
    )


def serialize_incident_out(
    incident: Incident,
    profile_map: dict[str, object] | None = None,
    *,
    timeline: list[IncidentEventOut] | None = None,
) -> IncidentOut:
    profile = profile_map.get(incident.table_fqn) if profile_map and incident.table_fqn else None
    masked = bool(getattr(profile, "masked_sensitive_fields", False))
    origin_payload = incident_origin_payload(incident.source_type, incident.evidence_json)
    if isinstance(incident.technical_origin_json, dict):
        origin_payload = {**origin_payload, **incident.technical_origin_json}
    impact_payload = incident_impact_payload(profile, source_type=incident.source_type)
    if incident.impact_summary:
        impact_payload["summary"] = incident.impact_summary
    if isinstance(incident.impact_json, dict):
        impact_payload = {**impact_payload, **incident.impact_json}
    if isinstance(incident.related_links_json, dict):
        related_links = incident.related_links_json
    elif isinstance(incident.related_links_json, list):
        related_links = {"items": incident.related_links_json}
    else:
        related_links = None
    return IncidentOut(
        id=incident.id,
        title=incident.title,
        description=incident.description,
        entity_type=incident.entity_type,
        table_fqn=incident.table_fqn,
        airflow_dag_id=incident.airflow_dag_id,
        detected_at=incident.detected_at,
        last_seen_at=incident.last_seen_at,
        acknowledged_at=incident.acknowledged_at,
        triaged_at=incident.triaged_at,
        mitigated_at=incident.mitigated_at,
        resolved_at=incident.resolved_at,
        closed_at=incident.closed_at,
        reopened_at=incident.reopened_at,
        sla_due_at=incident.sla_due_at,
        status=incident.status,
        severity=incident.severity,
        severity_label=SEVERITY_LABELS.get(incident.severity, incident.severity.upper()),
        owner_user_id=incident.owner_user_id,
        reporter_user_id=incident.reporter_user_id,
        owner_user=_user_ref(incident.owner_user),
        reporter_user=_user_ref(incident.reporter_user),
        tags=incident.tags,
        source_type=incident.source_type,
        source_ref_id=incident.source_ref_id,
        evidence_json=incident.evidence_json,
        technical_origin_json=incident.technical_origin_json,
        related_links_json=related_links,
        impact_json=incident.impact_json,
        mitigation_json=incident.mitigation_json,
        postmortem_json=incident.postmortem_json,
        root_cause=incident.root_cause,
        impact_summary=incident.impact_summary,
        mitigation_summary=incident.mitigation_summary,
        postmortem_summary=incident.postmortem_summary,
        domain_name=incident.domain_name,
        owner_team=incident.owner_team,
        squad_name=incident.squad_name,
        recurrence_count=incident.recurrence_count,
        occurrences=incident.occurrences,
        asset_context=_incident_asset_context(incident, profile, masked=masked),
        origin=IncidentOriginOut.model_validate(origin_payload),
        impact=IncidentImpactOut.model_validate(impact_payload),
        operational_sla=_incident_operational_sla(incident),
        timeline=timeline or [],
        created_at=incident.created_at,
        updated_at=incident.updated_at,
    )


def incident_query():
    return select(Incident).options(selectinload(Incident.owner_user), selectinload(Incident.reporter_user))


def get_incident_or_404(db: Session, incident_id: int) -> Incident:
    incident = db.scalar(incident_query().where(Incident.id == incident_id))
    if not incident:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Incident not found")
    return incident


def _normalize_severities(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    normalized: list[str] = []
    for value in values:
        key = value.strip().lower()
        mapped = SEVERITY_ALIASES.get(key)
        if mapped and mapped not in normalized:
            normalized.append(mapped)
    return normalized or None


def build_incident_filters(
    status: list[str] | None,
    severity: list[str] | None,
    entity_type: str | None,
    owner_id: int | None,
    reporter_id: int | None,
    source_type: str | None,
    source_ref_id: int | None,
    table_fqn: str | None,
    q: str | None,
    date_from: datetime | None,
    date_to: datetime | None,
) -> list:
    filters = []
    if status:
        filters.append(Incident.status.in_(status))
    normalized_severities = _normalize_severities(severity)
    if normalized_severities:
        filters.append(Incident.severity.in_(normalized_severities))
    if entity_type:
        filters.append(Incident.entity_type == entity_type)
    if owner_id is not None:
        filters.append(Incident.owner_user_id == owner_id)
    if reporter_id is not None:
        filters.append(Incident.reporter_user_id == reporter_id)
    if source_type:
        filters.append(Incident.source_type == source_type)
    if source_ref_id is not None:
        filters.append(Incident.source_ref_id == source_ref_id)
    if table_fqn:
        filters.append(Incident.table_fqn == table_fqn)
    if date_from is not None:
        filters.append(Incident.detected_at >= date_from)
    if date_to is not None:
        filters.append(Incident.detected_at <= date_to)
    if q:
        pattern = f"%{q.strip()}%"
        filters.append(
            or_(
                Incident.title.ilike(pattern),
                Incident.description.ilike(pattern),
                Incident.table_fqn.ilike(pattern),
                Incident.airflow_dag_id.ilike(pattern),
            )
        )
    return filters


def build_incident_summary(
    db: Session,
    *,
    days: int,
    status: list[str] | None,
    severity: list[str] | None,
    entity_type: str | None,
    owner_id: int | None,
    reporter_id: int | None,
    source_type: str | None,
    source_ref_id: int | None,
    table_fqn: str | None,
    q: str | None,
    date_from: datetime | None,
    date_to: datetime | None,
    current_user: User | None = None,
) -> IncidentSummaryOut:
    now = datetime.now(timezone.utc)
    effective_date_to = date_to or now
    effective_date_from = date_from or (effective_date_to - timedelta(days=max(days - 1, 0)))

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
        effective_date_from,
        effective_date_to,
    )

    base_query = select(Incident)
    if filters:
        base_query = base_query.where(and_(*filters))
    incidents = db.scalars(base_query.order_by(Incident.detected_at.desc(), Incident.id.desc())).all()
    incidents, _ = filter_incidents_for_user(db, incidents, user=current_user)

    total = len(incidents)
    open_count = sum(1 for item in incidents if item.status in ["open", "investigating", "mitigated", "reopened", "recurring"])
    mitigated_count = sum(1 for item in incidents if item.status == "mitigated")
    resolved_count = sum(1 for item in incidents if item.status in ["resolved", "closed"])
    critical_count = sum(1 for item in incidents if item.severity == "sev1")

    by_status: dict[str, int] = {}
    by_severity: dict[str, int] = {}
    by_entity_type: dict[str, int] = {}
    day_map: dict[str, int] = {}
    for incident in incidents:
        by_status[incident.status] = by_status.get(incident.status, 0) + 1
        by_severity[incident.severity] = by_severity.get(incident.severity, 0) + 1
        by_entity_type[incident.entity_type] = by_entity_type.get(incident.entity_type, 0) + 1
        detected_day = incident.detected_at.date().isoformat()
        day_map[detected_day] = day_map.get(detected_day, 0) + 1

    detected_per_day: list[dict[str, int | str]] = []
    current = effective_date_from.date()
    end = effective_date_to.date()
    while current <= end:
        iso_day = current.isoformat()
        detected_per_day.append({"date": iso_day, "count": int(day_map.get(iso_day, 0))})
        current += timedelta(days=1)

    total_last_7_days = sum(int(item["count"]) for item in detected_per_day[-7:])

    return IncidentSummaryOut(
        total=total,
        open=open_count,
        resolved=resolved_count,
        critical=critical_count,
        by_status=by_status,
        by_severity={SEVERITY_LABELS.get(key, key): value for key, value in by_severity.items()},
        counts_by_status=by_status,
        counts_by_severity=by_severity,
        counts_by_entity_type=by_entity_type,
        detected_per_day=detected_per_day,
        total_last_7_days=total_last_7_days,
    )


__all__ = [
    "SEVERITY_LABELS",
    "build_incident_filters",
    "build_incident_summary",
    "can_edit_incident",
    "get_incident_or_404",
    "incident_query",
    "serialize_incident_out",
]
