from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.incidents.query_support import filter_incidents_for_user, incident_query, serialize_incident_out
from t2c_data.models.auth import User
from t2c_data.models.incident import Incident, IncidentEvent
from t2c_data.schemas.incident import (
    IncidentCenterAssetOut,
    IncidentCenterKpiOut,
    IncidentCenterQueueOut,
    IncidentCenterSummaryOut,
    IncidentEventCreate,
    IncidentEventOut,
    IncidentEventUserRefOut,
)

_ACTIVE_STATUSES = {"open", "investigating", "mitigated", "reopened", "recurring"}
_RESOLVED_STATUSES = {"resolved", "closed"}
_STATUS_TONES = {
    "open": "warning",
    "investigating": "accent",
    "mitigated": "warning",
    "resolved": "success",
    "closed": "success",
    "reopened": "warning",
    "recurring": "warning",
}
_SEVERITY_LABELS = {
    "sev1": "Crítico",
    "sev2": "Alto",
    "sev3": "Médio",
    "sev4": "Baixo",
}


def _user_ref(user: User | None) -> IncidentEventUserRefOut | None:
    if user is None:
        return None
    return IncidentEventUserRefOut(id=user.id, name=user.name or user.full_name, email=user.email)


def _incident_time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _incident_domain(incident: Incident) -> str:
    if incident.domain_name and incident.domain_name.strip():
        return incident.domain_name.strip()
    if incident.asset_context and incident.asset_context.domain_name:
        return incident.asset_context.domain_name
    if incident.entity_type == "airflow_dag":
        return "Integração / Airflow"
    return "Sem domínio"


def _incident_owner_label(incident: Incident) -> str:
    if incident.owner_team and incident.owner_team.strip():
        return incident.owner_team.strip()
    if incident.squad_name and incident.squad_name.strip():
        return incident.squad_name.strip()
    if incident.asset_context and incident.asset_context.owner_name:
        return incident.asset_context.owner_name
    if incident.owner_user and (incident.owner_user.name or incident.owner_user.full_name):
        return incident.owner_user.name or incident.owner_user.full_name or incident.owner_user.email
    return "Sem responsável"


def _incident_sla_status(incident: Incident) -> str:
    now = datetime.now(timezone.utc)
    if incident.operational_sla is not None:
        return incident.operational_sla.status
    due_at = _incident_time(incident.sla_due_at)
    if due_at is None:
        return "within_sla"
    if due_at <= now:
        return "overdue"
    if due_at <= now + timedelta(hours=4):
        return "due_soon"
    return "within_sla"


def _sla_tone(status: str) -> str:
    if status == "overdue":
        return "danger"
    if status == "due_soon":
        return "warning"
    return "success"


def _queue_item(key: str, label: str, count: int, *, tone: str = "neutral", href: str | None = None, description: str | None = None) -> IncidentCenterQueueOut:
    return IncidentCenterQueueOut(key=key, label=label, count=count, tone=tone, href=href, description=description)


def _timeline_event_payload(
    *,
    incident: Incident,
    event_id: int,
    occurred_at: datetime,
    event_type: str,
    title: str,
    detail: str | None = None,
    status_from: str | None = None,
    status_to: str | None = None,
    actor_user: User | None = None,
    evidence_json: dict | list | None = None,
) -> IncidentEventOut:
    return IncidentEventOut(
        id=event_id,
        incident_id=incident.id,
        event_type=event_type,
        title=title,
        detail=detail,
        status_from=status_from,
        status_to=status_to,
        evidence_json=evidence_json,
        actor_user_id=actor_user.id if actor_user else None,
        actor_user=_user_ref(actor_user),
        actor_name=(actor_user.name or actor_user.full_name) if actor_user else None,
        actor_email=actor_user.email if actor_user else None,
        created_at=occurred_at,
        updated_at=occurred_at,
    )


def serialize_incident_event_out(event: IncidentEvent) -> IncidentEventOut:
    actor_user = event.actor_user
    return IncidentEventOut(
        id=event.id,
        incident_id=event.incident_id,
        event_type=event.event_type,
        title=event.title,
        detail=event.detail,
        status_from=event.status_from,
        status_to=event.status_to,
        evidence_json=event.evidence_json,
        actor_user_id=event.actor_user_id,
        actor_user=_user_ref(actor_user),
        actor_name=event.actor_name or ((actor_user.name or actor_user.full_name) if actor_user else None),
        actor_email=event.actor_email or (actor_user.email if actor_user else None),
        created_at=event.created_at,
        updated_at=event.updated_at,
    )


def load_incident_events(db: Session, incident_id: int) -> list[IncidentEvent]:
    return db.scalars(
        select(IncidentEvent)
        .options(selectinload(IncidentEvent.actor_user))
        .where(IncidentEvent.incident_id == incident_id)
        .order_by(IncidentEvent.created_at.asc(), IncidentEvent.id.asc())
    ).all()


def incident_timeline_payload(incident: Incident, events: Iterable[IncidentEvent]) -> list[IncidentEventOut]:
    items: list[IncidentEventOut] = [
        _timeline_event_payload(
            incident=incident,
            event_id=-(incident.id * 100) - 1,
            occurred_at=incident.created_at,
            event_type="created",
            title="Incidente criado",
            detail=incident.description,
            status_to=incident.status,
            evidence_json=incident.evidence_json,
        )
    ]

    status_steps = [
        ("acknowledged", "Incident reconhecido", incident.acknowledged_at),
        ("triaged", "Triagem registrada", incident.triaged_at),
        ("mitigated", "Incidente mitigado", incident.mitigated_at),
        ("resolved", "Incidente resolvido", incident.resolved_at),
        ("closed", "Incidente fechado", incident.closed_at),
        ("reopened", "Incidente reaberto", incident.reopened_at),
    ]
    for index, (event_type, title, occurred_at) in enumerate(status_steps, start=2):
        if occurred_at is None:
            continue
        items.append(
            _timeline_event_payload(
                incident=incident,
                event_id=-(incident.id * 100) - index,
                occurred_at=occurred_at,
                event_type=event_type,
                title=title,
                status_to=event_type,
            )
        )

    for key, title, value in (
        ("root_cause", "Causa raiz registrada", incident.root_cause),
        ("impact", "Impacto detalhado", incident.impact_summary),
        ("mitigation", "Mitigação registrada", incident.mitigation_summary),
        ("postmortem", "Postmortem atualizado", incident.postmortem_summary),
    ):
        if not value:
            continue
        items.append(
            _timeline_event_payload(
                incident=incident,
                event_id=-(incident.id * 1000) - len(items) - 1,
                occurred_at=incident.updated_at,
                event_type=key,
                title=title,
                detail=value,
            )
        )

    for event in events:
        items.append(serialize_incident_event_out(event))

    items.sort(key=lambda item: (item.created_at, item.id))
    return items


def build_incident_timeline(db: Session, incident: Incident) -> list[IncidentEventOut]:
    events = load_incident_events(db, incident.id)
    return incident_timeline_payload(incident, events)


def record_incident_event(
    db: Session,
    *,
    incident: Incident,
    event_type: str,
    title: str,
    actor_user: User | None = None,
    detail: str | None = None,
    status_from: str | None = None,
    status_to: str | None = None,
    evidence_json: dict | list | None = None,
) -> IncidentEvent:
    event = IncidentEvent(
        incident_id=incident.id,
        event_type=event_type,
        title=title,
        detail=detail,
        status_from=status_from,
        status_to=status_to,
        evidence_json=evidence_json,
        actor_user_id=actor_user.id if actor_user else None,
        actor_name=(actor_user.name or actor_user.full_name) if actor_user else None,
        actor_email=actor_user.email if actor_user else None,
    )
    db.add(event)
    return event


def build_incident_center_summary(
    db: Session,
    *,
    days: int = 30,
    current_user: User | None = None,
) -> IncidentCenterSummaryOut:
    now = datetime.now(timezone.utc)
    date_from = now - timedelta(days=max(days - 1, 0))
    query = incident_query().where(and_(Incident.detected_at >= date_from, Incident.detected_at <= now))
    incidents = db.scalars(query.order_by(Incident.detected_at.desc(), Incident.id.desc())).all()
    incidents, profile_map = filter_incidents_for_user(db, incidents, user=current_user)
    visible = [serialize_incident_out(item, profile_map) for item in incidents]

    active_items = [item for item in visible if item.status in _ACTIVE_STATUSES]
    overdue_items = [item for item in visible if (item.operational_sla and item.operational_sla.status == "overdue") or (item.sla_due_at and item.sla_due_at <= now and item.status in _ACTIVE_STATUSES)]
    unassigned_items = [item for item in visible if item.owner_user_id is None and not (item.owner_team or item.squad_name or (item.asset_context and item.asset_context.owner_name))]
    recurring_items = [item for item in visible if int(item.occurrences or 0) > 1 or int(item.recurrence_count or 0) > 0 or bool(item.operational_sla and item.operational_sla.recurrent)]

    def _avg_hours(values: list[float]) -> float | None:
        if not values:
            return None
        return round(sum(values) / len(values), 2)

    ack_hours: list[float] = []
    resolution_hours: list[float] = []
    for item in visible:
        detected = _incident_time(item.detected_at)
        if detected is None:
            continue
        ack = _incident_time(item.acknowledged_at or item.triaged_at)
        if ack is not None and ack >= detected:
            ack_hours.append((ack - detected).total_seconds() / 3600)
        resolved = _incident_time(item.resolved_at or item.closed_at or item.mitigated_at)
        if resolved is not None and resolved >= detected:
            resolution_hours.append((resolved - detected).total_seconds() / 3600)

    status_counts = Counter(item.status for item in visible)
    severity_counts = Counter(item.severity for item in visible)
    domain_counts = Counter(_incident_domain(item) for item in visible)
    owner_counts = Counter(_incident_owner_label(item) for item in visible)
    sla_counts = Counter(_incident_sla_status(item) for item in visible)

    def _link_for_queue(queue_key: str, queue_value: str) -> str | None:
        from urllib.parse import quote_plus

        if queue_key == "status":
            return f"/incidents/tickets?status={quote_plus(queue_value)}"
        if queue_key == "severity":
            return f"/incidents/tickets?severity={quote_plus(queue_value)}"
        if queue_key == "domain":
            return f"/incidents/tickets?domain_name={quote_plus(queue_value)}"
        if queue_key == "owner" and queue_value == "__unassigned__":
            return "/incidents/tickets?unassigned=1"
        if queue_key == "owner":
            return f"/incidents/tickets?owner_name={quote_plus(queue_value)}"
        if queue_key == "sla":
            return f"/incidents/tickets?sla_status={quote_plus(queue_value)}"
        return None

    by_status = [
        _queue_item(
            status,
            "Abertos" if status == "open" else "Investigando" if status == "investigating" else "Mitigados" if status == "mitigated" else "Resolvidos" if status == "resolved" else "Fechados" if status == "closed" else "Reabertos" if status == "reopened" else "Recorrentes" if status == "recurring" else status,
            count,
            tone=_STATUS_TONES.get(status, "neutral"),
            href=_link_for_queue("status", status),
        )
        for status, count in status_counts.most_common()
    ]
    by_severity = [
        _queue_item(
            severity,
            _SEVERITY_LABELS.get(severity, severity),
            count,
            tone="warning" if severity in {"sev1", "sev2"} else "success" if severity == "sev4" else "neutral",
            href=_link_for_queue("severity", severity),
        )
        for severity, count in sorted(severity_counts.items(), key=lambda item: item[0])
    ]
    by_domain = [
        _queue_item(
            domain,
            domain,
            count,
            tone="accent" if idx == 0 else "neutral",
            href=_link_for_queue("domain", domain),
            description="Fila de contexto por domínio.",
        )
        for idx, (domain, count) in enumerate(domain_counts.most_common(6))
    ]
    by_owner: list[IncidentCenterQueueOut] = []
    for idx, (owner, count) in enumerate(owner_counts.most_common(6)):
        key = "__unassigned__" if owner == "Sem responsável" else owner
        by_owner.append(
            _queue_item(
                key,
                owner,
                count,
                tone="warning" if owner == "Sem responsável" else "accent" if idx == 0 else "neutral",
                href=_link_for_queue("owner", key),
                description="Fila por responsável.",
            )
        )
    by_sla = [
        _queue_item(
            sla_status,
            "Fora do SLA" if sla_status == "overdue" else "Próximo do vencimento" if sla_status == "due_soon" else "Dentro do SLA",
            count,
            tone=_sla_tone(sla_status),
            href=_link_for_queue("sla", sla_status),
        )
        for sla_status, count in sla_counts.most_common()
    ]

    asset_groups: dict[str, dict[str, object]] = {}
    for item in active_items:
        asset_key = item.table_fqn or item.airflow_dag_id or f"incident-{item.id}"
        group = asset_groups.setdefault(
            asset_key,
            {
                "key": asset_key,
                "label": item.asset_context.table_name if item.asset_context and item.asset_context.table_name else item.title,
                "table_id": item.asset_context.table_id if item.asset_context else None,
                "table_fqn": item.asset_context.table_fqn if item.asset_context else item.table_fqn,
                "domain_name": _incident_domain(item),
                "owner_name": _incident_owner_label(item),
                "open_count": 0,
                "critical_count": 0,
                "overdue_count": 0,
                "last_detected_at": None,
                "href": item.asset_context.links.incidents if item.asset_context and item.asset_context.links else item.asset_context.links.explorer if item.asset_context and item.asset_context.links else None,
                "signals": [],
            },
        )
        group["open_count"] = int(group["open_count"]) + 1
        if item.severity == "sev1":
            group["critical_count"] = int(group["critical_count"]) + 1
        if (item.operational_sla and item.operational_sla.status == "overdue") or (item.sla_due_at and item.sla_due_at <= now):
            group["overdue_count"] = int(group["overdue_count"]) + 1
        last_detected = group["last_detected_at"]
        if last_detected is None or item.detected_at > last_detected:
            group["last_detected_at"] = item.detected_at
        signals = list(group["signals"])
        if item.operational_sla and item.operational_sla.recurrent and "Recorrência operacional" not in signals:
            signals.append("Recorrência operacional")
        if item.source_type and item.source_type.startswith("dq") and "Sinal de DQ" not in signals:
            signals.append("Sinal de DQ")
        group["signals"] = signals
    top_assets = [
        IncidentCenterAssetOut(
            key=str(payload["key"]),
            label=str(payload["label"]),
            table_id=payload["table_id"],
            table_fqn=payload["table_fqn"],
            domain_name=payload["domain_name"],
            owner_name=payload["owner_name"],
            open_count=int(payload["open_count"]),
            critical_count=int(payload["critical_count"]),
            overdue_count=int(payload["overdue_count"]),
            last_detected_at=payload["last_detected_at"],
            href=payload["href"],
            signals=list(payload["signals"]),
        )
        for payload in sorted(
            asset_groups.values(),
            key=lambda item: (int(item["critical_count"]), int(item["overdue_count"]), int(item["open_count"])),
            reverse=True,
        )[:8]
    ]

    recent_incidents = visible[:10]
    kpis = [
        IncidentCenterKpiOut(key="active", label="Incidentes ativos", value=float(len(active_items)), unit="casos", tone="warning", detail="Abertos, investigando, mitigados, reabertos e recorrentes."),
        IncidentCenterKpiOut(key="overdue", label="Fora do SLA", value=float(len(overdue_items)), unit="casos", tone="danger", detail="Incidentes com vencimento já estourado."),
        IncidentCenterKpiOut(key="unassigned", label="Sem responsável", value=float(len(unassigned_items)), unit="casos", tone="accent", detail="Sem owner, squad ou responsável explícito."),
        IncidentCenterKpiOut(key="recurring", label="Recorrentes", value=float(len(recurring_items)), unit="casos", tone="warning", detail="Ocorrências repetidas ou degradação recorrente."),
        IncidentCenterKpiOut(key="mtta", label="MTTA médio", value=_avg_hours(ack_hours) or 0.0, unit="h", tone="neutral", detail="Tempo médio até reconhecimento/triagem."),
        IncidentCenterKpiOut(key="mttr", label="MTTR médio", value=_avg_hours(resolution_hours) or 0.0, unit="h", tone="neutral", detail="Tempo médio até resolução/fechamento."),
    ]

    return IncidentCenterSummaryOut(
        generated_at=now,
        window_days=days,
        metrics=kpis,
        by_status=by_status,
        by_severity=by_severity,
        by_domain=by_domain,
        by_owner=by_owner,
        by_sla=by_sla,
        top_assets=top_assets,
        recent_incidents=recent_incidents,
    )
