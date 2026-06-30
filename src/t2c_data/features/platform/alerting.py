from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Request
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.core.json_utils import to_jsonable
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.platform.sensitive_data import redact_sensitive_metadata
from t2c_data.features.notifications import (
    create_user_inbox_notification,
    get_or_create_user_notification_preference,
    resolve_inbox_notification_recipients,
    send_user_alert_email,
)
from t2c_data.models.audit import AuditLog
from t2c_data.models.auth import User
from t2c_data.models.incident import Incident
from t2c_data.models.platform import IntegrationSyncJob, PlatformDomainEvent


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str:
    return (value or "").strip()


def _should_alert(status: str, severity: str) -> bool:
    normalized_status = _normalize_text(status).lower()
    normalized_severity = _normalize_text(severity).lower()
    return normalized_status in {"failed", "partial_success"} or normalized_severity in {"critical", "warning"}


def _event_severity(value: str | None) -> str:
    normalized = _normalize_text(value).lower()
    if normalized in {"critical", "warning", "info", "healthy"}:
        return "high" if normalized == "critical" else "medium" if normalized == "warning" else "low"
    return "medium"


def _actionable_message(*, impact: str | None, recommended_action: str | None, probable_cause: str | None) -> str:
    parts = [part for part in [impact, recommended_action, probable_cause] if part]
    if parts:
        return " | ".join(parts)
    return "Revisar execução operacional no Ops Cockpit."


def _payload_dict(payload: dict[str, Any] | None) -> dict[str, Any]:
    safe_payload = to_jsonable(payload or {})
    if not isinstance(safe_payload, dict):
        return {}
    return redact_sensitive_metadata(safe_payload)


def _emit_alert(
    session: Session,
    *,
    event_key: str,
    module_name: str,
    title: str,
    severity: str,
    summary: str,
    probable_cause: str | None,
    evidence: str | None,
    impact: str | None,
    recommended_action: str | None,
    runbook_url: str | None,
    correlation_id: str | None,
    payload: dict[str, Any],
    recipient_users: list[User],
    actor_user_id: int | None = None,
    entity_type: str = "integration_sync_job",
    entity_id: int | None = None,
    source_action: str | None = None,
    category: str = "operations",
    incident: dict[str, Any] | None = None,
    channel_hint: str = "inbox",
) -> bool:
    dedupe_key = f"{event_key}:{correlation_id or entity_id or 'global'}"
    safe_payload = _payload_dict(payload)
    safe_payload.update(
        {
            "title": title,
            "severity": severity,
            "probable_cause": probable_cause or None,
            "evidence": evidence or None,
            "impact": impact or None,
            "recommended_action": recommended_action or None,
            "runbook_url": runbook_url,
            "correlation_id": correlation_id,
            "diagnostic_probable_cause": probable_cause or None,
            "diagnostic_evidence": evidence or None,
            "diagnostic_impact": impact or None,
            "diagnostic_recommended_action": recommended_action or None,
            "diagnostic_runbook_url": runbook_url,
        }
    )
    safe_payload = redact_sensitive_metadata(safe_payload)

    event = PlatformDomainEvent(
        event_key=event_key,
        category=category,
        severity=_event_severity(severity),
        title=title,
        summary=summary[:2000],
        source_module="platform.alerting",
        source_action=source_action or event_key,
        entity_type=entity_type,
        entity_id=entity_id,
        actor_user_id=actor_user_id,
        manual_mode="automatic",
        correlation_key=correlation_id,
        payload_json=safe_payload,
    )
    session.add(event)
    session.flush()

    href = f"/ops-cockpit?jobId={entity_id}" if entity_type == "integration_sync_job" and entity_id is not None else "/ops/cockpit"
    message = _actionable_message(
        impact=impact,
        recommended_action=recommended_action,
        probable_cause=probable_cause,
    )
    for recipient in recipient_users:
        create_user_inbox_notification(
            session,
            user_id=recipient.id,
            dedupe_key=dedupe_key,
            category=category,
            severity="high" if severity == "critical" else "medium" if severity == "warning" else "low",
            source_module="platform.alerting",
            source_entity_type=entity_type,
            source_entity_id=str(entity_id or correlation_id or event.id),
            title=title,
            message=message[:2000],
            href=href,
            context_json=safe_payload,
            ignore_category_preferences=True,
        )
        preference = get_or_create_user_notification_preference(session, recipient)
        if preference.email_enabled:
            send_user_alert_email(user=recipient, title=title, message=message[:4000], href=href)
            runtime_metrics.internal_alert_generated(module=module_name, severity=severity or "warning", channel="email")

    runtime_metrics.internal_alert_generated(module=module_name, severity=severity or "warning", channel=channel_hint)

    if incident is not None:
        incident_entity_type = str(incident.get("entity_type") or "table")
        if incident_entity_type not in {"table", "airflow_dag"}:
            incident_entity_type = "table"
        incident_entity_id = incident.get("entity_id")
        incident_entity_id = int(incident_entity_id) if incident_entity_id is not None else None
        incident_source_ref_id = incident.get("source_ref_id")
        incident_source_ref_id = int(incident_source_ref_id) if incident_source_ref_id is not None else None
        existing = session.scalar(
            select(Incident)
            .where(
                Incident.source_type == incident.get("source_type"),
                Incident.source_ref_id == incident_source_ref_id,
                Incident.status.in_(["open", "investigating"]),
            )
            .order_by(Incident.updated_at.desc(), Incident.id.desc())
            .limit(1)
        )
        if existing is not None:
            existing.last_seen_at = _now()
            existing.description = redact_sensitive_metadata(incident.get("description")) or existing.description
            existing.severity = incident.get("severity") or existing.severity
            existing.evidence_json = redact_sensitive_metadata(incident.get("evidence_json")) or existing.evidence_json
            existing.impact_summary = redact_sensitive_metadata(incident.get("impact_summary")) or existing.impact_summary
            existing.recurrence_count = int(existing.recurrence_count or 0) + 1
            session.add(existing)
        else:
            incident_kwargs = {
                "title": incident.get("title") or title,
                "description": redact_sensitive_metadata(incident.get("description")) or summary,
                "entity_type": incident_entity_type,
                "table_fqn": incident.get("table_fqn"),
                "airflow_dag_id": incident.get("airflow_dag_id"),
                "detected_at": incident.get("detected_at") or _now(),
                "last_seen_at": incident.get("last_seen_at") or _now(),
                "status": incident.get("status") or "open",
                "severity": incident.get("severity") or "sev3",
                "source_type": incident.get("source_type"),
                "source_ref_id": incident_source_ref_id,
                "evidence_json": redact_sensitive_metadata(incident.get("evidence_json")),
                "impact_summary": redact_sensitive_metadata(incident.get("impact_summary")),
                "tags": incident.get("tags"),
                "occurrences": incident.get("occurrences", 1),
            }
            session.add(Incident(**incident_kwargs))

    return True


def emit_operational_alert_for_job(
    session: Session,
    *,
    job: IntegrationSyncJob,
    diagnostic: dict[str, Any],
) -> bool:
    severity = _normalize_text(diagnostic.get("diagnostic_severity")).lower()
    if not _should_alert(job.status, severity):
        return False

    title = _normalize_text(diagnostic.get("diagnostic_label")) or f"Falha em {job.source}:{job.job_type}"
    probable_cause = _normalize_text(diagnostic.get("diagnostic_probable_cause"))
    evidence = _normalize_text(diagnostic.get("diagnostic_evidence"))
    action = _normalize_text(diagnostic.get("diagnostic_recommended_action"))
    runbook = _normalize_text(diagnostic.get("diagnostic_runbook_url")) or None
    impact = _normalize_text(diagnostic.get("diagnostic_impact"))
    module_name = _normalize_text(diagnostic.get("diagnostic_module")) or _normalize_text(job.source) or "platform"
    recurrence_count = diagnostic.get("diagnostic_recurrence_count")
    if recurrence_count is not None:
        try:
            recurrence_count = int(recurrence_count)
        except Exception:  # noqa: BLE001
            recurrence_count = None

    payload = {
        "job_id": job.id,
        "job_key": job.job_key,
        "job_type": job.job_type,
        "source": job.source,
        "target_type": job.target_type,
        "target_id": job.target_id,
        "target_name": job.target_name,
        "status": job.status,
        "severity": severity,
        "probable_cause": probable_cause or None,
        "evidence": evidence or None,
        "impact": impact or None,
        "recommended_action": action or None,
        "runbook_url": runbook,
        "correlation_id": job.correlation_id,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "diagnostic_recurrence_count": recurrence_count,
    }
    summary_parts = [part for part in [impact, probable_cause, evidence] if part]
    summary = " | ".join(summary_parts) or _normalize_text(diagnostic.get("diagnostic_description")) or "Falha operacional registrada."

    recipients = resolve_inbox_notification_recipients(
        session,
        user_ids=[job.requested_by_user_id] if job.requested_by_user_id is not None else None,
        include_admins=True,
    )
    incident_payload: dict[str, Any] | None = None
    context = job.context_json if isinstance(job.context_json, dict) else {}
    table_fqn = context.get("table_fqn") or context.get("asset_fqn")
    if isinstance(table_fqn, str) and table_fqn.strip() and job.source in {"dq", "datasource"}:
        incident_payload = {
            "title": title,
            "description": summary[:4000],
            "entity_type": "table",
            "table_fqn": table_fqn.strip(),
            "severity": "sev1" if severity == "critical" else "sev2" if severity == "warning" else "sev3",
            "source_type": "platform_ops",
            "source_ref_id": job.id,
            "evidence_json": redact_value(payload),
            "impact_summary": impact or summary[:1000],
            "tags": ["platform", "automatic"],
            "occurrences": 1,
        }

    return _emit_alert(
        session,
        event_key=f"platform.alert.{module_name}",
        module_name=module_name,
        title=title,
        severity=severity,
        summary=summary,
        probable_cause=probable_cause or None,
        evidence=evidence or None,
        impact=impact or None,
        recommended_action=action or None,
        runbook_url=runbook,
        correlation_id=job.correlation_id,
        payload=payload,
        recipient_users=recipients,
        actor_user_id=job.requested_by_user_id,
        entity_type="integration_sync_job",
        entity_id=job.id,
        source_action=f"platform.alert.{module_name}",
        category="operations",
        incident=incident_payload,
    )


def emit_permission_denied_alert(
    session: Session,
    *,
    request: Request,
    current_user: User,
    permission_name: str,
) -> bool:
    permission = _normalize_text(permission_name)
    now = _now()
    lookback_start = now - timedelta(minutes=15)
    recent_denials = int(
        session.scalar(
            select(func.count(AuditLog.id)).where(
                AuditLog.action == "platform.permission.denied",
                AuditLog.user_id == current_user.id,
                AuditLog.field_name == permission,
                AuditLog.created_at >= lookback_start,
            )
        )
        or 0
    )
    export_permission = permission.endswith(":export")
    threshold = 3 if export_permission else 5
    if recent_denials < threshold:
        return False

    title = "Exportação negada repetidamente" if export_permission else "Permissão negada repetidamente"
    severity = "critical" if recent_denials >= threshold + 2 else "warning"
    probable_cause = "A conta tentou executar uma ação sem autorização suficiente."
    evidence = f"permission_name={permission} | recent_denials={recent_denials} | path={request.url.path}"
    impact = "O operador não consegue concluir a ação e pode estar repetindo uma tentativa sem permissão."
    recommended_action = "Revisar a permissão solicitada, orientar o usuário e validar se o acesso precisa ser concedido."
    runbook = "/docs/runbooks/export-denied.md" if export_permission else "/docs/runbooks/api-auth-failures.md"
    payload = {
        "permission_name": permission,
        "recent_denials": recent_denials,
        "path": request.url.path,
        "method": request.method,
        "ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("user-agent"),
        "runbook_url": runbook,
    }
    return _emit_alert(
        session,
        event_key="platform.permission.denied",
        module_name="platform",
        title=title,
        severity=severity,
        summary=f"{title} para {current_user.email}",
        probable_cause=probable_cause,
        evidence=evidence,
        impact=impact,
        recommended_action=recommended_action,
        runbook_url=runbook,
        correlation_id=request.headers.get("X-Correlation-Id") or request.headers.get("X-Request-Id"),
        payload=payload,
        recipient_users=resolve_inbox_notification_recipients(session, user_ids=[current_user.id], include_admins=True),
        actor_user_id=current_user.id,
        entity_type="permission",
        entity_id=current_user.id,
        source_action="platform.permission.denied",
        category="operations",
    )


def emit_api_key_abuse_alert(
    session: Session,
    *,
    request: Request,
    outcome: str,
    api_key_public_id: str | None = None,
) -> bool:
    normalized_outcome = _normalize_text(outcome)
    if normalized_outcome not in {"missing", "ip_denied", "scope_denied"}:
        return False
    now = _now()
    lookback_start = now - timedelta(minutes=15)
    recent_denials = int(
        session.scalar(
            select(func.count(AuditLog.id)).where(
                AuditLog.action == "platform.api_key.auth_failed",
                AuditLog.field_name == normalized_outcome,
                AuditLog.ip == (request.client.host if request.client else None),
                AuditLog.created_at >= lookback_start,
            )
        )
        or 0
    )
    if recent_denials < 5:
        return False

    severity = "critical" if recent_denials >= 10 else "warning"
    title = "API externa com abuso recorrente"
    probable_cause = "A API key está ausente, bloqueada por IP ou sem escopo suficiente em tentativas repetidas."
    evidence = f"outcome={normalized_outcome} | recent_denials={recent_denials} | ip={request.client.host if request.client else None}"
    impact = "Chamadas externas podem estar sendo bloqueadas ou sofrer tentativa indevida de acesso."
    recommended_action = "Revisar API key, IP allowlist, escopos e rotação do segredo."
    runbook = "/docs/runbooks/api-auth-failures.md"
    payload = {
        "outcome": normalized_outcome,
        "api_key_public_id": api_key_public_id,
        "recent_denials": recent_denials,
        "ip": request.client.host if request.client else None,
        "path": request.url.path,
        "method": request.method,
        "runbook_url": runbook,
    }
    return _emit_alert(
        session,
        event_key="platform.api_key.abuse",
        module_name="external_api",
        title=title,
        severity=severity,
        summary=f"{title} no endpoint {request.url.path}",
        probable_cause=probable_cause,
        evidence=evidence,
        impact=impact,
        recommended_action=recommended_action,
        runbook_url=runbook,
        correlation_id=request.headers.get("X-Correlation-Id") or request.headers.get("X-Request-Id"),
        payload=payload,
        recipient_users=resolve_inbox_notification_recipients(session, user_ids=None, include_admins=True),
        actor_user_id=None,
        entity_type="external_api_key",
        entity_id=None,
        source_action="platform.api_key.abuse",
        category="operations",
    )
