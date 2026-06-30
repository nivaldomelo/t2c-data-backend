from __future__ import annotations

from datetime import datetime

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session

from t2c_data.models.auth import User
from t2c_data.models.audit import AuditLog
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleRun, DQRun
from t2c_data.models.dq import dq_rule_notification_recipients
from t2c_data.models.incident import Incident
from t2c_data.features.data_quality.rule_builder import summarize_rule_definition
from t2c_data.features.data_quality.schedule_utils import compute_next_run_at, describe_schedule, infer_schedule_mode
from t2c_data.features.data_quality.engines import normalize_execution_engine
from t2c_data.schemas.dq import DQJobRunOut, DQRunProgressOut
from t2c_data.schemas.dq_rules import DQRuleOut, DQUserOption
from t2c_data.services.data_quality import configured_execution_engine


def _audit_actor_payload(entry: AuditLog | None) -> dict[str, object | None]:
    if entry is None:
        return {
            "user_id": None,
            "user_name": None,
            "user_email": None,
            "action": None,
            "at": None,
        }
    return {
        "user_id": entry.user_id,
        "user_name": entry.actor_name,
        "user_email": entry.user_email,
        "action": entry.action,
        "at": entry.created_at,
    }


def _rule_audit_payload(db: Session, rule_id: int) -> dict[str, dict[str, object | None]]:
    try:
        rows = db.scalars(
            select(AuditLog)
            .where(
                AuditLog.entity_type == "dq_rule",
                AuditLog.entity_id == str(rule_id),
                AuditLog.action.in_(["dq_rule.create", "dq_rule.update"]),
            )
            .order_by(AuditLog.created_at.asc(), AuditLog.id.asc())
        ).all()
    except DBAPIError:
        return {
            "created": _audit_actor_payload(None),
            "updated": _audit_actor_payload(None),
        }

    created_entry = next((row for row in rows if row.action == "dq_rule.create"), None)
    updated_entry = next((row for row in reversed(rows) if row.action == "dq_rule.update"), None) or created_entry
    return {
        "created": _audit_actor_payload(created_entry),
        "updated": _audit_actor_payload(updated_entry),
    }


def load_rule_audit_payloads(db: Session, rule_ids: list[int]) -> dict[int, dict[str, dict[str, object | None]]]:
    if not rule_ids:
        return {}
    try:
        rows = db.scalars(
            select(AuditLog)
            .where(
                AuditLog.entity_type == "dq_rule",
                AuditLog.entity_id.in_([str(rule_id) for rule_id in rule_ids]),
                AuditLog.action.in_(["dq_rule.create", "dq_rule.update"]),
            )
            .order_by(AuditLog.entity_id.asc(), AuditLog.created_at.asc(), AuditLog.id.asc())
        ).all()
    except DBAPIError:
        return {}

    grouped: dict[int, list[AuditLog]] = defaultdict(list)
    for row in rows:
        try:
            grouped[int(row.entity_id)].append(row)
        except Exception:
            continue

    payloads: dict[int, dict[str, dict[str, object | None]]] = {}
    for rule_id, entries in grouped.items():
        created_entry = next((entry for entry in entries if entry.action == "dq_rule.create"), None)
        updated_entry = next((entry for entry in reversed(entries) if entry.action == "dq_rule.update"), None) or created_entry
        payloads[rule_id] = {
            "created": _audit_actor_payload(created_entry),
            "updated": _audit_actor_payload(updated_entry),
        }
    return payloads


def load_rule_notification_recipients(db: Session, rule_ids: list[int]) -> dict[int, list[User]]:
    if not rule_ids:
        return {}
    try:
        rows = db.execute(
            select(dq_rule_notification_recipients.c.rule_id, User)
            .join(User, User.id == dq_rule_notification_recipients.c.user_id)
            .where(dq_rule_notification_recipients.c.rule_id.in_(rule_ids))
            .order_by(
                dq_rule_notification_recipients.c.rule_id.asc(),
                User.name.asc().nullslast(),
                User.email.asc(),
            )
        ).all()
    except DBAPIError:
        return {}

    grouped: dict[int, list[User]] = defaultdict(list)
    for rule_id, user in rows:
        grouped[int(rule_id)].append(user)
    return dict(grouped)


def _job_execution_metrics(latest_job_out: DQJobRunOut | None) -> dict[str, int | None]:
    payload = latest_job_out.result_json if latest_job_out and isinstance(latest_job_out.result_json, dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}

    def _safe_int(value) -> int | None:  # noqa: ANN001
        try:
            if value is None:
                return None
            return int(value)
        except Exception:  # noqa: BLE001
            return None

    return {
        "rows_checked": _safe_int(payload.get("rows_checked_total") or payload.get("row_count")),
        "violations_count": _safe_int(
            latest_job_out.violations_count
            if latest_job_out is not None
            else payload.get("violations_count_total")
        ),
        "total_rules": _safe_int(summary.get("total_rules")),
        "passed_rules": _safe_int(summary.get("passed_rules")),
        "failed_rules": _safe_int(summary.get("failed_rules")),
        "error_rules": _safe_int(summary.get("error_rules")),
    }


def build_dq_job_out(
    run,
    db: Session | None = None,
    *,
    dq_run: DQRun | None = None,
    requested_by_user: User | None = None,
) -> DQJobRunOut:
    if dq_run is None and db is not None and getattr(run, "dq_run_id", None):
        dq_run = db.get(DQRun, int(run.dq_run_id))
    if requested_by_user is None and db is not None and getattr(run, "requested_by_user_id", None):
        requested_by_user = db.get(User, int(run.requested_by_user_id))
    log_tail = (getattr(dq_run, "log_tail", None) if dq_run else None) or (
        (((run.stderr_log or "") + "\n" + (run.stdout_log or "")).strip()) or None
    )
    if log_tail and len(log_tail) > 4000:
        log_tail = log_tail[-4000:]
    violations_count = None
    if isinstance(run.result_json, dict):
        if isinstance(run.result_json.get("violations_count_total"), int):
            violations_count = int(run.result_json["violations_count_total"])
        elif isinstance(run.result_json.get("rules"), list):
            try:
                violations_count = sum(int((item or {}).get("violations_count") or 0) for item in run.result_json["rules"])
            except Exception:
                violations_count = None
    return DQJobRunOut(
        id=run.id,
        job_type=run.job_type,
        status=run.status,
        execution_engine=getattr(run, "execution_engine", "spark"),
        dq_run_id=getattr(run, "dq_run_id", None),
        profiling_schedule_id=getattr(dq_run, "profiling_schedule_id", None) if dq_run else None,
        table_id=run.table_id,
        table_fqn=run.table_fqn,
        datasource_id=run.datasource_id,
        requested_by_user_id=run.requested_by_user_id,
        requested_by_user_name=(
            (
                requested_by_user.name
                or requested_by_user.full_name
                or requested_by_user.email
            ).strip()
            if requested_by_user and (requested_by_user.name or requested_by_user.full_name or requested_by_user.email)
            else (requested_by_user.email if requested_by_user else None)
        ),
        requested_by_user_email=requested_by_user.email if requested_by_user else None,
        trigger_source=(
            "scheduled"
            if getattr(dq_run, "profiling_schedule_id", None)
            else "manual"
            if getattr(run, "requested_by_user_id", None)
            else "automatic"
        ),
        spark_app_id=run.spark_app_id,
        spark_master_url=getattr(run, "spark_master_url", None),
        logs_path=getattr(run, "logs_path", None),
        command=run.command,
        stdout_log=run.stdout_log,
        stderr_log=run.stderr_log,
        result_json=run.result_json,
        error_message=run.error_message,
        queued_at=(dq_run.queued_at if dq_run else None),
        started_at=(dq_run.started_at if dq_run else None),
        finished_at=(dq_run.finished_at if dq_run else None),
        duration_ms=(dq_run.duration_ms if dq_run else None),
        log_tail=log_tail,
        violations_count=violations_count,
        created_at=run.created_at,
        updated_at=run.updated_at,
    )


def build_dq_job_out_map(db: Session, jobs: list[DQJobRun]) -> dict[int, DQJobRunOut]:
    if not jobs:
        return {}
    dq_run_ids = sorted({int(job.dq_run_id) for job in jobs if getattr(job, "dq_run_id", None) is not None})
    requested_user_ids = sorted(
        {int(job.requested_by_user_id) for job in jobs if getattr(job, "requested_by_user_id", None) is not None}
    )
    dq_runs = (
        {
            run.id: run
            for run in db.scalars(select(DQRun).where(DQRun.id.in_(dq_run_ids))).all()
        }
        if dq_run_ids
        else {}
    )
    requested_users = (
        {
            user.id: user
            for user in db.scalars(select(User).where(User.id.in_(requested_user_ids))).all()
        }
        if requested_user_ids
        else {}
    )
    return {
        job.id: build_dq_job_out(
            job,
            dq_run=dq_runs.get(job.dq_run_id),
            requested_by_user=requested_users.get(job.requested_by_user_id),
        )
        for job in jobs
    }



def build_dq_run_progress_out(run: DQRun, db: Session) -> DQRunProgressOut:
    children = db.scalars(select(DQRun).where(DQRun.parent_run_id == run.id)).all()
    total = len(children)
    queued = sum(1 for item in children if item.status == "queued")
    running = sum(1 for item in children if item.status == "running")
    success = sum(1 for item in children if item.status in {"success", "no_data"})
    failed = sum(1 for item in children if item.status in {"failed", "timeout"})
    return DQRunProgressOut(
        id=run.id,
        scope=getattr(run, "scope", "table"),
        schema=getattr(run, "schema_name", None),
        status=run.status,
        execution_engine=getattr(run, "execution_engine", "spark"),
        datasource_id=run.datasource_id,
        table_id=run.table_id,
        parent_run_id=getattr(run, "parent_run_id", None),
        queued_at=getattr(run, "queued_at", None),
        started_at=getattr(run, "started_at", None),
        finished_at=getattr(run, "finished_at", None),
        duration_ms=getattr(run, "duration_ms", None),
        error_message=getattr(run, "error_message", None),
        spark_app_id=getattr(run, "spark_app_id", None),
        log_tail=getattr(run, "log_tail", None),
        total_items=total,
        queued_items=queued,
        running_items=running,
        success_items=success,
        failed_items=failed,
    )



def build_rule_out(
    rule: DQRule,
    latest_run: DQRuleRun | None = None,
    latest_job_run: DQJobRun | None = None,
    latest_job_out: DQJobRunOut | None = None,
    audit_payload: dict[str, dict[str, object | None]] | None = None,
) -> DQRuleOut:
    recipient_users = []
    seen_recipients: set[int] = set()
    rule_state = sa_inspect(rule)
    if "notification_recipients" not in rule_state.unloaded:
        for user in getattr(rule, "notification_recipients", []) or []:
            if user is None or user.id in seen_recipients:
                continue
            seen_recipients.add(user.id)
            recipient_users.append(
                DQUserOption(
                    id=user.id,
                    display_name=(user.name or user.full_name or user.email).strip() or user.email,
                    email=user.email,
                )
            )
    if not recipient_users and getattr(rule, "notification_recipient_user_id", None) is not None:
        recipient = getattr(rule, "notification_recipient_user", None)
        if recipient is not None:
            recipient_users.append(
                DQUserOption(
                    id=recipient.id,
                    display_name=(recipient.name or recipient.full_name or recipient.email).strip() or recipient.email,
                    email=recipient.email,
                )
            )
    primary_recipient = recipient_users[0] if recipient_users else None
    primary_recipient_id = getattr(rule, "notification_recipient_user_id", None) or getattr(primary_recipient, "id", None)
    primary_recipient_name = (
        getattr(rule.notification_recipient_user, "name", None)
        or getattr(rule.notification_recipient_user, "full_name", None)
        or getattr(primary_recipient, "display_name", None)
    )
    primary_recipient_email = getattr(rule.notification_recipient_user, "email", None) or getattr(primary_recipient, "email", None)

    last_run_status: str | None = None
    if latest_run:
        if latest_run.status == "error":
            last_run_status = "failed"
        elif latest_run.status in {"pass", "fail"}:
            last_run_status = "success"
        else:
            last_run_status = str(latest_run.status)
    rule_definition = getattr(rule, "rule_definition_json", None) if isinstance(getattr(rule, "rule_definition_json", None), dict) else None
    target = rule_definition.get("target", {}) if isinstance(rule_definition, dict) else {}
    rule_summary = summarize_rule_definition(rule_definition) if isinstance(rule_definition, dict) else "Regra SQL legada"
    quality_dimension = str(rule_definition.get("dimension") or "").strip() if isinstance(rule_definition, dict) else ""
    rule_category = str(rule_definition.get("category") or "").strip() if isinstance(rule_definition, dict) else ""
    template_key = str(rule_definition.get("template_key") or "").strip() if isinstance(rule_definition, dict) else ""
    created_audit = (audit_payload or {}).get("created") or {}
    updated_audit = (audit_payload or {}).get("updated") or {}
    job_metrics = _job_execution_metrics(latest_job_out)
    return DQRuleOut(
        id=rule.id,
        table_id=rule.table_id,
        datasource_id=target.get("datasource_id"),
        datasource_name=target.get("datasource_name"),
        schema_name=target.get("schema_name"),
        table_name=target.get("table_name"),
        execution_engine=configured_execution_engine(getattr(rule, "execution_engine", None)),
        rule_builder_version=getattr(rule, "rule_builder_version", None),
        rule_definition_json=rule_definition,
        rule_summary=rule_summary,
        quality_dimension=quality_dimension or None,
        rule_category=rule_category or None,
        template_key=template_key or None,
        legacy_mode=not isinstance(rule_definition, dict),
        notification_recipient_user_id=primary_recipient_id,
        notification_recipient_user_name=primary_recipient_name,
        notification_recipient_user_email=primary_recipient_email,
        notification_recipient_users=recipient_users,
        schedule_mode=infer_schedule_mode(
            schedule_mode=getattr(rule, "schedule_mode", None),
            schedule_enabled=getattr(rule, "schedule_enabled", None),
            schedule_every_minutes=getattr(rule, "schedule_every_minutes", None),
        ),
        schedule_enabled=getattr(rule, "schedule_enabled", True),
        schedule_time=getattr(rule, "schedule_time", None),
        schedule_day_of_week=getattr(rule, "schedule_day_of_week", None),
        schedule_day_of_month=getattr(rule, "schedule_day_of_month", None),
        schedule_anchor_date=(
            getattr(rule, "schedule_anchor_date", None).date()
            if getattr(rule, "schedule_anchor_date", None) is not None and hasattr(getattr(rule, "schedule_anchor_date", None), "date")
            else getattr(rule, "schedule_anchor_date", None)
        ),
        schedule_summary=describe_schedule(rule),
        table_fqn=rule.table_fqn,
        name=rule.name,
        description=rule.description,
        rule_type=rule.rule_type,
        severity=rule.severity,
        is_active=rule.is_active,
        created_at=rule.created_at,
        updated_at=rule.updated_at,
        created_by_user_id=created_audit.get("user_id") if isinstance(created_audit.get("user_id"), int) else None,
        created_by_user_name=created_audit.get("user_name") if isinstance(created_audit.get("user_name"), str) else None,
        created_by_user_email=created_audit.get("user_email") if isinstance(created_audit.get("user_email"), str) else None,
        updated_by_user_id=updated_audit.get("user_id") if isinstance(updated_audit.get("user_id"), int) else None,
        updated_by_user_name=updated_audit.get("user_name") if isinstance(updated_audit.get("user_name"), str) else None,
        updated_by_user_email=updated_audit.get("user_email") if isinstance(updated_audit.get("user_email"), str) else None,
        last_audit_action=updated_audit.get("action") if isinstance(updated_audit.get("action"), str) else None,
        last_audit_at=updated_audit.get("at") if isinstance(updated_audit.get("at"), datetime) else None,
        schedule_last_run_at=getattr(rule, "schedule_last_run_at", None),
        schedule_next_run_at=compute_next_run_at(rule),
        last_run_id=(latest_run.id if latest_run else None),
        last_run_status=last_run_status,
        last_run_engine=(getattr(latest_run, "execution_engine", None) if latest_run else None),
        last_run_at=(latest_run.created_at if latest_run else None),
        last_violations_count=(int(latest_run.violations_count or 0) if latest_run else 0),
        last_error_message=(latest_run.error_message if latest_run else None),
        last_job_run_id=(latest_job_run.id if latest_job_run else None),
        last_job_status=(getattr(latest_job_run, "status", None) if latest_job_run else None),
        last_job_engine=(getattr(latest_job_run, "execution_engine", None) if latest_job_run else None),
        last_job_duration_ms=(getattr(latest_job_out, "duration_ms", None) if latest_job_out else None),
        last_job_error_message=(getattr(latest_job_out, "error_message", None) if latest_job_out else None),
        last_job_log_tail=(getattr(latest_job_out, "log_tail", None) if latest_job_out else None),
        last_job_spark_app_id=(getattr(latest_job_run, "spark_app_id", None) if latest_job_run else None),
        last_job_requested_by_user_id=(getattr(latest_job_out, "requested_by_user_id", None) if latest_job_out else None),
        last_job_requested_by_user_name=(getattr(latest_job_out, "requested_by_user_name", None) if latest_job_out else None),
        last_job_requested_by_user_email=(getattr(latest_job_out, "requested_by_user_email", None) if latest_job_out else None),
        last_job_trigger_source=(getattr(latest_job_out, "trigger_source", None) if latest_job_out else None),
        last_job_started_at=(getattr(latest_job_out, "started_at", None) if latest_job_out else None),
        last_job_finished_at=(getattr(latest_job_out, "finished_at", None) if latest_job_out else None),
        last_rows_checked=job_metrics["rows_checked"],
        last_job_violations_count=job_metrics["violations_count"],
        last_job_total_rules=job_metrics["total_rules"],
        last_job_passed_rules=job_metrics["passed_rules"],
        last_job_failed_rules=job_metrics["failed_rules"],
        last_job_error_rules=job_metrics["error_rules"],
        open_incident_id=None,
        open_incident_status=None,
    )



def _open_incident_for_rule(db: Session, rule_id: int) -> Incident | None:
    try:
        return db.scalar(
            select(Incident)
            .where(
                Incident.source_type == "dq_rule",
                Incident.source_ref_id == rule_id,
                Incident.status.in_(["open", "investigating"]),
            )
            .order_by(Incident.updated_at.desc())
            .limit(1)
        )
    except DBAPIError:
        return None


def open_incidents_for_rule_ids(db: Session, rule_ids: list[int]) -> dict[int, Incident]:
    if not rule_ids:
        return {}
    try:
        rows = db.scalars(
            select(Incident)
            .where(
                Incident.source_type == "dq_rule",
                Incident.source_ref_id.in_(rule_ids),
                Incident.status.in_(["open", "investigating"]),
            )
            .order_by(Incident.source_ref_id.asc(), Incident.updated_at.desc(), Incident.id.desc())
        ).all()
    except DBAPIError:
        return {}
    incidents: dict[int, Incident] = {}
    for row in rows:
        if row.source_ref_id is None or row.source_ref_id in incidents:
            continue
        incidents[int(row.source_ref_id)] = row
    return incidents



def map_rule_out(
    db: Session,
    rule: DQRule,
    latest_run: DQRuleRun | None = None,
    latest_job_run: DQJobRun | None = None,
    latest_job_out: DQJobRunOut | None = None,
    incident: Incident | None = None,
    audit_payload: dict[str, dict[str, object | None]] | None = None,
    notification_recipients: list[User] | None = None,
) -> DQRuleOut:
    rule_state = sa_inspect(rule)
    if notification_recipients is not None:
        rule.notification_recipients = notification_recipients
    elif "notification_recipients" in rule_state.unloaded:
        try:
            rule.notification_recipients = db.scalars(
                select(User)
                .join(dq_rule_notification_recipients, User.id == dq_rule_notification_recipients.c.user_id)
                .where(dq_rule_notification_recipients.c.rule_id == rule.id)
                .order_by(User.name.asc().nullslast(), User.email.asc())
            ).all()
        except DBAPIError:
            pass
    effective_audit_payload = audit_payload or _rule_audit_payload(db, rule.id)
    out = build_rule_out(rule, latest_run, latest_job_run, latest_job_out, audit_payload=effective_audit_payload)
    incident = incident or _open_incident_for_rule(db, out.id)
    if incident:
        out.open_incident_id = incident.id
        out.open_incident_status = incident.status
    return out
