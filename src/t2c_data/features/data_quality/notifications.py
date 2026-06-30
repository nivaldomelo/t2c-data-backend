from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import urlencode

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.notifications import (
    create_user_inbox_notification,
    resolve_inbox_notification_recipients,
)
from t2c_data.models.auth import User
from t2c_data.models.catalog import TableEntity
from t2c_data.models.dq import DQProfilingSchedule, DQRule, DQRun, DQTableMetric

DQ_NOTIFICATION_CATEGORY = "data_quality"
DQ_NOTIFICATION_SOURCE = "dq"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_email(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    return normalized or None


def _notification_severity_from_rule(rule_severity: str | None) -> str:
    normalized = (rule_severity or "").strip().lower()
    if normalized in {"critical", "high", "medium", "low"}:
        return normalized
    return "medium"


def _notification_severity_from_profile(*, dq_score: float, failed_rules: int, sensitive: bool, trigger_codes: list[str]) -> str:
    normalized_codes = {str(code) for code in trigger_codes}
    if (
        "dq_score_below_60" in normalized_codes
        or "failed_rules_high" in normalized_codes
        or "dq_abrupt_drop" in normalized_codes
        or "sensitive_asset_dq_issue" in normalized_codes
        or dq_score < 55
        or failed_rules >= 3
        or (sensitive and dq_score < 70)
    ):
        return "critical"
    return "high"


def _repeat_window_seconds(session: Session, severity: str) -> int:
    settings_snapshot = get_governance_settings_snapshot(session)
    if severity in {"critical", "high"}:
        return max(settings_snapshot.governance_notification_critical_repeat_hours, 1) * 3600
    return max(settings_snapshot.governance_notification_repeat_days, 1) * 86400


def _dedupe_bucket(now: datetime, window_seconds: int) -> int:
    return int(now.timestamp() // max(window_seconds, 1))


def _resolve_dq_recipients(
    session: Session,
    *,
    rule: DQRule | None,
    table: TableEntity | None,
    reporter_user_id: int | None,
) -> list[tuple[User, str, dict[str, object]]]:
    recipients: list[tuple[User, str, dict[str, object]]] = []
    seen: set[int] = set()

    def add(user: User | None, reason: str, context: dict[str, object]) -> None:
        if user is None or not user.is_active or user.id in seen:
            return
        seen.add(user.id)
        recipients.append((user, reason, context))

    if rule is not None:
        for explicit_user in getattr(rule, "notification_recipients", []) or []:
            add(explicit_user, "explicit", {"explicit_user_id": explicit_user.id})

        primary_user_id = getattr(rule, "notification_recipient_user_id", None)
        if primary_user_id is not None:
            add(session.get(User, int(primary_user_id)), "explicit", {"explicit_user_id": int(primary_user_id)})

    owner_email = None
    if table is not None:
        owner_email = _normalize_email(getattr(getattr(table, "data_owner", None), "email", None) or table.owner_email)
    if owner_email:
        owner = session.scalar(
            select(User)
            .where(
                User.is_active.is_(True),
                func.lower(User.email) == owner_email,
            )
            .limit(1)
        )
        add(owner, "data_owner", {"owner_email": owner_email})

    if reporter_user_id is not None:
        add(session.get(User, reporter_user_id), "reporter", {"reporter_user_id": reporter_user_id})

    return recipients


def _resolve_profiling_recipients(
    session: Session,
    *,
    schedule: DQProfilingSchedule | None,
    table: TableEntity | None,
    reporter_user_id: int | None,
) -> list[tuple[User, str, dict[str, object]]]:
    recipients: list[tuple[User, str, dict[str, object]]] = []
    seen: set[int] = set()

    def add(user: User | None, reason: str, context: dict[str, object]) -> None:
        if user is None or not user.is_active or user.id in seen:
            return
        seen.add(user.id)
        recipients.append((user, reason, context))

    if schedule is not None:
        for explicit_user in getattr(schedule, "notification_recipients", []) or []:
            add(explicit_user, "explicit", {"explicit_user_id": explicit_user.id})

    owner_email = None
    if table is not None:
        owner_email = _normalize_email(getattr(getattr(table, "data_owner", None), "email", None) or table.owner_email)
    if owner_email:
        owner = session.scalar(
            select(User)
            .where(
                User.is_active.is_(True),
                func.lower(User.email) == owner_email,
            )
            .limit(1)
        )
        add(owner, "data_owner", {"owner_email": owner_email})

    if reporter_user_id is not None:
        add(session.get(User, reporter_user_id), "reporter", {"reporter_user_id": reporter_user_id})

    return recipients


def _dq_links(*, table: TableEntity | None, rule: DQRule | None, table_fqn: str | None) -> dict[str, str]:
    table_id = table.id if table is not None else (rule.table_id if rule is not None else None)
    links: dict[str, str] = {}
    if table is not None and getattr(table, "schema", None) is not None and getattr(table.schema, "database", None) is not None:
        try:
            links.update(
                build_asset_links(
                    table_id=table.id,
                    datasource_id=table.schema.database.datasource_id,
                    database_id=table.schema.database_id,
                    schema_id=table.schema_id,
                    data_owner_id=table.data_owner_id,
                )
            )
        except Exception:
            links = {}
    links.setdefault("explorer", f"/explorer?tableId={table_id}" if table_id is not None else "/explorer")
    links.setdefault("data_quality", f"/data-quality?tableId={table_id}" if table_id is not None else "/data-quality")
    links.setdefault("incidents", f"/incidents/tickets?tableId={table_id}" if table_id is not None else "/incidents/tickets")

    query_params: dict[str, str] = {}
    if rule is not None:
        query_params["rule_id"] = str(rule.id)
    if table_fqn:
        query_params["table_fqn"] = table_fqn
    if query_params:
        links["rules"] = f"/data-quality/rules?{urlencode(query_params)}"
    else:
        links["rules"] = "/data-quality/rules"
    return links


def notify_dq_rule_violation(
    session: Session,
    *,
    rule: DQRule,
    table: TableEntity | None,
    violations_count: int,
    preview_rows: list[dict],
    run_id: int | None,
    incident_id: int | None = None,
    reporter_user_id: int | None = None,
) -> None:
    recipients = _resolve_dq_recipients(
        session,
        rule=rule,
        table=table,
        reporter_user_id=reporter_user_id,
    )
    for admin in resolve_inbox_notification_recipients(session, user_ids=[], include_admins=True):
        if any(recipient.id == admin.id for recipient, _, _ in recipients):
            continue
        recipients.append((admin, "admin", {"admin_user_id": admin.id}))
    if not recipients:
        return

    table_fqn = rule.table_fqn
    definition = rule.rule_definition_json if isinstance(rule.rule_definition_json, dict) else {}
    dimension = str(definition.get("dimension") or "").strip() or None
    template_key = str(definition.get("template_key") or "").strip() or None
    severity = _notification_severity_from_rule(rule.severity)
    now = _now()
    window_seconds = _repeat_window_seconds(session, severity)
    bucket = _dedupe_bucket(now, window_seconds)
    links = _dq_links(table=table, rule=rule, table_fqn=table_fqn)
    title = "Violação de qualidade detectada"
    message = f"A regra '{rule.name}' foi violada na tabela {table_fqn} com {violations_count} ocorrência(s)."
    if dimension:
        message = f"{message} Dimensão afetada: {dimension}."
    if incident_id is not None:
        message = f"{message} Um incidente relacionado foi atualizado."

    recipient_ids = [recipient.id for recipient, _, _ in recipients]
    for recipient, recipient_reason, recipient_context in recipients:
        context_json = {
            "kind": "dq_rule_violation",
            "table_id": table.id if table is not None else rule.table_id,
            "table_fqn": table_fqn,
            "rule_id": rule.id,
            "rule_name": rule.name,
            "rule_severity": rule.severity,
            "quality_dimension": dimension,
            "template_key": template_key,
            "violations_count": violations_count,
            "dq_rule_run_id": run_id,
            "incident_id": incident_id,
            "severity": severity,
            "notification_window_seconds": window_seconds,
            "notification_bucket": bucket,
            "recipient_reason": recipient_reason,
            "recipient_user_id": recipient.id,
            "recipient_user_ids": recipient_ids,
            "preview_rows": json.loads(json.dumps(preview_rows[:3] if preview_rows else [], default=str)),
            "links": links,
            **recipient_context,
        }

        create_user_inbox_notification(
            session,
            user_id=recipient.id,
            dedupe_key=f"dq-rule:{rule.id}:{table.id if table is not None else rule.table_id or 'global'}:{severity}:{bucket}",
            category=DQ_NOTIFICATION_CATEGORY,
            severity=severity,
            source_module=DQ_NOTIFICATION_SOURCE,
            source_entity_type="dq_rule",
            source_entity_id=rule.id,
            title=title,
            message=message,
            href=links["rules"],
            context_json=context_json,
            ignore_category_preferences=True,
        )


def _failure_kind_from_error(error_message: str | None) -> str:
    normalized = (error_message or "").strip().lower()
    if not normalized:
        return "unknown"
    if any(token in normalized for token in ("timeout", "timed out", "connection refused", "could not connect", "connection")):
        return "connection"
    if any(token in normalized for token in ("sql", "syntax", "column does not exist", "undefinedcolumn", "relation")):
        return "sql"
    if any(token in normalized for token in ("permission denied", "forbidden", "not allowed")):
        return "permission"
    if any(token in normalized for token in ("spark", "job failed", "spark-submit", "executor")):
        return "spark"
    return "unknown"


def notify_dq_profiling_failure(
    session: Session,
    *,
    schedule: DQProfilingSchedule | None,
    table: TableEntity | None,
    table_fqn: str | None,
    dq_run: DQRun | None,
    error_message: str | None,
    reporter_user_id: int | None,
    incident_id: int | None = None,
) -> None:
    recipients = _resolve_profiling_recipients(session, schedule=schedule, table=table, reporter_user_id=reporter_user_id)
    for admin in resolve_inbox_notification_recipients(session, user_ids=[], include_admins=True):
        if any(recipient.id == admin.id for recipient, _, _ in recipients):
            continue
        recipients.append((admin, "admin", {"admin_user_id": admin.id}))
    if not recipients:
        return

    dq_score = None
    failed_rules = None
    if dq_run is not None:
        latest_metric = session.scalar(
            select(DQTableMetric)
            .where(DQTableMetric.run_id == dq_run.id)
            .order_by(DQTableMetric.id.desc())
            .limit(1)
        )
        if latest_metric is not None:
            dq_score = float(latest_metric.dq_score or 0.0)
            failed_rules = int(latest_metric.failed_rules or 0)

    now = _now()
    failure_kind = _failure_kind_from_error(error_message)
    severity = "critical" if failure_kind in {"connection", "sql", "spark"} else "high"
    window_seconds = _repeat_window_seconds(session, severity)
    bucket = _dedupe_bucket(now, window_seconds)
    target_label = table_fqn or (
        f"{table.schema.database.datasource.name}.{table.schema.name}.{table.name}"
        if table is not None and getattr(table, "schema", None) is not None and getattr(table.schema, "database", None) is not None and getattr(table.schema.database, "datasource", None) is not None
        else "perfilamento"
    )
    links = _dq_links(table=table, rule=None, table_fqn=table_fqn)
    title = "Falha de perfilamento de DQ"
    message = f"O perfilamento de {target_label} falhou ({failure_kind})."
    if error_message:
        message = f"{message} {error_message[:240]}"
    if incident_id is not None:
        message = f"{message} Um incidente relacionado foi atualizado."

    recipient_ids = [recipient.id for recipient, _, _ in recipients]
    for recipient, recipient_reason, recipient_context in recipients:
        context_json = {
            "kind": "dq_profile_failure",
            "failure_kind": failure_kind,
            "error_message": error_message,
            "table_id": table.id if table is not None else None,
            "table_fqn": table_fqn,
            "dq_run_id": dq_run.id if dq_run is not None else None,
            "dq_score": dq_score,
            "failed_rules": failed_rules,
            "severity": severity,
            "notification_window_seconds": window_seconds,
            "notification_bucket": bucket,
            "recipient_reason": recipient_reason,
            "recipient_user_id": recipient.id,
            "recipient_user_ids": recipient_ids,
            "schedule_id": schedule.id if schedule is not None else None,
            "target_scope": getattr(schedule, "scope", None),
            "incident_id": incident_id,
            "links": links,
            **recipient_context,
        }

        create_user_inbox_notification(
            session,
            user_id=recipient.id,
            dedupe_key=f"dq-profile-failure:{schedule.id if schedule is not None else (table.id if table is not None else table_fqn or 'global')}:{failure_kind}:{bucket}",
            category=DQ_NOTIFICATION_CATEGORY,
            severity=severity,
            source_module=DQ_NOTIFICATION_SOURCE,
            source_entity_type="dq_profile_failure",
            source_entity_id=(schedule.id if schedule is not None else (table.id if table is not None else table_fqn or "global")),
            title=title,
            message=message,
            href=links["data_quality"],
            context_json=context_json,
            ignore_category_preferences=True,
        )


def notify_dq_profile_issue(
    session: Session,
    *,
    table: TableEntity,
    dq_run: DQRun,
    table_metric: DQTableMetric,
    reporter_user_id: int | None,
    trigger_codes: list[str],
    previous_score: float | None,
    incident_id: int | None = None,
) -> None:
    sensitive = bool(table.sensitivity_level) or bool(table.has_personal_data) or bool(table.has_sensitive_personal_data)
    dq_score = float(table_metric.dq_score or 0.0)
    failed_rules = int(table_metric.failed_rules or 0)
    dimension = str((table_metric.metrics_json or {}).get("dimension") or "").strip() if isinstance(table_metric.metrics_json, dict) else None
    severity = _notification_severity_from_profile(
        dq_score=dq_score,
        failed_rules=failed_rules,
        sensitive=sensitive,
        trigger_codes=trigger_codes,
    )

    recipients = _resolve_dq_recipients(session, rule=None, table=table, reporter_user_id=reporter_user_id)
    for admin in resolve_inbox_notification_recipients(session, user_ids=[], include_admins=True):
        if any(recipient.id == admin.id for recipient, _, _ in recipients):
            continue
        recipients.append((admin, "admin", {"admin_user_id": admin.id}))
    if not recipients:
        return

    now = _now()
    window_seconds = _repeat_window_seconds(session, severity)
    bucket = _dedupe_bucket(now, window_seconds)
    links = _dq_links(table=table, rule=None, table_fqn=f"{table.schema.database.datasource.name}.{table.schema.name}.{table.name}" if getattr(table, "schema", None) is not None and getattr(table.schema, "database", None) is not None and getattr(table.schema.database, "datasource", None) is not None else None)
    title = "Degradação de qualidade detectada" if severity != "critical" else "Qualidade de dados crítica"
    message = (
        f"O ativo {table.schema.database.datasource.name}.{table.schema.name}.{table.name} atingiu score "
        f"{round(dq_score, 1)} com {failed_rules} regra(s) em falha."
    )
    if dimension:
        message = f"{message} Dimensão afetada: {dimension}."
    if previous_score is not None:
        delta = round(previous_score - dq_score, 1)
        if delta > 0:
            message = f"{message} Queda de {delta} ponto(s) em relação ao run anterior."
    if trigger_codes:
        message = f"{message} Sinais: {', '.join(trigger_codes)}."
    if incident_id is not None:
        message = f"{message} Um incidente relacionado foi atualizado."

    table_fqn = f"{table.schema.database.datasource.name}.{table.schema.name}.{table.name}"
    recipient_ids = [recipient.id for recipient, _, _ in recipients]
    for recipient, recipient_reason, recipient_context in recipients:
        context_json = {
            "kind": "dq_profile_issue",
            "table_id": table.id,
            "table_fqn": table_fqn,
            "dq_run_id": dq_run.id,
            "dq_table_metric_id": table_metric.id,
            "dq_score": dq_score,
            "failed_rules": failed_rules,
            "quality_dimension": dimension,
            "duplicates_count": int(table_metric.duplicates_count or 0),
            "previous_score": previous_score,
            "trigger_codes": trigger_codes,
            "incident_id": incident_id,
            "severity": severity,
            "notification_window_seconds": window_seconds,
            "notification_bucket": bucket,
            "recipient_reason": recipient_reason,
            "recipient_user_id": recipient.id,
            "recipient_user_ids": recipient_ids,
            "links": links,
            **recipient_context,
        }

        create_user_inbox_notification(
            session,
            user_id=recipient.id,
            dedupe_key=f"dq-profile:{table.id}:{severity}:{bucket}",
            category=DQ_NOTIFICATION_CATEGORY,
            severity=severity,
            source_module=DQ_NOTIFICATION_SOURCE,
            source_entity_type="dq_profile",
            source_entity_id=table.id,
            title=title,
            message=message,
            href=links["data_quality"],
            context_json=context_json,
            ignore_category_preferences=True,
        )
