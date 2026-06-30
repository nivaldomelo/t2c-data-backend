from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.metadata_actions import patch_table_with_audit
from t2c_data.features.dashboard.operational_intelligence import build_operational_intelligence
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.data_quality.spark_launch_commands import launch_single_rule_run
from t2c_data.features.ingestion import load_ingestion_operational_overview
from t2c_data.features.notifications import create_user_inbox_notification, resolve_inbox_notification_recipients
from t2c_data.features.platform.operational_actions import (
    open_operational_incident,
    reprocess_datasource_scan,
    rerun_table_profiling,
)
from t2c_data.features.stewardship.workflow import create_stewardship_request
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.contracts import DataContractValidation
from t2c_data.models.dq import DQRule
from t2c_data.models.incident import Incident
from t2c_data.models.platform import PlatformAutomationExecution, PlatformAutomationRule
from t2c_data.schemas.catalog import TablePatch
from t2c_data.schemas.stewardship import StewardshipRequestCreateIn
from t2c_data.services.audit import request_audit_kwargs, serialize_model, write_audit_log_sync


@dataclass(frozen=True)
class AutomationActionDefinition:
    key: str
    label: str
    description: str
    category: str
    executable: bool
    destructive: bool = False
    suggestion_only: bool = False
    requires_target: bool = False
    target_types: list[str] = None  # type: ignore[assignment]
    scope_kinds: list[str] = None  # type: ignore[assignment]
    hints: list[str] = None  # type: ignore[assignment]
    default_payload_json: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "description": self.description,
            "category": self.category,
            "category_label": _ACTION_CATEGORY_LABELS.get(self.category, self.category.replace("_", " ").title()),
            "executable": self.executable,
            "destructive": self.destructive,
            "suggestion_only": self.suggestion_only,
            "requires_target": self.requires_target,
            "target_types": list(self.target_types or []),
            "scope_kinds": list(self.scope_kinds or []),
            "hints": list(self.hints or []),
            "default_payload_json": self.default_payload_json,
        }


_ACTION_CATEGORY_LABELS = {
    "operations": "Operações",
    "dq": "Data Quality",
    "integrations": "Integrações",
    "governance": "Governança",
    "notifications": "Notificações",
    "reviews": "Revisões",
}

ACTION_CATALOG: list[AutomationActionDefinition] = [
    AutomationActionDefinition(
        key="reprocess_pipeline",
        label="Reprocessar pipeline",
        description="Reexecuta o scan operacional da origem para reavaliar o estado do pipeline.",
        category="operations",
        executable=True,
        requires_target=True,
        target_types=["datasource", "table"],
        scope_kinds=["asset", "pipeline"],
        hints=["Precisa de datasource_id ou table_id."],
    ),
    AutomationActionDefinition(
        key="revalidate_quality_rule",
        label="Revalidar regra de qualidade",
        description="Executa novamente uma regra de Data Quality para confirmar a condição observada.",
        category="dq",
        executable=True,
        requires_target=True,
        target_types=["dq_rule", "table"],
        scope_kinds=["asset", "domain", "global"],
        hints=["Precisa de dq_rule_id ou table_id."],
    ),
    AutomationActionDefinition(
        key="reexecute_dag",
        label="Reexecutar DAG",
        description="Sugere nova execução da DAG. A plataforma registra a intenção, mas a orquestração é confirmada no Airflow.",
        category="operations",
        executable=False,
        suggestion_only=True,
        requires_target=True,
        target_types=["airflow_dag"],
        scope_kinds=["pipeline"],
        hints=["Ação sugerida: use o Airflow para reexecutar a DAG."]
    ),
    AutomationActionDefinition(
        key="notify_owner",
        label="Notificar owner",
        description="Cria uma notificação interna para o owner ou usuário associado ao ativo.",
        category="notifications",
        executable=True,
        requires_target=True,
        target_types=["table", "data_owner", "user"],
        scope_kinds=["asset", "domain", "product"],
        hints=["Precisa de table_id ou data_owner_id."],
    ),
    AutomationActionDefinition(
        key="open_incident",
        label="Abrir incidente",
        description="Abre um incidente operacional a partir do contexto avaliado.",
        category="operations",
        executable=True,
        destructive=True,
        requires_target=True,
        target_types=["table"],
        scope_kinds=["asset", "domain", "product", "pipeline"],
        hints=["Precisa de table_id."],
    ),
    AutomationActionDefinition(
        key="update_classification",
        label="Atualizar classificação",
        description="Atualiza a classificação e atributos de privacidade do ativo.",
        category="governance",
        executable=True,
        destructive=True,
        requires_target=True,
        target_types=["table"],
        scope_kinds=["asset"],
        hints=["Precisa de table_id e payload com os campos a atualizar."],
    ),
    AutomationActionDefinition(
        key="request_review",
        label="Solicitar revisão",
        description="Cria uma solicitação de stewardship para owner, privacidade, glossário ou certificação.",
        category="reviews",
        executable=True,
        requires_target=True,
        target_types=["table"],
        scope_kinds=["asset", "domain", "product"],
        hints=["Precisa de table_id e request_type."],
    ),
]

ACTION_BY_KEY = {item.key: item for item in ACTION_CATALOG}


def list_available_automation_actions() -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(ACTION_CATALOG),
        "items": [item.as_dict() for item in ACTION_CATALOG],
    }


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: Any | None) -> str:
    return (str(value or "").strip()).lower()


def _normalize_scope_kind(value: str | None) -> str:
    normalized = _normalize_text(value)
    if normalized in {"table", "asset"}:
        return "asset"
    if normalized in {"domain", "product", "pipeline", "integration", "global"}:
        return normalized
    return "asset"


def _load_table(session: Session, table_id: int) -> TableEntity:
    table = session.scalar(
        select(TableEntity)
        .options(
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
        .where(TableEntity.id == table_id)
    )
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ativo não encontrado.")
    return table


def _table_fqn(table: TableEntity) -> str:
    return f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}"


def _table_lookup_values(table: TableEntity) -> set[str]:
    datasource_name = table.schema.database.datasource.name
    database_name = table.schema.database.name
    schema_name = table.schema.name
    return {
        str(table.id),
        _table_fqn(table),
        f"{schema_name}.{table.name}",
        f"{database_name}.{schema_name}.{table.name}",
        table.name,
        schema_name,
        datasource_name,
    }


def _match_scope_item(items: list[dict[str, Any]], scope_kind: str, scope_value: str | None) -> dict[str, Any] | None:
    if not items:
        return None
    normalized = _normalize_text(scope_value)
    if not normalized:
        return items[0]
    if scope_kind == "global":
        return items[0]
    for item in items:
        candidates = {
            _normalize_text(item.get("key")),
            _normalize_text(item.get("label")),
            _normalize_text(item.get("domain_name")),
            _normalize_text(item.get("owner_name")),
            _normalize_text(item.get("href")),
        }
        if normalized in candidates:
            return item
    return None


def _build_recent_incident_map(session: Session, *, window_days: int = 30) -> dict[str, int]:
    since = _utcnow() - timedelta(days=max(window_days, 1))
    rows = session.execute(
        select(
            Incident.table_fqn.label("table_fqn"),
            func.count(Incident.id).label("count"),
        )
        .where(Incident.detected_at >= since)
        .group_by(Incident.table_fqn)
    ).all()
    return {str(table_fqn): int(count or 0) for table_fqn, count in rows if table_fqn}


def _build_recent_occurrence_map(session: Session, *, window_days: int = 30) -> dict[str, int]:
    since = _utcnow() - timedelta(days=max(window_days, 1))
    rows = session.execute(
        select(
            Incident.table_fqn.label("table_fqn"),
            func.coalesce(func.sum(Incident.occurrences), 0).label("count"),
        )
        .where(Incident.detected_at >= since)
        .group_by(Incident.table_fqn)
    ).all()
    return {str(table_fqn): int(count or 0) for table_fqn, count in rows if table_fqn}


def _build_contract_validation_map(session: Session, *, window_days: int = 30) -> dict[int, int]:
    since = _utcnow() - timedelta(days=max(window_days, 1))
    rows = session.execute(
        select(
            DataContractValidation.table_id,
            func.count(DataContractValidation.id).label("count"),
        )
        .where(
            DataContractValidation.checked_at >= since,
            DataContractValidation.status != "success",
        )
        .group_by(DataContractValidation.table_id)
    ).all()
    return {int(table_id): int(count or 0) for table_id, count in rows if table_id is not None}


def _automation_context(session: Session, *, current_user: User | None = None) -> dict[str, Any]:
    now = _utcnow()
    profiles = load_table_profiles(session, now, current_user=current_user)
    table_refs = [
        {
            "table_id": profile.table_id,
            "table_name": profile.table_name,
            "table_fqn": profile.table_fqn,
            "schema_name": profile.schema_name,
            "criticality_score": 80 if profile.critical_open_incidents > 0 else 60 if profile.recent_dq_failure_runs_30d > 0 else 30,
        }
        for profile in profiles
    ]
    ingestion_summary = load_ingestion_operational_overview(
        session,
        table_refs=table_refs,
        limit=max(len(table_refs), 8),
    )
    recent_incident_map = _build_recent_incident_map(session)
    recent_occurrence_map = _build_recent_occurrence_map(session)
    critical_changes_payload = None
    try:
        from t2c_data.features.governance import get_governance_critical_changes

        critical_changes_payload = get_governance_critical_changes(session, limit=200, current_user=current_user)
    except Exception:  # noqa: BLE001
        critical_changes_payload = {"items": []}
    intelligence = build_operational_intelligence(
        session,
        profiles=profiles,
        recent_incident_map=recent_incident_map,
        recent_occurrence_map=recent_occurrence_map,
        ingestion_summary=ingestion_summary,
        critical_changes=list((critical_changes_payload or {}).get("items", [])),
        current_user=current_user,
    )
    return {
        "generated_at": now,
        "profiles": profiles,
        "ingestion_summary": ingestion_summary,
        "recent_incident_map": recent_incident_map,
        "recent_occurrence_map": recent_occurrence_map,
        "contract_validation_map": _build_contract_validation_map(session),
        "intelligence": intelligence,
    }


def _serialize_rule(rule: PlatformAutomationRule, execution_rows: list[PlatformAutomationExecution]) -> dict[str, Any]:
    by_status = {
        "suggested": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "running": 0,
    }
    for execution in execution_rows:
        by_status[execution.status] = int(by_status.get(execution.status, 0)) + 1
    payload = {
        "id": rule.id,
        "name": rule.name,
        "description": rule.description,
        "status": rule.status,
        "scope_kind": rule.scope_kind,
        "scope_value": rule.scope_value,
        "condition_kind": rule.condition_kind,
        "condition_operator": rule.condition_operator,
        "threshold_value": rule.threshold_value,
        "window_days": rule.window_days,
        "action_key": rule.action_key,
        "action_target_json": rule.action_target_json,
        "execution_mode": rule.execution_mode,
        "notify_owner": bool(rule.notify_owner),
        "open_incident": bool(rule.open_incident),
        "schedule_enabled": bool(rule.schedule_enabled),
        "notes": rule.notes,
        "created_by_user_id": rule.created_by_user_id,
        "created_at": rule.created_at,
        "updated_at": rule.updated_at,
        "last_evaluated_at": rule.last_evaluated_at,
        "last_triggered_at": rule.last_triggered_at,
        "last_triggered_status": rule.last_triggered_status,
        "last_triggered_summary_json": rule.last_triggered_summary_json,
        "execution_count": len(execution_rows),
        "suggested_count": int(by_status.get("suggested", 0)),
        "succeeded_count": int(by_status.get("succeeded", 0)),
        "failed_count": int(by_status.get("failed", 0)),
    }
    return payload


def _serialize_execution(execution: PlatformAutomationExecution) -> dict[str, Any]:
    return {
        "id": execution.id,
        "rule_id": execution.rule_id,
        "rule_name": execution.rule.name if execution.rule is not None else None,
        "action_key": execution.action_key,
        "action_label": execution.action_label,
        "execution_mode": execution.execution_mode,
        "status": execution.status,
        "trigger_source": execution.trigger_source,
        "scope_kind": execution.scope_kind,
        "scope_value": execution.scope_value,
        "entity_type": execution.entity_type,
        "entity_id": execution.entity_id,
        "table_id": execution.table_id,
        "datasource_id": execution.datasource_id,
        "domain_name": execution.domain_name,
        "product_name": execution.product_name,
        "target_json": execution.target_json,
        "input_json": execution.input_json,
        "result_json": execution.result_json,
        "impact_json": execution.impact_json,
        "error_message": execution.error_message,
        "started_at": execution.started_at,
        "finished_at": execution.finished_at,
        "created_by_user_id": execution.created_by_user_id,
        "executed_by_user_id": execution.executed_by_user_id,
        "created_at": execution.created_at,
        "updated_at": execution.updated_at,
    }


def list_automation_rules(session: Session) -> dict[str, Any]:
    rules = session.scalars(
        select(PlatformAutomationRule).options(selectinload(PlatformAutomationRule.executions))
        .order_by(PlatformAutomationRule.created_at.desc(), PlatformAutomationRule.id.desc())
    ).all()
    items = [_serialize_rule(rule, list(rule.executions or [])) for rule in rules]
    return {
        "generated_at": _utcnow().isoformat(),
        "total": len(items),
        "items": items,
    }


def list_automation_executions(session: Session, *, limit: int = 50) -> dict[str, Any]:
    rows = session.scalars(
        select(PlatformAutomationExecution)
        .options(selectinload(PlatformAutomationExecution.rule))
        .order_by(PlatformAutomationExecution.created_at.desc(), PlatformAutomationExecution.id.desc())
        .limit(max(1, min(limit, 200)))
    ).all()
    items = [_serialize_execution(row) for row in rows]
    return {
        "generated_at": _utcnow().isoformat(),
        "total": len(items),
        "items": items,
    }


def _upsert_rule_from_payload(
    session: Session,
    *,
    payload,
    current_user: User | None,
    rule_id: int | None = None,
) -> PlatformAutomationRule:
    action_key = str(payload.action_key).strip()
    if action_key not in ACTION_BY_KEY:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Ação de automação não suportada.")
    action = ACTION_BY_KEY[action_key]
    scope_kind = _normalize_scope_kind(payload.scope_kind)
    if action.scope_kinds and scope_kind not in action.scope_kinds and scope_kind != "global":
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Escopo incompatível com a ação selecionada.")
    if payload.execution_mode == "automatic" and not action.executable:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Ação apenas sugerida não pode ser configurada como automática.")

    rule = session.get(PlatformAutomationRule, rule_id) if rule_id is not None else None
    if rule is None:
        rule = PlatformAutomationRule(created_by_user_id=current_user.id if current_user is not None else None)
        session.add(rule)

    rule.name = str(payload.name).strip()
    rule.description = (payload.description or "").strip() or None
    rule.status = str(payload.status or "active").strip() or "active"
    rule.scope_kind = scope_kind
    rule.scope_value = (payload.scope_value or "").strip() or None
    rule.condition_kind = str(payload.condition_kind).strip()
    rule.condition_operator = str(payload.condition_operator or "gte").strip() or "gte"
    rule.threshold_value = payload.threshold_value
    rule.window_days = int(payload.window_days or 7)
    rule.action_key = action_key
    rule.action_target_json = payload.action_target_json
    rule.execution_mode = str(payload.execution_mode or "automatic").strip() or "automatic"
    rule.notify_owner = bool(payload.notify_owner)
    rule.open_incident = bool(payload.open_incident)
    rule.schedule_enabled = bool(payload.schedule_enabled)
    rule.notes = (payload.notes or "").strip() or None
    session.flush()
    return rule


def create_automation_rule(session: Session, *, payload, current_user: User, audit_kwargs: dict[str, Any] | None = None) -> PlatformAutomationRule:
    rule = _upsert_rule_from_payload(session, payload=payload, current_user=current_user)
    write_audit_log_sync(
        session,
        action="platform.automation.rule.create",
        entity_type="platform_automation_rule",
        entity_id=rule.id,
        after=serialize_model(rule),
        metadata={"message": "Automation rule created"},
        source_module="platform.automation",
        **(audit_kwargs or {}),
    )
    session.commit()
    session.refresh(rule)
    return rule


def update_automation_rule(
    session: Session,
    *,
    rule_id: int,
    payload,
    current_user: User,
    audit_kwargs: dict[str, Any] | None = None,
) -> PlatformAutomationRule:
    rule = session.get(PlatformAutomationRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Regra não encontrada.")
    before = serialize_model(rule)
    updated = _upsert_rule_from_payload(session, payload=payload, current_user=current_user, rule_id=rule_id)
    write_audit_log_sync(
        session,
        action="platform.automation.rule.update",
        entity_type="platform_automation_rule",
        entity_id=updated.id,
        before=before,
        after=serialize_model(updated),
        metadata={"message": "Automation rule updated"},
        source_module="platform.automation",
        **(audit_kwargs or {}),
    )
    session.commit()
    session.refresh(updated)
    return updated


def delete_automation_rule(
    session: Session,
    *,
    rule_id: int,
    current_user: User,
    audit_kwargs: dict[str, Any] | None = None,
) -> None:
    rule = session.get(PlatformAutomationRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Regra não encontrada.")
    before = serialize_model(rule)
    session.delete(rule)
    write_audit_log_sync(
        session,
        action="platform.automation.rule.delete",
        entity_type="platform_automation_rule",
        entity_id=rule_id,
        before=before,
        metadata={"message": "Automation rule deleted"},
        source_module="platform.automation",
        **(audit_kwargs or {}),
    )
    session.commit()


def _resolve_table_from_execution_payload(session: Session, payload: dict[str, Any]) -> TableEntity | None:
    table_id = payload.get("table_id")
    if table_id is None:
        return None
    try:
        return _load_table(session, int(table_id))
    except Exception:  # noqa: BLE001
        return None


def _resolve_actor_user(session: Session, current_user: User | None, rule: PlatformAutomationRule | None = None) -> User:
    if current_user is not None:
        return current_user
    if rule is not None and rule.created_by_user is not None:
        return rule.created_by_user
    user = session.scalar(select(User).where(User.is_active.is_(True)).order_by(User.id.asc()).limit(1))
    if user is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Não foi possível resolver um usuário para executar a automação.")
    return user


def _build_execution_record(
    session: Session,
    *,
    action_key: str,
    action_label: str,
    execution_mode: str,
    trigger_source: str,
    scope_kind: str,
    scope_value: str | None,
    target_json: dict[str, Any] | list | None,
    current_user: User,
    rule: PlatformAutomationRule | None = None,
    entity_type: str | None = None,
    entity_id: int | None = None,
    table_id: int | None = None,
    datasource_id: int | None = None,
    domain_name: str | None = None,
    product_name: str | None = None,
    input_json: dict[str, Any] | list | None = None,
) -> PlatformAutomationExecution:
    execution = PlatformAutomationExecution(
        rule_id=rule.id if rule is not None else None,
        action_key=action_key,
        action_label=action_label,
        execution_mode=execution_mode,
        status="running" if execution_mode != "suggested" else "suggested",
        trigger_source=trigger_source,
        scope_kind=scope_kind,
        scope_value=scope_value,
        entity_type=entity_type,
        entity_id=entity_id,
        table_id=table_id,
        datasource_id=datasource_id,
        domain_name=domain_name,
        product_name=product_name,
        target_json=target_json,
        input_json=input_json,
        created_by_user_id=current_user.id,
        executed_by_user_id=current_user.id if execution_mode != "suggested" else None,
        started_at=_utcnow() if execution_mode != "suggested" else None,
    )
    session.add(execution)
    session.flush()
    return execution


def _finish_execution(
    session: Session,
    execution: PlatformAutomationExecution,
    *,
    status_value: str,
    result_json: dict[str, Any] | list | None = None,
    impact_json: dict[str, Any] | list | None = None,
    error_message: str | None = None,
) -> PlatformAutomationExecution:
    execution.status = status_value
    execution.result_json = result_json
    execution.impact_json = impact_json
    execution.error_message = error_message
    execution.finished_at = _utcnow()
    session.add(execution)
    session.flush()
    return execution


def execute_automation_action(
    session: Session,
    *,
    action_key: str,
    current_user: User | None,
    table_id: int | None = None,
    datasource_id: int | None = None,
    dq_rule_id: int | None = None,
    delivery_id: int | None = None,
    incident_id: int | None = None,
    data_owner_id: int | None = None,
    request_type: str | None = None,
    scope_kind: str | None = None,
    scope_value: str | None = None,
    target_json: dict[str, Any] | None = None,
    rule: PlatformAutomationRule | None = None,
    execution_mode: str = "manual",
    trigger_source: str = "manual",
    audit_kwargs: dict[str, Any] | None = None,
) -> PlatformAutomationExecution:
    action = ACTION_BY_KEY.get(action_key)
    if action is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Ação de automação não suportada.")
    actor_user = _resolve_actor_user(session, current_user, rule)

    payload_target: dict[str, Any] = dict(target_json or {})
    if table_id is not None:
        payload_target.setdefault("table_id", table_id)
    if datasource_id is not None:
        payload_target.setdefault("datasource_id", datasource_id)
    if dq_rule_id is not None:
        payload_target.setdefault("dq_rule_id", dq_rule_id)
    if delivery_id is not None:
        payload_target.setdefault("delivery_id", delivery_id)
    if incident_id is not None:
        payload_target.setdefault("incident_id", incident_id)
    if data_owner_id is not None:
        payload_target.setdefault("data_owner_id", data_owner_id)
    if request_type is not None:
        payload_target.setdefault("request_type", request_type)

    resolved_scope_kind = _normalize_scope_kind(scope_kind or (rule.scope_kind if rule is not None else None))
    resolved_scope_value = scope_value or (rule.scope_value if rule is not None else None)

    if action.suggestion_only and execution_mode != "suggested":
        execution_mode = "suggested"

    execution = _build_execution_record(
        session,
        action_key=action.key,
        action_label=action.label,
        execution_mode=execution_mode,
        trigger_source=trigger_source,
        scope_kind=resolved_scope_kind,
        scope_value=resolved_scope_value,
        target_json=payload_target,
        current_user=actor_user,
        rule=rule,
        input_json=payload_target,
    )

    try:
        if action.key == "reprocess_pipeline":
            resolved_table = _resolve_table_from_execution_payload(session, payload_target)
            resolved_datasource_id = datasource_id
            if resolved_datasource_id is None and resolved_table is not None:
                resolved_datasource_id = int(resolved_table.schema.database.datasource_id)
            if resolved_datasource_id is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe datasource_id ou table_id para reprocessar o pipeline.")
            result = reprocess_datasource_scan(
                session,
                datasource_id=int(resolved_datasource_id),
                current_user=actor_user,
                audit_kwargs=audit_kwargs or {},
            )
            execution.datasource_id = int(resolved_datasource_id)
            if resolved_table is not None:
                execution.table_id = resolved_table.id
                execution.entity_type = "table"
                execution.entity_id = resolved_table.id
            _finish_execution(session, execution, status_value="succeeded", result_json=result, impact_json={"message": "Pipeline reprocessado"})
        elif action.key == "revalidate_quality_rule":
            resolved_table = _resolve_table_from_execution_payload(session, payload_target)
            resolved_dq_rule = None
            if dq_rule_id is not None:
                resolved_dq_rule = session.get(DQRule, int(dq_rule_id))
            elif resolved_table is not None:
                table_fqn = _table_fqn(resolved_table)
                resolved_dq_rule = session.scalar(
                    select(DQRule)
                    .where(DQRule.table_fqn == table_fqn)
                    .order_by(DQRule.created_at.desc(), DQRule.id.desc())
                )
            if resolved_dq_rule is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe dq_rule_id ou table_id para revalidar a regra.")
            job = launch_single_rule_run(
                db=session,
                rule_id=resolved_dq_rule.id,
                current_user=actor_user,
                execution_engine="spark",
                audit_kwargs=audit_kwargs or {},
            )
            execution.table_id = resolved_table.id if resolved_table is not None else None
            execution.entity_type = "dq_rule"
            execution.entity_id = resolved_dq_rule.id
            _finish_execution(
                session,
                execution,
                status_value="succeeded",
                result_json={
                    "status": "queued",
                    "message": "Revalidação da regra enfileirada no cluster Spark.",
                    "job_run_id": job.id,
                    "dq_run_id": job.dq_run_id,
                    "execution_engine": job.execution_engine,
                },
                impact_json={"dq_job_run_id": job.id, "dq_run_id": job.dq_run_id},
            )
        elif action.key == "reexecute_dag":
            dag_id = str(payload_target.get("dag_id") or payload_target.get("airflow_dag_id") or "").strip()
            if not dag_id:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe dag_id para registrar a sugestão de reexecução.")
            execution.entity_type = "airflow_dag"
            execution.entity_id = None
            _finish_execution(
                session,
                execution,
                status_value="suggested",
                result_json={
                    "ok": True,
                    "message": "Reexecução da DAG sugerida. A orquestração deve ser confirmada no Airflow.",
                    "dag_id": dag_id,
                    "airflow_href": payload_target.get("airflow_href") or payload_target.get("airflow_dag_href"),
                },
                impact_json={"airflow_dag_id": dag_id},
            )
        elif action.key == "notify_owner":
            resolved_table = _resolve_table_from_execution_payload(session, payload_target)
            target_user = None
            resolved_data_owner_id = data_owner_id if data_owner_id is not None else payload_target.get("data_owner_id")
            if resolved_data_owner_id is not None:
                data_owner = session.get(DataOwner, int(resolved_data_owner_id))
                if data_owner is not None:
                    target_user = session.scalar(select(User).where(User.email == data_owner.email, User.is_active.is_(True)).limit(1))
            if target_user is None and resolved_table is not None:
                for email in [resolved_table.owner_email, resolved_table.data_owner.email if resolved_table.data_owner else None]:
                    normalized_email = _normalize_text(email)
                    if not normalized_email:
                        continue
                    target_user = session.scalar(select(User).where(User.email == normalized_email, User.is_active.is_(True)).limit(1))
                    if target_user is not None:
                        break
            if target_user is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Não foi possível identificar um owner elegível para notificação.")
            recipients = resolve_inbox_notification_recipients(session, user_ids=[target_user.id], include_admins=False)
            created_notifications: list[int] = []
            for recipient in recipients:
                inbox = create_user_inbox_notification(
                    session,
                    user_id=recipient.id,
                    dedupe_key=f"automation.notify_owner.{execution.id}.{recipient.id}",
                    category="operations",
                    severity="warning",
                    source_module="platform.automation",
                    source_entity_type="platform_automation_execution",
                    source_entity_id=execution.id,
                    title=f"Ação operacional sugerida para {resolved_table.name if resolved_table is not None else 'ativo'}",
                    message="A automação identificou risco operacional e sugeriu uma ação para o owner.",
                    href=f"/ops/automations?executionId={execution.id}",
                    context_json={
                        "action_key": action.key,
                        "table_id": resolved_table.id if resolved_table is not None else None,
                        "rule_id": rule.id if rule is not None else None,
                    },
                    ignore_category_preferences=True,
                )
                created_notifications.append(int(inbox.id))
            execution.entity_type = "user"
            execution.entity_id = target_user.id
            if resolved_table is not None:
                execution.table_id = resolved_table.id
            _finish_execution(
                session,
                execution,
                status_value="succeeded",
                result_json={"ok": True, "notifications": created_notifications, "target_user_id": target_user.id},
                impact_json={"user_id": target_user.id},
            )
        elif action.key == "open_incident":
            resolved_table = _resolve_table_from_execution_payload(session, payload_target)
            if resolved_table is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe table_id para abrir o incidente.")
            opened = open_operational_incident(
                session,
                table_id=resolved_table.id,
                current_user=actor_user,
                audit_kwargs=audit_kwargs or {},
                mode="manual",
            )
            execution.table_id = resolved_table.id
            execution.entity_type = "incident"
            execution.entity_id = int(opened.get("target_id") or 0) or None
            _finish_execution(session, execution, status_value="succeeded", result_json=opened, impact_json={"incident_id": opened.get("target_id")})
        elif action.key == "update_classification":
            resolved_table = _resolve_table_from_execution_payload(session, payload_target)
            if resolved_table is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe table_id para atualizar a classificação.")
            classification_payload = {
                "description_manual": payload_target.get("description_manual"),
                "data_owner_id": payload_target.get("data_owner_id"),
                "owner": payload_target.get("owner"),
                "owner_email": payload_target.get("owner_email"),
                "lifecycle_status": payload_target.get("lifecycle_status"),
                "sensitivity_level": payload_target.get("sensitivity_level"),
                "has_personal_data": payload_target.get("has_personal_data"),
                "has_sensitive_personal_data": payload_target.get("has_sensitive_personal_data"),
                "legal_basis": payload_target.get("legal_basis"),
                "retention_policy": payload_target.get("retention_policy"),
                "is_masked": payload_target.get("is_masked"),
                "external_sharing": payload_target.get("external_sharing"),
                "access_scope": payload_target.get("access_scope"),
                "access_roles": payload_target.get("access_roles"),
                "privacy_notes": payload_target.get("privacy_notes"),
            }
            table_patch = TablePatch(**{key: value for key, value in classification_payload.items() if value is not None})
            updated_table = patch_table_with_audit(
                db=session,
                table_id=resolved_table.id,
                payload=table_patch,
                user=actor_user,
                audit_kwargs=audit_kwargs or {},
                commit=False,
            )
            execution.table_id = updated_table.id
            execution.entity_type = "table"
            execution.entity_id = updated_table.id
            _finish_execution(session, execution, status_value="succeeded", result_json={"ok": True, "table_id": updated_table.id}, impact_json={"table_id": updated_table.id})
        elif action.key == "request_review":
            resolved_table = _resolve_table_from_execution_payload(session, payload_target)
            if resolved_table is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Informe table_id para solicitar a revisão.")
            review_type = _normalize_text(request_type or payload_target.get("request_type")) or "owner_review"
            request_payload = StewardshipRequestCreateIn(
                table_id=resolved_table.id,
                request_type=review_type,
                requester_comment=payload_target.get("requester_comment") or payload_target.get("notes") or "Solicitação criada por automação operacional.",
                approver_user_id=payload_target.get("approver_user_id"),
                request_origin="automation",
                description_manual=payload_target.get("description_manual"),
                data_owner_id=payload_target.get("data_owner_id"),
                term_ids=payload_target.get("term_ids"),
            )
            request_item = create_stewardship_request(
                session,
                payload=request_payload,
                user=actor_user,
                audit_kwargs=audit_kwargs or {},
            )
            execution.table_id = resolved_table.id
            execution.entity_type = "stewardship_request"
            execution.entity_id = request_item.id
            _finish_execution(session, execution, status_value="succeeded", result_json={"ok": True, "request_id": request_item.id}, impact_json={"request_id": request_item.id})
        else:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Ação de automação não implementada.")
    except Exception as exc:  # noqa: BLE001
        try:
            session.rollback()
        except Exception:  # noqa: BLE001
            pass
        execution.started_at = execution.started_at or _utcnow()
        execution.finished_at = _utcnow()
        execution.status = "failed"
        execution.error_message = str(exc)
        session.add(execution)
        session.flush()
        write_audit_log_sync(
            session,
            action="platform.automation.action.execute",
            entity_type="platform_automation_execution",
            entity_id=execution.id,
            after=serialize_model(execution),
            metadata={"message": "Automation action failed", "action_key": action.key},
            source_module="platform.automation",
            **(audit_kwargs or {}),
        )
        session.commit()
        raise

    write_audit_log_sync(
        session,
        action="platform.automation.action.execute",
        entity_type="platform_automation_execution",
        entity_id=execution.id,
        after=serialize_model(execution),
        metadata={"message": "Automation action executed", "action_key": action.key},
        source_module="platform.automation",
        **(audit_kwargs or {}),
    )
    session.commit()
    session.refresh(execution)
    return execution


def _comparison_ok(value: float | int | None, operator: str, threshold: int | float | None) -> bool:
    if operator in {"exists", "present"}:
        return value is not None and value != 0
    if threshold is None:
        return False
    if value is None:
        return False
    numeric = float(value)
    target = float(threshold)
    if operator in {"gte", "ge", ">="}:
        return numeric >= target
    if operator in {"gt", ">"}:
        return numeric > target
    if operator in {"lte", "le", "<="}:
        return numeric <= target
    if operator in {"lt", "<"}:
        return numeric < target
    if operator in {"eq", "=", "=="}:
        return numeric == target
    if operator in {"ne", "!=", "<>"}:
        return numeric != target
    return False


def _value_for_rule(
    rule: PlatformAutomationRule,
    *,
    context: dict[str, Any],
) -> tuple[float | int | None, dict[str, Any], list[dict[str, Any]]]:
    intelligence = context["intelligence"]
    profile_by_id = {profile.table_id: profile for profile in context["profiles"]}
    asset_items = list(intelligence.get("by_asset", []))
    domain_items = list(intelligence.get("by_domain", []))
    product_items = list(intelligence.get("by_product", []))
    pipeline_items = list(intelligence.get("by_pipeline", []))

    scope_kind = _normalize_scope_kind(rule.scope_kind)
    scope_value = _normalize_text(rule.scope_value)
    matched_items: list[dict[str, Any]] = []
    if scope_kind == "domain":
        matched_items = [item for item in domain_items if not scope_value or scope_value in {_normalize_text(item.get("label")), _normalize_text(item.get("domain_name")), _normalize_text(item.get("key"))}]
    elif scope_kind == "product":
        matched_items = [item for item in product_items if not scope_value or scope_value in {_normalize_text(item.get("label")), _normalize_text(item.get("key"))}]
    elif scope_kind == "pipeline":
        matched_items = [item for item in pipeline_items if not scope_value or scope_value in {_normalize_text(item.get("label")), _normalize_text(item.get("key")), _normalize_text(item.get("scope_value"))}]
    elif scope_kind == "global":
        matched_items = asset_items or domain_items or product_items or pipeline_items
    else:
        matched_items = [item for item in asset_items if not scope_value or scope_value in {_normalize_text(item.get("label")), _normalize_text(item.get("key")), _normalize_text(item.get("table_id"))}]

    if scope_kind == "global":
        aggregate = {
            "score": int(round(sum(int(item["score"]) for item in matched_items) / max(len(matched_items), 1))) if matched_items else 0,
            "priority_score": int(round(sum(int(item["priority_score"]) for item in matched_items) / max(len(matched_items), 1))) if matched_items else 0,
            "open_incidents": sum(int(item["open_incidents"]) for item in matched_items),
            "critical_open_incidents": sum(int(item["critical_open_incidents"]) for item in matched_items),
            "recent_dq_failure_runs_30d": sum(int(item["recent_dq_failure_runs_30d"]) for item in matched_items),
            "change_events_30d": sum(int(item["change_events_30d"]) for item in matched_items),
            "search_clicks_30d": sum(int(item["search_clicks_30d"]) for item in matched_items),
            "stale_hours": max((int(item["stale_hours"]) for item in matched_items if item.get("stale_hours") is not None), default=None),
            "owner_missing": sum(1 for profile in profile_by_id.values() if not profile.owner_defined),
        }
        value_map = {
            "risk_score": aggregate["priority_score"],
            "priority_score": aggregate["priority_score"],
            "open_incidents": aggregate["open_incidents"],
            "critical_open_incidents": aggregate["critical_open_incidents"],
            "dq_failures": aggregate["recent_dq_failure_runs_30d"],
            "recent_dq_failures": aggregate["recent_dq_failure_runs_30d"],
            "stale_hours": aggregate["stale_hours"] or 0,
            "owner_missing": aggregate["owner_missing"],
        }
        return value_map.get(rule.condition_kind, aggregate["priority_score"]), aggregate, matched_items

    selected_item = _match_scope_item(matched_items, scope_kind, scope_value)
    if selected_item is None and matched_items:
        selected_item = matched_items[0]
    if selected_item is None:
        return None, {}, []

    profile = profile_by_id.get(int(selected_item.get("table_id") or 0))
    if rule.condition_kind in {"risk_score", "priority_score"}:
        return int(selected_item.get("priority_score") or 0), selected_item, matched_items
    if rule.condition_kind in {"open_incidents", "incident_count"}:
        return int(selected_item.get("open_incidents") or 0), selected_item, matched_items
    if rule.condition_kind in {"critical_open_incidents", "critical_incidents"}:
        return int(selected_item.get("critical_open_incidents") or 0), selected_item, matched_items
    if rule.condition_kind in {"dq_failures", "recent_dq_failures"}:
        return int(selected_item.get("recent_dq_failure_runs_30d") or 0), selected_item, matched_items
    if rule.condition_kind == "change_events":
        return int(selected_item.get("change_events_30d") or 0), selected_item, matched_items
    if rule.condition_kind == "stale_hours":
        stale_hours = selected_item.get("stale_hours")
        return int(stale_hours or 0), selected_item, matched_items
    if rule.condition_kind == "owner_missing":
        return 0 if selected_item.get("owner_name") else 1, selected_item, matched_items
    if rule.condition_kind == "contract_validation_failed":
        table_id = int(selected_item.get("table_id") or (profile.table_id if profile else 0))
        return int(context["contract_validation_map"].get(table_id, 0)), selected_item, matched_items
    if rule.condition_kind == "pipeline_failed":
        return 1 if int(selected_item.get("failed_pipelines") or 0) > 0 else 0, selected_item, matched_items
    return int(selected_item.get("priority_score") or 0), selected_item, matched_items


def evaluate_automation_rules(
    session: Session,
    *,
    current_user: User | None = None,
    audit_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = _automation_context(session, current_user=current_user)
    actor_user = _resolve_actor_user(session, current_user, None)
    rules = session.scalars(
        select(PlatformAutomationRule)
        .options(selectinload(PlatformAutomationRule.executions))
        .where(PlatformAutomationRule.status == "active")
        .order_by(PlatformAutomationRule.created_at.asc(), PlatformAutomationRule.id.asc())
    ).all()
    triggered: list[dict[str, Any]] = []
    suggestions: list[dict[str, Any]] = []
    executed_count = 0
    suggested_count = 0
    skipped_count = 0
    for rule in rules:
        rule.last_evaluated_at = _utcnow()
        value, selected_item, matched_items = _value_for_rule(rule, context=context)
        matched = _comparison_ok(value, rule.condition_operator, rule.threshold_value)
        rule.last_triggered_at = _utcnow() if matched else rule.last_triggered_at
        rule.last_triggered_status = "matched" if matched else "skipped"
        rule.last_triggered_summary_json = {
            "condition_kind": rule.condition_kind,
            "condition_operator": rule.condition_operator,
            "threshold_value": rule.threshold_value,
            "observed_value": value,
            "scope_kind": rule.scope_kind,
            "scope_value": rule.scope_value,
            "matched_items": len(matched_items),
            "selected_key": selected_item.get("key") if selected_item else None,
        }
        if not matched:
            skipped_count += 1
            session.add(rule)
            continue

        payload_target = dict(rule.action_target_json or {})
        if selected_item and selected_item.get("table_id") is not None:
            payload_target.setdefault("table_id", int(selected_item["table_id"]))
        if selected_item and selected_item.get("href"):
            payload_target.setdefault("source_href", selected_item["href"])
        if rule.scope_kind in {"domain", "product", "pipeline"} and selected_item is not None:
            payload_target.setdefault("scope_label", selected_item.get("label"))

        action = ACTION_BY_KEY[rule.action_key]
        if rule.execution_mode == "automatic" and action.executable:
            execution = execute_automation_action(
                session,
                action_key=rule.action_key,
                current_user=actor_user,
                table_id=payload_target.get("table_id"),
                datasource_id=payload_target.get("datasource_id"),
                dq_rule_id=payload_target.get("dq_rule_id"),
                delivery_id=payload_target.get("delivery_id"),
                incident_id=payload_target.get("incident_id"),
                data_owner_id=payload_target.get("data_owner_id"),
                request_type=payload_target.get("request_type"),
                scope_kind=rule.scope_kind,
                scope_value=rule.scope_value,
                target_json=payload_target,
                rule=rule,
                execution_mode="automatic",
                trigger_source="rule",
                audit_kwargs=audit_kwargs or {},
            )
            triggered.append(_serialize_execution(execution))
            executed_count += 1
        else:
            execution = _build_execution_record(
                session,
                action_key=rule.action_key,
                action_label=action.label,
                execution_mode="suggested",
                trigger_source="rule",
                scope_kind=rule.scope_kind,
                scope_value=rule.scope_value,
                target_json=payload_target,
                current_user=actor_user,
                rule=rule,
                input_json={"observed_value": value, "condition_kind": rule.condition_kind},
                entity_type=selected_item.get("entity_kind") if selected_item else None,
                entity_id=int(selected_item.get("table_id")) if selected_item and selected_item.get("table_id") is not None else None,
                table_id=int(selected_item.get("table_id")) if selected_item and selected_item.get("table_id") is not None else None,
                domain_name=selected_item.get("domain_name") if selected_item else None,
                product_name=selected_item.get("label") if selected_item and selected_item.get("entity_kind") == "product" else None,
            )
            _finish_execution(
                session,
                execution,
                status_value="suggested",
                result_json={
                    "ok": True,
                    "message": "Ação sugerida pela regra, aguardando execução manual.",
                    "observed_value": value,
                    "condition_kind": rule.condition_kind,
                },
                impact_json={"suggested": True, "rule_id": rule.id},
            )
            suggestions.append(_serialize_execution(execution))
            suggested_count += 1
        session.add(rule)
    session.commit()
    return {
        "generated_at": _utcnow().isoformat(),
        "rules_evaluated": len(rules),
        "suggestions_created": suggested_count,
        "actions_executed": executed_count,
        "skipped": skipped_count,
        "items": triggered + suggestions,
    }


def run_automation_rule(
    session: Session,
    *,
    rule_id: int,
    current_user: User | None,
    audit_kwargs: dict[str, Any] | None = None,
) -> PlatformAutomationExecution:
    rule = session.get(PlatformAutomationRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Regra não encontrada.")
    if rule.status != "active":
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="A regra não está ativa.")
    actor_user = _resolve_actor_user(session, current_user, rule)
    action_target = dict(rule.action_target_json or {})
    execution = execute_automation_action(
        session,
        action_key=rule.action_key,
        current_user=actor_user,
        table_id=action_target.get("table_id"),
        datasource_id=action_target.get("datasource_id"),
        dq_rule_id=action_target.get("dq_rule_id"),
        delivery_id=action_target.get("delivery_id"),
        incident_id=action_target.get("incident_id"),
        data_owner_id=action_target.get("data_owner_id"),
        request_type=action_target.get("request_type"),
        scope_kind=rule.scope_kind,
        scope_value=rule.scope_value,
        target_json=action_target,
        rule=rule,
        execution_mode="manual",
        trigger_source="manual_rule",
        audit_kwargs=audit_kwargs or {},
    )
    return execution


__all__ = [
    "ACTION_CATALOG",
    "ACTION_BY_KEY",
    "AutomationActionDefinition",
    "create_automation_rule",
    "delete_automation_rule",
    "evaluate_automation_rules",
    "execute_automation_action",
    "list_automation_actions",
    "list_automation_executions",
    "list_automation_rules",
    "run_automation_rule",
    "update_automation_rule",
]
