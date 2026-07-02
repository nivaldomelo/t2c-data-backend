from __future__ import annotations

import csv
import json
from io import BytesIO, StringIO
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import Integer, and_, case, cast, func, or_, select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.export_jobs import ExportArtifactResult, enqueue_export_job, register_export_request_audit, serialize_export_job
from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, neutralize_spreadsheet_formula, redact_export_value, resolve_export_limit
from t2c_data.features.privacy_access import can_view_table
from t2c_data.features.platform.visibility import mask_audit_event_payload, table_visibility_decision_from_entity
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.auth import User
from t2c_data.schemas.audit import (
    AuditHistoryEventOut,
    AuditHistoryExportRowOut,
    AuditHistoryFilterOptionsOut,
    AuditHistoryPageOut,
    AuditLogOut,
    AuditLogPageOut,
)
from t2c_data.schemas.platform import IntegrationSyncJobOut

router = APIRouter(prefix="/audit", tags=["audit"])

AUDIT_ENTITY_LABELS = {
    "table": "Tabela",
    "column": "Coluna",
    "glossary_term": "Termo de glossário",
    "tag": "Tag",
    "owner": "Owner",
    "datasource": "Fonte",
    "database": "Banco",
    "schema": "Schema",
    "classification": "Classificação",
    "governance_settings": "Configuração de governança",
}
AUDIT_FIELD_LABELS = {
    "owner": "Responsável",
    "description": "Descrição",
    "definition": "Definição",
    "classification": "Classificação",
    "certification_status": "Status de certificação",
    "certification_criticality": "Criticidade de certificação",
    "certification_badges": "Selos de certificação",
    "certification_notes": "Notas de certificação",
    "certification_review_at": "Data de revisão da certificação",
    "lifecycle_status": "Ciclo de vida",
    "legal_basis": "Base legal",
    "retention_policy": "Política de retenção",
    "access_scope": "Escopo de acesso",
    "access_roles": "Perfis de acesso",
    "privacy_notes": "Notas de privacidade",
    "has_personal_data": "Dado pessoal",
    "has_sensitive_personal_data": "Dado pessoal sensível",
    "privacy_purpose": "Finalidade LGPD",
    "is_masked": "Mascaramento",
    "external_sharing": "Compartilhamento externo",
    "glossary_terms": "Termos de glossário",
    "tags": "Tags",
    "dictionary_description": "Descrição do dicionário",
    "dictionary_comment": "Observação do dicionário",
    "existing_comment": "Comentário existente",
    "friendly_name": "Nome amigável",
    "alias": "Alias",
    "synonym": "Sinônimo",
    "label_kind": "Tipo do rótulo",
}
AUDIT_CHANGE_TYPE_LABELS = {
    "assign": "Atribuição",
    "unassign": "Desassociação",
    "update": "Atualização",
    "certify": "Certificação",
    "decertify": "Descertificação",
    "reclassify": "Reclassificação",
    "create": "Criação",
    "delete": "Remoção",
}
AUDIT_SOURCE_MODULE_LABELS = {
    "catalog": "Catálogo",
    "glossary": "Glossário",
    "tags": "Tags",
    "certification": "Certificação",
    "privacy_access": "Privacidade e acesso",
    "privacy-access": "Privacidade e acesso",
    "governance": "Governança",
    "search": "Busca",
    "dashboard": "Dashboard",
    "incidents": "Incidentes",
    "lineage": "Linhagem",
    "admin": "Administração",
}


def build_audit_history_csv_export_artifact(
    db: Session,
    *,
    current_user: User,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    actor: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    parent_entity_type: str | None = None,
    parent_entity_id: str | None = None,
    change_type: str | None = None,
    field_name: str | None = None,
    source_module: str | None = None,
    sensitive_only: bool = False,
    datasource: str | None = None,
    database: str | None = None,
    schema: str | None = None,
    q: str | None = None,
    **_: Any,
) -> ExportArtifactResult:
    stmt = _history_query(
        db=db,
        date_from=date_from,
        date_to=date_to,
        actor=actor,
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        change_type=change_type,
        field_name=field_name,
        source_module=source_module,
        sensitive_only=sensitive_only,
        datasource=datasource,
        database=database,
        schema=schema,
        q=q,
    )
    export_limit = resolve_export_limit(source_module="audit", entity_type="audit_history")
    rows = _history_export_rows(db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc())).all())
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "data_hora",
            "usuario",
            "email",
            "tipo_entidade",
            "tipo_entidade_tecnico",
            "entidade_id",
            "fonte",
            "banco",
            "schema",
            "tabela",
            "campo",
            "campo_tecnico",
            "tipo_mudanca",
            "tipo_mudanca_tecnico",
            "modulo_origem",
            "modulo_origem_tecnico",
            "change_set_id",
            "mudanca_sensivel",
            "categoria_sensivel",
            "valor_anterior",
            "valor_novo",
            "contexto_adicional",
        ]
    )
    for item in rows:
        writer.writerow(
            [
                item.changed_at.isoformat(),
                neutralize_spreadsheet_formula(item.actor_name or ""),
                neutralize_spreadsheet_formula(item.actor_email or ""),
                _label_entity_type(item.entity_type),
                neutralize_spreadsheet_formula(item.entity_type or ""),
                neutralize_spreadsheet_formula(item.entity_id or ""),
                neutralize_spreadsheet_formula(item.datasource_name or ""),
                neutralize_spreadsheet_formula(item.database_name or ""),
                neutralize_spreadsheet_formula(item.schema_name or ""),
                neutralize_spreadsheet_formula(item.table_name or ""),
                _label_field_name(item.field_name),
                neutralize_spreadsheet_formula(item.field_name or ""),
                _label_change_type(item.change_type),
                neutralize_spreadsheet_formula(item.change_type or ""),
                _label_source_module(item.source_module),
                neutralize_spreadsheet_formula(item.source_module or ""),
                neutralize_spreadsheet_formula(item.change_set_id or ""),
                "Sim" if item.is_sensitive_change else "Não",
                neutralize_spreadsheet_formula(item.sensitive_category or ""),
                redact_export_value(item.before_value, field_name=item.field_name),
                redact_export_value(item.after_value, field_name=item.field_name),
                redact_export_value(item.metadata_json, field_name="metadata"),
            ]
        )
    return ExportArtifactResult(
        payload=buffer.getvalue().encode("utf-8-sig"),
        filename="auditoria.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(rows),
        truncated=truncated,
        export_format="csv",
    )


def build_audit_history_xlsx_export_artifact(
    db: Session,
    *,
    current_user: User,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    actor: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    parent_entity_type: str | None = None,
    parent_entity_id: str | None = None,
    change_type: str | None = None,
    field_name: str | None = None,
    source_module: str | None = None,
    sensitive_only: bool = False,
    datasource: str | None = None,
    database: str | None = None,
    schema: str | None = None,
    q: str | None = None,
    **_: Any,
) -> ExportArtifactResult:
    try:
        from openpyxl import Workbook
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="openpyxl não está instalado.") from exc
    stmt = _history_query(
        db=db,
        date_from=date_from,
        date_to=date_to,
        actor=actor,
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        change_type=change_type,
        field_name=field_name,
        source_module=source_module,
        sensitive_only=sensitive_only,
        datasource=datasource,
        database=database,
        schema=schema,
        q=q,
    )
    export_limit = resolve_export_limit(source_module="audit", entity_type="audit_history")
    rows = _history_export_rows(db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc())).all())
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Auditoria"
    sheet.append(
        [
            "Data/hora",
            "Usuário",
            "Email",
            "Tipo entidade",
            "Tipo entidade técnico",
            "Entidade ID",
            "Fonte",
            "Banco",
            "Schema",
            "Tabela",
            "Campo",
            "Campo técnico",
            "Tipo mudança",
            "Tipo mudança técnico",
            "Módulo",
            "Módulo técnico",
            "Change set",
            "Sensível",
            "Categoria sensível",
            "Antes",
            "Depois",
            "Contexto adicional",
        ]
    )
    for item in rows:
        sheet.append(
            [
                item.changed_at.isoformat(),
                neutralize_spreadsheet_formula(item.actor_name or ""),
                neutralize_spreadsheet_formula(item.actor_email or ""),
                _label_entity_type(item.entity_type),
                neutralize_spreadsheet_formula(item.entity_type or ""),
                neutralize_spreadsheet_formula(item.entity_id or ""),
                neutralize_spreadsheet_formula(item.datasource_name or ""),
                neutralize_spreadsheet_formula(item.database_name or ""),
                neutralize_spreadsheet_formula(item.schema_name or ""),
                neutralize_spreadsheet_formula(item.table_name or ""),
                _label_field_name(item.field_name),
                neutralize_spreadsheet_formula(item.field_name or ""),
                _label_change_type(item.change_type),
                neutralize_spreadsheet_formula(item.change_type or ""),
                _label_source_module(item.source_module),
                neutralize_spreadsheet_formula(item.source_module or ""),
                neutralize_spreadsheet_formula(item.change_set_id or ""),
                "Sim" if item.is_sensitive_change else "Não",
                neutralize_spreadsheet_formula(item.sensitive_category or ""),
                redact_export_value(item.before_value, field_name=item.field_name),
                redact_export_value(item.after_value, field_name=item.field_name),
                redact_export_value(item.metadata_json, field_name="metadata"),
            ]
        )
    stream = BytesIO()
    workbook.save(stream)
    return ExportArtifactResult(
        payload=stream.getvalue(),
        filename="auditoria.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        row_count=len(rows),
        truncated=truncated,
        export_format="xlsx",
    )


def _label_entity_type(value: str | None) -> str:
    if not value:
        return ""
    return AUDIT_ENTITY_LABELS.get(value, value.replace("_", " "))


def _label_field_name(value: str | None) -> str:
    if not value:
        return ""
    return AUDIT_FIELD_LABELS.get(value, value.replace("_", " "))


def _label_change_type(value: str | None) -> str:
    if not value:
        return ""
    return AUDIT_CHANGE_TYPE_LABELS.get(value, value.replace("_", " "))


def _label_source_module(value: str | None) -> str:
    if not value:
        return ""
    return AUDIT_SOURCE_MODULE_LABELS.get(value, value.replace("_", " "))


def _serialize_audit_item(item: AuditLog) -> AuditLogOut:
    payload = {
        "id": item.id,
        "user_id": item.user_id,
        "actor_name": item.actor_name,
        "user_email": item.user_email,
        "ip": (str(item.ip) if item.ip is not None else None),
        "user_agent": item.user_agent,
        "action": item.action,
        "entity_type": item.entity_type,
        "entity_id": item.entity_id,
        "parent_entity_type": item.parent_entity_type,
        "parent_entity_id": item.parent_entity_id,
        "change_set_id": item.change_set_id,
        "change_type": item.change_type,
        "field_name": item.field_name,
        "source_module": item.source_module,
        "is_sensitive_change": item.is_sensitive_change,
        "sensitive_category": item.sensitive_category,
        "route": item.route,
        "method": item.method,
        "status_code": item.status_code,
        "request_id": item.request_id,
        "before_json": item.before_json,
        "after_json": item.after_json,
        "metadata_json": item.metadata_json,
        "created_at": item.created_at,
    }
    return AuditLogOut.model_validate(payload)


def _scalar_display(value):
    if isinstance(value, dict):
        return value.get("label") or value.get("value") or value
    return value


def _history_row_to_out(row) -> AuditHistoryEventOut:
    item: AuditLog = row.AuditLog
    table_id = row.table_id if hasattr(row, "table_id") else None
    return AuditHistoryEventOut(
        id=item.id,
        change_set_id=item.change_set_id,
        changed_at=item.created_at,
        actor_user_id=item.user_id,
        actor_name=item.actor_name,
        actor_email=item.user_email,
        action=item.action,
        change_type=item.change_type,
        field_name=item.field_name,
        entity_type=item.entity_type,
        entity_id=item.entity_id,
        parent_entity_type=item.parent_entity_type,
        parent_entity_id=item.parent_entity_id,
        source_module=item.source_module,
        is_sensitive_change=item.is_sensitive_change,
        sensitive_category=item.sensitive_category,
        before_value=_scalar_display(item.before_json),
        after_value=_scalar_display(item.after_json),
        metadata_json=item.metadata_json,
        route=item.route,
        method=item.method,
        status_code=item.status_code,
        table_id=int(table_id) if table_id is not None else None,
        table_name=getattr(row, "table_name", None),
        schema_name=getattr(row, "schema_name", None),
        database_name=getattr(row, "database_name", None),
        datasource_name=getattr(row, "datasource_name", None),
    )


def _mask_audit_event(event: AuditHistoryEventOut) -> AuditHistoryEventOut:
    return AuditHistoryEventOut(**mask_audit_event_payload(event.model_dump()))


def _build_audit_filters(
    *,
    date_from: datetime | None,
    date_to: datetime | None,
    user_id: int | None,
    user_email: str | None,
    action: str | None,
    entity_type: str | None,
    entity_id: str | None,
    q: str | None,
) -> list:
    filters = [AuditLog.action != "http_request"]
    if date_from is not None:
        filters.append(AuditLog.created_at >= date_from)
    if date_to is not None:
        filters.append(AuditLog.created_at <= date_to)
    if user_id is not None:
        filters.append(AuditLog.user_id == user_id)
    if user_email:
        filters.append(AuditLog.user_email.ilike(f"%{user_email.strip()}%"))
    if action:
        filters.append(AuditLog.action.ilike(f"%{action.strip()}%"))
    if entity_type:
        filters.append(AuditLog.entity_type.ilike(f"%{entity_type.strip()}%"))
    if entity_id:
        filters.append(AuditLog.entity_id.ilike(f"%{entity_id.strip()}%"))
    if q:
        q_like = f"%{q.strip()}%"
        filters.append(
            or_(
                AuditLog.action.ilike(q_like),
                AuditLog.user_email.ilike(q_like),
                AuditLog.entity_type.ilike(q_like),
                AuditLog.entity_id.ilike(q_like),
                AuditLog.route.ilike(q_like),
                AuditLog.method.ilike(q_like),
                AuditLog.request_id.ilike(q_like),
            )
        )
    return filters


def _history_base_query():
    table_id_expr = func.coalesce(
        cast(
            func.nullif(
                case(
                    (AuditLog.entity_type == "table", AuditLog.entity_id),
                    else_=None,
                ),
                "",
            ),
            Integer,
        ),
        cast(
            func.nullif(
                case(
                    (AuditLog.parent_entity_type == "table", AuditLog.parent_entity_id),
                    else_=None,
                ),
                "",
            ),
            Integer,
        ),
    )
    stmt = (
        select(
            AuditLog,
            table_id_expr.label("table_id"),
            TableEntity.name.label("table_name"),
            Schema.name.label("schema_name"),
            Database.name.label("database_name"),
            DataSource.name.label("datasource_name"),
        )
        .outerjoin(TableEntity, TableEntity.id == table_id_expr)
        .outerjoin(Schema, Schema.id == TableEntity.schema_id)
        .outerjoin(Database, Database.id == Schema.database_id)
        .outerjoin(DataSource, DataSource.id == Database.datasource_id)
    )
    return stmt, table_id_expr


@router.get("/logs", response_model=AuditLogPageOut)
def list_audit_logs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    user_id: int | None = Query(default=None),
    user_email: str | None = Query(default=None),
    action: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> AuditLogPageOut:
    filters = _build_audit_filters(
        date_from=date_from,
        date_to=date_to,
        user_id=user_id,
        user_email=user_email,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        q=q,
    )

    stmt = select(AuditLog)
    count_stmt = select(func.count()).select_from(AuditLog)
    if filters:
        where_clause = and_(*filters)
        stmt = stmt.where(where_clause)
        count_stmt = count_stmt.where(where_clause)

    total = int(db.scalar(count_stmt) or 0)
    items = db.scalars(
        stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    return AuditLogPageOut(items=[_serialize_audit_item(item) for item in items], total=total, page=page, page_size=page_size)


@router.get("/logs/latest", response_model=list[AuditLogOut])
def list_latest_audit_logs(
    limit: int = Query(default=10, ge=1, le=50),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    user_id: int | None = Query(default=None),
    user_email: str | None = Query(default=None),
    action: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> list[AuditLogOut]:
    filters = _build_audit_filters(
        date_from=date_from,
        date_to=date_to,
        user_id=user_id,
        user_email=user_email,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        q=q,
    )
    stmt = select(AuditLog)
    if filters:
        stmt = stmt.where(and_(*filters))
    items = db.scalars(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).limit(limit)).all()
    return [_serialize_audit_item(item) for item in items]


@router.get("/history/options", response_model=AuditHistoryFilterOptionsOut)
def audit_history_filter_options(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> AuditHistoryFilterOptionsOut:
    def values(column, *, kind: str):
        return [
            option
            for value in db.scalars(
                select(column)
                .where(column.is_not(None), AuditLog.action != "http_request")
                .distinct()
                .order_by(column)
            ).all()
            if (option := _audit_select_option(value, kind=kind)) is not None
        ]

    users = [
        option
        for value in db.scalars(
            select(AuditLog.user_email).where(AuditLog.user_email.is_not(None), AuditLog.action != "http_request").distinct()
        ).all()
        if (option := _audit_select_option(value, kind="user")) is not None
    ]
    return AuditHistoryFilterOptionsOut(
        entity_types=values(AuditLog.entity_type, kind="entity_type"),
        change_types=values(AuditLog.change_type, kind="change_type"),
        field_names=values(AuditLog.field_name, kind="field_name"),
        source_modules=values(AuditLog.source_module, kind="source_module"),
        users=sorted(users, key=lambda item: item["label"]),
    )


def _apply_history_filters(
    stmt,
    *,
    date_from: datetime | None,
    date_to: datetime | None,
    actor: str | None,
    entity_type: str | None,
    entity_id: str | None,
    parent_entity_type: str | None,
    parent_entity_id: str | None,
    change_type: str | None,
    field_name: str | None,
    source_module: str | None,
    sensitive_only: bool,
    q: str | None,
    datasource: str | None,
    database: str | None,
    schema: str | None,
):
    stmt = stmt.where(AuditLog.action != "http_request")
    if date_from is not None:
        stmt = stmt.where(AuditLog.created_at >= date_from)
    if date_to is not None:
        stmt = stmt.where(AuditLog.created_at <= date_to)
    if actor:
        pattern = f"%{actor.strip()}%"
        stmt = stmt.where(or_(AuditLog.user_email.ilike(pattern), AuditLog.actor_name.ilike(pattern)))
    if entity_type:
        stmt = stmt.where(AuditLog.entity_type == entity_type)
    if entity_id:
        stmt = stmt.where(AuditLog.entity_id == entity_id)
    if parent_entity_type:
        stmt = stmt.where(AuditLog.parent_entity_type == parent_entity_type)
    if parent_entity_id:
        stmt = stmt.where(AuditLog.parent_entity_id == parent_entity_id)
    if change_type:
        stmt = stmt.where(AuditLog.change_type == change_type)
    if field_name:
        stmt = stmt.where(AuditLog.field_name == field_name)
    if source_module:
        stmt = stmt.where(AuditLog.source_module == source_module)
    if sensitive_only:
        stmt = stmt.where(AuditLog.is_sensitive_change.is_(True))
    if datasource:
        stmt = stmt.where(DataSource.name == datasource)
    if database:
        stmt = stmt.where(Database.name == database)
    if schema:
        stmt = stmt.where(Schema.name == schema)
    if q:
        pattern = f"%{q.strip()}%"
        stmt = stmt.where(
            or_(
                AuditLog.action.ilike(pattern),
                AuditLog.actor_name.ilike(pattern),
                AuditLog.user_email.ilike(pattern),
                AuditLog.entity_type.ilike(pattern),
                AuditLog.field_name.ilike(pattern),
                AuditLog.change_type.ilike(pattern),
                TableEntity.name.ilike(pattern),
                Schema.name.ilike(pattern),
                Database.name.ilike(pattern),
                DataSource.name.ilike(pattern),
            )
        )
    return stmt


def _history_query(
    *,
    db: Session,
    date_from: datetime | None,
    date_to: datetime | None,
    actor: str | None,
    entity_type: str | None,
    entity_id: str | None,
    parent_entity_type: str | None,
    parent_entity_id: str | None,
    change_type: str | None,
    field_name: str | None,
    source_module: str | None,
    sensitive_only: bool,
    datasource: str | None,
    database: str | None,
    schema: str | None,
    q: str | None,
):
    stmt, _ = _history_base_query()
    stmt = _apply_history_filters(
        stmt,
        date_from=date_from,
        date_to=date_to,
        actor=actor,
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        change_type=change_type,
        field_name=field_name,
        source_module=source_module,
        sensitive_only=sensitive_only,
        datasource=datasource,
        database=database,
        schema=schema,
        q=q,
    )
    return stmt


def _history_export_rows(rows) -> list[AuditHistoryExportRowOut]:
    items: list[AuditHistoryExportRowOut] = []
    for row in rows:
        event = _history_row_to_out(row)
        items.append(
            AuditHistoryExportRowOut(
                changed_at=event.changed_at,
                actor_name=event.actor_name,
                actor_email=event.actor_email,
                entity_type=event.entity_type,
                entity_id=event.entity_id,
                table_name=event.table_name,
                schema_name=event.schema_name,
                database_name=event.database_name,
                datasource_name=event.datasource_name,
                field_name=event.field_name,
                change_type=event.change_type,
                source_module=event.source_module,
                change_set_id=event.change_set_id,
                is_sensitive_change=event.is_sensitive_change,
                sensitive_category=event.sensitive_category,
                before_value=event.before_value,
                after_value=event.after_value,
                metadata_json=event.metadata_json,
            )
        )
    return items


def _stringify_export_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _audit_select_option(value: str | None, *, kind: str) -> dict[str, str] | None:
    if value in ("", None):
        return None
    labelers = {
        "entity_type": _label_entity_type,
        "change_type": _label_change_type,
        "field_name": _label_field_name,
        "source_module": _label_source_module,
    }
    labeler = labelers.get(kind, lambda item: item or "")
    return {"value": str(value), "label": labeler(str(value))}


@router.get("/history", response_model=AuditHistoryPageOut)
def list_audit_history(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=200),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    actor: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    parent_entity_type: str | None = Query(default=None),
    parent_entity_id: str | None = Query(default=None),
    change_type: str | None = Query(default=None),
    field_name: str | None = Query(default=None),
    source_module: str | None = Query(default=None),
    sensitive_only: bool = Query(default=False),
    datasource: str | None = Query(default=None),
    database: str | None = Query(default=None),
    schema: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> AuditHistoryPageOut:
    stmt = _history_query(
        db=db,
        date_from=date_from,
        date_to=date_to,
        actor=actor,
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        change_type=change_type,
        field_name=field_name,
        source_module=source_module,
        sensitive_only=sensitive_only,
        datasource=datasource,
        database=database,
        schema=schema,
        q=q,
    )
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = int(db.scalar(count_stmt) or 0)
    rows = db.execute(
        stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset((page - 1) * page_size).limit(page_size)
    ).all()
    return AuditHistoryPageOut(items=[_history_row_to_out(row) for row in rows], total=total, page=page, page_size=page_size)


@router.get("/history/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_audit_history_csv(
    request: Request,
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    actor: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    parent_entity_type: str | None = Query(default=None),
    parent_entity_id: str | None = Query(default=None),
    change_type: str | None = Query(default=None),
    field_name: str | None = Query(default=None),
    source_module: str | None = Query(default=None),
    sensitive_only: bool = Query(default=False),
    datasource: str | None = Query(default=None),
    database: str | None = Query(default=None),
    schema: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("audit:export")),
) -> StreamingResponse:
    job = enqueue_export_job(
        db,
        job_type="audit.history.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "actor": actor,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "parent_entity_type": parent_entity_type,
            "parent_entity_id": parent_entity_id,
            "change_type": change_type,
            "field_name": field_name,
            "source_module": source_module,
            "sensitive_only": sensitive_only,
            "datasource": datasource,
            "database": database,
            "schema": schema,
            "q": q,
            "export_format": "csv",
        },
        context_json={
            "filters": {
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None,
                "actor": actor,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "parent_entity_type": parent_entity_type,
                "parent_entity_id": parent_entity_id,
                "change_type": change_type,
                "field_name": field_name,
                "source_module": source_module,
                "sensitive_only": sensitive_only,
                "datasource": datasource,
                "database": database,
                "schema": schema,
                "q": q,
            }
        },
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="audit.history.export_requested",
        entity_type="audit_history",
        source_module="audit",
        export_format="csv",
        filters={
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "actor": actor,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "parent_entity_type": parent_entity_type,
            "parent_entity_id": parent_entity_id,
            "change_type": change_type,
            "field_name": field_name,
            "source_module": source_module,
            "sensitive_only": sensitive_only,
            "datasource": datasource,
            "database": database,
            "schema": schema,
            "q": q,
        },
    )
    return serialize_export_job(job, request=request)


@router.get("/history/export.xlsx", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def export_audit_history_excel(
    request: Request,
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    actor: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    entity_id: str | None = Query(default=None),
    parent_entity_type: str | None = Query(default=None),
    parent_entity_id: str | None = Query(default=None),
    change_type: str | None = Query(default=None),
    field_name: str | None = Query(default=None),
    source_module: str | None = Query(default=None),
    sensitive_only: bool = Query(default=False),
    datasource: str | None = Query(default=None),
    database: str | None = Query(default=None),
    schema: str | None = Query(default=None),
    q: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("audit:export")),
) -> StreamingResponse:
    job = enqueue_export_job(
        db,
        job_type="audit.history.xlsx",
        requested_by_user_id=current_user.id,
        payload_json={
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "actor": actor,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "parent_entity_type": parent_entity_type,
            "parent_entity_id": parent_entity_id,
            "change_type": change_type,
            "field_name": field_name,
            "source_module": source_module,
            "sensitive_only": sensitive_only,
            "datasource": datasource,
            "database": database,
            "schema": schema,
            "q": q,
            "export_format": "xlsx",
        },
        context_json={
            "filters": {
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None,
                "actor": actor,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "parent_entity_type": parent_entity_type,
                "parent_entity_id": parent_entity_id,
                "change_type": change_type,
                "field_name": field_name,
                "source_module": source_module,
                "sensitive_only": sensitive_only,
                "datasource": datasource,
                "database": database,
                "schema": schema,
                "q": q,
            }
        },
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="audit.history.export_requested",
        entity_type="audit_history",
        source_module="audit",
        export_format="xlsx",
        filters={
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "actor": actor,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "parent_entity_type": parent_entity_type,
            "parent_entity_id": parent_entity_id,
            "change_type": change_type,
            "field_name": field_name,
            "source_module": source_module,
            "sensitive_only": sensitive_only,
            "datasource": datasource,
            "database": database,
            "schema": schema,
            "q": q,
        },
    )
    return serialize_export_job(job, request=request)
    try:
        from openpyxl import Workbook
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="openpyxl não está instalado.") from exc

    stmt = _history_query(
        db=db,
        date_from=date_from,
        date_to=date_to,
        actor=actor,
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        change_type=change_type,
        field_name=field_name,
        source_module=source_module,
        sensitive_only=sensitive_only,
        datasource=datasource,
        database=database,
        schema=schema,
        q=q,
    )
    export_limit = resolve_export_limit(source_module="audit", entity_type="audit_history")
    rows = _history_export_rows(db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc())).all())
    rows, truncated = enforce_export_limit(rows, limit=export_limit)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="audit.history.export_xlsx",
        entity_type="audit_history",
        source_module="audit",
        row_count=len(rows),
        truncated=truncated,
        limit=export_limit,
        filters={
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "actor": actor,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "parent_entity_type": parent_entity_type,
            "parent_entity_id": parent_entity_id,
            "change_type": change_type,
            "field_name": field_name,
            "source_module": source_module,
            "sensitive_only": sensitive_only,
            "datasource": datasource,
            "database": database,
            "schema": schema,
            "q": q,
        },
    )
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Auditoria"
    sheet.append(
        [
            "Data/hora",
            "Usuário",
            "Email",
            "Tipo entidade",
            "Tipo entidade técnico",
            "Entidade ID",
            "Fonte",
            "Banco",
            "Schema",
            "Tabela",
            "Campo",
            "Campo técnico",
            "Tipo mudança",
            "Tipo mudança técnico",
            "Módulo",
            "Módulo técnico",
            "Change set",
            "Sensível",
            "Categoria sensível",
            "Antes",
            "Depois",
            "Contexto adicional",
        ]
    )
    for item in rows:
        sheet.append(
            [
                item.changed_at.isoformat(),
                neutralize_spreadsheet_formula(item.actor_name or ""),
                neutralize_spreadsheet_formula(item.actor_email or ""),
                _label_entity_type(item.entity_type),
                neutralize_spreadsheet_formula(item.entity_type or ""),
                neutralize_spreadsheet_formula(item.entity_id or ""),
                neutralize_spreadsheet_formula(item.datasource_name or ""),
                neutralize_spreadsheet_formula(item.database_name or ""),
                neutralize_spreadsheet_formula(item.schema_name or ""),
                neutralize_spreadsheet_formula(item.table_name or ""),
                _label_field_name(item.field_name),
                neutralize_spreadsheet_formula(item.field_name or ""),
                _label_change_type(item.change_type),
                neutralize_spreadsheet_formula(item.change_type or ""),
                _label_source_module(item.source_module),
                neutralize_spreadsheet_formula(item.source_module or ""),
                neutralize_spreadsheet_formula(item.change_set_id or ""),
                "Sim" if item.is_sensitive_change else "Não",
                neutralize_spreadsheet_formula(item.sensitive_category or ""),
                redact_export_value(item.before_value, field_name=item.field_name),
                redact_export_value(item.after_value, field_name=item.field_name),
                redact_export_value(item.metadata_json, field_name="metadata"),
            ]
        )
    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="auditoria.xlsx"'},
    )


@router.get("/history/entity/{entity_type}/{entity_id}", response_model=AuditHistoryPageOut)
def list_entity_history(
    entity_type: str,
    entity_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AuditHistoryPageOut:
    table = None
    if entity_type == "table" and entity_id.isdigit():
        table = db.get(TableEntity, int(entity_id))
        if table and not can_view_table(current_user, table):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    stmt, _ = _history_base_query()
    stmt = stmt.where(AuditLog.entity_type == entity_type, AuditLog.entity_id == entity_id)
    total = int(db.scalar(select(func.count()).select_from(stmt.subquery())) or 0)
    rows = db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset((page - 1) * page_size).limit(page_size)).all()
    items = [_history_row_to_out(row) for row in rows]
    if table and table_visibility_decision_from_entity(table, user=current_user).masked:
        items = [_mask_audit_event(item) for item in items]
    return AuditHistoryPageOut(items=items, total=total, page=page, page_size=page_size)


@router.get("/history/table/{table_id}", response_model=AuditHistoryPageOut)
def list_table_history(
    table_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AuditHistoryPageOut:
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    stmt, table_id_expr = _history_base_query()
    stmt = stmt.where(table_id_expr == table_id)
    total = int(db.scalar(select(func.count()).select_from(stmt.subquery())) or 0)
    rows = db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset((page - 1) * page_size).limit(page_size)).all()
    items = [_history_row_to_out(row) for row in rows]
    if table_visibility_decision_from_entity(table, user=current_user).masked:
        items = [_mask_audit_event(item) for item in items]
    return AuditHistoryPageOut(items=items, total=total, page=page, page_size=page_size)


@router.get("/history/column/{column_id}", response_model=AuditHistoryPageOut)
def list_column_history(
    column_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AuditHistoryPageOut:
    stmt, _ = _history_base_query()
    stmt = stmt.where(AuditLog.entity_type == "column", AuditLog.entity_id == str(column_id))
    total = int(db.scalar(select(func.count()).select_from(stmt.subquery())) or 0)
    rows = db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset((page - 1) * page_size).limit(page_size)).all()
    items = [_history_row_to_out(row) for row in rows]
    visible_items = items
    if items and items[0].table_id is not None:
        table = db.get(TableEntity, items[0].table_id)
        if table and not can_view_table(current_user, table):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
        if table and table_visibility_decision_from_entity(table, user=current_user).masked:
            visible_items = [_mask_audit_event(item) for item in items]
    return AuditHistoryPageOut(items=visible_items, total=total, page=page, page_size=page_size)


@router.get("/history/owner-changes", response_model=AuditHistoryPageOut)
def list_owner_changes(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> AuditHistoryPageOut:
    stmt, _ = _history_base_query()
    stmt = stmt.where(AuditLog.field_name == "owner")
    total = int(db.scalar(select(func.count()).select_from(stmt.subquery())) or 0)
    rows = db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset((page - 1) * page_size).limit(page_size)).all()
    return AuditHistoryPageOut(items=[_history_row_to_out(row) for row in rows], total=total, page=page, page_size=page_size)


@router.get("/history/certification-changes", response_model=AuditHistoryPageOut)
def list_certification_changes(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> AuditHistoryPageOut:
    stmt, _ = _history_base_query()
    stmt = stmt.where(or_(AuditLog.action == "table.certification.patch", AuditLog.field_name.like("certification_%")))
    total = int(db.scalar(select(func.count()).select_from(stmt.subquery())) or 0)
    rows = db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset((page - 1) * page_size).limit(page_size)).all()
    return AuditHistoryPageOut(items=[_history_row_to_out(row) for row in rows], total=total, page=page, page_size=page_size)


@router.get("/history/classification-changes", response_model=AuditHistoryPageOut)
def list_classification_changes(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> AuditHistoryPageOut:
    stmt, _ = _history_base_query()
    stmt = stmt.where(AuditLog.field_name == "classification")
    total = int(db.scalar(select(func.count()).select_from(stmt.subquery())) or 0)
    rows = db.execute(stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset((page - 1) * page_size).limit(page_size)).all()
    return AuditHistoryPageOut(items=[_history_row_to_out(row) for row in rows], total=total, page=page, page_size=page_size)
