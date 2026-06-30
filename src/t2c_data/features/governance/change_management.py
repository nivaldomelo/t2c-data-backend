from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.catalog.tree_queries import TreeTableColumnsOut
from t2c_data.features.catalog.tree_queries import TreeTableColumnsPageOut
from t2c_data.models.catalog import ColumnEntity, DataOwner, Database, Schema, TableEntity
from t2c_data.models.governance import AssetSla, MetadataChangeRequest, MetadataChangeRequestEvent
from t2c_data.services.audit import AuditFieldChange, log_field_changes, write_audit_log_sync

_ASSET_TYPES = {"table", "column"}
_CHANGE_REQUEST_STATUS_LABELS = {
    "draft": "Rascunho",
    "review": "Em revisão",
    "approved": "Aprovada",
    "applied": "Aplicada",
    "rejected": "Rejeitada",
}
_CHANGE_REQUEST_EVENT_LABELS = {
    "created": "Criada",
    "reviewed": "Revisada",
    "approved": "Aprovada",
    "applied": "Aplicada",
    "rejected": "Rejeitada",
    "apply_failed": "Falha ao aplicar",
}
_CLASSIFICATION_FIELDS = {
    "sensitivity_level",
    "has_personal_data",
    "has_sensitive_personal_data",
    "legal_basis",
    "privacy_purpose",
    "retention_policy",
    "is_masked",
    "external_sharing",
    "access_scope",
    "access_roles",
    "privacy_notes",
}
_TABLE_DESCRIPTION_FIELDS = {"description_manual", "description_source"}
_COLUMN_DESCRIPTION_FIELDS = {"description_manual", "description_source", "dictionary_description", "dictionary_comment"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_asset_type(asset_type: str) -> str:
    normalized = (asset_type or "").strip().lower()
    if normalized not in _ASSET_TYPES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported asset_type")
    return normalized


def _normalize_asset_id(asset_id: int) -> int:
    try:
        normalized = int(asset_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid asset_id") from exc
    if normalized <= 0:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid asset_id")
    return normalized


def _display_user(user: Any | None) -> dict[str, object] | None:
    if user is None:
        return None
    return {
        "id": getattr(user, "id", None),
        "name": getattr(user, "name", None) or getattr(user, "full_name", None),
        "email": getattr(user, "email", None),
        "display_name": getattr(user, "name", None) or getattr(user, "full_name", None) or getattr(user, "email", None),
        "is_active": getattr(user, "is_active", None),
    }


def _asset_context(session: Session, *, asset_type: str, asset_id: int) -> dict[str, object]:
    normalized_asset_type = _normalize_asset_type(asset_type)
    normalized_asset_id = _normalize_asset_id(asset_id)

    if normalized_asset_type == "table":
        table = session.scalar(
            select(TableEntity)
            .options(
                selectinload(TableEntity.data_owner),
                selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
            )
            .where(TableEntity.id == normalized_asset_id)
        )
        if table is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
        asset_name = table.name
        asset_fqn = f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}"
        links = build_asset_links(
            table_id=table.id,
            datasource_id=table.schema.database.datasource.id,
            database_id=table.schema.database.id,
            schema_id=table.schema.id,
            data_owner_id=table.data_owner_id,
        )
        return {
            "asset_type": normalized_asset_type,
            "asset_id": normalized_asset_id,
            "table": table,
            "column": None,
            "table_id": table.id,
            "column_id": None,
            "asset_name": asset_name,
            "asset_fqn": asset_fqn,
            "links": links,
        }

    column = session.scalar(
        select(ColumnEntity)
        .options(
            selectinload(ColumnEntity.data_owner),
            selectinload(ColumnEntity.owner_reviewed_by_user),
            selectinload(ColumnEntity.table)
            .selectinload(TableEntity.data_owner),
            selectinload(ColumnEntity.table)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
        )
        .where(ColumnEntity.id == normalized_asset_id)
    )
    if column is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
    table = column.table
    asset_name = column.name
    asset_fqn = (
        f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}.{column.name}"
    )
    links = build_asset_links(
        table_id=table.id,
        datasource_id=table.schema.database.datasource.id,
        database_id=table.schema.database.id,
        schema_id=table.schema.id,
        data_owner_id=table.data_owner_id,
        column_id=column.id,
    )
    return {
        "asset_type": normalized_asset_type,
        "asset_id": normalized_asset_id,
        "table": table,
        "column": column,
        "table_id": table.id,
        "column_id": column.id,
        "asset_name": asset_name,
        "asset_fqn": asset_fqn,
        "links": links,
    }


def _serialize_asset_sla(row: AssetSla) -> dict[str, object]:
    return {
        "id": row.id,
        "asset_type": row.asset_type,
        "asset_id": row.asset_id,
        "sla_kind": row.sla_kind,
        "sla_hours": row.sla_hours,
        "status": row.status,
        "source_kind": row.source_kind,
        "source_ref": row.source_ref,
        "context_json": dict(row.context_json or {}),
        "table_id": row.table_id,
        "column_id": row.column_id,
        "asset_name": row.asset_name,
        "asset_fqn": row.asset_fqn,
        "reviewed_by_user_id": row.reviewed_by_user_id,
        "reviewed_by_user": _display_user(row.reviewed_by_user),
        "reviewed_at": row.reviewed_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _serialize_change_request_event(row: MetadataChangeRequestEvent) -> dict[str, object]:
    return {
        "id": row.id,
        "metadata_change_request_id": row.metadata_change_request_id,
        "event_type": row.event_type,
        "previous_status": row.previous_status,
        "next_status": row.next_status,
        "actor_user_id": row.actor_user_id,
        "actor_user": _display_user(row.actor_user),
        "comment": row.comment,
        "payload_json": dict(row.payload_json or {}),
        "created_at": row.created_at,
    }


def _request_status_label(status_value: str) -> str:
    return _CHANGE_REQUEST_STATUS_LABELS.get(status_value, status_value.replace("_", " ").title())


def _request_event_type(next_status: str | None, *, is_created: bool = False) -> str:
    if is_created:
        return "created"
    if next_status == "review":
        return "reviewed"
    if next_status in {"approved", "applied", "rejected"}:
        return next_status
    return "updated"


def _request_allowed_transitions(status_value: str) -> set[str]:
    transitions = {
        "draft": {"review", "rejected"},
        "review": {"approved", "rejected"},
        "approved": {"applied", "rejected"},
        "applied": set(),
        "rejected": set(),
    }
    return transitions.get(status_value, set())


def _serialize_change_request(row: MetadataChangeRequest) -> dict[str, object]:
    links = None
    if row.table_id is not None:
        try:
            links = _asset_context(
                row._sa_instance_state.session,  # type: ignore[attr-defined]
                asset_type=row.asset_type,
                asset_id=row.asset_id,
            )["links"]
        except Exception:
            links = None
    return {
        "id": row.id,
        "request_key": row.request_key,
        "asset_type": row.asset_type,
        "asset_id": row.asset_id,
        "table_id": row.table_id,
        "column_id": row.column_id,
        "asset_name": row.asset_name,
        "asset_fqn": row.asset_fqn,
        "change_kind": row.change_kind,
        "status": row.status,
        "status_label": _request_status_label(row.status),
        "title": row.title,
        "description": row.description,
        "requested_by_user_id": row.requested_by_user_id,
        "requested_by_user": _display_user(row.requested_by_user),
        "reviewed_by_user_id": row.reviewed_by_user_id,
        "reviewed_by_user": _display_user(row.reviewed_by_user),
        "approved_by_user_id": row.approved_by_user_id,
        "approved_by_user": _display_user(row.approved_by_user),
        "applied_by_user_id": row.applied_by_user_id,
        "applied_by_user": _display_user(row.applied_by_user),
        "rejected_by_user_id": row.rejected_by_user_id,
        "rejected_by_user": _display_user(row.rejected_by_user),
        "reviewed_at": row.reviewed_at,
        "approved_at": row.approved_at,
        "applied_at": row.applied_at,
        "rejected_at": row.rejected_at,
        "policy_rule_key": row.policy_rule_key,
        "recommendation_id": row.recommendation_id,
        "current_value_json": dict(row.current_value_json or {}),
        "proposed_value_json": dict(row.proposed_value_json or {}),
        "context_json": dict(row.context_json or {}),
        "apply_error": row.apply_error,
        "can_review": row.status == "draft",
        "can_approve": row.status == "review",
        "can_apply": row.status == "approved",
        "can_reject": row.status in {"draft", "review", "approved"},
        "links": links,
        "events": [_serialize_change_request_event(event) for event in row.events],
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _load_change_request(session: Session, request_ref: str) -> MetadataChangeRequest:
    normalized_ref = str(request_ref or "").strip()
    if not normalized_ref:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Change request not found")
    stmt = (
        select(MetadataChangeRequest)
        .options(
            selectinload(MetadataChangeRequest.requested_by_user),
            selectinload(MetadataChangeRequest.reviewed_by_user),
            selectinload(MetadataChangeRequest.approved_by_user),
            selectinload(MetadataChangeRequest.applied_by_user),
            selectinload(MetadataChangeRequest.rejected_by_user),
            selectinload(MetadataChangeRequest.recommendation),
            selectinload(MetadataChangeRequest.table)
            .selectinload(TableEntity.data_owner),
            selectinload(MetadataChangeRequest.table)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
            selectinload(MetadataChangeRequest.column)
            .selectinload(ColumnEntity.data_owner),
            selectinload(MetadataChangeRequest.column)
            .selectinload(ColumnEntity.table)
            .selectinload(TableEntity.data_owner),
            selectinload(MetadataChangeRequest.column)
            .selectinload(ColumnEntity.table)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
            selectinload(MetadataChangeRequest.events).selectinload(MetadataChangeRequestEvent.actor_user),
        )
    )
    row = None
    if normalized_ref.isdigit():
        row = session.get(MetadataChangeRequest, int(normalized_ref))
    if row is None:
        row = session.scalar(stmt.where(MetadataChangeRequest.request_key == normalized_ref))
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Change request not found")
    return row


def list_asset_slas(session: Session, *, asset_type: str, asset_id: int) -> dict[str, object]:
    ctx = _asset_context(session, asset_type=asset_type, asset_id=asset_id)
    rows = session.scalars(
        select(AssetSla)
        .options(selectinload(AssetSla.reviewed_by_user))
        .where(
            AssetSla.asset_type == ctx["asset_type"],
            AssetSla.asset_id == ctx["asset_id"],
        )
        .order_by(AssetSla.updated_at.desc(), AssetSla.id.desc())
    ).all()
    items = [_serialize_asset_sla(row) for row in rows]
    return {
        "generated_at": _now().isoformat(),
        "asset_type": ctx["asset_type"],
        "asset_id": ctx["asset_id"],
        "asset_name": ctx["asset_name"],
        "asset_fqn": ctx["asset_fqn"],
        "total": len(items),
        "items": items,
    }


def upsert_asset_sla(
    session: Session,
    *,
    asset_type: str,
    asset_id: int,
    sla_kind: str,
    sla_hours: int,
    asset_status: str = "active",
    source_kind: str = "manual",
    source_ref: str | None = None,
    context_json: dict[str, object] | None = None,
    actor_user_id: int | None = None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    ctx = _asset_context(session, asset_type=asset_type, asset_id=asset_id)
    normalized_sla_kind = (sla_kind or "").strip().lower()
    if not normalized_sla_kind:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="sla_kind is required")

    row = session.scalar(
        select(AssetSla)
        .options(selectinload(AssetSla.reviewed_by_user))
        .where(
            AssetSla.asset_type == ctx["asset_type"],
            AssetSla.asset_id == ctx["asset_id"],
            AssetSla.sla_kind == normalized_sla_kind,
        )
    )
    now = _now()
    before_payload = None
    if row is None:
        row = AssetSla(
            asset_type=ctx["asset_type"],
            asset_id=ctx["asset_id"],
            table_id=ctx["table_id"],
            column_id=ctx["column_id"],
            asset_name=ctx["asset_name"],
            asset_fqn=ctx["asset_fqn"],
            sla_kind=normalized_sla_kind,
            sla_hours=int(sla_hours),
            status=(asset_status or "active").strip().lower() or "active",
            source_kind=(source_kind or "manual").strip().lower() or "manual",
            source_ref=source_ref,
            context_json=context_json or {},
            reviewed_by_user_id=actor_user_id,
            reviewed_at=now,
        )
        session.add(row)
    else:
        before_payload = _serialize_asset_sla(row)
        row.table_id = ctx["table_id"]
        row.column_id = ctx["column_id"]
        row.asset_name = ctx["asset_name"]
        row.asset_fqn = ctx["asset_fqn"]
        row.sla_hours = int(sla_hours)
        row.status = (asset_status or row.status or "active").strip().lower() or "active"
        row.source_kind = (source_kind or row.source_kind or "manual").strip().lower() or "manual"
        row.source_ref = source_ref
        row.context_json = context_json or {}
        row.reviewed_by_user_id = actor_user_id
        row.reviewed_at = now
    session.flush()
    after_payload = _serialize_asset_sla(row)
    write_audit_log_sync(
        session,
        action="governance.asset_sla.upsert",
        user_id=actor_user_id,
        entity_type="asset_sla",
        entity_id=row.id,
        source_module="governance.change_management",
        before=before_payload,
        after=after_payload,
        metadata={
            "asset_type": row.asset_type,
            "asset_id": row.asset_id,
            "sla_kind": row.sla_kind,
            "request_audit": request_audit or {},
        },
    )
    return after_payload


def list_metadata_change_requests(
    session: Session,
    *,
    asset_type: str | None = None,
    asset_id: int | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, object]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(min(int(page_size or 20), 100), 1)
    stmt = (
        select(MetadataChangeRequest)
        .options(
            selectinload(MetadataChangeRequest.requested_by_user),
            selectinload(MetadataChangeRequest.reviewed_by_user),
            selectinload(MetadataChangeRequest.approved_by_user),
            selectinload(MetadataChangeRequest.applied_by_user),
            selectinload(MetadataChangeRequest.rejected_by_user),
            selectinload(MetadataChangeRequest.recommendation),
            selectinload(MetadataChangeRequest.table)
            .selectinload(TableEntity.data_owner),
            selectinload(MetadataChangeRequest.table)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
            selectinload(MetadataChangeRequest.column)
            .selectinload(ColumnEntity.data_owner),
            selectinload(MetadataChangeRequest.column)
            .selectinload(ColumnEntity.owner_reviewed_by_user),
            selectinload(MetadataChangeRequest.column)
            .selectinload(ColumnEntity.table)
            .selectinload(TableEntity.data_owner),
            selectinload(MetadataChangeRequest.column)
            .selectinload(ColumnEntity.table)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
            selectinload(MetadataChangeRequest.events).selectinload(MetadataChangeRequestEvent.actor_user),
        )
        .order_by(MetadataChangeRequest.updated_at.desc(), MetadataChangeRequest.id.desc())
    )
    if asset_type:
        stmt = stmt.where(MetadataChangeRequest.asset_type == _normalize_asset_type(asset_type))
    if asset_id is not None:
        stmt = stmt.where(MetadataChangeRequest.asset_id == _normalize_asset_id(asset_id))
    if status:
        stmt = stmt.where(MetadataChangeRequest.status == str(status).strip().lower())

    total = int(
        session.scalar(
            select(func.count(MetadataChangeRequest.id))
            .where(*stmt._where_criteria)  # type: ignore[attr-defined]
        )
        or 0
    )
    rows = session.scalars(stmt.offset((normalized_page - 1) * normalized_page_size).limit(normalized_page_size)).all()
    items = [_serialize_change_request(row) for row in rows]
    return {
        "generated_at": _now().isoformat(),
        "total": total,
        "page": normalized_page,
        "page_size": normalized_page_size,
        "items": items,
    }


def create_metadata_change_request(
    session: Session,
    *,
    asset_type: str,
    asset_id: int,
    change_kind: str,
    title: str,
    description: str | None = None,
    policy_rule_key: str | None = None,
    recommendation_id: int | None = None,
    current_value_json: dict[str, object] | None = None,
    proposed_value_json: dict[str, object] | None = None,
    context_json: dict[str, object] | None = None,
    actor_user_id: int | None = None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    ctx = _asset_context(session, asset_type=asset_type, asset_id=asset_id)
    normalized_change_kind = (change_kind or "").strip().lower()
    if not normalized_change_kind:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="change_kind is required")
    normalized_title = (title or "").strip()
    if not normalized_title:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="title is required")
    row = MetadataChangeRequest(
        request_key=f"mcr-{uuid4().hex[:12]}",
        asset_type=ctx["asset_type"],
        asset_id=ctx["asset_id"],
        table_id=ctx["table_id"],
        column_id=ctx["column_id"],
        asset_name=ctx["asset_name"],
        asset_fqn=ctx["asset_fqn"],
        change_kind=normalized_change_kind,
        status="draft",
        title=normalized_title,
        description=(description or "").strip() or None,
        requested_by_user_id=actor_user_id,
        policy_rule_key=policy_rule_key,
        recommendation_id=recommendation_id,
        current_value_json=current_value_json or {},
        proposed_value_json=proposed_value_json or {},
        context_json=context_json or {},
    )
    session.add(row)
    session.flush()
    event = MetadataChangeRequestEvent(
        metadata_change_request_id=row.id,
        event_type="created",
        previous_status=None,
        next_status="draft",
        actor_user_id=actor_user_id,
        comment=description,
        payload_json={"request_audit": request_audit or {}},
    )
    session.add(event)
    write_audit_log_sync(
        session,
        action="governance.metadata_change_request.created",
        user_id=actor_user_id,
        entity_type="metadata_change_request",
        entity_id=row.id,
        source_module="governance.change_management",
        after=_serialize_change_request(row),
        metadata={"request_audit": request_audit or {}},
    )
    return _serialize_change_request(row)


def _apply_request_to_table(
    session: Session,
    *,
    request: MetadataChangeRequest,
    table: TableEntity,
    actor_user_id: int | None,
) -> list[AuditFieldChange]:
    proposed = dict(request.proposed_value_json or {})
    now = _now()
    changes: list[AuditFieldChange] = []

    if request.change_kind in {"owner_assignment", "owner_review", "owner"} or "data_owner_id" in proposed:
        owner_id = proposed.get("data_owner_id")
        before_owner_id = table.data_owner_id
        before_owner = table.owner
        before_owner_email = table.owner_email
        if owner_id in {None, ""}:
            table.data_owner_id = None
            table.data_owner = None
            table.owner = None
            table.owner_email = None
        else:
            owner = session.get(DataOwner, int(owner_id))
            if owner is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Data owner not found")
            table.data_owner = owner
            table.data_owner_id = owner.id
            table.owner = owner.name
            table.owner_email = owner.email
        table.owner_reviewed_by_user_id = actor_user_id
        table.owner_reviewed_at = now
        changes.extend(
            [
                AuditFieldChange(field_name="data_owner_id", before=before_owner_id, after=table.data_owner_id),
                AuditFieldChange(field_name="owner", before=before_owner, after=table.owner),
                AuditFieldChange(field_name="owner_email", before=before_owner_email, after=table.owner_email),
                AuditFieldChange(field_name="owner_reviewed_by_user_id", before=None, after=table.owner_reviewed_by_user_id),
                AuditFieldChange(field_name="owner_reviewed_at", before=None, after=table.owner_reviewed_at),
            ]
        )

    if request.change_kind in {"classification_update", "privacy_update", "classification"} or any(
        field in proposed for field in _CLASSIFICATION_FIELDS
    ):
        for field in _CLASSIFICATION_FIELDS:
            if field not in proposed:
                continue
            before_value = getattr(table, field)
            after_value = proposed[field]
            setattr(table, field, after_value)
            changes.append(AuditFieldChange(field_name=field, before=before_value, after=after_value))
        table.privacy_reviewed_by_user_id = actor_user_id
        table.privacy_reviewed_at = now
        changes.extend(
            [
                AuditFieldChange(field_name="privacy_reviewed_by_user_id", before=None, after=table.privacy_reviewed_by_user_id),
                AuditFieldChange(field_name="privacy_reviewed_at", before=None, after=table.privacy_reviewed_at),
            ]
        )

    if request.change_kind in {"description_update", "description"} or any(field in proposed for field in _TABLE_DESCRIPTION_FIELDS):
        for field in _TABLE_DESCRIPTION_FIELDS:
            if field not in proposed:
                continue
            before_value = getattr(table, field)
            after_value = proposed[field]
            setattr(table, field, after_value)
            changes.append(AuditFieldChange(field_name=field, before=before_value, after=after_value))

    if not changes:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported table change request")

    session.flush()
    return changes


def _apply_request_to_column(
    session: Session,
    *,
    request: MetadataChangeRequest,
    column: ColumnEntity,
    actor_user_id: int | None,
) -> list[AuditFieldChange]:
    proposed = dict(request.proposed_value_json or {})
    now = _now()
    changes: list[AuditFieldChange] = []

    if request.change_kind in {"owner_assignment", "owner_review", "owner"} or "data_owner_id" in proposed:
        owner_id = proposed.get("data_owner_id")
        before_owner_id = column.data_owner_id
        before_reviewed_by = column.owner_reviewed_by_user_id
        before_reviewed_at = column.owner_reviewed_at
        if owner_id in {None, ""}:
            column.data_owner_id = None
            column.data_owner = None
        else:
            owner = session.get(DataOwner, int(owner_id))
            if owner is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Data owner not found")
            column.data_owner = owner
            column.data_owner_id = owner.id
        column.owner_reviewed_by_user_id = actor_user_id
        column.owner_reviewed_at = now
        changes.extend(
            [
                AuditFieldChange(field_name="data_owner_id", before=before_owner_id, after=column.data_owner_id),
                AuditFieldChange(field_name="owner_reviewed_by_user_id", before=before_reviewed_by, after=column.owner_reviewed_by_user_id),
                AuditFieldChange(field_name="owner_reviewed_at", before=before_reviewed_at, after=column.owner_reviewed_at),
            ]
        )

    if request.change_kind in {"description_update", "description"} or any(field in proposed for field in _COLUMN_DESCRIPTION_FIELDS):
        for field in _COLUMN_DESCRIPTION_FIELDS:
            if field not in proposed:
                continue
            before_value = getattr(column, field)
            after_value = proposed[field]
            setattr(column, field, after_value)
            changes.append(AuditFieldChange(field_name=field, before=before_value, after=after_value))

    if not changes:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported column change request")

    session.flush()
    return changes


def _apply_change_request(
    session: Session,
    *,
    request: MetadataChangeRequest,
    actor_user_id: int | None,
    request_audit: dict[str, object] | None,
) -> dict[str, object]:
    ctx = _asset_context(session, asset_type=request.asset_type, asset_id=request.asset_id)
    now = _now()
    payload: dict[str, object] = {"request_audit": request_audit or {}}

    if request.change_kind in {"sla", "sla_update", "freshness_sla"}:
        proposed = dict(request.proposed_value_json or {})
        if "sla_hours" not in proposed and "sla_kind" not in proposed:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="SLA payload is missing")
        sla = upsert_asset_sla(
            session,
            asset_type=ctx["asset_type"],
            asset_id=ctx["asset_id"],
            sla_kind=str(proposed.get("sla_kind") or "freshness"),
            sla_hours=int(proposed.get("sla_hours") or 0),
            status=str(proposed.get("status") or "active"),
            source_kind=str(proposed.get("source_kind") or "manual"),
            source_ref=proposed.get("source_ref"),
            context_json=dict(proposed.get("context_json") or {}),
            actor_user_id=actor_user_id,
            request_audit=request_audit,
        )
        payload["applied_sla"] = sla
        return payload

    if ctx["asset_type"] == "table":
        table = ctx["table"]
        assert isinstance(table, TableEntity)
        changes = _apply_request_to_table(session, request=request, table=table, actor_user_id=actor_user_id)
        if changes:
            log_field_changes(
                session,
                action="governance.metadata_change_request.apply",
                entity_type="table",
                entity_id=table.id,
                changes=changes,
                source_module="governance.change_management",
                metadata={
                    "message": "Metadata change request applied",
                    "request_key": request.request_key,
                    "change_kind": request.change_kind,
                },
                audit_kwargs=request_audit,
                actor_user_id=actor_user_id,
            )
        payload["applied_changes"] = [change.field_name for change in changes]
        payload["table_id"] = table.id
        return payload

    column = ctx["column"]
    assert isinstance(column, ColumnEntity)
    changes = _apply_request_to_column(session, request=request, column=column, actor_user_id=actor_user_id)
    if changes:
        log_field_changes(
            session,
            action="governance.metadata_change_request.apply",
            entity_type="column",
            entity_id=column.id,
            changes=changes,
            source_module="governance.change_management",
            metadata={
                "message": "Metadata change request applied",
                "request_key": request.request_key,
                "change_kind": request.change_kind,
            },
            audit_kwargs=request_audit,
            actor_user_id=actor_user_id,
        )
    payload["applied_changes"] = [change.field_name for change in changes]
    payload["column_id"] = column.id
    return payload


def transition_metadata_change_request(
    session: Session,
    *,
    request_ref: str,
    transition: str,
    comment: str | None = None,
    actor_user_id: int | None = None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    row = _load_change_request(session, request_ref)
    current_status = (row.status or "draft").strip().lower()
    normalized_transition = (transition or "").strip().lower()
    if normalized_transition not in {"review", "approve", "apply", "reject"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported transition")

    next_status_map = {
        "review": "review",
        "approve": "approved",
        "apply": "applied",
        "reject": "rejected",
    }
    next_status = next_status_map[normalized_transition]

    if next_status not in _request_allowed_transitions(current_status) and not (
        normalized_transition == "reject" and current_status in {"draft", "review", "approved"}
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Transition '{normalized_transition}' not allowed from status '{current_status}'",
        )

    now = _now()
    event_type = _request_event_type(next_status, is_created=False)
    payload: dict[str, object] = {
        "transition": normalized_transition,
        "previous_status": current_status,
        "next_status": next_status,
        "comment": comment,
        "request_audit": request_audit or {},
    }

    if normalized_transition == "review":
        row.status = "review"
        row.reviewed_by_user_id = actor_user_id
        row.reviewed_at = now
    elif normalized_transition == "approve":
        row.status = "approved"
        row.approved_by_user_id = actor_user_id
        row.approved_at = now
    elif normalized_transition == "reject":
        row.status = "rejected"
        row.rejected_by_user_id = actor_user_id
        row.rejected_at = now
    elif normalized_transition == "apply":
        row.status = "approved"
        row.approved_by_user_id = row.approved_by_user_id or actor_user_id
        row.approved_at = row.approved_at or now
        try:
            payload.update(
                _apply_change_request(
                    session,
                    request=row,
                    actor_user_id=actor_user_id,
                    request_audit=request_audit,
                )
            )
            row.status = "applied"
            row.applied_by_user_id = actor_user_id
            row.applied_at = now
            row.apply_error = None
        except HTTPException as exc:
            row.apply_error = str(exc.detail)
            event = MetadataChangeRequestEvent(
                metadata_change_request_id=row.id,
                event_type="apply_failed",
                previous_status="approved",
                next_status="approved",
                actor_user_id=actor_user_id,
                comment=comment or str(exc.detail),
                payload_json=payload,
            )
            session.add(event)
            session.flush()
            write_audit_log_sync(
                session,
                action="governance.metadata_change_request.apply_failed",
                user_id=actor_user_id,
                entity_type="metadata_change_request",
                entity_id=row.id,
                source_module="governance.change_management",
                before=_serialize_change_request(row),
                after={"status": "approved", "apply_error": row.apply_error},
                metadata=payload,
            )
            session.commit()
            raise

    event = MetadataChangeRequestEvent(
        metadata_change_request_id=row.id,
        event_type=event_type,
        previous_status=current_status,
        next_status=row.status,
        actor_user_id=actor_user_id,
        comment=comment,
        payload_json=payload,
    )
    session.add(event)
    session.flush()
    write_audit_log_sync(
        session,
        action=f"governance.metadata_change_request.{normalized_transition}",
        user_id=actor_user_id,
        entity_type="metadata_change_request",
        entity_id=row.id,
        source_module="governance.change_management",
        before={"status": current_status},
        after={"status": row.status},
        metadata=payload,
    )
    return _serialize_change_request(row)


def review_metadata_change_request(
    session: Session,
    *,
    request_ref: str,
    comment: str | None = None,
    actor_user_id: int | None = None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    return transition_metadata_change_request(
        session,
        request_ref=request_ref,
        transition="review",
        comment=comment,
        actor_user_id=actor_user_id,
        request_audit=request_audit,
    )


def approve_metadata_change_request(
    session: Session,
    *,
    request_ref: str,
    comment: str | None = None,
    actor_user_id: int | None = None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    return transition_metadata_change_request(
        session,
        request_ref=request_ref,
        transition="approve",
        comment=comment,
        actor_user_id=actor_user_id,
        request_audit=request_audit,
    )


def apply_metadata_change_request(
    session: Session,
    *,
    request_ref: str,
    comment: str | None = None,
    actor_user_id: int | None = None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    return transition_metadata_change_request(
        session,
        request_ref=request_ref,
        transition="apply",
        comment=comment,
        actor_user_id=actor_user_id,
        request_audit=request_audit,
    )


def reject_metadata_change_request(
    session: Session,
    *,
    request_ref: str,
    comment: str | None = None,
    actor_user_id: int | None = None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    return transition_metadata_change_request(
        session,
        request_ref=request_ref,
        transition="reject",
        comment=comment,
        actor_user_id=actor_user_id,
        request_audit=request_audit,
    )


def get_metadata_change_request(session: Session, *, request_ref: str) -> dict[str, object]:
    row = _load_change_request(session, request_ref)
    return _serialize_change_request(row)


__all__ = [
    "apply_metadata_change_request",
    "approve_metadata_change_request",
    "create_metadata_change_request",
    "get_metadata_change_request",
    "list_asset_slas",
    "list_metadata_change_requests",
    "reject_metadata_change_request",
    "review_metadata_change_request",
    "transition_metadata_change_request",
    "upsert_asset_sla",
]
