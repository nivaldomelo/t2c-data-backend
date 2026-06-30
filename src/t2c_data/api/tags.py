from __future__ import annotations

from io import BytesIO

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, Response, UploadFile, status
from fastapi.responses import StreamingResponse
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.audit import AuditFieldChange
from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, enforce_export_permission, resolve_export_limit
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.tag import Tag, TagAssignment, TagAutomationRule
from t2c_data.features.tags.intelligence import manual_assign_tag, manual_unassign_tag, reprocess_table_tag_intelligence
from t2c_data.features.tags.intelligence import (
    apply_tag_intelligence_event,
    batch_apply_tag_intelligence_events,
    batch_dismiss_tag_intelligence_events,
    dismiss_tag_intelligence_event,
    load_pending_tag_intelligence_events,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.schemas.maintenance import DestructiveActionConfirmIn
from t2c_data.schemas.pagination import PageOut
from t2c_data.schemas.tag import (
    TagAssignRequest,
    TagAssignmentOut,
    TagCreate,
    TagDetailOut,
    TagListFiltersOut,
    TagOut,
    TagSummaryOut,
    TagIntelligenceEventOut,
    TagIntelligenceBatchActionIn,
    TagIntelligenceBatchActionOut,
    TagAutomationRuleCreate,
    TagAutomationRuleOut,
    TagAutomationRuleUpdate,
    TagIntelligenceReprocessBatchIn,
    TagIntelligenceReprocessBatchOut,
    TagIntelligenceReprocessByFqnIn,
    TagIntelligenceReprocessOut,
    TagResetOut,
    TagSpreadsheetImportResult,
    TagUpdate,
)
from t2c_data.services.audit import log_field_changes, request_audit_kwargs, serialize_model, write_audit_log_sync
from t2c_data.features.tags.spreadsheet import (
    TAG_SPREADSHEET_HEADERS,
    TagSpreadsheetError,
    build_tag_workbook,
    import_tags_from_workbook,
)
from t2c_data.features.tags.api_support import (
    build_tag_out_from_model,
    find_existing_tag_conflict,
    get_tag_detail_payload,
    list_tags_payload,
    normalize_tag_payload,
    resolve_datasource_id,
    reset_tags,
    row_to_tag_out,
)

router = APIRouter(prefix="/tags", tags=["tags"])

TAG_AUDIT_FIELDS = {
    "name",
    "description",
    "group_name",
    "subgroup_name",
    "synonyms",
    "status",
    "notes",
    "tag_type",
    "color",
    "suggested_scope",
    "example_of_use",
}


def _entity_audit_context(db: Session, entity_type: str, entity_id: int) -> tuple[str, int, str | None, int | None]:
    if entity_type == "table":
        return "table", entity_id, None, None
    if entity_type == "column":
        column = db.get(ColumnEntity, entity_id)
        if column:
            return "column", entity_id, "table", int(column.table_id)
    return entity_type, entity_id, None, None


def _reprocess_table_for_entity(db: Session, *, entity_type: str, entity_id: int, actor_user_id: int | None, audit_kwargs: dict | None) -> None:
    table_id: int | None = None
    if entity_type == "table":
        table_id = entity_id
    elif entity_type == "column":
        table_id = db.scalar(select(ColumnEntity.table_id).where(ColumnEntity.id == entity_id))
    if table_id is None:
        return
    reprocess_table_tag_intelligence(
        db,
        table_id=table_id,
        actor_user_id=actor_user_id,
        audit_kwargs=audit_kwargs,
        source_module="tags",
        metadata={"origin": "manual_tag_assignment"},
    )


def _rule_to_out(rule: TagAutomationRule, tag: Tag | None) -> TagAutomationRuleOut:
    payload = {
        "id": rule.id,
        "tag_id": rule.tag_id,
        "name": rule.name,
        "scope": rule.scope,
        "status": rule.status,
        "action": rule.action,
        "category": rule.category,
        "priority": int(rule.priority or 0),
        "match_fields": rule.match_fields or [],
        "keywords": rule.keywords or [],
        "aliases": rule.aliases or [],
        "regex_pattern": rule.regex_pattern,
        "min_confidence": int(rule.min_confidence or 0),
        "notes": rule.notes,
        "tag_name": tag.name if tag else None,
        "tag_slug": tag.slug if tag else None,
        "created_at": rule.created_at,
        "updated_at": rule.updated_at,
    }
    return TagAutomationRuleOut(**payload)


@router.get("/filters", response_model=TagListFiltersOut)
def get_tag_filters(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TagListFiltersOut:
    def values_for(column) -> list[str]:
        return sorted(
            value
            for value in db.scalars(select(column).where(column.is_not(None)).distinct()).all()
            if value and str(value).strip()
        )

    return TagListFiltersOut(
        groups=values_for(Tag.group_name),
        subgroups=values_for(Tag.subgroup_name),
        statuses=values_for(Tag.status),
        tag_types=values_for(Tag.tag_type),
    )


@router.get("/summary", response_model=TagSummaryOut)
def get_tag_summary(
    query: str | None = Query(None, min_length=1),
    group: str | None = Query(None),
    subgroup: str | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    tag_type: str | None = Query(None),
    in_use: bool | None = Query(default=None),
    without_use: bool | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TagSummaryOut:
    items = list_tags_payload(
        db=db,
        query=query,
        group=group,
        subgroup=subgroup,
        status_filter=status_filter,
        tag_type=tag_type,
        in_use=in_use,
        without_use=without_use,
    )
    return TagSummaryOut(
        total=len(items),
        active=sum(1 for item in items if item.status == "active"),
        in_use=sum(1 for item in items if (item.tables_count + item.columns_count) > 0),
        groups=len({item.group_name or "Sem grupo" for item in items}),
    )


@router.get("", response_model=PageOut[TagOut])
def list_tags(
    query: str | None = Query(None, min_length=1),
    group: str | None = Query(None),
    subgroup: str | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    tag_type: str | None = Query(None),
    in_use: bool | None = Query(default=None),
    without_use: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[TagOut]:
    return paginate_items(
        list_tags_payload(
            db=db,
            query=query,
            group=group,
            subgroup=subgroup,
            status_filter=status_filter,
            tag_type=tag_type,
            in_use=in_use,
            without_use=without_use,
        ),
        page=page,
        page_size=page_size,
    )


@router.get("/template", response_model=None)
def download_tag_template(
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StreamingResponse:
    workbook = build_tag_workbook([], include_readme=True)
    filename = "tags_template.xlsx"
    return StreamingResponse(
        BytesIO(workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/export", response_model=None)
def export_tags(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StreamingResponse:
    enforce_export_permission(current_user, "tag:export")
    tags = db.scalars(select(Tag).order_by(Tag.group_name.nulls_last(), Tag.subgroup_name.nulls_last(), Tag.name)).all()
    export_limit = resolve_export_limit(source_module="tags", entity_type="tag")
    tags, truncated = enforce_export_limit(tags, limit=export_limit)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="tag.export_xlsx",
        entity_type="tag",
        source_module="tags",
        row_count=len(tags),
        limit=export_limit,
        truncated=truncated,
        export_format="xlsx",
        permission_name="tag:export",
    )
    workbook = build_tag_workbook(tags, include_readme=True)
    filename = "tags_export.xlsx"
    return StreamingResponse(
        BytesIO(workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/import", response_model=TagSpreadsheetImportResult)
async def import_tags(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagSpreadsheetImportResult:
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Envie um arquivo .xlsx válido.")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Arquivo vazio.")
    try:
        result = import_tags_from_workbook(db, content)
    except TagSpreadsheetError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Conflito de unicidade ao importar tags.") from exc

    write_audit_log_sync(
        db,
        action="tag.import",
        entity_type="tag",
        metadata={
            "filename": file.filename,
            "processed": result.processed,
            "imported": result.imported,
            "updated": result.updated,
            "rejected": result.rejected,
            "headers": TAG_SPREADSHEET_HEADERS,
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return result


@router.post("", response_model=TagOut, status_code=status.HTTP_201_CREATED)
def create_tag(
    payload: TagCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagOut:
    data = normalize_tag_payload(payload.model_dump())
    existing = find_existing_tag_conflict(db, name=data["name"], slug=data["slug"])
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Tag já existe com este nome ou slug.")
    tag = Tag(**data)
    db.add(tag)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug ou ID da tag já existe.") from exc
    db.refresh(tag)
    log_field_changes(
        db,
        action="tag.create",
        entity_type="tag",
        entity_id=tag.id,
        source_module="tags",
        changes=[
            AuditFieldChange(
                field_name="tag",
                before=None,
                after={"id": tag.id, "label": tag.name},
                change_type="create",
            )
        ],
        metadata={"message": "Tag criada"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return row_to_tag_out(
        {
            **serialize_model(tag),
            "tables_count": 0,
        }
    )


@router.get("/{tag_id}", response_model=TagDetailOut)
def get_tag(
    tag_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TagDetailOut:
    payload = get_tag_detail_payload(db=db, tag_id=tag_id)
    if not payload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    return TagDetailOut(**payload)


@router.patch("/{tag_id}", response_model=TagOut)
def patch_tag(
    tag_id: int,
    payload: TagUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagOut:
    tag = db.get(Tag, tag_id)
    if not tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    before = serialize_model(tag)
    updates = normalize_tag_payload({**serialize_model(tag), **payload.model_dump(exclude_unset=True)})
    for key, value in updates.items():
        setattr(tag, key, value)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug ou ID da tag já existe.") from exc
    db.refresh(tag)
    log_field_changes(
        db,
        action="tag.update",
        entity_type="tag",
        entity_id=tag.id,
        source_module="tags",
        changes=[
            AuditFieldChange(field_name=field_name, before=before.get(field_name), after=serialize_model(tag).get(field_name))
            for field_name in sorted(TAG_AUDIT_FIELDS)
            if before.get(field_name) != serialize_model(tag).get(field_name)
        ],
        metadata={"message": "Tag atualizada"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return build_tag_out_from_model(db, tag)


@router.delete("/{tag_id}", response_model=dict[str, bool])
def delete_tag(
    tag_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, bool]:
    tag = db.get(Tag, tag_id)
    if not tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    assignments_count = db.scalar(select(func.count(TagAssignment.id)).where(TagAssignment.tag_id == tag_id)) or 0
    before = serialize_model(tag)
    if assignments_count:
        db.execute(delete(TagAssignment).where(TagAssignment.tag_id == tag_id))
    db.delete(tag)
    db.commit()
    log_field_changes(
        db,
        action="tag.delete",
        entity_type="tag",
        entity_id=tag_id,
        source_module="tags",
        changes=[
            AuditFieldChange(
                field_name="tag",
                before={"id": tag_id, "label": before.get("name")},
                after=None,
                change_type="delete",
            )
        ],
        metadata={"assignments_count": int(assignments_count), "message": "Tag removida"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return {"ok": True}


@router.post("/reset", response_model=TagResetOut)
def reset_all_tags(
    _payload: DestructiveActionConfirmIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> TagResetOut:
    deleted_tags, deleted_assignments, deleted_overrides, deleted_events = reset_tags(
        db,
        actor_user_id=current_user.id,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return TagResetOut(
        deleted_tags=deleted_tags,
        deleted_assignments=deleted_assignments,
        deleted_overrides=deleted_overrides,
        deleted_events=deleted_events,
    )


@router.post("/assignments", response_model=TagAssignmentOut, status_code=status.HTTP_201_CREATED)
def assign_tag(
    payload: TagAssignRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagAssignmentOut:
    tag = db.get(Tag, payload.tag_id)
    if not tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    datasource_id = resolve_datasource_id(db, payload.entity_type, payload.entity_id)
    assignment = manual_assign_tag(
        db,
        tag_id=payload.tag_id,
        entity_type=payload.entity_type,
        entity_id=payload.entity_id,
        datasource_id=datasource_id,
        actor_user_id=current_user.id,
        reason="Atribuição manual via API de tags.",
    )
    audit_entity_type, audit_entity_id, parent_entity_type, parent_entity_id = _entity_audit_context(
        db, payload.entity_type, payload.entity_id
    )
    log_field_changes(
        db,
        action="tag.assign",
        entity_type=audit_entity_type,
        entity_id=audit_entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module="tags",
        changes=[
            AuditFieldChange(
                field_name="tags",
                before=None,
                after={"id": tag.id, "label": tag.name},
                change_type="assign",
                metadata={"assignment_id": assignment.id, "tag_id": payload.tag_id},
            )
        ],
        metadata={"message": "Tag associada à entidade"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    _reprocess_table_for_entity(
        db,
        entity_type=payload.entity_type,
        entity_id=payload.entity_id,
        actor_user_id=current_user.id,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return assignment


@router.get("/assignments", response_model=list[TagAssignmentOut])
def list_assignments(
    entity_type: str | None = None,
    entity_id: int | None = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TagAssignmentOut]:
    stmt = select(TagAssignment)
    if entity_type is not None:
        stmt = stmt.where(TagAssignment.entity_type == entity_type)
    if entity_id is not None:
        stmt = stmt.where(TagAssignment.entity_id == entity_id)
    return db.scalars(stmt.order_by(TagAssignment.id.desc())).all()


@router.delete("/assignments/{assignment_id}", response_model=dict[str, bool])
def unassign_tag(
    assignment_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, bool]:
    assignment = db.get(TagAssignment, assignment_id)
    if not assignment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
    before = serialize_model(assignment)
    tag = db.get(Tag, assignment.tag_id)
    datasource_id = assignment.datasource_id or resolve_datasource_id(db, assignment.entity_type, assignment.entity_id)
    audit_entity_type, audit_entity_id, parent_entity_type, parent_entity_id = _entity_audit_context(
        db, assignment.entity_type, assignment.entity_id
    )
    manual_unassign_tag(
        db,
        tag_id=assignment.tag_id,
        entity_type=assignment.entity_type,
        entity_id=assignment.entity_id,
        datasource_id=datasource_id,
        actor_user_id=current_user.id,
        reason="Remoção manual via API de tags.",
    )
    log_field_changes(
        db,
        action="tag.unassign",
        entity_type=audit_entity_type,
        entity_id=audit_entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module="tags",
        changes=[
            AuditFieldChange(
                field_name="tags",
                before={"id": assignment.tag_id, "label": tag.name if tag else assignment.tag_id},
                after=None,
                change_type="unassign",
                metadata={"assignment_id": assignment_id, "tag_id": assignment.tag_id, "assignment": before},
            )
        ],
        metadata={"message": "Tag desassociada da entidade"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    _reprocess_table_for_entity(
        db,
        entity_type=assignment.entity_type,
        entity_id=assignment.entity_id,
        actor_user_id=current_user.id,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return {"ok": True}


@router.get("/intelligence/events", response_model=PageOut[TagIntelligenceEventOut])
def list_tag_intelligence_events(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    limit: int | None = Query(default=None, ge=1, le=200),
    entity_type: str | None = Query(None),
    table_id: int | None = Query(None),
    column_id: int | None = Query(None),
    table_query: str | None = Query(None, alias="table"),
    column_query: str | None = Query(None, alias="column"),
    tag_slug: str | None = Query(None),
    inference_source: str | None = Query(None),
    review_status: str | None = Query(None),
    risk_band: str | None = Query(None),
    min_confidence: int | None = Query(None, ge=0, le=100),
    max_confidence: int | None = Query(None, ge=0, le=100),
    sort_by: str = Query("risk_desc"),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> PageOut[TagIntelligenceEventOut]:
    items = [
        TagIntelligenceEventOut(**item)
        for item in load_pending_tag_intelligence_events(
            db,
            limit=limit,
            entity_type=entity_type,
            table_id=table_id,
            column_id=column_id,
            table_query=table_query,
            column_query=column_query,
            tag_slug=tag_slug,
            inference_source=inference_source,
            review_status=review_status,
            risk_band=risk_band,
            min_confidence=min_confidence,
            max_confidence=max_confidence,
            sort_by=sort_by,
        )
    ]
    return paginate_items(items, page=page, page_size=page_size)


@router.post("/intelligence/events/apply-batch", response_model=TagIntelligenceBatchActionOut)
def apply_tag_intelligence_suggestions_batch(
    payload: TagIntelligenceBatchActionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagIntelligenceBatchActionOut:
    if not payload.event_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Informe ao menos uma sugestão.")
    result = batch_apply_tag_intelligence_events(db, event_ids=payload.event_ids, actor_user_id=current_user.id)
    write_audit_log_sync(
        db,
        action="tag.intelligence.event.apply_batch",
        entity_type="tag",
        entity_id="batch",
        source_module="tags",
        metadata={
            "message": "Lote de sugestões de tag aplicado",
            "requested": result["requested"],
            "succeeded": result["succeeded"],
            "failed": result["failed"],
            "event_ids": payload.event_ids,
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return TagIntelligenceBatchActionOut(**result)


@router.post("/intelligence/events/block-batch", response_model=TagIntelligenceBatchActionOut)
def block_tag_intelligence_suggestions_batch(
    payload: TagIntelligenceBatchActionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagIntelligenceBatchActionOut:
    if not payload.event_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Informe ao menos uma sugestão.")
    result = batch_dismiss_tag_intelligence_events(db, event_ids=payload.event_ids, actor_user_id=current_user.id)
    write_audit_log_sync(
        db,
        action="tag.intelligence.event.block_batch",
        entity_type="tag",
        entity_id="batch",
        source_module="tags",
        metadata={
            "message": "Lote de sugestões de tag bloqueado",
            "requested": result["requested"],
            "succeeded": result["succeeded"],
            "failed": result["failed"],
            "event_ids": payload.event_ids,
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return TagIntelligenceBatchActionOut(**result)


@router.post("/intelligence/events/{event_id}/apply", response_model=dict[str, object])
def apply_tag_intelligence_suggestion(
    event_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, object]:
    payload = apply_tag_intelligence_event(db, event_id=event_id, actor_user_id=current_user.id)
    write_audit_log_sync(
        db,
        action="tag.intelligence.event.apply",
        entity_type="tag",
        entity_id=str(event_id),
        source_module="tags",
        metadata={"message": "Sugestão de tag aplicada", "event_id": event_id, "status": payload["status"]},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return payload


@router.post("/intelligence/events/{event_id}/block", response_model=dict[str, object])
def block_tag_intelligence_suggestion(
    event_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, object]:
    payload = dismiss_tag_intelligence_event(db, event_id=event_id, actor_user_id=current_user.id)
    write_audit_log_sync(
        db,
        action="tag.intelligence.event.block",
        entity_type="tag",
        entity_id=str(event_id),
        source_module="tags",
        metadata={"message": "Sugestão de tag bloqueada", "event_id": event_id, "status": payload["status"]},
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return payload


@router.post("/intelligence/tables/{table_id}/reprocess", response_model=TagIntelligenceReprocessOut)
def reprocess_table_tags(
    table_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagIntelligenceReprocessOut:
    payload = reprocess_table_tag_intelligence(
        db,
        table_id=table_id,
        actor_user_id=current_user.id,
        audit_kwargs=request_audit_kwargs(request, current_user),
        source_module="tags.api",
        metadata={"trigger": "manual_reprocess"},
    )
    return TagIntelligenceReprocessOut(**payload)


@router.post("/intelligence/tables/reprocess-by-fqn", response_model=TagIntelligenceReprocessOut)
def reprocess_table_tags_by_fqn(
    payload: TagIntelligenceReprocessByFqnIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagIntelligenceReprocessOut:
    stmt = (
        select(TableEntity)
        .join(Schema, Schema.id == TableEntity.schema_id)
        .join(Database, Database.id == Schema.database_id)
        .join(DataSource, DataSource.id == Database.datasource_id)
        .where(
            TableEntity.name == payload.table_name,
            Schema.name == payload.schema_name,
            Database.name == payload.database_name,
        )
    )
    if payload.datasource_name:
        stmt = stmt.where(DataSource.name == payload.datasource_name)
    matches = db.scalars(stmt).all()
    if not matches:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found for provided FQN")
    if len(matches) > 1:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Multiple tables match the provided FQN")
    table = matches[0]
    payload_result = reprocess_table_tag_intelligence(
        db,
        table_id=table.id,
        actor_user_id=current_user.id,
        audit_kwargs=request_audit_kwargs(request, current_user),
        source_module="tags.api",
        metadata={
            "trigger": "manual_reprocess",
            "table_fqn": f"{payload.database_name}.{payload.schema_name}.{payload.table_name}",
        },
    )
    return TagIntelligenceReprocessOut(**payload_result)


@router.post("/intelligence/tables/reprocess-batch", response_model=TagIntelligenceReprocessBatchOut)
def reprocess_table_tags_batch(
    payload: TagIntelligenceReprocessBatchIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagIntelligenceReprocessBatchOut:
    stmt = select(TableEntity).join(Schema, Schema.id == TableEntity.schema_id).join(Database, Database.id == Schema.database_id)
    if payload.datasource_id is not None:
        stmt = stmt.where(Database.datasource_id == payload.datasource_id)
    if payload.database_name:
        stmt = stmt.where(Database.name == payload.database_name)
    if payload.schema_name:
        stmt = stmt.where(Schema.name == payload.schema_name)
    tables = db.scalars(stmt.limit(payload.limit)).all()
    processed_ids: list[int] = []
    for table in tables:
        reprocess_table_tag_intelligence(
            db,
            table_id=table.id,
            actor_user_id=current_user.id,
            audit_kwargs=request_audit_kwargs(request, current_user),
            source_module="tags.api",
            metadata={"trigger": "batch_reprocess"},
        )
        processed_ids.append(table.id)
    return TagIntelligenceReprocessBatchOut(total=len(tables), processed=len(processed_ids), table_ids=processed_ids)


@router.get("/automation-rules", response_model=list[TagAutomationRuleOut])
def list_tag_automation_rules(
    status_filter: str | None = Query(None, alias="status"),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TagAutomationRuleOut]:
    stmt = select(TagAutomationRule, Tag).join(Tag, Tag.id == TagAutomationRule.tag_id)
    if status_filter:
        stmt = stmt.where(TagAutomationRule.status == status_filter)
    rows = db.execute(stmt.order_by(TagAutomationRule.priority.asc(), TagAutomationRule.id.asc())).all()
    return [_rule_to_out(rule, tag) for rule, tag in rows]


@router.post("/automation-rules", response_model=TagAutomationRuleOut, status_code=status.HTTP_201_CREATED)
def create_tag_automation_rule(
    payload: TagAutomationRuleCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagAutomationRuleOut:
    tag = db.get(Tag, payload.tag_id)
    if tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    rule = TagAutomationRule(
        tag_id=payload.tag_id,
        name=payload.name.strip(),
        scope=payload.scope,
        status=payload.status,
        action=payload.action,
        category=payload.category,
        priority=payload.priority,
        match_fields=payload.match_fields or [],
        keywords=payload.keywords or [],
        aliases=payload.aliases or [],
        regex_pattern=payload.regex_pattern,
        min_confidence=payload.min_confidence,
        notes=payload.notes,
        created_by_user_id=current_user.id,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    write_audit_log_sync(
        db,
        action="tag.automation_rule.create",
        entity_type="tag",
        entity_id=str(rule.id),
        source_module="tags",
        metadata={"message": "Regra automática de tag criada", "rule_id": rule.id, "tag_id": payload.tag_id},
        **request_audit_kwargs(request, current_user),
    )
    return _rule_to_out(rule, tag)


@router.patch("/automation-rules/{rule_id}", response_model=TagAutomationRuleOut)
def update_tag_automation_rule(
    rule_id: int,
    payload: TagAutomationRuleUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TagAutomationRuleOut:
    rule = db.get(TagAutomationRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    data = payload.model_dump(exclude_unset=True)
    for key, value in data.items():
        setattr(rule, key, value)
    db.add(rule)
    db.commit()
    db.refresh(rule)
    tag = db.get(Tag, rule.tag_id)
    write_audit_log_sync(
        db,
        action="tag.automation_rule.update",
        entity_type="tag",
        entity_id=str(rule.id),
        source_module="tags",
        metadata={"message": "Regra automática de tag atualizada", "rule_id": rule.id, "tag_id": rule.tag_id},
        **request_audit_kwargs(request, current_user),
    )
    return _rule_to_out(rule, tag)


@router.delete("/automation-rules/{rule_id}", response_model=dict[str, bool])
def delete_tag_automation_rule(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, bool]:
    rule = db.get(TagAutomationRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    db.delete(rule)
    db.commit()
    write_audit_log_sync(
        db,
        action="tag.automation_rule.delete",
        entity_type="tag",
        entity_id=str(rule_id),
        source_module="tags",
        metadata={"message": "Regra automática de tag removida", "rule_id": rule_id},
        **request_audit_kwargs(request, current_user),
    )
    return {"ok": True}
