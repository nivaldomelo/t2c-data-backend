from __future__ import annotations

from io import BytesIO

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import StreamingResponse
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.features.audit import AuditFieldChange
from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, enforce_export_permission, resolve_export_limit
from t2c_data.features.pagination import paginate_items
from t2c_data.schemas.glossary import (
    GlossaryAssignmentOut,
    GlossaryAssignRequest,
    GlossaryTermCreate,
    GlossaryTermDetailOut,
    GlossaryTermFiltersOut,
    GlossaryTermOut,
    GlossarySummaryOut,
    GlossaryResetOut,
    GlossaryTermUpdate,
    GlossarySpreadsheetImportResult,
)
from t2c_data.schemas.maintenance import DestructiveActionConfirmIn
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import log_field_changes, request_audit_kwargs, serialize_model, write_audit_log_sync
from t2c_data.features.glossary.spreadsheet import (
    GLOSSARY_SPREADSHEET_HEADERS,
    TagSpreadsheetError,
    build_glossary_workbook,
    glossary_priority_label,
    glossary_status_label,
    import_glossary_from_workbook,
)
from t2c_data.features.glossary.api_support import (
    base_term_select,
    build_term_out_from_model,
    find_existing_assignment,
    find_existing_term_conflict,
    get_term_detail_payload,
    glossary_summary_payload,
    list_terms_payload,
    normalize_term_payload,
    resolve_datasource_id,
    reset_glossary_terms,
    row_to_term_out,
)

router = APIRouter(prefix="/glossary", tags=["glossary"])

GLOSSARY_AUDIT_FIELDS = {
    "name",
    "definition",
    "description",
    "category",
    "subcategory",
    "synonyms",
    "status",
    "notes",
    "tag_labels",
    "steward",
    "suggested_priority",
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


@router.get("/filters", response_model=GlossaryTermFiltersOut)
def get_term_filters(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GlossaryTermFiltersOut:
    def values_for(column) -> list[str]:
        return sorted(
            value
            for value in db.scalars(select(column).where(column.is_not(None)).distinct()).all()
            if value and str(value).strip()
        )

    return GlossaryTermFiltersOut(
        categories=values_for(GlossaryTerm.category),
        subcategories=values_for(GlossaryTerm.subcategory),
        statuses=values_for(GlossaryTerm.status),
        priorities=values_for(GlossaryTerm.suggested_priority),
    )


@router.get("/summary", response_model=GlossarySummaryOut)
def get_term_summary(
    query: str | None = Query(None, min_length=1),
    category: str | None = Query(None),
    subcategory: str | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    priority: str | None = Query(None),
    in_use: bool | None = Query(default=None),
    without_use: bool | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GlossarySummaryOut:
    return GlossarySummaryOut(
        **glossary_summary_payload(
            db=db,
            query=query,
            category=category,
            subcategory=subcategory,
            status_filter=status_filter,
            priority=priority,
            in_use=in_use,
            without_use=without_use,
        )
    )


@router.get("/template", response_model=None)
def download_glossary_template(
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StreamingResponse:
    workbook = build_glossary_workbook([], include_readme=True)
    return StreamingResponse(
        BytesIO(workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="glossario_template.xlsx"'},
    )


@router.get("/export", response_model=None)
def export_glossary(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StreamingResponse:
    enforce_export_permission(current_user, "glossary:export")
    items = db.scalars(
        select(GlossaryTerm).order_by(
            GlossaryTerm.category.nulls_last(),
            GlossaryTerm.subcategory.nulls_last(),
            GlossaryTerm.name,
        )
    ).all()
    export_limit = resolve_export_limit(source_module="glossary", entity_type="glossary_term")
    items, truncated = enforce_export_limit(items, limit=export_limit)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="glossary.export_xlsx",
        entity_type="glossary_term",
        source_module="glossary",
        row_count=len(items),
        limit=export_limit,
        truncated=truncated,
        export_format="xlsx",
        permission_name="glossary:export",
    )
    workbook = build_glossary_workbook(items, include_readme=True)
    return StreamingResponse(
        BytesIO(workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="glossario_export.xlsx"'},
    )


@router.post("/import", response_model=GlossarySpreadsheetImportResult)
async def import_glossary(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> GlossarySpreadsheetImportResult:
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Envie um arquivo .xlsx válido.")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Arquivo vazio.")
    try:
        result = import_glossary_from_workbook(db, content)
    except TagSpreadsheetError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Conflito de unicidade ao importar termos.") from exc

    write_audit_log_sync(
        db,
        action="glossary.import",
        entity_type="glossary_term",
        metadata={
            "filename": file.filename,
            "processed": result.processed,
            "imported": result.imported,
            "updated": result.updated,
            "rejected": result.rejected,
            "headers": GLOSSARY_SPREADSHEET_HEADERS,
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return result


@router.post("/terms", response_model=GlossaryTermOut, status_code=status.HTTP_201_CREATED)
def create_term(
    payload: GlossaryTermCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> GlossaryTermOut:
    data = normalize_term_payload(payload.model_dump())
    existing = find_existing_term_conflict(db, name=data["name"], slug=data["slug"])
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Term already exists with this name or slug")
    term = GlossaryTerm(**data)
    db.add(term)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug or ID already exists.") from exc
    db.refresh(term)
    log_field_changes(
        db,
        action="glossary.term.create",
        entity_type="glossary_term",
        entity_id=term.id,
        source_module="glossary",
        changes=[
            AuditFieldChange(
                field_name="term",
                before=None,
                after={"id": term.id, "label": term.name},
                change_type="create",
            )
        ],
        metadata={"message": "Termo criado no glossário"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return row_to_term_out({**serialize_model(term), "tables_count": 0})


@router.get("/terms", response_model=PageOut[GlossaryTermOut])
def list_terms(
    query: str | None = Query(None, min_length=1),
    category: str | None = Query(None),
    subcategory: str | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    priority: str | None = Query(None),
    in_use: bool | None = Query(default=None),
    without_use: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[GlossaryTermOut]:
    return paginate_items(
        list_terms_payload(
        db=db,
        query=query,
            category=category,
            subcategory=subcategory,
            status_filter=status_filter,
            priority=priority,
            in_use=in_use,
            without_use=without_use,
        ),
        page=page,
        page_size=page_size,
    )


@router.get("/terms/{term_id}", response_model=GlossaryTermDetailOut)
def get_term(
    term_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GlossaryTermDetailOut:
    payload = get_term_detail_payload(db=db, term_id=term_id)
    if not payload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Term not found")
    return GlossaryTermDetailOut(**payload)


@router.patch("/terms/{term_id}", response_model=GlossaryTermOut)
def patch_term(
    term_id: int,
    payload: GlossaryTermUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> GlossaryTermOut:
    term = db.get(GlossaryTerm, term_id)
    if not term:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Term not found")
    before = serialize_model(term)
    updates = normalize_term_payload({**serialize_model(term), **payload.model_dump(exclude_unset=True)})
    for key, value in updates.items():
        setattr(term, key, value)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug or ID already exists.") from exc
    db.refresh(term)
    log_field_changes(
        db,
        action="glossary.term.update",
        entity_type="glossary_term",
        entity_id=term.id,
        source_module="glossary",
        changes=[
            AuditFieldChange(field_name=field_name, before=before.get(field_name), after=serialize_model(term).get(field_name))
            for field_name in sorted(GLOSSARY_AUDIT_FIELDS)
            if before.get(field_name) != serialize_model(term).get(field_name)
        ],
        metadata={"message": "Termo atualizado no glossário"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return build_term_out_from_model(db, term)


@router.delete("/terms/{term_id}", response_model=dict[str, bool])
def delete_term(
    term_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, bool]:
    term = db.get(GlossaryTerm, term_id)
    if not term:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Term not found")
    assignments_count = db.scalar(select(func.count(GlossaryAssignment.id)).where(GlossaryAssignment.term_id == term_id)) or 0
    before = serialize_model(term)
    if assignments_count:
        db.execute(delete(GlossaryAssignment).where(GlossaryAssignment.term_id == term_id))
    db.delete(term)
    db.commit()
    log_field_changes(
        db,
        action="glossary.term.delete",
        entity_type="glossary_term",
        entity_id=term_id,
        source_module="glossary",
        changes=[
            AuditFieldChange(
                field_name="term",
                before={"id": term_id, "label": before.get("name")},
                after=None,
                change_type="delete",
            )
        ],
        metadata={"assignments_count": int(assignments_count), "message": "Termo removido do glossário"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return {"ok": True}


@router.post("/reset", response_model=GlossaryResetOut)
def reset_all_terms(
    _payload: DestructiveActionConfirmIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> GlossaryResetOut:
    deleted_terms, deleted_assignments = reset_glossary_terms(
        db,
        actor_user_id=current_user.id,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    return GlossaryResetOut(deleted_terms=deleted_terms, deleted_assignments=deleted_assignments)


@router.post("/assignments", response_model=GlossaryAssignmentOut, status_code=status.HTTP_201_CREATED)
def assign_term(
    payload: GlossaryAssignRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> GlossaryAssignmentOut:
    term = db.get(GlossaryTerm, payload.term_id)
    if not term:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Term not found")

    existing = find_existing_assignment(
        db,
        term_id=payload.term_id,
        entity_type=payload.entity_type,
        entity_id=payload.entity_id,
    )
    if existing:
        return existing

    assignment = GlossaryAssignment(
        term_id=payload.term_id,
        datasource_id=resolve_datasource_id(db, payload.entity_type, payload.entity_id),
        entity_type=payload.entity_type,
        entity_id=payload.entity_id,
    )
    db.add(assignment)
    db.commit()
    db.refresh(assignment)
    audit_entity_type, audit_entity_id, parent_entity_type, parent_entity_id = _entity_audit_context(
        db, payload.entity_type, payload.entity_id
    )
    log_field_changes(
        db,
        action="glossary.term.assign",
        entity_type=audit_entity_type,
        entity_id=audit_entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module="glossary",
        changes=[
            AuditFieldChange(
                field_name="glossary_terms",
                before=None,
                after={"id": term.id, "label": term.name},
                change_type="assign",
                metadata={"assignment_id": assignment.id, "term_id": payload.term_id},
            )
        ],
        metadata={"message": "Termo associado à entidade"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return assignment


@router.get("/assignments", response_model=list[GlossaryAssignmentOut])
def list_assignments(
    entity_type: str | None = None,
    entity_id: int | None = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[GlossaryAssignmentOut]:
    stmt = select(GlossaryAssignment)
    if entity_type is not None:
        stmt = stmt.where(GlossaryAssignment.entity_type == entity_type)
    if entity_id is not None:
        stmt = stmt.where(GlossaryAssignment.entity_id == entity_id)
    return db.scalars(stmt.order_by(GlossaryAssignment.id.desc())).all()


@router.delete("/assignments/{assignment_id}", response_model=dict[str, bool])
def unassign_term(
    assignment_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> dict[str, bool]:
    assignment = db.get(GlossaryAssignment, assignment_id)
    if not assignment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
    before = serialize_model(assignment)
    term = db.get(GlossaryTerm, assignment.term_id)
    audit_entity_type, audit_entity_id, parent_entity_type, parent_entity_id = _entity_audit_context(
        db, assignment.entity_type, assignment.entity_id
    )
    db.delete(assignment)
    db.commit()
    log_field_changes(
        db,
        action="glossary.term.unassign",
        entity_type=audit_entity_type,
        entity_id=audit_entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module="glossary",
        changes=[
            AuditFieldChange(
                field_name="glossary_terms",
                before={"id": assignment.term_id, "label": term.name if term else assignment.term_id},
                after=None,
                change_type="unassign",
                metadata={"assignment_id": assignment_id, "term_id": assignment.term_id, "assignment": before},
            )
        ],
        metadata={"message": "Termo desassociado da entidade"},
        audit_kwargs=request_audit_kwargs(request, current_user),
        actor_user_id=current_user.id,
    )
    db.commit()
    return {"ok": True}
