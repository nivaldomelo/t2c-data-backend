from __future__ import annotations

from io import BytesIO

from fastapi import APIRouter, Depends, File, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.export_security import enforce_export_permission
from t2c_data.features.lineage.application import run_lineage_export, run_lineage_import_commit
from t2c_data.features.lineage.api_support import read_xlsx_upload_async, wrap_lineage_spreadsheet_error
from t2c_data.features.lineage.spreadsheet import LineageSpreadsheetError, preview_lineage_import
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import LineageImportCommitOut, LineageImportPreviewOut
from t2c_data.services.audit import request_audit_kwargs

router = APIRouter(tags=["lineage"])


@router.post("/import/preview", response_model=LineageImportPreviewOut)
async def preview_lineage_spreadsheet(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> LineageImportPreviewOut:
    content = await read_xlsx_upload_async(file)
    try:
        return LineageImportPreviewOut.model_validate(preview_lineage_import(db, content))
    except LineageSpreadsheetError as exc:
        raise wrap_lineage_spreadsheet_error(exc) from exc


@router.post("/import/commit", response_model=LineageImportCommitOut)
async def commit_lineage_spreadsheet(
    request: Request,
    mode: str = Query(default="merge"),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> LineageImportCommitOut:
    content = await read_xlsx_upload_async(file)
    try:
        result = run_lineage_import_commit(
            db=db,
            content=content,
            mode=mode,
            filename=file.filename,
            audit_kwargs=request_audit_kwargs(request, current_user),
        )
    except LineageSpreadsheetError as exc:
        raise wrap_lineage_spreadsheet_error(exc) from exc
    return LineageImportCommitOut.model_validate(result)


@router.get("/export", response_model=None)
def export_lineage_spreadsheet(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StreamingResponse:
    enforce_export_permission(current_user, "lineage:export")
    try:
        workbook = run_lineage_export(
            db=db,
            request=request,
            current_user=current_user,
        )
    except LineageSpreadsheetError as exc:
        raise wrap_lineage_spreadsheet_error(exc) from exc
    return StreamingResponse(
        BytesIO(workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="lineage_export.xlsx"'},
    )
