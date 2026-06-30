from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import PlainTextResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.scanner.execution_diagnostics import serialize_scan_run_detail
from t2c_data.features.scanner.application import enqueue_datasource_scan
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource
from t2c_data.models.scan import ScanDiff, ScanRun
from t2c_data.schemas.scan import ScanDiffOut, ScanRunDetailOut, ScanRunOut

router = APIRouter(prefix="/scan-runs", tags=["scan"])


@router.post("/datasource/{datasource_id}", response_model=ScanRunOut)
def scan_datasource(
    datasource_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> ScanRunOut:
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")
    run, _job = enqueue_datasource_scan(db, datasource=datasource, started_by=user.id, trigger_mode="manual")
    return run


@router.get("", response_model=list[ScanRunOut])
def list_scan_runs(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[ScanRunOut]:
    return db.scalars(select(ScanRun).order_by(ScanRun.id.desc())).all()


@router.get("/{scan_run_id}", response_model=ScanRunDetailOut)
def get_scan_run(
    scan_run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ScanRunDetailOut:
    run = db.get(ScanRun, scan_run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scan run not found")
    datasource = db.get(DataSource, run.datasource_id)
    return ScanRunDetailOut.model_validate(serialize_scan_run_detail(run, datasource=datasource))


@router.get("/{scan_run_id}/logs", response_class=PlainTextResponse)
def get_scan_run_logs(
    scan_run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PlainTextResponse:
    run = db.get(ScanRun, scan_run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scan run not found")
    summary = run.summary if isinstance(run.summary, dict) else {}
    logs_path = summary.get("logs_path")
    if not isinstance(logs_path, str) or not logs_path.strip():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Logs not available for this scan run")
    try:
        content = Path(logs_path).read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Logs not available for this scan run") from exc
    return PlainTextResponse(content)


@router.get("/{scan_run_id}/diffs", response_model=list[ScanDiffOut])
def list_scan_diffs(
    scan_run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[ScanDiffOut]:
    run = db.get(ScanRun, scan_run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scan run not found")
    return db.scalars(select(ScanDiff).where(ScanDiff.scan_run_id == scan_run_id).order_by(ScanDiff.id)).all()
