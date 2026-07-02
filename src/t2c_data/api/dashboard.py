from __future__ import annotations

import csv
from io import BytesIO, StringIO

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.export_security import safe_csv_writer, safe_sheet_append, DEFAULT_EXPORT_LIMIT, audit_export_event, resolve_export_limit
from t2c_data.features.dashboard.executive_queries import (
    get_dashboard_executive_asset_details,
    get_dashboard_executive_campaign_queue,
    get_dashboard_executive_overview,
    get_dashboard_executive_secondary,
    get_dashboard_executive_summary,
    normalize_filters,
)
from t2c_data.features.dashboard.strategy_queries import build_platform_strategic_summary
from t2c_data.features.dashboard.queries import get_dashboard_summary
from t2c_data.models.auth import User
from t2c_data.schemas.dashboard import (
    DashboardExecutiveAssetDetailsOut,
    DashboardExecutiveOverviewOut,
    DashboardExecutiveSecondaryOut,
    DashboardExecutiveSummaryOut,
    DashboardStrategicSummaryOut,
    DashboardSummaryOut,
)
from t2c_data.schemas.governance import GovernanceCampaignQueueOut

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def executive_filters(
    domain: str | None = Query(default=None),
    data_source_id: int | None = Query(default=None),
    source: str | None = Query(default=None),
    database: str | None = Query(default=None),
    schema_key: str | None = Query(default=None),
    schema: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    certification_status: str | None = Query(default=None),
    dq_band: str | None = Query(default=None),
    incidents: str | None = Query(default=None),
    q: str | None = Query(default=None),
):
    return normalize_filters(
        domain=domain,
        data_source_id=data_source_id,
        source=source,
        database=database,
        schema_key=schema_key,
        schema=schema,
        owner=owner,
        certification_status=certification_status,
        dq_band=dq_band,
        incidents=incidents,
        q=q,
    )


@router.get("/summary", response_model=DashboardSummaryOut)
def dashboard_summary(
    filters=Depends(executive_filters),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DashboardSummaryOut:
    return DashboardSummaryOut(**get_dashboard_summary(db, current_user=current_user, filters=filters))


@router.get("/strategic/summary", response_model=DashboardStrategicSummaryOut)
def dashboard_strategic_summary(
    days: int = Query(default=30, ge=7, le=180),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DashboardStrategicSummaryOut:
    return DashboardStrategicSummaryOut(**build_platform_strategic_summary(db, days=days, current_user=current_user))


@router.get("/executive/summary", response_model=DashboardExecutiveSummaryOut)
def dashboard_executive_summary(
    filters=Depends(executive_filters),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DashboardExecutiveSummaryOut:
    return DashboardExecutiveSummaryOut(**get_dashboard_executive_summary(db, filters, current_user=current_user))


@router.get("/executive/overview", response_model=DashboardExecutiveOverviewOut)
def dashboard_executive_overview(
    filters=Depends(executive_filters),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DashboardExecutiveOverviewOut:
    return DashboardExecutiveOverviewOut(**get_dashboard_executive_overview(db, filters, current_user=current_user))


@router.get("/executive/secondary", response_model=DashboardExecutiveSecondaryOut)
def dashboard_executive_secondary(
    filters=Depends(executive_filters),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DashboardExecutiveSecondaryOut:
    return DashboardExecutiveSecondaryOut(**get_dashboard_executive_secondary(db, filters, current_user=current_user))


# NOTE: the granular executive endpoints (top-critical, certification, governance-gaps,
# dq, incidents, risk-by-domain) were removed — the frontend reads all of this from
# /executive/secondary. The underlying service functions remain in t2c_data.features.dashboard
# for reuse/tests.


@router.get("/executive/asset/{table_id}/details", response_model=DashboardExecutiveAssetDetailsOut)
def dashboard_executive_asset_details(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DashboardExecutiveAssetDetailsOut:
    payload = get_dashboard_executive_asset_details(db, table_id, current_user=current_user)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ativo não encontrado")
    return DashboardExecutiveAssetDetailsOut(**payload)


@router.get("/executive/campaigns/{campaign_key}/items", response_model=GovernanceCampaignQueueOut)
def dashboard_executive_campaign_items(
    campaign_key: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    filters=Depends(executive_filters),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceCampaignQueueOut:
    try:
        payload = get_dashboard_executive_campaign_queue(db, campaign_key, filters, page=page, page_size=page_size, current_user=current_user)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return GovernanceCampaignQueueOut(**payload)


@router.get("/executive/campaigns/{campaign_key}/export.csv", response_model=None)
def dashboard_executive_campaign_export_csv(
    request: Request,
    campaign_key: str,
    filters=Depends(executive_filters),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("governance:export")),
) -> StreamingResponse:
    export_limit = resolve_export_limit(source_module="dashboard", entity_type="dashboard_campaign")
    try:
        payload = get_dashboard_executive_campaign_queue(db, campaign_key, filters, page=1, page_size=export_limit, current_user=current_user)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="dashboard.executive_campaign.export_csv",
        entity_type="dashboard_campaign",
        source_module="dashboard",
        row_count=len(payload["items"]),
        truncated=payload["total"] > len(payload["items"]),
        limit=export_limit,
        filters={"campaign_key": campaign_key, **dict(filters or {})},
    )
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "tabela",
            "fonte",
            "banco",
            "schema",
            "owner",
            "score_governanca",
            "maturidade_governanca",
            "certificacao",
            "sensibilidade",
            "ultima_revisao",
            "explorer",
            "data_quality",
            "incidentes",
        ]
    )
    for item in payload["items"]:
        writer.writerow(
            [
                item["table_fqn"],
                item["datasource_name"],
                item["database_name"],
                item["schema_name"],
                item["owner_name"],
                item["governance_score"]["score"],
                item["governance_score"]["label"],
                item["certification_status_label"],
                item["sensitivity_label"],
                item["last_review_at"] or "",
                item["links"]["explorer"],
                item["links"]["data_quality"],
                item["links"]["incidents"],
            ]
        )
    return StreamingResponse(
        iter([buffer.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="dashboard_campaign_{campaign_key}.csv"'},
    )


@router.get("/executive/campaigns/{campaign_key}/export.xlsx", response_model=None)
def dashboard_executive_campaign_export_xlsx(
    request: Request,
    campaign_key: str,
    filters=Depends(executive_filters),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("governance:export")),
) -> StreamingResponse:
    from openpyxl import Workbook

    export_limit = resolve_export_limit(source_module="dashboard", entity_type="dashboard_campaign")
    try:
        payload = get_dashboard_executive_campaign_queue(db, campaign_key, filters, page=1, page_size=export_limit, current_user=current_user)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="dashboard.executive_campaign.export_xlsx",
        entity_type="dashboard_campaign",
        source_module="dashboard",
        row_count=len(payload["items"]),
        truncated=payload["total"] > len(payload["items"]),
        limit=export_limit,
        filters={"campaign_key": campaign_key, **dict(filters or {})},
    )
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Campanha"
    safe_sheet_append(sheet, 
        [
            "Tabela",
            "Fonte",
            "Banco",
            "Schema",
            "Owner",
            "Score de governança",
            "Maturidade",
            "Certificação",
            "Sensibilidade",
            "Última revisão",
            "Explorer",
            "Data Quality",
            "Incidentes",
        ]
    )
    for item in payload["items"]:
        safe_sheet_append(sheet, 
            [
                item["table_fqn"],
                item["datasource_name"],
                item["database_name"],
                item["schema_name"],
                item["owner_name"],
                item["governance_score"]["score"],
                item["governance_score"]["label"],
                item["certification_status_label"],
                item["sensitivity_label"],
                item["last_review_at"] or "",
                item["links"]["explorer"],
                item["links"]["data_quality"],
                item["links"]["incidents"],
            ]
        )
    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="dashboard_campaign_{campaign_key}.xlsx"'},
    )
