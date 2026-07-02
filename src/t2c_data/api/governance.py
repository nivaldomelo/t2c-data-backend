from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO, StringIO
import csv
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.export_jobs import ExportArtifactResult, enqueue_export_job, register_export_request_audit, serialize_export_job
from t2c_data.features.export_security import safe_csv_writer, safe_sheet_append, DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_limit, redact_export_value, resolve_export_limit
from t2c_data.features.governance import (
    apply_metadata_change_request,
    apply_governance_policy_recommendations,
    approve_metadata_change_request,
    execute_governance_assistant_action,
    create_metadata_change_request,
    get_governance_campaigns,
    get_governance_campaign_queue,
    get_governance_classification_review,
    get_governance_critical_changes,
    get_governance_notification_summary,
    get_governance_notifications,
    get_governance_pending_center,
    get_governance_pending_center_campaigns,
    get_governance_pending_center_queue,
    get_governance_pending_center_summary,
    get_governance_pending_center_summary_light,
    get_ownership_summary,
    get_metadata_change_request,
    get_governance_playbooks,
    get_governance_recommendation_context,
    get_governance_recommendations,
    get_governance_review_summary,
    list_asset_slas,
    list_metadata_change_requests,
    mark_owner_review,
    mark_privacy_review,
    get_ownership_export_rows,
    promote_governance_classification_review_tables,
    reject_metadata_change_request,
    review_metadata_change_request,
    set_governance_recommendation_feedback,
    resolve_governance_recommendations,
    upsert_asset_sla,
)
from t2c_data.features.governance.column_classification import (
    column_classification_payload,
    column_classification_version_payload,
    load_column_classification_history,
    load_column_classifications,
    record_column_classification_decision,
)
from t2c_data.features.governance.intelligence_feed import (
    build_governance_intelligence_feed,
    build_governance_intelligence_timeline,
)
from t2c_data.features.timeline.service import get_governance_timeline, record_timeline_episode_action
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, TableEntity
from t2c_data.models.classification import ColumnClassification
from t2c_data.schemas.governance import (
    GovernanceCampaignsOut,
    GovernanceCampaignQueueOut,
    ClassificationReviewOut,
    ClassificationReviewBatchPromoteIn,
    ClassificationReviewBatchPromoteOut,
    AssetSlaIn,
    AssetSlaListOut,
    AssetSlaOut,
    GovernanceCriticalChangesOut,
    GovernancePlaybooksOut,
    GovernanceRecommendationFeedbackIn,
    GovernanceRecommendationFeedbackOut,
    GovernanceNotificationListOut,
    GovernanceNotificationSummaryOut,
    GovernancePendingCenterOut,
    GovernancePendingCenterCampaignsOut,
    GovernancePendingCenterQueueOut,
    GovernancePendingCenterSummaryLightOut,
    GovernancePendingCenterSummaryOut,
    GovernanceRecommendationContextOut,
    GovernanceAssistantActionIn,
    GovernanceAssistantActionOut,
    GovernanceRecommendationResolutionIn,
    GovernanceRecommendationResolutionOut,
    GovernanceRecommendationsOut,
    GovernanceReviewMarkOut,
    GovernanceReviewSummaryOut,
    MetadataChangeRequestIn,
    MetadataChangeRequestListOut,
    MetadataChangeRequestOut,
    MetadataChangeRequestTransitionIn,
    ColumnClassificationHistoryOut,
    ColumnClassificationOut,
    ColumnClassificationReviewIn,
    ColumnClassificationVersionOut,
)
from t2c_data.schemas.governance_intelligence import (
    GovernanceIntelligenceFeedOut,
    GovernanceIntelligenceTimelineOut,
)
from t2c_data.schemas.platform import IntegrationSyncJobOut
from t2c_data.schemas.data_owner import OwnershipSummaryOut
from t2c_data.services.audit import AuditFieldChange, log_field_changes, request_audit_kwargs
from t2c_data.schemas.timeline import TimelineEpisodeActionIn, TimelineEpisodeActionOut, TimelinePageOut

router = APIRouter(prefix="/governance", tags=["governance"])


@router.get("/owners/summary", response_model=OwnershipSummaryOut)
def governance_owners_summary(
    query: str | None = Query(default=None),
    status: str | None = Query(default=None),
    area: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    include_unowned: bool = Query(default=True),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=200),
    critical_only: bool = Query(default=False),
    privacy_risk_only: bool = Query(default=False),
    certification_pending_only: bool = Query(default=False),
    schema_name: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> OwnershipSummaryOut:
    return get_ownership_summary(
        db,
        current_user=current_user,
        query=query,
        status=status,
        area=area,
        owner_id=owner_id,
        include_unowned=include_unowned,
        page=page,
        page_size=page_size,
        critical_only=critical_only,
        privacy_risk_only=privacy_risk_only,
        certification_pending_only=certification_pending_only,
        schema_name=schema_name,
        database_name=database_name,
    )


def build_ownership_export_artifact(
    db: Session,
    *,
    current_user: User,
    query: str | None = None,
    status: str | None = None,
    area: str | None = None,
    owner_id: int | None = None,
    include_unowned: bool = True,
    risk_level: str | None = None,
    schema_name: str | None = None,
    database_name: str | None = None,
    **_: Any,
) -> ExportArtifactResult:
    owner_rows, unowned_assets = get_ownership_export_rows(
        db,
        current_user=current_user,
        query=query,
        status=status,
        area=area,
        owner_id=owner_id,
        include_unowned=include_unowned,
        risk_level=risk_level,
        schema_name=schema_name,
        database_name=database_name,
    )
    owner_export_limit = resolve_export_limit(source_module="governance", entity_type="ownership_export")
    owner_rows, owner_rows_truncated = enforce_export_limit(owner_rows, limit=owner_export_limit)
    unowned_assets, unowned_truncated = enforce_export_limit(unowned_assets, limit=owner_export_limit)

    headers = [
        "row_type",
        "owner_id",
        "owner_name",
        "owner_email",
        "area",
        "status",
        "updated_at",
        "asset_id",
        "asset_name",
        "connection",
        "database",
        "schema",
        "criticality",
        "certification_status",
        "privacy_signal",
        "open_incidents",
        "asset_recommended_action",
        "asset_count",
        "certified_assets",
        "certification_pending_assets",
        "eligible_assets",
        "not_eligible_assets",
        "in_review_assets",
        "rejected_assets",
        "revalidation_assets",
        "dq_monitored_assets",
        "dq_unmonitored_assets",
        "average_quality_score",
        "average_governance_score",
        "average_readiness_score",
        "privacy_pending_assets",
        "personal_data_assets",
        "sensitive_data_assets",
        "restricted_assets",
        "possible_personal_data_assets",
        "without_legal_basis_assets",
        "without_privacy_review_assets",
        "assets_without_description",
        "assets_without_tags",
        "assets_without_terms",
        "assets_without_sla",
        "risk_level",
        "main_blocker",
        "recommended_action",
    ]
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(headers)

    def _empty_row() -> dict[str, str]:
        return {header: "" for header in headers}

    for owner in owner_rows:
        row = _empty_row()
        row.update(
            {
                "row_type": "owner",
                "owner_id": str(owner.id),
                "owner_name": redact_export_value(owner.name, field_name="owner_name"),
                "owner_email": redact_export_value(owner.email, field_name="owner_email"),
                "area": owner.area or "",
                "status": owner.status,
                "updated_at": owner.updated_at.isoformat() if owner.updated_at else "",
                "asset_count": str(owner.asset_count),
                "certified_assets": str(owner.certified_assets),
                "certification_pending_assets": str(owner.certification_pending_assets),
                "eligible_assets": str(owner.eligible_assets),
                "not_eligible_assets": str(owner.not_eligible_assets),
                "in_review_assets": str(owner.in_review_assets),
                "rejected_assets": str(owner.rejected_assets),
                "revalidation_assets": str(owner.revalidation_pending_assets),
                "dq_monitored_assets": str(owner.dq_monitored_assets),
                "dq_unmonitored_assets": str(owner.dq_unmonitored_assets),
                "average_quality_score": "" if owner.average_quality_score is None else str(owner.average_quality_score),
                "average_governance_score": "" if owner.average_governance_score is None else str(owner.average_governance_score),
                "average_readiness_score": "" if owner.average_readiness_score is None else str(owner.average_readiness_score),
                "privacy_pending_assets": str(owner.privacy_pending_assets),
                "personal_data_assets": str(owner.personal_data_assets),
                "sensitive_data_assets": str(owner.sensitive_data_assets),
                "restricted_assets": str(owner.restricted_assets),
                "possible_personal_data_assets": str(owner.possible_personal_data_assets),
                "without_legal_basis_assets": str(owner.assets_without_legal_basis),
                "without_privacy_review_assets": str(owner.assets_without_privacy_review),
                "assets_without_description": str(owner.assets_without_description),
                "assets_without_tags": str(owner.assets_without_tags),
                "assets_without_terms": str(owner.assets_without_terms),
                "assets_without_sla": str(owner.assets_without_sla),
                "risk_level": owner.risk_level,
                "main_blocker": owner.main_blocker,
                "recommended_action": owner.recommended_action,
            }
        )
        writer.writerow([row[header] for header in headers])

    for asset in unowned_assets:
        row = _empty_row()
        row.update(
            {
                "row_type": "unowned_asset",
                "asset_id": str(asset.id),
                "asset_name": asset.name,
                "connection": asset.connection_name or "",
                "database": asset.database_name or "",
                "schema": asset.schema_name or "",
                "criticality": asset.criticality or "",
                "certification_status": asset.certification_status,
                "privacy_signal": asset.privacy_signal or "",
                "open_incidents": str(asset.open_incidents),
                "asset_recommended_action": asset.recommended_action or "",
                "risk_level": getattr(asset, "risk_level", None) or asset.criticality or "",
                "main_blocker": asset.privacy_signal or "",
                "recommended_action": asset.recommended_action or "",
            }
        )
        writer.writerow([row[header] for header in headers])

    payload = buffer.getvalue().encode("utf-8")
    return ExportArtifactResult(
        payload=payload,
        filename="ownership_export.csv",
        content_type="text/csv; charset=utf-8",
        row_count=len(owner_rows) + len(unowned_assets),
        truncated=owner_rows_truncated or unowned_truncated,
        export_format="csv",
    )


@router.get("/owners/export.csv", response_model=IntegrationSyncJobOut, status_code=status.HTTP_202_ACCEPTED)
def governance_owners_export_csv(
    request: Request,
    query: str | None = Query(default=None),
    status: str | None = Query(default=None),
    area: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    include_unowned: bool = Query(default=True),
    risk_level: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("owners.export")),
) -> StreamingResponse:
    job = enqueue_export_job(
        db,
        job_type="governance.owners.csv",
        requested_by_user_id=current_user.id,
        payload_json={
            "query": query,
            "status": status,
            "area": area,
            "owner_id": owner_id,
            "include_unowned": include_unowned,
            "risk_level": risk_level,
            "schema_name": schema_name,
            "database_name": database_name,
            "export_format": "csv",
        },
        context_json={
            "filters": {
                "query": query,
                "status": status,
                "area": area,
                "owner_id": owner_id,
                "include_unowned": include_unowned,
                "risk_level": risk_level,
                "schema_name": schema_name,
                "database_name": database_name,
            }
        },
    )
    register_export_request_audit(
        db,
        request=request,
        current_user=current_user,
        job=job,
        action="governance.owners.export_requested",
        entity_type="ownership_export",
        source_module="governance",
        export_format="csv",
        filters={
            "query": query,
            "status": status,
            "area": area,
            "owner_id": owner_id,
            "include_unowned": include_unowned,
            "risk_level": risk_level,
            "schema_name": schema_name,
            "database_name": database_name,
        },
    )
    return serialize_export_job(job, request=request)
    owner_rows, unowned_assets = get_ownership_export_rows(
        db,
        current_user=current_user,
        query=query,
        status=status,
        area=area,
        owner_id=owner_id,
        include_unowned=include_unowned,
        risk_level=risk_level,
        schema_name=schema_name,
        database_name=database_name,
    )
    owner_export_limit = resolve_export_limit(source_module="governance", entity_type="ownership_export")
    owner_rows, owner_rows_truncated = enforce_export_limit(owner_rows, limit=owner_export_limit)
    unowned_assets, unowned_truncated = enforce_export_limit(unowned_assets, limit=owner_export_limit)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="governance.owners.export_csv",
        entity_type="ownership_export",
        source_module="governance",
        row_count=len(owner_rows) + len(unowned_assets),
        truncated=owner_rows_truncated or unowned_truncated,
        limit=owner_export_limit,
        filters={
            "query": query,
            "status": status,
            "area": area,
            "owner_id": owner_id,
            "include_unowned": include_unowned,
            "risk_level": risk_level,
            "schema_name": schema_name,
            "database_name": database_name,
        },
    )

    headers = [
        "row_type",
        "owner_id",
        "owner_name",
        "owner_email",
        "area",
        "status",
        "updated_at",
        "asset_id",
        "asset_name",
        "connection",
        "database",
        "schema",
        "criticality",
        "certification_status",
        "privacy_signal",
        "open_incidents",
        "asset_recommended_action",
        "asset_count",
        "certified_assets",
        "certification_pending_assets",
        "eligible_assets",
        "not_eligible_assets",
        "in_review_assets",
        "rejected_assets",
        "revalidation_assets",
        "dq_monitored_assets",
        "dq_unmonitored_assets",
        "average_quality_score",
        "average_governance_score",
        "average_readiness_score",
        "privacy_pending_assets",
        "personal_data_assets",
        "sensitive_data_assets",
        "restricted_assets",
        "possible_personal_data_assets",
        "without_legal_basis_assets",
        "without_privacy_review_assets",
        "assets_without_description",
        "assets_without_tags",
        "assets_without_terms",
        "assets_without_sla",
        "risk_level",
        "main_blocker",
        "recommended_action",
    ]
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(headers)

    def _empty_row() -> dict[str, str]:
        return {header: "" for header in headers}

    for owner in owner_rows:
        row = _empty_row()
        row.update(
            {
                "row_type": "owner",
                "owner_id": str(owner.id),
                "owner_name": redact_export_value(owner.name, field_name="owner_name"),
                "owner_email": redact_export_value(owner.email, field_name="owner_email"),
                "area": owner.area or "",
                "status": owner.status,
                "updated_at": owner.updated_at.isoformat() if owner.updated_at else "",
                "asset_count": str(owner.asset_count),
                "certified_assets": str(owner.certified_assets),
                "certification_pending_assets": str(owner.certification_pending_assets),
                "eligible_assets": str(owner.eligible_assets),
                "not_eligible_assets": str(owner.not_eligible_assets),
                "in_review_assets": str(owner.in_review_assets),
                "rejected_assets": str(owner.rejected_assets),
                "revalidation_assets": str(owner.revalidation_pending_assets),
                "dq_monitored_assets": str(owner.dq_monitored_assets),
                "dq_unmonitored_assets": str(owner.dq_unmonitored_assets),
                "average_quality_score": "" if owner.average_quality_score is None else str(owner.average_quality_score),
                "average_governance_score": "" if owner.average_governance_score is None else str(owner.average_governance_score),
                "average_readiness_score": "" if owner.average_readiness_score is None else str(owner.average_readiness_score),
                "privacy_pending_assets": str(owner.privacy_pending_assets),
                "personal_data_assets": str(owner.personal_data_assets),
                "sensitive_data_assets": str(owner.sensitive_data_assets),
                "restricted_assets": str(owner.restricted_assets),
                "possible_personal_data_assets": str(owner.possible_personal_data_assets),
                "without_legal_basis_assets": str(owner.assets_without_legal_basis),
                "without_privacy_review_assets": str(owner.assets_without_privacy_review),
                "assets_without_description": str(owner.assets_without_description),
                "assets_without_tags": str(owner.assets_without_tags),
                "assets_without_terms": str(owner.assets_without_terms),
                "assets_without_sla": str(owner.assets_without_sla),
                "risk_level": owner.risk_level,
                "main_blocker": owner.main_blocker or "",
                "recommended_action": owner.recommended_action or "",
            }
        )
        writer.writerow([row[header] for header in headers])

    for asset in unowned_assets:
        row = _empty_row()
        row.update(
            {
                "row_type": "asset",
                "asset_id": str(asset.id),
                "asset_name": asset.name,
                "connection": asset.connection_name,
                "database": asset.database_name,
                "schema": asset.schema_name,
                "criticality": asset.criticality or "",
                "certification_status": asset.certification_status,
                "privacy_signal": asset.privacy_signal or "",
                "open_incidents": str(asset.open_incidents),
                "asset_recommended_action": asset.recommended_action,
                "recommended_action": asset.recommended_action,
                "risk_level": "high" if asset.privacy_signal or (asset.criticality or "").lower() in {"critical", "high"} else "medium",
            }
        )
        writer.writerow([row[header] for header in headers])

    payload = buffer.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        iter([payload]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="ownership_export.csv"'},
    )


@router.get("/reviews/summary", response_model=GovernanceReviewSummaryOut)
def governance_review_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceReviewSummaryOut:
    return GovernanceReviewSummaryOut(**get_governance_review_summary(db, current_user=current_user))


@router.get("/classification-review", response_model=ClassificationReviewOut)
def governance_classification_review(
    q: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    entity_level: str | None = Query(default=None),
    review_status: str | None = Query(default=None),
    source: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    tag: str | None = Query(default=None),
    min_confidence: int | None = Query(default=None, ge=0, le=100),
    max_confidence: int | None = Query(default=None, ge=0, le=100),
    contains_pii: bool | None = Query(default=None),
    contains_sensitive: bool | None = Query(default=None),
    contains_critical: bool | None = Query(default=None),
    sort_by: str = Query(default="risk_desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ClassificationReviewOut:
    return ClassificationReviewOut(
        **get_governance_classification_review(
            db,
            current_user=current_user,
            q=q,
            kind=kind,
            entity_level=entity_level,
            review_status=review_status,
            source=source,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            owner=owner,
            tag=tag,
            min_confidence=min_confidence,
            max_confidence=max_confidence,
            contains_pii=contains_pii,
            contains_sensitive=contains_sensitive,
            contains_critical=contains_critical,
            sort_by=sort_by,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/column-classifications", response_model=list[ColumnClassificationOut])
def governance_column_classifications(
    table_id: int | None = Query(default=None, ge=1),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[ColumnClassificationOut]:
    if table_id is None:
        rows = db.scalars(
            select(ColumnClassification).order_by(ColumnClassification.reviewed_at.desc().nulls_last(), ColumnClassification.updated_at.desc())
        ).all()
    else:
        rows = db.scalars(
            select(ColumnClassification)
            .join(ColumnEntity, ColumnEntity.id == ColumnClassification.column_id)
            .where(ColumnEntity.table_id == table_id)
            .order_by(ColumnEntity.ordinal_position.asc(), ColumnClassification.updated_at.desc())
        ).all()
    return [ColumnClassificationOut(**column_classification_payload(row)) for row in rows]


@router.get("/column-classifications/{column_id}/history", response_model=ColumnClassificationHistoryOut)
def governance_column_classification_history(
    column_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ColumnClassificationHistoryOut:
    current = db.scalar(select(ColumnClassification).where(ColumnClassification.column_id == column_id))
    history = load_column_classification_history(db, column_id)
    return ColumnClassificationHistoryOut(
        generated_at=datetime.now(timezone.utc),
        column_id=column_id,
        current=ColumnClassificationOut(**column_classification_payload(current)) if current is not None else None,
        items=[ColumnClassificationVersionOut(**column_classification_version_payload(row)) for row in history],
    )


@router.post("/column-classifications/{column_id}/review", response_model=ColumnClassificationHistoryOut)
def governance_column_classification_review(
    column_id: int,
    payload: ColumnClassificationReviewIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ColumnClassificationHistoryOut:
    record_column_classification_decision(
        db,
        column_id=column_id,
        taxonomy_key=payload.taxonomy_key,
        source_kind=payload.source_kind,
        confidence_score=payload.confidence_score,
        decision_status=payload.decision_status,
        evidence_json=payload.evidence_json,
        notes=payload.notes,
        reviewed_by_user_id=current_user.id,
        reviewed_at=datetime.now(timezone.utc),
        persist_current=payload.decision_status != "rejected",
    )
    db.commit()
    current = db.scalar(select(ColumnClassification).where(ColumnClassification.column_id == column_id))
    history = load_column_classification_history(db, column_id)
    return ColumnClassificationHistoryOut(
        generated_at=datetime.now(timezone.utc),
        column_id=column_id,
        current=ColumnClassificationOut(**column_classification_payload(current)) if current is not None else None,
        items=[ColumnClassificationVersionOut(**column_classification_version_payload(row)) for row in history],
    )


@router.get("/playbooks", response_model=GovernancePlaybooksOut)
def governance_playbooks(
    table_id: int | None = Query(default=None),
    include_inactive: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> GovernancePlaybooksOut:
    return GovernancePlaybooksOut(**get_governance_playbooks(db, table_id=table_id, include_inactive=include_inactive))


@router.get("/change-management/asset-slas", response_model=AssetSlaListOut)
def governance_change_management_list_asset_slas(
    asset_type: str = Query(default="table"),
    asset_id: int = Query(default=1, ge=1),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner", "viewer")),
) -> AssetSlaListOut:
    return AssetSlaListOut(**list_asset_slas(db, asset_type=asset_type, asset_id=asset_id))


@router.post("/change-management/asset-slas", response_model=AssetSlaOut)
def governance_change_management_upsert_asset_sla(
    payload: AssetSlaIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> AssetSlaOut:
    result = upsert_asset_sla(
        db,
        asset_type=payload.asset_type,
        asset_id=payload.asset_id,
        sla_kind=payload.sla_kind,
        sla_hours=payload.sla_hours,
        asset_status=payload.status,
        source_kind=payload.source_kind,
        source_ref=payload.source_ref,
        context_json=payload.context_json,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return AssetSlaOut(**result)


@router.get("/change-management/requests", response_model=MetadataChangeRequestListOut)
def governance_change_management_list_requests(
    asset_type: str | None = Query(default=None),
    asset_id: int | None = Query(default=None, ge=1),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner", "viewer")),
) -> MetadataChangeRequestListOut:
    return MetadataChangeRequestListOut(
        **list_metadata_change_requests(
            db,
            asset_type=asset_type,
            asset_id=asset_id,
            status=status,
            page=page,
            page_size=page_size,
        )
    )


@router.post("/change-management/requests", response_model=MetadataChangeRequestOut)
def governance_change_management_create_request(
    payload: MetadataChangeRequestIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> MetadataChangeRequestOut:
    result = create_metadata_change_request(
        db,
        asset_type=payload.asset_type,
        asset_id=payload.asset_id,
        change_kind=payload.change_kind,
        title=payload.title,
        description=payload.description,
        policy_rule_key=payload.policy_rule_key,
        recommendation_id=payload.recommendation_id,
        current_value_json=payload.current_value_json,
        proposed_value_json=payload.proposed_value_json,
        context_json=payload.context_json,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return MetadataChangeRequestOut(**result)


@router.get("/change-management/requests/{request_ref}", response_model=MetadataChangeRequestOut)
def governance_change_management_get_request(
    request_ref: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner", "viewer")),
) -> MetadataChangeRequestOut:
    return MetadataChangeRequestOut(**get_metadata_change_request(db, request_ref=request_ref))


@router.post("/change-management/requests/{request_ref}/review", response_model=MetadataChangeRequestOut)
def governance_change_management_review_request(
    request_ref: str,
    payload: MetadataChangeRequestTransitionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> MetadataChangeRequestOut:
    result = review_metadata_change_request(
        db,
        request_ref=request_ref,
        comment=payload.comment,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return MetadataChangeRequestOut(**result)


@router.post("/change-management/requests/{request_ref}/approve", response_model=MetadataChangeRequestOut)
def governance_change_management_approve_request(
    request_ref: str,
    payload: MetadataChangeRequestTransitionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> MetadataChangeRequestOut:
    result = approve_metadata_change_request(
        db,
        request_ref=request_ref,
        comment=payload.comment,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return MetadataChangeRequestOut(**result)


@router.post("/change-management/requests/{request_ref}/apply", response_model=MetadataChangeRequestOut)
def governance_change_management_apply_request(
    request_ref: str,
    payload: MetadataChangeRequestTransitionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> MetadataChangeRequestOut:
    result = apply_metadata_change_request(
        db,
        request_ref=request_ref,
        comment=payload.comment,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return MetadataChangeRequestOut(**result)


@router.post("/change-management/requests/{request_ref}/reject", response_model=MetadataChangeRequestOut)
def governance_change_management_reject_request(
    request_ref: str,
    payload: MetadataChangeRequestTransitionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> MetadataChangeRequestOut:
    result = reject_metadata_change_request(
        db,
        request_ref=request_ref,
        comment=payload.comment,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return MetadataChangeRequestOut(**result)


@router.post("/classification-review/batch/promote", response_model=ClassificationReviewBatchPromoteOut)
def governance_classification_review_batch_promote(
    payload: ClassificationReviewBatchPromoteIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ClassificationReviewBatchPromoteOut:
    result = promote_governance_classification_review_tables(
        db,
        table_ids=payload.table_ids,
        current_user=current_user,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return ClassificationReviewBatchPromoteOut(**result)


@router.get("/pending-center", response_model=GovernancePendingCenterOut)
def governance_pending_center(
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=5, ge=1, le=200),
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernancePendingCenterOut:
    return GovernancePendingCenterOut(
        **get_governance_pending_center(
            db,
            q=q,
            severity=severity,
            origin=origin,
            owner_id=owner_id,
            owner=owner,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            status_filter=status_filter,
            page=page,
            page_size=page_size,
            current_user=user,
        )
    )


@router.get("/pending-center/summary", response_model=GovernancePendingCenterSummaryOut)
def governance_pending_center_summary(
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernancePendingCenterSummaryOut:
    return GovernancePendingCenterSummaryOut(
        **get_governance_pending_center_summary(
            db,
            q=q,
            severity=severity,
            origin=origin,
            owner_id=owner_id,
            owner=owner,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            status_filter=status_filter,
            current_user=user,
        )
    )


@router.get("/pending-center/summary-light", response_model=GovernancePendingCenterSummaryLightOut)
def governance_pending_center_summary_light(
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernancePendingCenterSummaryLightOut:
    return GovernancePendingCenterSummaryLightOut(
        **get_governance_pending_center_summary_light(
            db,
            q=q,
            severity=severity,
            origin=origin,
            owner_id=owner_id,
            owner=owner,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            status_filter=status_filter,
            current_user=user,
        )
    )


@router.get("/intelligence/feed", response_model=GovernanceIntelligenceFeedOut)
def governance_intelligence_feed(
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer", "stewardship", "data_owner")),
) -> GovernanceIntelligenceFeedOut:
    """Decision-oriented feed for the Governance Intelligence page.

    Reuses the executive dashboard intelligence and surfaces the most at-risk
    assets, action tracks and next-best-actions, prioritizing tables consumed by
    Metabase dashboards.
    """
    return GovernanceIntelligenceFeedOut(
        **build_governance_intelligence_feed(db, current_user=user)
    )


@router.get("/intelligence/timeline", response_model=GovernanceIntelligenceTimelineOut)
def governance_intelligence_timeline(
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer", "stewardship", "data_owner")),
) -> GovernanceIntelligenceTimelineOut:
    """Recent correlated episodes (intelligent timeline) for the decision center."""
    return GovernanceIntelligenceTimelineOut(
        **build_governance_intelligence_timeline(db, current_user=user)
    )


@router.get("/pending-center/campaigns", response_model=GovernancePendingCenterCampaignsOut)
def governance_pending_center_campaigns(
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernancePendingCenterCampaignsOut:
    return GovernancePendingCenterCampaignsOut(
        **get_governance_pending_center_campaigns(
            db,
            q=q,
            severity=severity,
            origin=origin,
            owner_id=owner_id,
            owner=owner,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            status_filter=status_filter,
            current_user=user,
        )
    )


@router.get("/pending-center/queue", response_model=GovernancePendingCenterQueueOut)
def governance_pending_center_queue(
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=5, ge=1, le=200),
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernancePendingCenterQueueOut:
    return GovernancePendingCenterQueueOut(
        **get_governance_pending_center_queue(
            db,
            q=q,
            severity=severity,
            origin=origin,
            owner_id=owner_id,
            owner=owner,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            status_filter=status_filter,
            page=page,
            page_size=page_size,
            current_user=user,
        )
    )


@router.get("/notifications", response_model=GovernanceNotificationListOut)
def governance_notifications(
    status_filter: str = Query(default="active", alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceNotificationListOut:
    return GovernanceNotificationListOut(
        **get_governance_notifications(
            db,
            status_filter=status_filter,
            limit=limit,
        )
    )


@router.get("/notifications/summary", response_model=GovernanceNotificationSummaryOut)
def governance_notification_summary(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceNotificationSummaryOut:
    return GovernanceNotificationSummaryOut(**get_governance_notification_summary(db))


@router.get("/timeline", response_model=TimelinePageOut)
def governance_timeline(
    q: str | None = Query(default=None),
    source: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    certification_status: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    category: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    manual_only: bool = Query(default=False),
    automatic_only: bool = Query(default=False),
    contains_pii: bool | None = Query(default=None),
    contains_sensitive: bool | None = Query(default=None),
    contains_critical: bool | None = Query(default=None),
    open_incidents: bool | None = Query(default=None),
    dq_recent: bool | None = Query(default=None),
    table_id: int | None = Query(default=None, ge=1),
    column_id: int | None = Query(default=None, ge=1),
    episode_status: str | None = Query(default=None),
    episode_type: str | None = Query(default=None),
    min_importance_score: int | None = Query(default=None, ge=0, le=100),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    episode_page: int = Query(default=1, ge=1),
    episode_page_size: int = Query(default=12, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TimelinePageOut:
    return get_governance_timeline(
        db,
        current_user=current_user,
        q=q,
        source=source,
        datasource=datasource,
        schema_name=schema_name,
        owner=owner,
        certification_status=certification_status,
        event_type=event_type,
        category=category,
        severity=severity,
        manual_only=manual_only,
        automatic_only=automatic_only,
        contains_pii=contains_pii,
        contains_sensitive=contains_sensitive,
        contains_critical=contains_critical,
        open_incidents=open_incidents,
        dq_recent=dq_recent,
        table_id=table_id,
        column_id=column_id,
        episode_status=episode_status,
        episode_type=episode_type,
        min_importance_score=min_importance_score,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
        episode_page=episode_page,
        episode_page_size=episode_page_size,
    )


@router.post("/timeline/episodes/actions", response_model=TimelineEpisodeActionOut, status_code=status.HTTP_201_CREATED)
def governance_timeline_episode_action(
    payload: TimelineEpisodeActionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "stewardship", "data_owner")),
) -> TimelineEpisodeActionOut:
    action = record_timeline_episode_action(
        db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return action


@router.get("/recommendations", response_model=GovernanceRecommendationsOut)
def governance_recommendations(
    q: str | None = Query(default=None),
    status: str | None = Query(default="open"),
    severity: str | None = Query(default=None),
    impact: str | None = Query(default=None),
    source: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    min_confidence: int | None = Query(default=None, ge=0, le=100),
    max_confidence: int | None = Query(default=None, ge=0, le=100),
    policy_driven: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=12, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceRecommendationsOut:
    return GovernanceRecommendationsOut(
        **get_governance_recommendations(
            db,
            q=q,
            status=status,
            severity=severity,
            impact=impact,
            source=source,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            owner=owner,
            min_confidence=min_confidence,
            max_confidence=max_confidence,
            policy_driven=policy_driven,
            page=page,
            page_size=page_size,
            current_user=current_user,
        )
    )


@router.get("/recommendations/{recommendation_ref}/context", response_model=GovernanceRecommendationContextOut)
def governance_recommendation_context(
    recommendation_ref: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceRecommendationContextOut:
    try:
        return GovernanceRecommendationContextOut(
            **get_governance_recommendation_context(db, recommendation_ref=recommendation_ref, current_user=current_user)
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/recommendations/{recommendation_ref}/feedback", response_model=GovernanceRecommendationFeedbackOut)
def governance_recommendation_feedback(
    recommendation_ref: str,
    payload: GovernanceRecommendationFeedbackIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceRecommendationFeedbackOut:
    try:
        return GovernanceRecommendationFeedbackOut(
            **set_governance_recommendation_feedback(
                db,
                recommendation_ref=recommendation_ref,
                feedback_rating=payload.feedback_rating,
                feedback_note=payload.feedback_note,
                actor_user_id=current_user.id,
                request_audit=request_audit_kwargs(request, current_user),
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/recommendations/batch/resolve", response_model=GovernanceRecommendationResolutionOut)
def governance_recommendations_batch_resolve(
    payload: GovernanceRecommendationResolutionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> GovernanceRecommendationResolutionOut:
    result = resolve_governance_recommendations(
        db,
        recommendation_ids=payload.recommendation_ids,
        resolution_action=payload.resolution_action,
        resolution_note=payload.resolution_note,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return GovernanceRecommendationResolutionOut(**result)


@router.post("/recommendations/batch/apply-policy", response_model=GovernanceRecommendationResolutionOut)
def governance_recommendations_batch_apply_policy(
    payload: GovernanceRecommendationResolutionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> GovernanceRecommendationResolutionOut:
    result = apply_governance_policy_recommendations(
        db,
        recommendation_ids=payload.recommendation_ids,
        resolution_note=payload.resolution_note,
        actor_user_id=current_user.id,
        request_audit=request_audit_kwargs(request, current_user),
    )
    db.commit()
    return GovernanceRecommendationResolutionOut(**result)


@router.post("/recommendations/{recommendation_ref}/assistant/execute", response_model=GovernanceAssistantActionOut)
def governance_recommendation_assistant_execute(
    recommendation_ref: str,
    payload: GovernanceAssistantActionIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceAssistantActionOut:
    try:
        return GovernanceAssistantActionOut(
            **execute_governance_assistant_action(
                db,
                recommendation_ref=recommendation_ref,
                tool_key=payload.tool_key,
                confirm=payload.confirm,
                resolution_note=payload.resolution_note,
                actor_user_id=current_user.id,
                request_audit=request_audit_kwargs(request, current_user),
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


@router.get("/pending-center/export.csv", response_model=None)
def export_governance_pending_center_csv(
    request: Request,
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("governance:export")),
) -> StreamingResponse:
    pending_center_limit = resolve_export_limit(source_module="governance", entity_type="governance_pending_center")
    payload = get_governance_pending_center(
        db,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        page=1,
        page_size=pending_center_limit,
        current_user=user,
    )
    audit_export_event(
        db,
        request=request,
        current_user=user,
        action="governance.pending_center.export_csv",
        entity_type="governance_pending_center",
        source_module="governance",
        row_count=len(payload["items"]),
        truncated=payload["total"] > len(payload["items"]),
        limit=pending_center_limit,
        filters={
            "q": q,
            "severity": severity,
            "origin": origin,
            "owner_id": owner_id,
            "owner": owner,
            "datasource": datasource,
            "schema_name": schema_name,
            "domain": domain,
            "status": status_filter,
        },
    )
    buffer = StringIO()
    writer = safe_csv_writer(buffer)
    writer.writerow(
        [
            "pendencia",
            "severidade",
            "origem",
            "status",
            "ativo",
            "fonte",
            "banco",
            "schema",
            "owner",
            "score_governanca",
            "faixa_governanca",
            "detectada_em",
            "aging_dias",
            "sla_dias",
            "vencimento",
            "status_sla",
            "contexto",
            "acao",
            "href_acao",
            "href_explorer",
        ]
    )
    for item in payload["items"]:
        writer.writerow(
            [
                item["title"],
                item["severity_label"],
                item["origin_label"],
                item["status_label"],
                item["table_fqn"],
                item["datasource_name"],
                item["database_name"],
                item["schema_name"],
                item["owner_name"],
                item["governance_score"]["score"],
                item["governance_score"]["label"],
                item["detected_at"],
                item["aging_days"],
                item["sla_days"] or "",
                item["due_at"] or "",
                item["sla_status_label"] or "",
                item["context_value"] or "",
                item["action_label"],
                item["action_href"],
                item["links"]["explorer"],
            ]
        )
    return StreamingResponse(
        iter([buffer.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="governance_pending_center.csv"'},
    )


@router.get("/pending-center/export.xlsx", response_model=None)
def export_governance_pending_center_xlsx(
    request: Request,
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("governance:export")),
) -> StreamingResponse:
    from openpyxl import Workbook

    pending_center_limit = resolve_export_limit(source_module="governance", entity_type="governance_pending_center")
    payload = get_governance_pending_center(
        db,
        q=q,
        severity=severity,
        origin=origin,
        owner_id=owner_id,
        owner=owner,
        datasource=datasource,
        schema_name=schema_name,
        domain=domain,
        status_filter=status_filter,
        page=1,
        page_size=pending_center_limit,
        current_user=user,
    )
    audit_export_event(
        db,
        request=request,
        current_user=user,
        action="governance.pending_center.export_xlsx",
        entity_type="governance_pending_center",
        source_module="governance",
        row_count=len(payload["items"]),
        truncated=payload["total"] > len(payload["items"]),
        limit=pending_center_limit,
        filters={
            "q": q,
            "severity": severity,
            "origin": origin,
            "owner_id": owner_id,
            "owner": owner,
            "datasource": datasource,
            "schema_name": schema_name,
            "domain": domain,
            "status": status_filter,
        },
    )
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Pendencias"
    safe_sheet_append(sheet, 
        [
            "Pendência",
            "Severidade",
            "Origem",
            "Status",
            "Ativo",
            "Fonte",
            "Banco",
            "Schema",
            "Owner",
            "Score de governança",
            "Faixa de maturidade",
            "Detectada em",
            "Aging (dias)",
            "SLA (dias)",
            "Vencimento",
            "Status do SLA",
            "Contexto",
            "Ação",
            "Link da ação",
            "Explorer",
        ]
    )
    for item in payload["items"]:
        safe_sheet_append(sheet, 
            [
                item["title"],
                item["severity_label"],
                item["origin_label"],
                item["status_label"],
                item["table_fqn"],
                item["datasource_name"],
                item["database_name"],
                item["schema_name"],
                item["owner_name"],
                item["governance_score"]["score"],
                item["governance_score"]["label"],
                item["detected_at"],
                item["aging_days"],
                item["sla_days"] or "",
                item["due_at"] or "",
                item["sla_status_label"] or "",
                redact_export_value(item["context_value"], field_name="context_value"),
                item["action_label"],
                item["action_href"],
                item["links"]["explorer"],
            ]
        )
    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="governance_pending_center.xlsx"'},
    )


@router.get("/campaigns", response_model=GovernanceCampaignsOut)
def governance_campaigns(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceCampaignsOut:
    return GovernanceCampaignsOut(**get_governance_campaigns(db, current_user=current_user))


@router.get("/campaigns/{campaign_key}/items", response_model=GovernanceCampaignQueueOut)
def governance_campaign_queue(
    campaign_key: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceCampaignQueueOut:
    return GovernanceCampaignQueueOut(**get_governance_campaign_queue(db, campaign_key=campaign_key, page=page, page_size=page_size, current_user=current_user))


@router.get("/campaigns/{campaign_key}/export.csv", response_model=None)
def export_governance_campaign_csv(
    request: Request,
    campaign_key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("governance:export")),
) -> StreamingResponse:
    campaign_export_limit = resolve_export_limit(source_module="governance", entity_type="governance_campaign")
    payload = get_governance_campaign_queue(db, campaign_key=campaign_key, page=1, page_size=campaign_export_limit, current_user=current_user)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="governance.campaign.export_csv",
        entity_type="governance_campaign",
        source_module="governance",
        row_count=len(payload["items"]),
        truncated=payload["total"] > len(payload["items"]),
        limit=campaign_export_limit,
        filters={"campaign_key": campaign_key},
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
        headers={"Content-Disposition": f'attachment; filename="governance_{campaign_key}.csv"'},
    )


@router.get("/campaigns/{campaign_key}/export.xlsx", response_model=None)
def export_governance_campaign_xlsx(
    request: Request,
    campaign_key: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permission("governance:export")),
) -> StreamingResponse:
    from openpyxl import Workbook

    campaign_export_limit = resolve_export_limit(source_module="governance", entity_type="governance_campaign")
    payload = get_governance_campaign_queue(db, campaign_key=campaign_key, page=1, page_size=campaign_export_limit, current_user=current_user)
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="governance.campaign.export_xlsx",
        entity_type="governance_campaign",
        source_module="governance",
        row_count=len(payload["items"]),
        truncated=payload["total"] > len(payload["items"]),
        limit=campaign_export_limit,
        filters={"campaign_key": campaign_key},
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
        headers={"Content-Disposition": f'attachment; filename="governance_{campaign_key}.xlsx"'},
    )


@router.get("/critical-changes", response_model=GovernanceCriticalChangesOut)
def governance_critical_changes(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> GovernanceCriticalChangesOut:
    return GovernanceCriticalChangesOut(**get_governance_critical_changes(db, limit=limit, current_user=current_user))


@router.post("/tables/{table_id}/owner-review", response_model=GovernanceReviewMarkOut)
def confirm_owner_review(
    table_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> GovernanceReviewMarkOut:
    table = db.get(TableEntity, table_id)
    before = table.owner_reviewed_at.isoformat() if table and table.owner_reviewed_at else None
    payload = mark_owner_review(db, table_id=table_id, user=user)
    log_field_changes(
        db,
        action="table.owner.review",
        entity_type="table",
        entity_id=table_id,
        changes=[
            AuditFieldChange(
                field_name="owner_reviewed_at",
                before=before,
                after=payload["reviewed_at"],
                change_type="update",
            )
        ],
        source_module="governance",
        metadata={"message": "Owner review confirmed"},
        audit_kwargs=request_audit_kwargs(request, user),
        actor_user_id=user.id,
    )
    db.commit()
    return GovernanceReviewMarkOut(**payload)


@router.post("/tables/{table_id}/privacy-review", response_model=GovernanceReviewMarkOut)
def confirm_privacy_review(
    table_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> GovernanceReviewMarkOut:
    table = db.get(TableEntity, table_id)
    before = table.privacy_reviewed_at.isoformat() if table and table.privacy_reviewed_at else None
    payload = mark_privacy_review(db, table_id=table_id, user=user)
    log_field_changes(
        db,
        action="table.privacy.review",
        entity_type="table",
        entity_id=table_id,
        changes=[
            AuditFieldChange(
                field_name="privacy_reviewed_at",
                before=before,
                after=payload["reviewed_at"],
                change_type="update",
            )
        ],
        source_module="governance",
        metadata={"message": "Privacy review confirmed", "is_sensitive_change": True, "sensitive_category": "classification"},
        audit_kwargs=request_audit_kwargs(request, user),
        actor_user_id=user.id,
    )
    db.commit()
    return GovernanceReviewMarkOut(**payload)
