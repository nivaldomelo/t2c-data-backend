from __future__ import annotations

from datetime import timedelta

from sqlalchemy.orm import Session

from t2c_data.features.catalog.row_count_metrics import build_row_count_metrics
from t2c_data.features.governance.rules import certification_review_due, owner_review_due, privacy_review_due
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.platform.sensitive_data import mask_payload_by_policy
from t2c_data.features.platform.visibility import mask_table_payload
from t2c_data.schemas.catalog import DataContractSummaryOut, TableDetailOut
from t2c_data.models.catalog import TableEntity


def build_table_detail_out(
    db: Session,
    table: TableEntity,
    *,
    masked: bool = False,
    can_view_sensitive: bool = False,
    data_contract: DataContractSummaryOut | None = None,
) -> TableDetailOut:
    payload = TableDetailOut.model_validate(table).model_dump()
    payload["data_contract"] = data_contract.model_dump() if data_contract is not None else None
    payload["data_owner_is_active"] = table.data_owner.is_active if table.data_owner is not None else None
    payload["steward_user_id"] = table.steward_user_id
    payload["steward_name"] = table.steward.name if table.steward is not None else None
    payload["steward_email"] = table.steward.email if table.steward is not None else None
    settings_snapshot = get_governance_settings_snapshot(db)
    payload["owner_review_due"] = owner_review_due(table, settings_snapshot=settings_snapshot)
    payload["privacy_review_due"] = privacy_review_due(table, settings_snapshot=settings_snapshot)
    payload["certification_review_due"] = certification_review_due(table, settings_snapshot=settings_snapshot)
    if table.owner_reviewed_at is not None:
        payload["owner_review_next_at"] = table.owner_reviewed_at + timedelta(days=settings_snapshot.owner_review_interval_days)
    else:
        payload["owner_review_next_at"] = None
    if table.privacy_reviewed_at is not None:
        interval = (
            settings_snapshot.sensitive_privacy_review_interval_days
            if table.sensitivity_level or table.has_personal_data or table.has_sensitive_personal_data
            else settings_snapshot.privacy_review_interval_days
        )
        payload["privacy_review_next_at"] = table.privacy_reviewed_at + timedelta(days=interval)
    else:
        payload["privacy_review_next_at"] = None
    payload["certification_next_review_at"] = table.certification_expires_at or table.certification_review_at
    row_count_metrics = build_row_count_metrics(db=db, table_id=table.id)
    payload["row_count_metrics"] = row_count_metrics.model_dump() if row_count_metrics is not None else None
    from t2c_data.features.metabase.impact import get_table_metabase_impact

    metabase_impact = get_table_metabase_impact(db, table.id)
    payload["metabase_impact"] = metabase_impact.model_dump() if metabase_impact is not None else None
    if masked:
        payload = mask_table_payload(payload)
    if not can_view_sensitive:
        payload = mask_payload_by_policy(payload, can_view_sensitive=False)
        payload["owner"] = "[masked]" if payload.get("owner") is not None else None
        payload["owner_email"] = "[masked]" if payload.get("owner_email") is not None else None
        payload["steward_name"] = "[masked]" if payload.get("steward_name") is not None else None
        payload["steward_email"] = "[masked]" if payload.get("steward_email") is not None else None
        if isinstance(payload.get("data_owner"), dict):
            payload["data_owner"]["name"] = "[masked]"
            payload["data_owner"]["email"] = "[masked]"
    return TableDetailOut(**payload)
