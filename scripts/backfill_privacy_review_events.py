from __future__ import annotations

from datetime import timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from t2c_data.core.db import SessionLocal
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.privacy_access import normalize_access_roles, suspected_personal_data_columns
from t2c_data.models.catalog import Schema, TableEntity
from t2c_data.models.governance import PrivacyReviewEvent
from t2c_data.api.privacy_access import _privacy_risk_level


def main() -> None:
    session = SessionLocal()
    try:
        settings = get_governance_settings_snapshot(session)
        tables = session.scalars(
            select(TableEntity).options(
                selectinload(TableEntity.schema).selectinload(Schema.database),
                selectinload(TableEntity.columns),
                selectinload(TableEntity.privacy_reviewed_by_user),
            )
        ).all()

        created = 0
        skipped = 0
        for table in tables:
            existing_event = session.scalar(select(PrivacyReviewEvent.id).where(PrivacyReviewEvent.table_id == table.id).limit(1))
            if existing_event:
                skipped += 1
                continue
            has_policy = any(
                [
                    table.sensitivity_level,
                    table.has_personal_data,
                    table.has_sensitive_personal_data,
                    table.legal_basis,
                    table.privacy_purpose,
                    table.retention_policy,
                    table.access_scope,
                    table.access_roles,
                    table.privacy_notes,
                    table.is_masked,
                    table.external_sharing,
                    table.privacy_reviewed_at,
                ]
            )
            if not has_policy:
                skipped += 1
                continue

            created_at = table.privacy_reviewed_at or table.updated_at
            aware_created_at = created_at.astimezone(timezone.utc) if created_at and created_at.tzinfo else created_at
            risk_after = _privacy_risk_level(
                table,
                {
                    "sensitivity_level": table.sensitivity_level,
                    "has_personal_data": table.has_personal_data,
                    "has_sensitive_personal_data": table.has_sensitive_personal_data,
                    "legal_basis": table.legal_basis,
                    "privacy_purpose": table.privacy_purpose,
                    "retention_policy": table.retention_policy,
                    "is_masked": table.is_masked,
                    "external_sharing": table.external_sharing,
                    "access_scope": table.access_scope,
                    "access_roles": normalize_access_roles(table.access_roles),
                    "privacy_notes": table.privacy_notes,
                    "privacy_reviewed_at": table.privacy_reviewed_at,
                    "possible_personal_data": bool(suspected_personal_data_columns(getattr(table, "columns", None))),
                },
            )
            interval_days = (
                settings.sensitive_privacy_review_interval_days
                if table.has_sensitive_personal_data
                else settings.privacy_review_interval_days
            )
            next_review_at = aware_created_at + timedelta(days=interval_days) if aware_created_at else None
            changed_fields = [
                {"field": "classification", "previous": None, "new": table.sensitivity_level},
                {"field": "has_personal_data", "previous": None, "new": table.has_personal_data},
                {"field": "has_sensitive_personal_data", "previous": None, "new": table.has_sensitive_personal_data},
                {"field": "legal_basis", "previous": None, "new": table.legal_basis},
                {"field": "privacy_purpose", "previous": None, "new": table.privacy_purpose},
                {"field": "retention_policy", "previous": None, "new": table.retention_policy},
                {"field": "access_scope", "previous": None, "new": table.access_scope},
                {"field": "access_roles", "previous": None, "new": normalize_access_roles(table.access_roles)},
                {"field": "is_masked", "previous": None, "new": table.is_masked},
                {"field": "external_sharing", "previous": None, "new": table.external_sharing},
                {"field": "privacy_notes", "previous": None, "new": table.privacy_notes},
            ]
            changed_fields = [item for item in changed_fields if item["new"] not in (None, "", [], False)]

            session.add(
                PrivacyReviewEvent(
                    table_id=table.id,
                    table_name=table.name,
                    database_name=table.schema.database.name,
                    schema_name=table.schema.name,
                    previous_sensitivity_level=None,
                    new_sensitivity_level=table.sensitivity_level,
                    previous_has_personal_data=None,
                    new_has_personal_data=table.has_personal_data,
                    previous_has_sensitive_personal_data=None,
                    new_has_sensitive_personal_data=table.has_sensitive_personal_data,
                    previous_legal_basis=None,
                    new_legal_basis=table.legal_basis,
                    previous_privacy_purpose=None,
                    new_privacy_purpose=table.privacy_purpose,
                    previous_retention_policy=None,
                    new_retention_policy=table.retention_policy,
                    previous_access_scope=None,
                    new_access_scope=table.access_scope,
                    previous_access_roles=None,
                    new_access_roles=normalize_access_roles(table.access_roles) or None,
                    previous_is_masked=None,
                    new_is_masked=table.is_masked,
                    previous_external_sharing=None,
                    new_external_sharing=table.external_sharing,
                    previous_privacy_notes=None,
                    new_privacy_notes=table.privacy_notes,
                    review_type="classification",
                    review_source="migration",
                    reviewer_user_id=table.privacy_reviewed_by_user_id,
                    reviewer_name=table.privacy_reviewed_by_user_name,
                    reviewer_email=table.privacy_reviewed_by_user_email,
                    notes="Evento inicial gerado a partir da política atual de privacidade.",
                    risk_before="unknown",
                    risk_after=risk_after,
                    next_review_at=next_review_at,
                    metadata_json={"backfill": True, "changed_fields": changed_fields},
                    created_at=aware_created_at,
                    updated_at=aware_created_at,
                )
            )
            created += 1

        session.commit()
        print({"created": created, "skipped": skipped})
    finally:
        session.close()


if __name__ == "__main__":
    main()
