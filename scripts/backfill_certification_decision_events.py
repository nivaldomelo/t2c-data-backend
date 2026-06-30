from __future__ import annotations

from datetime import timezone

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from t2c_data.core.db import SessionLocal
from t2c_data.features.certification.api_support import build_certification_summary_out
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.models.catalog import Schema, TableEntity
from t2c_data.models.governance import CertificationDecisionEvent


def infer_decision_type(status: str) -> str:
    if status == "certified":
        return "certification"
    if status == "rejected":
        return "refusal"
    if status == "revalidation_pending":
        return "revalidation"
    if status == "expired":
        return "expiration"
    if status == "in_review":
        return "status_change"
    return "review"


def main() -> None:
    session = SessionLocal()
    try:
        settings_snapshot = get_governance_settings_snapshot(session)
        tables = session.scalars(
            select(TableEntity).options(
                selectinload(TableEntity.schema).selectinload(Schema.database),
                selectinload(TableEntity.data_owner),
                selectinload(TableEntity.certification_decided_by_user),
            )
        ).all()

        created = 0
        skipped = 0
        for table in tables:
            event_exists = session.scalar(
                select(CertificationDecisionEvent.id).where(CertificationDecisionEvent.asset_id == table.id).limit(1)
            )
            if event_exists:
                skipped += 1
                continue
            if not table.certification_decided_at and not table.certification_review_at:
                skipped += 1
                continue
            summary = build_certification_summary_out(session, table, settings_snapshot=settings_snapshot)
            created_at = table.certification_decided_at or table.certification_review_at
            event = CertificationDecisionEvent(
                asset_id=table.id,
                asset_name=f"{table.schema.name}.{table.name}",
                database_name=table.schema.database.name,
                schema_name=table.schema.name,
                table_name=table.name,
                previous_status=None,
                new_status=summary.certification_status,
                previous_readiness=None,
                new_readiness=summary.readiness_score,
                decision_type=infer_decision_type(summary.certification_status),
                decision_source="migration",
                reviewer_user_id=table.certification_decided_by_user_id,
                reviewer=table.certification_decided_by_user_name,
                reviewer_email=table.certification_decided_by_user_email,
                observation=table.certification_notes,
                reason=summary.certification_status_reason,
                valid_until=table.certification_expires_at,
                revalidation_due_at=table.certification_review_at,
                metadata_json={"backfill": True},
                created_at=created_at.astimezone(timezone.utc) if created_at and created_at.tzinfo else created_at,
                updated_at=created_at.astimezone(timezone.utc) if created_at and created_at.tzinfo else created_at,
            )
            session.add(event)
            created += 1
        session.commit()
        print({"created": created, "skipped": skipped})
    finally:
        session.close()


if __name__ == "__main__":
    main()
