from __future__ import annotations

from datetime import datetime, timezone

import logging

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.certification.api_support import certification_status_label
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.column_classification import build_column_classification_map
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.governance.trust_score import build_trust_score_for_profile
from t2c_data.features.ingestion.service import load_table_ingestion_summary
from t2c_data.features.lineage.table_summary import get_table_summary
from t2c_data.features.privacy_access import can_view_table
from t2c_data.features.privacy_access.policy import sensitivity_label
from t2c_data.features.tags.api_support import load_entity_tag_contexts
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import ColumnEntity, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.tag import TagIntelligenceEvent
from t2c_data.schemas.canonical_asset import (
    CanonicalAssetClassificationOut,
    CanonicalAssetColumnPreviewOut,
    CanonicalAssetEvidenceOut,
    CanonicalAssetOwnerOut,
    CanonicalAssetOut,
    CanonicalAssetSourceOut,
    CanonicalPipelineOut,
    CanonicalGovernanceEventOut,
)
from t2c_data.schemas.glossary import GlossaryTermOut
from t2c_data.schemas.tag import TagOut

logger = logging.getLogger(__name__)


def _table_query():
    return (
        select(TableEntity)
        .options(
            selectinload(TableEntity.columns).selectinload(ColumnEntity.data_owner),
            selectinload(TableEntity.columns).selectinload(ColumnEntity.owner_reviewed_by_user),
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
    )


def _table_or_404(db: Session, table_id: int, *, current_user=None) -> TableEntity:
    table = db.scalar(_table_query().where(TableEntity.id == table_id))
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if current_user is not None and not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return table


def _normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _table_fqn(table: TableEntity) -> str:
    return f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}"


def _table_terms(db: Session, table_id: int) -> list[GlossaryTermOut]:
    rows = db.execute(
        select(GlossaryTerm)
        .join(GlossaryAssignment, GlossaryAssignment.term_id == GlossaryTerm.id)
        .where(GlossaryAssignment.entity_type == "table", GlossaryAssignment.entity_id == table_id)
        .order_by(GlossaryTerm.name)
    ).scalars().all()
    return [GlossaryTermOut.model_validate(term, from_attributes=True) for term in rows]


def _table_tags(db: Session, table_id: int) -> list[TagOut]:
    return load_entity_tag_contexts(db, entity_type="table", entity_ids=[table_id]).get(table_id, [])


def _column_tags(db: Session, column_ids: list[int]) -> dict[int, list[TagOut]]:
    return load_entity_tag_contexts(db, entity_type="column", entity_ids=column_ids)


def _recent_events(db: Session, *, table: TableEntity, column_ids: list[int] | None = None) -> list[CanonicalGovernanceEventOut]:
    table_id = str(table.id)
    entity_ids = {table_id}
    if column_ids:
        entity_ids.update(str(column_id) for column_id in column_ids)

    audit_rows = db.execute(
        select(AuditLog)
        .where(
            AuditLog.entity_type.in_(["table", "column"]),
            AuditLog.entity_id.in_(sorted(entity_ids)),
        )
        .order_by(AuditLog.created_at.desc())
        .limit(12)
    ).scalars().all()
    tag_rows = db.execute(
        select(TagIntelligenceEvent)
        .where(
            TagIntelligenceEvent.entity_type.in_(["table", "column"]),
            TagIntelligenceEvent.entity_id.in_(
                [table.id, *(column_ids or [])],
            ),
        )
        .order_by(TagIntelligenceEvent.created_at.desc())
        .limit(12)
    ).scalars().all()

    items: list[CanonicalGovernanceEventOut] = []
    for row in audit_rows:
        items.append(
            CanonicalGovernanceEventOut(
                id=f"audit:{row.id}",
                event_type=row.action,
                category="audit",
                label=row.action.replace(".", " ").replace("_", " ").title(),
                detail=(row.metadata_json or {}).get("message") if isinstance(row.metadata_json, dict) else None,
                source=row.source_module or "audit",
                actor_name=row.actor_name,
                actor_email=row.user_email,
                created_at=row.created_at,
            )
        )
    for row in tag_rows:
        items.append(
            CanonicalGovernanceEventOut(
                id=f"tag:{row.id}",
                event_type="tag_intelligence",
                category="classification",
                label=f"Tag {row.review_status.replace('_', ' ').title()}",
                detail=row.inference_reason,
                source=row.inference_source or "tags",
                actor_name=None,
                actor_email=None,
                created_at=row.created_at,
            )
        )
    items.sort(key=lambda item: item.created_at, reverse=True)
    deduped: list[CanonicalGovernanceEventOut] = []
    seen: set[str] = set()
    for item in items:
        if item.id in seen:
            continue
        seen.add(item.id)
        deduped.append(item)
    return deduped[:8]


def _pipeline_payload(db: Session, table: TableEntity, *, current_user=None) -> CanonicalPipelineOut | None:
    try:
        summary = load_table_ingestion_summary(
            db,
            schema_name=table.schema.name,
            table_name=table.name,
            airflow_ui_base_url=None,
        )
    except SQLAlchemyError as exc:
        db.rollback()
        logger.warning("canonical pipeline payload failed table_id=%s error=%s", table.id, exc)
        return None
    except Exception as exc:
        logger.warning("canonical pipeline payload failed table_id=%s error=%s", table.id, exc)
        return None

    stability = None
    history: list[dict[str, object]] = []
    return CanonicalPipelineOut(
        linked=bool(summary.get("linked")),
        state=str(summary.get("state") or "unknown"),
        message=summary.get("message"),
        table_schema=str(summary.get("table_schema") or table.schema.name),
        table_name=str(summary.get("table_name") or table.name),
        pipeline_count=int(summary.get("pipeline_count") or 0),
        primary_pipeline=summary.get("primary_pipeline"),
        pipelines=list(summary.get("pipelines") or []),
        stability=stability,
        history=history[:5],
    )


def _pipeline_events(pipeline: CanonicalPipelineOut | None) -> list[CanonicalGovernanceEventOut]:
    if pipeline is None or pipeline.primary_pipeline is None:
        return []
    primary = pipeline.primary_pipeline
    events: list[CanonicalGovernanceEventOut] = []
    status_label = str(primary.latest_status_label or primary.latest_status or "Pendente")
    events.append(
        CanonicalGovernanceEventOut(
            id=f"pipeline:{primary.pipeline_id or primary.pipeline_name or 'primary'}",
            event_type="pipeline_status",
            category="operation",
            label=f"Pipeline {status_label}",
            detail=primary.last_error or primary.pipeline_history_href,
            source="ingestion",
            actor_name=None,
            actor_email=None,
            created_at=primary.last_execution_finished_at
            or primary.last_execution_started_at
            or primary.last_success_at
            or datetime.now(timezone.utc),
        )
    )
    if pipeline.stability and pipeline.stability.points:
        recent_points = pipeline.stability.points[:3]
        for point in recent_points:
            events.append(
                CanonicalGovernanceEventOut(
                    id=f"pipeline-stability:{point.execution_id}",
                    event_type="pipeline_run",
                    category="operation",
                    label=point.status_label,
                    detail=f"{point.rows_written or 0} linha(s) escritas",
                    source="ingestion",
                    actor_name=None,
                    actor_email=None,
                    created_at=point.occurred_at or datetime.now(timezone.utc),
                )
            )
    return events


def _lineage_summary(db: Session, table: TableEntity, *, current_user=None):
    try:
        return get_table_summary(db, table.id, current_user=current_user)
    except SQLAlchemyError as exc:
        db.rollback()
        logger.warning("canonical lineage summary failed table_id=%s error=%s", table.id, exc)
        return None
    except Exception as exc:
        logger.warning("canonical lineage summary failed table_id=%s error=%s", table.id, exc)
        return None


def _governance_payload(table_profile, *, settings_snapshot) -> CanonicalAssetClassificationOut:
    governance_score = build_governance_score_for_profile(table_profile, settings_snapshot=settings_snapshot)
    trust_score = build_trust_score_for_profile(table_profile, settings_snapshot=settings_snapshot)
    return CanonicalAssetClassificationOut(
        certification_status=table_profile.certification_status,
        certification_status_label=certification_status_label(table_profile.certification_status),
        certification_criticality=table_profile.certification_criticality,
        certification_badges=list(table_profile.certification_badges or []),
        sensitivity_level=table_profile.sensitivity_level,
        sensitivity_label=sensitivity_label(table_profile.sensitivity_level),
        classification_defined=bool(table_profile.classification_defined),
        has_personal_data=bool(table_profile.has_personal_data),
        has_sensitive_personal_data=bool(table_profile.has_sensitive_personal_data),
        tags_count=int(table_profile.tags_count or 0),
        terms_count=int(table_profile.terms_count or 0),
        readiness_score=int(table_profile.readiness_score),
        governance_score=int(governance_score["score"]),
        governance_label=str(governance_score["label"]),
        governance_tone=str(governance_score["tone"]),
        trust_score=int(trust_score.score),
        trust_label=str(trust_score.label),
        trust_tone=str(trust_score.tone),
        total_columns=int(table_profile.total_columns or 0),
        classified_columns=int(table_profile.classified_columns or 0),
        personal_classified_columns=int(table_profile.personal_classified_columns or 0),
        sensitive_classified_columns=int(table_profile.sensitive_classified_columns or 0),
        financial_classified_columns=int(table_profile.financial_classified_columns or 0),
        operational_classified_columns=int(table_profile.operational_classified_columns or 0),
        classification_coverage_pct=float(table_profile.classification_coverage_pct or 0.0),
        column_classification_reviewed_at=_normalize_dt(table_profile.column_classification_reviewed_at),
    )


def load_table_canonical_context(db: Session, table_id: int, *, current_user=None) -> CanonicalAssetOut:
    table = _table_or_404(db, table_id, current_user=current_user)
    table_profiles = load_table_profiles(db, datetime.now(timezone.utc), table_ids=[table.id], current_user=current_user)
    if not table_profiles:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Canonical context not found")
    table_profile = table_profiles[0]
    settings_snapshot = get_governance_settings_snapshot(db)
    lineage_summary = _lineage_summary(db, table, current_user=current_user)
    pipeline = _pipeline_payload(db, table, current_user=current_user)
    tags = _table_tags(db, table.id)
    terms = _table_terms(db, table.id)
    column_tags = _column_tags(db, [column.id for column in table.columns])
    column_classifications = build_column_classification_map(db, table_id=table.id)
    ordered_columns = sorted(table.columns, key=lambda column: (column.ordinal_position, column.id))
    recent_events = _recent_events(db, table=table, column_ids=[column.id for column in ordered_columns])
    recent_events.extend(_pipeline_events(pipeline))
    recent_events.sort(key=lambda item: item.created_at, reverse=True)
    columns = []
    for column in ordered_columns:
        column_classification = column_classifications.get(str(column.id), {})
        columns.append(
            CanonicalAssetColumnPreviewOut(
                id=column.id,
                name=column.name,
                data_type=column.data_type,
                ordinal_position=column.ordinal_position,
                is_nullable=column.is_nullable,
                is_primary_key=column.is_primary_key,
                description_complete=bool(
                    (column.dictionary_description or "").strip()
                    or (column.description_manual or "").strip()
                    or (column.description_source or "").strip()
                ),
                classification_taxonomy_key=column_classification.get("taxonomy_key"),
                classification_taxonomy_label=column_classification.get("taxonomy_label"),
                classification_taxonomy_group=column_classification.get("taxonomy_group"),
                classification_review_status=column_classification.get("review_status"),
                classification_confidence_score=column_classification.get("confidence_score"),
                classification_is_personal_data=bool(column_classification.get("is_personal_data")),
                classification_is_sensitive_data=bool(column_classification.get("is_sensitive_data")),
                classification_is_financial_data=bool(column_classification.get("is_financial_data")),
                classification_is_operational_data=bool(column_classification.get("is_operational_data")),
                tags=column_tags.get(column.id, []),
            )
        )
    links = build_asset_links(
        table_id=table.id,
        datasource_id=table.schema.database.datasource_id,
        database_id=table.schema.database.id,
        schema_id=table.schema_id,
        data_owner_id=table.data_owner_id,
    )
    return CanonicalAssetOut(
        entity_kind="table",
        table_id=table.id,
        table_name=table.name,
        table_fqn=_table_fqn(table),
        table_type=table.table_type,
        asset_key=f"table:{table.id}",
        display_name=f"{table.schema.name}.{table.name}",
        source=CanonicalAssetSourceOut(
            datasource_id=table.schema.database.datasource_id,
            datasource_name=table.schema.database.datasource.name,
            database_id=table.schema.database.id,
            database_name=table.schema.database.name,
            schema_id=table.schema_id,
            schema_name=table.schema.name,
            table_type=table.table_type,
            engine=table.schema.database.datasource.db_type,
        ),
        owner=CanonicalAssetOwnerOut(
            data_owner_id=table.data_owner_id,
            owner_name=table.owner or (table.data_owner.name if table.data_owner else None),
            owner_email=table.owner_email or (table.data_owner.email if table.data_owner else None),
            owner_defined=bool(table.owner or table.data_owner_id or table.data_owner),
        ),
        classification=_governance_payload(table_profile, settings_snapshot=settings_snapshot),
        evidence=CanonicalAssetEvidenceOut(
            description_complete=bool(table_profile.description_complete),
            dictionary_complete=bool(table_profile.dictionary_complete),
            dq_score=float(table_profile.dq_score) if table_profile.dq_score is not None else None,
            completeness_pct_avg=float(table_profile.completeness_pct_avg) if table_profile.completeness_pct_avg is not None else None,
            freshness_seconds=table_profile.freshness_seconds,
            open_incidents=int(table_profile.open_incidents or 0),
            critical_open_incidents=int(table_profile.critical_open_incidents or 0),
            active_dq_violation=bool(table_profile.active_dq_violation),
            active_dq_rule_names=list(table_profile.active_dq_rule_names or []),
            last_review_at=_normalize_dt(table_profile.last_review_at),
            last_sync_at=_normalize_dt(table_profile.last_sync_at),
            last_updated_at=_normalize_dt(table_profile.last_updated_at),
            trust_summary=build_trust_score_for_profile(table_profile, settings_snapshot=settings_snapshot).summary,
        ),
        lineage=lineage_summary,
        tags=tags,
        terms=terms,
        columns=columns,
        recent_events=recent_events[:8],
        pipeline=pipeline,
        links=links,
        generated_at=datetime.now(timezone.utc),
    )


def _compact_pipeline(pipeline: CanonicalPipelineOut | None) -> CanonicalPipelineOut | None:
    if pipeline is None:
        return None
    stability = pipeline.stability
    compact_stability = None
    if stability is not None:
        compact_stability = stability.model_copy(update={"points": list(stability.points[:3])})
    return pipeline.model_copy(
        update={
            "pipelines": list(pipeline.pipelines[:3]),
            "history": [],
            "stability": compact_stability,
        }
    )


def compact_canonical_asset_context(asset: CanonicalAssetOut) -> CanonicalAssetOut:
    return asset.model_copy(
        update={
            "tags": list(asset.tags[:8]),
            "terms": list(asset.terms[:6]),
            "columns": list(asset.columns[:8]),
            "recent_events": list(asset.recent_events[:4]),
            "pipeline": _compact_pipeline(asset.pipeline),
            "generated_at": datetime.now(timezone.utc),
        }
    )


def load_column_canonical_context(db: Session, column_id: int, *, current_user=None) -> CanonicalAssetOut:
    column = db.scalar(
        select(ColumnEntity)
        .options(
            selectinload(ColumnEntity.table)
            .selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
            selectinload(ColumnEntity.table).selectinload(TableEntity.data_owner),
        )
        .where(ColumnEntity.id == column_id)
    )
    if column is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
    if current_user is not None and not can_view_table(current_user, column.table):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Column not found")
    table_payload = load_table_canonical_context(db, column.table_id, current_user=current_user)
    column_tags = _column_tags(db, [column.id]).get(column.id, [])
    return CanonicalAssetOut(
        entity_kind="column",
        table_id=table_payload.table_id,
        table_name=table_payload.table_name,
        table_fqn=table_payload.table_fqn,
        table_type=table_payload.table_type,
        column_id=column.id,
        column_name=column.name,
        column_data_type=column.data_type,
        column_ordinal_position=column.ordinal_position,
        asset_key=f"column:{column.id}",
        display_name=f"{table_payload.table_fqn} · {column.name}",
        source=table_payload.source,
        owner=table_payload.owner,
        classification=table_payload.classification,
        evidence=CanonicalAssetEvidenceOut(
            description_complete=bool(
                (column.dictionary_description or "").strip()
                or (column.description_manual or "").strip()
                or (column.description_source or "").strip()
            ),
            dictionary_complete=bool(column.dictionary_description and column.dictionary_description.strip()),
            dq_score=table_payload.evidence.dq_score,
            completeness_pct_avg=table_payload.evidence.completeness_pct_avg,
            freshness_seconds=table_payload.evidence.freshness_seconds,
            open_incidents=table_payload.evidence.open_incidents,
            critical_open_incidents=table_payload.evidence.critical_open_incidents,
            active_dq_violation=table_payload.evidence.active_dq_violation,
            active_dq_rule_names=table_payload.evidence.active_dq_rule_names,
            last_review_at=table_payload.evidence.last_review_at,
            last_sync_at=table_payload.evidence.last_sync_at,
            last_updated_at=_normalize_dt(column.updated_at),
        ),
        lineage=table_payload.lineage,
        tags=column_tags,
        terms=table_payload.terms,
        columns=[],
        recent_events=table_payload.recent_events,
        pipeline=table_payload.pipeline,
        links=build_asset_links(
            table_id=column.table_id,
            datasource_id=column.table.schema.database.datasource_id,
            database_id=column.table.schema.database.id,
            schema_id=column.table.schema_id,
            data_owner_id=column.table.data_owner_id,
            column_id=column.id,
        ),
        generated_at=datetime.now(timezone.utc),
    )


__all__ = [
    "load_column_canonical_context",
    "load_table_canonical_context",
]
