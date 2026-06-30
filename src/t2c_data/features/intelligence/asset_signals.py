from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import desc, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from t2c_data.features.catalog.canonical_assets import compact_canonical_asset_context, load_table_canonical_context
from t2c_data.features.catalog.correlation import build_table_correlation_summary
from t2c_data.features.dashboard.executive_scoring import compute_final_priority_score
from t2c_data.features.metabase.impact import get_table_metabase_impact
from t2c_data.models.auth import User
from t2c_data.models.platform import DataLakeInventoryTable
from t2c_data.models.search import SearchResultClick
from t2c_data.schemas.asset_intelligence import AssetImpactOut, AssetIntelligenceOut, AssetSignalOut

logger = logging.getLogger(__name__)


_SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3, "critical": 4}
_RISK_LEVEL_WEIGHT = {"none": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}


def _signal(signal_type: str, severity: str) -> AssetSignalOut:
    return AssetSignalOut(type=signal_type, severity=severity)


def _append_signal(signals: list[AssetSignalOut], signal_type: str, severity: str) -> None:
    current = next((item for item in signals if item.type == signal_type), None)
    if current is None:
        signals.append(_signal(signal_type, severity))
        return
    if _SEVERITY_ORDER.get(severity, 0) > _SEVERITY_ORDER.get(current.severity, 0):
        current.severity = severity


def _clamp_score(value: int | float | None) -> int:
    if value is None:
        return 0
    return max(0, min(100, int(round(float(value)))))


def _risk_weight(value: str | None) -> int:
    return _RISK_LEVEL_WEIGHT.get((value or "none").strip().lower(), 0)


def _safe_correlation_summary(db: Session, *, table_id: int, current_user: User):
    try:
        return build_table_correlation_summary(db=db, table_id=table_id, current_user=current_user)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            return None
        raise
    except SQLAlchemyError as exc:
        db.rollback()
        logger.warning("asset intelligence correlation failed table_id=%s error=%s", table_id, exc)
        return None
    except Exception as exc:
        logger.warning("asset intelligence correlation failed table_id=%s error=%s", table_id, exc)
        return None


def _safe_metabase_impact(db: Session, *, table_id: int):
    try:
        return get_table_metabase_impact(db, table_id)
    except SQLAlchemyError as exc:
        db.rollback()
        logger.warning("asset intelligence metabase impact failed table_id=%s error=%s", table_id, exc)
        return None
    except Exception as exc:
        logger.warning("asset intelligence metabase impact failed table_id=%s error=%s", table_id, exc)
        return None


def _recent_search_users(db: Session, *, table_id: int) -> int:
    since = datetime.now(timezone.utc) - timedelta(days=30)
    try:
        return int(
            db.scalar(
                select(func.count(func.distinct(SearchResultClick.user_id))).where(
                    SearchResultClick.created_at >= since,
                    SearchResultClick.entity_type == "table",
                    SearchResultClick.entity_id == table_id,
                    SearchResultClick.user_id.is_not(None),
                )
            )
            or 0
        )
    except SQLAlchemyError as exc:
        db.rollback()
        logger.warning("asset intelligence user impact failed table_id=%s error=%s", table_id, exc)
        return 0


def _latest_data_lake_signal(db: Session, *, table_name: str):
    try:
        return db.scalar(
            select(DataLakeInventoryTable)
            .where(
                or_(
                    DataLakeInventoryTable.table_name == table_name,
                    DataLakeInventoryTable.path_base.ilike(f"%/{table_name}"),
                    DataLakeInventoryTable.path_base.ilike(f"%/{table_name}/%"),
                )
            )
            .order_by(
                DataLakeInventoryTable.data_last_scan_at.desc().nullslast(),
                DataLakeInventoryTable.updated_at.desc().nullslast(),
                desc(DataLakeInventoryTable.id),
            )
        )
    except SQLAlchemyError as exc:
        db.rollback()
        logger.warning("asset intelligence data lake signal failed table_name=%s error=%s", table_name, exc)
        return None


def _action_labels(operational_context: dict[str, Any] | None) -> list[str]:
    if not operational_context:
        return []
    actions = list(operational_context.get("recommended_actions") or [])
    for item in operational_context.get("actions") or []:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()
        if label and label not in actions:
            actions.append(label)
    return actions[:8]


def build_asset_intelligence(db: Session, *, asset_id: int, current_user: User) -> AssetIntelligenceOut:
    asset = compact_canonical_asset_context(load_table_canonical_context(db, asset_id, current_user=current_user))
    correlation = _safe_correlation_summary(db, table_id=asset.table_id, current_user=current_user)
    metabase_impact = _safe_metabase_impact(db, table_id=asset.table_id)
    data_lake_table = _latest_data_lake_signal(db, table_name=asset.table_name)

    signals: list[AssetSignalOut] = []
    evidence = asset.evidence
    classification = asset.classification
    operational_context = (
        correlation.operational_context.model_dump(by_alias=True)
        if correlation is not None and correlation.operational_context is not None
        else None
    )

    if evidence.dq_score is None:
        _append_signal(signals, "dq_not_evaluated", "medium")
    elif evidence.dq_score < 70:
        _append_signal(signals, "dq_low", "high")
    elif evidence.dq_score < 90:
        _append_signal(signals, "dq_attention", "medium")
    if evidence.active_dq_violation:
        _append_signal(signals, "dq_active_violation", "high")

    if evidence.freshness_seconds is not None:
        if evidence.freshness_seconds >= 86_400:
            _append_signal(signals, "freshness_delayed", "high")
        elif evidence.freshness_seconds >= 21_600:
            _append_signal(signals, "freshness_attention", "medium")

    if evidence.critical_open_incidents > 0:
        _append_signal(signals, "critical_incident_open", "critical")
    elif evidence.open_incidents > 0:
        _append_signal(signals, "incident_open", "high")

    if not asset.owner.owner_defined:
        _append_signal(signals, "no_owner", "medium")

    if not evidence.description_complete:
        _append_signal(signals, "description_incomplete", "medium")
    if not evidence.dictionary_complete:
        _append_signal(signals, "dictionary_incomplete", "medium")
    if classification.tags_count <= 0:
        _append_signal(signals, "missing_tags", "low")
    if classification.terms_count <= 0:
        _append_signal(signals, "missing_glossary_terms", "low")
    if classification.trust_score < 50:
        _append_signal(signals, "trust_low", "high")
    elif classification.trust_score < 70:
        _append_signal(signals, "trust_attention", "medium")

    if operational_context:
        if operational_context.get("owner_review_due"):
            _append_signal(signals, "owner_review_due", "medium")
        if operational_context.get("privacy_review_due"):
            _append_signal(signals, "privacy_review_due", "medium")
        if operational_context.get("certification_review_due"):
            _append_signal(signals, "certification_review_due", "medium")

    if asset.pipeline is None or not asset.pipeline.linked:
        _append_signal(signals, "airflow_not_linked", "low")
    elif asset.pipeline.state in {"failed", "error", "degraded"}:
        _append_signal(signals, "airflow_failure", "high")
    elif asset.pipeline.state not in {"success", "ok", "healthy", "unknown"}:
        _append_signal(signals, "airflow_attention", "medium")

    if metabase_impact is not None and metabase_impact.available:
        if metabase_impact.dashboard_count > 0:
            _append_signal(signals, "metabase_dashboard_impact", "medium")
        if max(_risk_weight(metabase_impact.break_risk_on_drop), _risk_weight(metabase_impact.break_risk_on_change)) >= 3:
            _append_signal(signals, "metabase_break_risk_high", "high")

    lineage_dashboard_count = 0
    lineage_upstream_count = 0
    lineage_downstream_count = 0
    if asset.lineage is not None:
        lineage_dashboard_count = int(asset.lineage.impact.dashboard_count or 0)
        lineage_upstream_count = int(asset.lineage.impact.upstream_count or 0)
        lineage_downstream_count = int(asset.lineage.impact.downstream_count or 0)
        if asset.lineage.impact.downstream_count > 0:
            _append_signal(signals, "lineage_downstream_impact", "medium")
        if asset.lineage.impact.impact_level in {"high", "critical"}:
            _append_signal(signals, "lineage_impact_high", "high")

    if data_lake_table is None:
        _append_signal(signals, "data_lake_not_mapped", "low")
    else:
        if data_lake_table.status_scan not in {"ok", "success", "healthy"}:
            _append_signal(signals, "data_lake_scan_attention", "medium")
        if data_lake_table.error_message:
            _append_signal(signals, "data_lake_error", "high")
        if data_lake_table.last_quality_score is not None and data_lake_table.last_quality_score < 70:
            _append_signal(signals, "data_lake_quality_low", "high")

    correlation_score = int(correlation.priority_score) if correlation is not None else 0
    operational_score = int(operational_context.get("criticality_score") or 0) if operational_context else 0
    risk_score = _clamp_score(max(correlation_score, operational_score))
    trust_score = _clamp_score(classification.trust_score)
    dashboard_count = max(int(metabase_impact.dashboard_count if metabase_impact else 0), lineage_dashboard_count)
    user_count = _recent_search_users(db, table_id=asset.table_id)
    priority_score = compute_final_priority_score(
        risk_score,
        dashboards=dashboard_count,
        users=user_count,
        upstream=lineage_upstream_count,
        downstream=lineage_downstream_count,
        freshness_seconds=evidence.freshness_seconds,
        dq_score=evidence.dq_score,
    )

    return AssetIntelligenceOut(
        risk_score=risk_score,
        priority_score=priority_score,
        trust_score=trust_score,
        signals=sorted(signals, key=lambda item: (-_SEVERITY_ORDER.get(item.severity, 0), item.type)),
        impact=AssetImpactOut(
            dashboards=dashboard_count,
            users=user_count,
        ),
        recommended_actions=_action_labels(operational_context),
    )
