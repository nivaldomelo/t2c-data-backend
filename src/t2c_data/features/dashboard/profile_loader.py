from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.dashboard.support import TableProfile, normalize_dt
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.governance.trust_score import build_trust_score_for_profile
from t2c_data.features.privacy_access import can_view_table
from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.classification import ColumnClassification
from t2c_data.models.dq import DQRule, DQRuleRun, DQRun, DQTableMetric
from t2c_data.models.glossary import GlossaryAssignment
from t2c_data.models.incident import Incident
from t2c_data.models.governance import AssetSla
from t2c_data.models.search import SearchResultClick
from t2c_data.models.tag import TagAssignment


def latest_metrics_subquery():
    ranked = (
        select(
            DQTableMetric.table_id.label("table_id"),
            DQTableMetric.dq_score.label("dq_score"),
            DQTableMetric.completeness_pct_avg.label("completeness_pct_avg"),
            DQRun.created_at.label("run_at"),
            func.row_number()
            .over(partition_by=DQTableMetric.table_id, order_by=DQRun.created_at.desc())
            .label("rn"),
        )
        .join(DQRun, DQTableMetric.run_id == DQRun.id)
        .where(DQRun.status == "success")
        .subquery()
    )
    return (
        select(
            ranked.c.table_id,
            ranked.c.dq_score,
            ranked.c.completeness_pct_avg,
            ranked.c.run_at,
        )
        .where(ranked.c.rn == 1)
        .subquery()
    )


def _fqn_candidates(fqn: str | None) -> list[str]:
    normalized = [part.strip() for part in str(fqn or "").split(".") if part and part.strip()]
    candidates: list[str] = []
    if str(fqn or "").strip():
        candidates.append(str(fqn).strip())
    if len(normalized) >= 2:
        candidates.append(".".join(normalized[-2:]))
    if normalized:
        candidates.append(normalized[-1])
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def load_table_profiles(
    session: Session,
    now: datetime,
    *,
    table_ids: list[int] | None = None,
    incident_fqns: list[str] | None = None,
    current_user=None,
) -> list[TableProfile]:
    settings_snapshot = get_governance_settings_snapshot(session)
    latest_sq = latest_metrics_subquery()
    search_clicks_sq = (
        select(
            SearchResultClick.entity_id.label("table_id"),
            func.count(SearchResultClick.id).label("search_clicks_30d"),
        )
        .where(
            SearchResultClick.entity_type == "table",
            SearchResultClick.created_at >= now - timedelta(days=30),
        )
        .group_by(SearchResultClick.entity_id)
        .subquery()
    )
    column_stats_sq = (
        select(
            ColumnEntity.table_id.label("table_id"),
            func.count(ColumnEntity.id).label("total_columns"),
            func.sum(
                case(
                    (
                        func.length(
                            func.trim(
                                func.coalesce(
                                    ColumnEntity.dictionary_description,
                                    ColumnEntity.description_manual,
                                    ColumnEntity.description_source,
                                    "",
                                )
                            )
                        )
                        > 0,
                        1,
                    ),
                    else_=0,
                )
            ).label("documented_columns"),
        )
        .group_by(ColumnEntity.table_id)
        .subquery()
    )
    classification_stats_sq = (
        select(
            ColumnEntity.table_id.label("table_id"),
            func.count(ColumnClassification.id).label("classified_columns"),
            func.sum(case((ColumnClassification.is_personal_data.is_(True), 1), else_=0)).label("personal_classified_columns"),
            func.sum(case((ColumnClassification.is_sensitive_data.is_(True), 1), else_=0)).label("sensitive_classified_columns"),
            func.sum(case((ColumnClassification.is_financial_data.is_(True), 1), else_=0)).label("financial_classified_columns"),
            func.sum(case((ColumnClassification.is_operational_data.is_(True), 1), else_=0)).label("operational_classified_columns"),
            func.max(func.coalesce(ColumnClassification.reviewed_at, ColumnClassification.updated_at)).label(
                "column_classification_reviewed_at"
            ),
        )
        .join(ColumnClassification, ColumnClassification.column_id == ColumnEntity.id)
        .group_by(ColumnEntity.table_id)
        .subquery()
    )
    tag_counts_sq = (
        select(
            TagAssignment.entity_id.label("table_id"),
            func.count(TagAssignment.id).label("tags_count"),
        )
        .where(TagAssignment.entity_type == "table")
        .group_by(TagAssignment.entity_id)
        .subquery()
    )
    term_counts_sq = (
        select(
            GlossaryAssignment.entity_id.label("table_id"),
            func.count(GlossaryAssignment.id).label("terms_count"),
        )
        .where(GlossaryAssignment.entity_type == "table")
        .group_by(GlossaryAssignment.entity_id)
        .subquery()
    )
    incident_counts_sq = (
        select(
            Incident.table_fqn.label("table_fqn"),
            func.sum(case((Incident.status.in_(["open", "investigating"]), 1), else_=0)).label(
                "open_incidents"
            ),
            func.sum(
                case(
                    (
                        Incident.status.in_(["open", "investigating"]) & (Incident.severity == "sev1"),
                        1,
                    ),
                    else_=0,
                )
            ).label("critical_open_incidents"),
        )
        .where(Incident.entity_type == "table")
        .group_by(Incident.table_fqn)
        .subquery()
    )
    dq_latest_runs_sq = (
        select(
            DQRule.table_fqn.label("table_fqn"),
            DQRule.id.label("rule_id"),
            DQRule.name.label("rule_name"),
            DQRuleRun.status.label("status"),
            DQRuleRun.violations_count.label("violations_count"),
            func.row_number()
            .over(
                partition_by=DQRule.id,
                order_by=(DQRuleRun.created_at.desc(), DQRuleRun.id.desc()),
            )
            .label("rn"),
        )
        .join(DQRuleRun, DQRuleRun.rule_id == DQRule.id)
        .where(DQRule.is_active.is_(True))
        .subquery()
    )
    recent_dq_failures_sq = (
        select(
            DQRun.table_id.label("table_id"),
            func.count(DQRun.id).label("recent_dq_failure_runs_30d"),
        )
        .where(
            DQRun.table_id.is_not(None),
            DQRun.status == "failed",
            DQRun.created_at >= now - timedelta(days=30),
        )
        .group_by(DQRun.table_id)
        .subquery()
    )
    latest_sla_sq = (
        select(
            AssetSla.table_id.label("table_id"),
            AssetSla.id.label("sla_id"),
            AssetSla.sla_kind.label("sla_kind"),
            AssetSla.sla_hours.label("sla_hours"),
            AssetSla.status.label("sla_status"),
            func.row_number()
            .over(
                partition_by=AssetSla.table_id,
                order_by=(AssetSla.updated_at.desc(), AssetSla.id.desc()),
            )
            .label("rn"),
        )
        .where(
            AssetSla.asset_type == "table",
            AssetSla.status == "active",
        )
        .subquery()
    )

    stmt = (
        select(
            TableEntity.id.label("table_id"),
            DataSource.id.label("datasource_id"),
            Database.id.label("database_id"),
            Schema.id.label("schema_id"),
            TableEntity.name.label("table_name"),
            TableEntity.table_type.label("table_type"),
            TableEntity.description_manual.label("description_manual"),
            TableEntity.description_source.label("description_source"),
            TableEntity.owner.label("owner"),
            TableEntity.owner_email.label("owner_email"),
            TableEntity.data_owner_id.label("data_owner_id"),
            DataOwner.name.label("data_owner_name"),
            DataOwner.email.label("data_owner_email"),
            DataOwner.area.label("data_owner_area"),
            DataOwner.is_active.label("data_owner_is_active"),
            TableEntity.certification_status.label("certification_status"),
            TableEntity.certification_criticality.label("certification_criticality"),
            TableEntity.certification_badges.label("certification_badges"),
            TableEntity.certification_decided_at.label("certification_decided_at"),
            TableEntity.certification_review_at.label("certification_review_at"),
            TableEntity.certification_expires_at.label("certification_expires_at"),
            TableEntity.owner_reviewed_at.label("owner_reviewed_at"),
            TableEntity.privacy_reviewed_at.label("privacy_reviewed_at"),
            TableEntity.sensitivity_level.label("sensitivity_level"),
            TableEntity.has_personal_data.label("has_personal_data"),
            TableEntity.has_sensitive_personal_data.label("has_sensitive_personal_data"),
            TableEntity.updated_at.label("table_updated_at"),
            Schema.name.label("schema_name"),
            Database.name.label("database_name"),
            DataSource.name.label("datasource_name"),
            DataSource.db_type.label("engine"),
            func.coalesce(column_stats_sq.c.total_columns, 0).label("total_columns"),
            func.coalesce(column_stats_sq.c.documented_columns, 0).label("documented_columns"),
            func.coalesce(classification_stats_sq.c.classified_columns, 0).label("classified_columns"),
            func.coalesce(classification_stats_sq.c.personal_classified_columns, 0).label("personal_classified_columns"),
            func.coalesce(classification_stats_sq.c.sensitive_classified_columns, 0).label("sensitive_classified_columns"),
            func.coalesce(classification_stats_sq.c.financial_classified_columns, 0).label("financial_classified_columns"),
            func.coalesce(classification_stats_sq.c.operational_classified_columns, 0).label("operational_classified_columns"),
            classification_stats_sq.c.column_classification_reviewed_at.label("column_classification_reviewed_at"),
            func.coalesce(tag_counts_sq.c.tags_count, 0).label("tags_count"),
            func.coalesce(term_counts_sq.c.terms_count, 0).label("terms_count"),
            func.coalesce(search_clicks_sq.c.search_clicks_30d, 0).label("search_clicks_30d"),
            latest_sq.c.dq_score.label("dq_score"),
            latest_sq.c.completeness_pct_avg.label("completeness_pct_avg"),
            latest_sq.c.run_at.label("run_at"),
            func.coalesce(incident_counts_sq.c.open_incidents, 0).label("open_incidents"),
            func.coalesce(incident_counts_sq.c.critical_open_incidents, 0).label(
                "critical_open_incidents"
            ),
            func.coalesce(recent_dq_failures_sq.c.recent_dq_failure_runs_30d, 0).label(
                "recent_dq_failure_runs_30d"
            ),
            latest_sla_sq.c.sla_id.label("sla_id"),
            latest_sla_sq.c.sla_kind.label("sla_kind"),
            latest_sla_sq.c.sla_hours.label("sla_hours"),
            latest_sla_sq.c.sla_status.label("sla_status"),
        )
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .outerjoin(DataOwner, TableEntity.data_owner_id == DataOwner.id)
        .outerjoin(column_stats_sq, column_stats_sq.c.table_id == TableEntity.id)
        .outerjoin(classification_stats_sq, classification_stats_sq.c.table_id == TableEntity.id)
        .outerjoin(tag_counts_sq, tag_counts_sq.c.table_id == TableEntity.id)
        .outerjoin(term_counts_sq, term_counts_sq.c.table_id == TableEntity.id)
        .outerjoin(search_clicks_sq, search_clicks_sq.c.table_id == TableEntity.id)
        .outerjoin(latest_sq, latest_sq.c.table_id == TableEntity.id)
        .outerjoin(recent_dq_failures_sq, recent_dq_failures_sq.c.table_id == TableEntity.id)
        .outerjoin(
            latest_sla_sq,
            (latest_sla_sq.c.table_id == TableEntity.id) & (latest_sla_sq.c.rn == 1),
        )
        .outerjoin(
            incident_counts_sq,
            incident_counts_sq.c.table_fqn == (Schema.name + "." + TableEntity.name),
        )
    )
    if table_ids:
        stmt = stmt.where(TableEntity.id.in_(table_ids))
    if incident_fqns:
        stmt = stmt.where((Schema.name + "." + TableEntity.name).in_(incident_fqns))

    rows = session.execute(stmt).all()
    if not rows:
        return []

    table_fqn_candidates: set[str] = set()
    scoped_table_ids = [int(row.table_id) for row in rows if row.table_id is not None]
    for row in rows:
        datasource_name = str(row.datasource_name or "").strip()
        schema_name = str(row.schema_name or "").strip()
        table_name = str(row.table_name or "").strip()
        for candidate in _fqn_candidates(f"{datasource_name}.{schema_name}.{table_name}"):
            table_fqn_candidates.add(candidate)
        for candidate in _fqn_candidates(f"{schema_name}.{table_name}"):
            table_fqn_candidates.add(candidate)

    scoped_fqns = sorted(table_fqn_candidates)
    active_dq_rows = session.execute(
        select(
            dq_latest_runs_sq.c.table_fqn,
            dq_latest_runs_sq.c.rule_name,
            dq_latest_runs_sq.c.status,
            dq_latest_runs_sq.c.violations_count,
        ).where(
            dq_latest_runs_sq.c.rn == 1,
            dq_latest_runs_sq.c.table_fqn.in_(scoped_fqns),
        )
    ).all()
    active_dq_map: dict[str, list[str]] = {}
    for row in active_dq_rows:
        table_fqn = str(row.table_fqn or "").strip()
        if not table_fqn:
            continue
        if str(row.status).lower() == "fail" and int(row.violations_count or 0) > 0:
            active_dq_map.setdefault(table_fqn, []).append(str(row.rule_name))
    active_dq_rule_count_rows = session.execute(
        select(
            DQRule.table_fqn,
            func.count(DQRule.id).label("active_dq_rules_count"),
        )
        .where(DQRule.is_active.is_(True))
        .where(DQRule.table_fqn.in_(scoped_fqns))
        .group_by(DQRule.table_fqn)
    ).all()
    active_dq_rule_count_map = {
        str(row.table_fqn): int(row.active_dq_rules_count or 0)
        for row in active_dq_rule_count_rows
        if row.table_fqn
    }
    active_dq_incident_rows = session.execute(
        select(
            Incident.table_fqn,
            func.count(Incident.id).label("dq_incidents"),
        )
        .where(
            Incident.entity_type == "table",
            Incident.source_type == "dq_rule",
            Incident.status.in_(["open", "investigating"]),
            Incident.table_fqn.in_(scoped_fqns),
        )
        .group_by(Incident.table_fqn)
    ).all()
    active_dq_incident_map = {str(row.table_fqn): int(row.dq_incidents or 0) for row in active_dq_incident_rows if row.table_fqn}

    review_threshold = now - timedelta(days=90)
    tables: list[TableProfile] = []
    for row in rows:
        decided_at = normalize_dt(row.certification_decided_at)
        certification_review_at = normalize_dt(row.certification_review_at)
        certification_expires_at = normalize_dt(row.certification_expires_at)
        owner_reviewed_at = normalize_dt(row.owner_reviewed_at)
        privacy_review_at = normalize_dt(row.privacy_reviewed_at)
        run_at = normalize_dt(row.run_at)
        last_updated_at = normalize_dt(row.table_updated_at)
        total_columns = int(row.total_columns or 0)
        documented_columns = int(row.documented_columns or 0)
        classified_columns = int(row.classified_columns or 0)
        classification_coverage_pct = round((classified_columns / total_columns) * 100.0, 1) if total_columns else 0.0
        table_fqn = f"{row.datasource_name}.{row.schema_name}.{row.table_name}"
        table_fqn_candidates = _fqn_candidates(table_fqn) + _fqn_candidates(f"{row.schema_name}.{row.table_name}")
        active_dq_rule_names: list[str] = []
        active_dq_incident_count = 0
        for candidate in table_fqn_candidates:
            active_dq_rule_names.extend(active_dq_map.get(candidate, []))
            active_dq_incident_count = max(active_dq_incident_count, int(active_dq_incident_map.get(candidate, 0)))
        active_dq_rule_names = list(dict.fromkeys(active_dq_rule_names))
        owner_name = (
            (row.data_owner_name or "").strip()
            or (row.owner or "").strip()
            or (row.data_owner_email or "").strip()
            or (row.owner_email or "").strip()
            or None
        )
        review_candidates = [candidate for candidate in [decided_at, certification_review_at, owner_reviewed_at, privacy_review_at] if candidate]
        last_review_at = max(review_candidates) if review_candidates else None
        profile = TableProfile(
                table_id=int(row.table_id),
                datasource_id=int(row.datasource_id),
                database_id=int(row.database_id) if row.database_id is not None else None,
                schema_id=int(row.schema_id),
                table_name=row.table_name,
                table_type=row.table_type,
                schema_name=row.schema_name,
                database_name=row.database_name,
                datasource_name=row.datasource_name,
                engine=(row.engine or "other").lower(),
                owner_defined=bool(row.data_owner_id or (row.owner or "").strip()),
                description_complete=bool(
                    (row.description_manual or "").strip() or (row.description_source or "").strip()
                ),
                dictionary_complete=bool(total_columns) and documented_columns == total_columns,
                classification_defined=bool(row.certification_criticality) or bool(row.certification_badges),
                tags_count=int(row.tags_count or 0),
                terms_count=int(row.terms_count or 0),
                total_columns=total_columns,
                documented_columns=documented_columns,
                classified_columns=classified_columns,
                personal_classified_columns=int(row.personal_classified_columns or 0),
                sensitive_classified_columns=int(row.sensitive_classified_columns or 0),
                financial_classified_columns=int(row.financial_classified_columns or 0),
                operational_classified_columns=int(row.operational_classified_columns or 0),
                classification_coverage_pct=classification_coverage_pct,
                column_classification_reviewed_at=normalize_dt(row.column_classification_reviewed_at),
                certification_status=row.certification_status or "not_eligible",
                certification_criticality=row.certification_criticality,
                certification_badges=list(row.certification_badges or []),
                certification_decided_at=decided_at,
                certification_review_at=certification_review_at,
                certification_expires_at=certification_expires_at,
                review_recent=bool(last_review_at and last_review_at >= review_threshold),
                dq_score=float(row.dq_score) if row.dq_score is not None else None,
                completeness_pct_avg=float(row.completeness_pct_avg)
                if row.completeness_pct_avg is not None
                else None,
                freshness_seconds=int((now - run_at).total_seconds()) if run_at else None,
                open_incidents=int(row.open_incidents or 0),
                critical_open_incidents=int(row.critical_open_incidents or 0),
                active_dq_violation=bool(active_dq_rule_names or active_dq_incident_count > 0),
                active_dq_violation_count=len(active_dq_rule_names) + active_dq_incident_count,
                active_dq_rule_names=active_dq_rule_names,
                owner_name=owner_name,
                data_owner_id=int(row.data_owner_id) if row.data_owner_id is not None else None,
                data_owner_is_active=bool(row.data_owner_is_active) if row.data_owner_is_active is not None else None,
                domain_name=(str(row.data_owner_area).strip() or None) if row.data_owner_area else None,
                sensitivity_level=row.sensitivity_level,
                has_personal_data=bool(row.has_personal_data),
                has_sensitive_personal_data=bool(row.has_sensitive_personal_data),
                owner_reviewed_at=owner_reviewed_at,
                privacy_reviewed_at=privacy_review_at,
                last_review_at=last_review_at,
                last_sync_at=run_at,
                last_updated_at=last_updated_at,
                search_clicks_30d=int(row.search_clicks_30d or 0),
                active_dq_rules_count=max(
                    int(active_dq_rule_count_map.get(candidate, 0) or 0)
                    for candidate in table_fqn_candidates
                ),
                recent_dq_failure_runs_30d=int(row.recent_dq_failure_runs_30d or 0),
                sla_defined=bool(row.sla_id),
                sla_hours=int(row.sla_hours) if row.sla_hours is not None else None,
            )
        trust_payload = build_trust_score_for_profile(profile, settings_snapshot=settings_snapshot)
        profile.trust_score = int(trust_payload.score)
        profile.trust_label = trust_payload.label
        profile.trust_tone = trust_payload.tone
        tables.append(profile)

    if current_user is None or is_admin_role(user_role_names(current_user)):
        return tables

    visible_table_ids = _visible_table_ids_for_user(session, [table.table_id for table in tables], current_user=current_user)
    if len(visible_table_ids) == len(tables):
        return tables
    visible_set = set(visible_table_ids)
    return [table for table in tables if table.table_id in visible_set]


def _visible_table_ids_for_user(session: Session, table_ids: list[int], *, current_user) -> list[int]:
    if not table_ids:
        return []
    visible_tables = {
        table.id
        for table in session.scalars(
            select(TableEntity)
            .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
            .where(TableEntity.id.in_(table_ids))
        ).all()
        if can_view_table(current_user, table)
    }
    return [table_id for table_id in table_ids if table_id in visible_tables]


def filter_table_profiles_for_user(session: Session, tables: list[TableProfile], *, current_user=None) -> list[TableProfile]:
    if current_user is None or is_admin_role(user_role_names(current_user)):
        return tables
    visible_table_ids = set(_visible_table_ids_for_user(session, [table.table_id for table in tables], current_user=current_user))
    if not visible_table_ids:
        return []
    return [table for table in tables if table.table_id in visible_table_ids]
