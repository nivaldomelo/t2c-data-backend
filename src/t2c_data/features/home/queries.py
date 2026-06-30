from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import case, desc, func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.features.shared_cache import get_cached_value, session_cache_key, set_cached_value
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRun, DQTableMetric
from t2c_data.models.glossary import GlossaryTerm
from t2c_data.models.scan import ScanRun
from t2c_data.models.glossary import GlossaryAssignment
from t2c_data.models.tag import Tag, TagAssignment
from t2c_data.features.privacy_access import can_view_table

_CACHE_TTL_SECONDS = 60


def _empty_counts() -> dict[str, int]:
    return {
        "active_datasources": 0,
        "datasources": 0,
        "schemas": 0,
        "tables": 0,
        "monitored_tables": 0,
        "columns": 0,
        "tags": 0,
        "glossary_terms": 0,
    }


def get_home_summary(session: Session, current_user: User | None = None) -> dict:
    now = datetime.now(timezone.utc)
    cacheable = current_user is None or is_admin_role(user_role_names(current_user))
    if cacheable:
        cache_key = (
            session_cache_key(session),
            getattr(current_user, "id", None),
            tuple(sorted(user_role_names(current_user))) if current_user is not None else (),
        )
        cached = get_cached_value("home_summary", cache_key, now=now)
        if isinstance(cached, dict):
            return cached

    latest_metrics_sq = (
        select(
            DQTableMetric.table_id.label("table_id"),
            DQTableMetric.dq_score.label("dq_score"),
            DQTableMetric.completeness_pct_avg.label("completeness_pct_avg"),
            DQTableMetric.row_count.label("row_count"),
            DQRun.created_at.label("run_at"),
            func.row_number()
            .over(partition_by=DQTableMetric.table_id, order_by=DQRun.created_at.desc())
            .label("rn"),
        )
        .join(DQRun, DQTableMetric.run_id == DQRun.id)
        .where(DQRun.status == "success")
        .subquery()
    )

    latest_sq = (
        select(
            latest_metrics_sq.c.table_id,
            latest_metrics_sq.c.dq_score,
            latest_metrics_sq.c.completeness_pct_avg,
            latest_metrics_sq.c.row_count,
            latest_metrics_sq.c.run_at,
        )
        .where(latest_metrics_sq.c.rn == 1)
        .subquery()
    )

    counts = _empty_counts()
    restricted_scope = current_user is not None and not is_admin_role(user_role_names(current_user))
    visible_tables: list[TableEntity] = []
    if restricted_scope:
        visible_tables = [
            table
            for table in session.scalars(
                select(TableEntity)
                .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
                .options(selectinload(TableEntity.columns))
            ).all()
            if can_view_table(current_user, table)
        ]
        visible_table_ids = [table.id for table in visible_tables]
        visible_datasource_ids = {table.schema.database.datasource_id for table in visible_tables if table.schema and table.schema.database}
        visible_schema_ids = {table.schema_id for table in visible_tables}
        counts["active_datasources"] = int(
            session.scalar(
                select(func.count(DataSource.id)).where(
                    DataSource.id.in_(visible_datasource_ids),
                    DataSource.is_active.is_(True),
                )
            )
            or 0
        )
        counts["datasources"] = len(visible_datasource_ids)
        counts["schemas"] = len(visible_schema_ids)
        counts["tables"] = len(visible_tables)
        counts["columns"] = sum(len(table.columns) for table in visible_tables)
        counts["tags"] = int(
            session.scalar(
                select(func.count(func.distinct(TagAssignment.tag_id))).where(
                    TagAssignment.entity_type == "table",
                    TagAssignment.entity_id.in_(visible_table_ids),
                )
            )
            or 0
        )
        counts["glossary_terms"] = int(
            session.scalar(
                select(func.count(func.distinct(GlossaryAssignment.term_id))).where(
                    GlossaryAssignment.entity_type == "table",
                    GlossaryAssignment.entity_id.in_(visible_table_ids),
                )
            )
            or 0
        )
    else:
        counts["active_datasources"] = int(
            session.scalar(select(func.count(DataSource.id)).where(DataSource.is_active.is_(True))) or 0
        )
        counts["datasources"] = int(session.scalar(select(func.count(DataSource.id))) or 0)
        counts["schemas"] = int(session.scalar(select(func.count(Schema.id))) or 0)
        counts["tables"] = int(session.scalar(select(func.count(TableEntity.id))) or 0)
        counts["columns"] = int(session.scalar(select(func.count(ColumnEntity.id))) or 0)
        counts["tags"] = int(session.scalar(select(func.count(Tag.id))) or 0)
        counts["glossary_terms"] = int(session.scalar(select(func.count(GlossaryTerm.id))) or 0)

    freshness_sla_seconds = 24 * 3600
    sla_threshold = now - timedelta(seconds=freshness_sla_seconds)

    if restricted_scope:
        monitored_tables = int(
            session.scalar(select(func.count(latest_sq.c.table_id)).where(latest_sq.c.table_id.in_(visible_table_ids))) or 0
        )
    else:
        monitored_tables = int(session.scalar(select(func.count(latest_sq.c.table_id))) or 0)
    counts["monitored_tables"] = monitored_tables

    dq_avg_score = float(session.scalar(select(func.avg(latest_sq.c.dq_score))) or 0.0)
    completeness_avg = float(session.scalar(select(func.avg(latest_sq.c.completeness_pct_avg))) or 0.0)
    freshness_ok = int(
        session.scalar(
            select(func.sum(case((latest_sq.c.run_at >= sla_threshold, 1), else_=0)))
        )
        or 0
    )
    freshness_sla_pct = round((freshness_ok / monitored_tables) * 100.0, 2) if monitored_tables > 0 else 0.0

    issue_base = (
        select(
            TableEntity.id.label("table_id"),
            DataSource.name.label("datasource"),
            Schema.name.label("schema"),
            TableEntity.name.label("table"),
            latest_sq.c.dq_score,
            latest_sq.c.completeness_pct_avg,
            latest_sq.c.row_count,
            latest_sq.c.run_at,
            TableEntity.sensitivity_level.label("sensitivity_level"),
            TableEntity.has_personal_data.label("has_personal_data"),
            TableEntity.access_scope.label("access_scope"),
        )
        .join(latest_sq, latest_sq.c.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    )

    visible_tables_by_id: dict[int, TableEntity] = {}
    if current_user is not None:
        candidates = session.scalars(
            select(TableEntity)
            .join(latest_sq, latest_sq.c.table_id == TableEntity.id)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .order_by(TableEntity.id)
        ).all()
        visible_tables_by_id = {table.id: table for table in candidates if can_view_table(current_user, table)}

    def _visible_rows(ordering):
        rows = session.execute(issue_base.order_by(ordering).limit(20)).all()
        if current_user is None:
            return rows[:5]
        return [row for row in rows if row.table_id in visible_tables_by_id][:5]

    critical_rows = _visible_rows(latest_sq.c.dq_score.asc())
    stale_rows = _visible_rows(latest_sq.c.run_at.asc())

    def _issue_row(row: tuple) -> dict:
        run_at = row.run_at
        freshness_seconds = int((now - run_at).total_seconds()) if run_at else 0
        table_fqn = f"{row.datasource}.{row.schema}.{row.table}"
        return {
            "table_id": int(row.table_id),
            "datasource": row.datasource,
            "schema": row.schema,
            "table": row.table,
            "table_fqn": table_fqn,
            "dq_score": float(row.dq_score or 0.0),
            "completeness_pct_avg": float(row.completeness_pct_avg or 0.0),
            "row_count": int(row.row_count or 0),
            "freshness_seconds": freshness_seconds,
            "run_at": run_at,
            "sensitivity_level": row.sensitivity_level,
            "has_personal_data": bool(row.has_personal_data),
            "access_scope": row.access_scope,
        }

    top_critical_tables = [_issue_row(row) for row in critical_rows]
    top_stale_tables = [_issue_row(row) for row in stale_rows]

    history_rows = session.execute(
        select(
            DQRun.id.label("run_id"),
            DQRun.created_at.label("run_at"),
            DQTableMetric.dq_score.label("dq_score"),
            DQTableMetric.completeness_pct_avg.label("completeness_pct_avg"),
            DQTableMetric.row_count.label("row_count"),
        )
        .join(DQTableMetric, DQRun.id == DQTableMetric.run_id)
        .where(DQRun.status == "success")
        .order_by(desc(DQRun.created_at))
        .limit(14)
    ).all()

    history = []
    for row in reversed(history_rows):
        history.append(
            {
                "run_id": int(row.run_id),
                "run_at": row.run_at,
                "dq_score": float(row.dq_score or 0.0),
                "completeness_pct_avg": float(row.completeness_pct_avg or 0.0),
                "row_count": int(row.row_count or 0),
                "freshness_seconds": int((now - row.run_at).total_seconds()) if row.run_at else 0,
            }
        )

    payload = {
        "counts": counts,
        "dq_avg_score": round(dq_avg_score, 2),
        "completeness_avg": round(completeness_avg, 2),
        "freshness_sla_pct": freshness_sla_pct,
        "freshness_sla_seconds": freshness_sla_seconds,
        "last_scan_at": session.scalar(select(func.max(ScanRun.created_at))),
        "top_critical_tables": top_critical_tables,
        "top_stale_tables": top_stale_tables,
        "history": history,
    }

    if cacheable:
        cache_key = (
            session_cache_key(session),
            getattr(current_user, "id", None),
            tuple(sorted(user_role_names(current_user))) if current_user is not None else (),
        )
        set_cached_value("home_summary", cache_key, payload, ttl_seconds=_CACHE_TTL_SECONDS, now=now)

    return payload
