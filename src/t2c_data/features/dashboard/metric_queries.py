from __future__ import annotations

from datetime import datetime

from sqlalchemy import case, desc, func, select
from sqlalchemy.orm import Session

from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.support import TableProfile, normalize_dt
from t2c_data.features.dashboard.source_distribution import build_source_distribution_summary
from t2c_data.models.catalog import DataSource
from t2c_data.models.dq import DQRun, DQTableMetric
from t2c_data.models.incident import Incident


def load_dashboard_query_metrics(
    session: Session,
    now: datetime,
    tables: list[TableProfile],
    *,
    datasources: list[DataSource] | None = None,
) -> dict[str, object]:
    source_distribution = build_source_distribution_summary(session, tables, datasources=datasources)
    databases_count = len({table.database_id for table in tables if table.database_id is not None})
    datasources_count = int(source_distribution["total_sources"])
    active_datasources_count = sum(1 for item in source_distribution["items"] if item.get("is_active"))
    table_fqns = [table.incident_lookup_key for table in tables]
    if table_fqns:
        open_incidents_rows = session.scalars(
            select(Incident).where(
                Incident.entity_type == "table",
                Incident.status.in_(["open", "investigating"]),
                Incident.table_fqn.in_(table_fqns),
            )
        ).all()
    else:
        open_incidents_rows = []
    open_incidents_total = len(open_incidents_rows)
    critical_open_incidents_total = sum(1 for incident in open_incidents_rows if incident.severity == "sev1")

    trend_rows = session.execute(
        select(
            func.date(DQRun.created_at).label("day"),
            func.avg(DQTableMetric.dq_score).label("avg_score"),
        )
        .join(DQTableMetric, DQTableMetric.run_id == DQRun.id)
        .where(
            DQRun.status == "success",
            DQTableMetric.table_id.in_([table.table_id for table in tables] or [-1]),
        )
        .group_by(func.date(DQRun.created_at))
        .order_by(desc(func.date(DQRun.created_at)))
        .limit(10)
    ).all()
    trend = [
        {
            "label": str(row.day)[5:] if row.day else "-",
            "value": round(float(row.avg_score or 0.0), 1),
        }
        for row in reversed(trend_rows)
    ]

    if table_fqns:
        incident_status_rows = session.execute(
            select(Incident.status, func.count(Incident.id))
            .where(
                Incident.status.in_(["open", "investigating", "mitigated", "resolved", "closed"]),
                Incident.entity_type == "table",
                Incident.table_fqn.in_(table_fqns),
            )
            .group_by(Incident.status)
        ).all()
        incident_severity_rows = session.execute(
            select(Incident.severity, func.count(Incident.id))
            .where(
                Incident.entity_type == "table",
                Incident.table_fqn.in_(table_fqns),
            )
            .group_by(Incident.severity)
        ).all()
        top_incidents_rows = session.scalars(
            select(Incident)
            .where(Incident.entity_type == "table", Incident.table_fqn.in_(table_fqns))
            .order_by(
                case(
                    (Incident.severity == "sev1", 0),
                    (Incident.severity == "sev2", 1),
                    (Incident.severity == "sev3", 2),
                    else_=3,
                ),
                Incident.detected_at.asc(),
            )
            .limit(6)
        ).all()
        open_incidents = open_incidents_rows
    else:
        incident_status_rows = []
        incident_severity_rows = []
        top_incidents_rows = []
        open_incidents = []
    if open_incidents:
        avg_open_age_hours = round(
            sum(
                max(
                    0.0,
                    (now - (normalize_dt(incident.detected_at) or now)).total_seconds(),
                )
                for incident in open_incidents
            )
            / (3600 * len(open_incidents)),
            1,
        )
    else:
        avg_open_age_hours = 0.0

    certified_fqns = {
        table.incident_lookup_key
        for table in tables
        if resolve_certification_status_for_profile(table, now=now) == "certified"
    }
    open_on_certified_assets = sum(
        1
        for incident in open_incidents
        if incident.entity_type == "table" and incident.table_fqn in certified_fqns
    )

    return {
        "databases_count": databases_count,
        "datasources_count": datasources_count,
        "active_datasources_count": active_datasources_count,
        "source_distribution": source_distribution,
        "open_incidents_total": open_incidents_total,
        "critical_open_incidents_total": critical_open_incidents_total,
        "trend": trend,
        "incident_status_rows": incident_status_rows,
        "incident_severity_rows": incident_severity_rows,
        "top_incidents_rows": top_incidents_rows,
        "avg_open_age_hours": avg_open_age_hours,
        "open_on_certified_assets": open_on_certified_assets,
    }
