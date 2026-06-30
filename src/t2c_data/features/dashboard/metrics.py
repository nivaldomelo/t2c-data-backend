from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from t2c_data.features.dashboard.metric_queries import load_dashboard_query_metrics
from t2c_data.features.dashboard.metric_rollups import build_dashboard_rollups
from t2c_data.features.dashboard.support import TableProfile
from t2c_data.models.catalog import DataSource


def build_dashboard_metrics(
    session: Session,
    now: datetime,
    tables: list[TableProfile],
    *,
    datasources: list[DataSource] | None = None,
) -> dict[str, object]:
    metrics = build_dashboard_rollups(tables)
    metrics.update(load_dashboard_query_metrics(session, now, tables, datasources=datasources))
    return metrics
