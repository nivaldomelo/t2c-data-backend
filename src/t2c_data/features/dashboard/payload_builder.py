from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from t2c_data.features.dashboard.metrics import build_dashboard_metrics
from t2c_data.features.dashboard.sections import build_dashboard_sections
from t2c_data.features.dashboard.support import TableProfile
from t2c_data.models.catalog import DataSource


def build_dashboard_payload(
    session: Session,
    now: datetime,
    tables: list[TableProfile],
    *,
    datasources: list[DataSource] | None = None,
) -> dict:
    metrics = build_dashboard_metrics(session, now, tables, datasources=datasources)
    return build_dashboard_sections(now, tables, metrics)
