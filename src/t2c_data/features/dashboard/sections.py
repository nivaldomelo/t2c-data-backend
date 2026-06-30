from __future__ import annotations

from datetime import datetime

from t2c_data.features.dashboard.overview_sections import build_overview_sections
from t2c_data.features.dashboard.quality_sections import build_quality_sections
from t2c_data.features.dashboard.support import TableProfile


def build_dashboard_sections(now: datetime, tables: list[TableProfile], metrics: dict[str, object]) -> dict:
    total_tables = metrics["total_tables"]
    dq_with_metrics = metrics["dq_with_metrics"]
    payload = {"generated_at": now}
    payload.update(build_overview_sections(metrics, total_tables, len(dq_with_metrics), tables))
    payload.update(build_quality_sections(metrics, total_tables, dq_with_metrics, tables))
    return payload
