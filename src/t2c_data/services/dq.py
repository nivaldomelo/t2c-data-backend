from __future__ import annotations

"""Data quality domain services and compatibility exports.

Read-side, rule execution and profiling helpers are being moved under
`app.features.data_quality` and remain re-exported here while older imports are
phased out.
"""

from t2c_data.features.data_quality.profiling import profile_table
from t2c_data.features.data_quality.queries import latest_table_metrics, resolve_table_context_by_fqn, table_metrics_with_history
from t2c_data.features.data_quality.rules import run_dq_rule, upsert_incident_for_dq_rule

__all__ = [
    "latest_table_metrics",
    "profile_table",
    "resolve_table_context_by_fqn",
    "run_dq_rule",
    "table_metrics_with_history",
    "upsert_incident_for_dq_rule",
]
