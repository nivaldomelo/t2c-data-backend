from __future__ import annotations

from datetime import datetime, timezone

from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.support import (
    _INCIDENT_SEVERITY_LABELS,
    _INCIDENT_STATUS_LABELS,
)


def build_quality_sections(metrics: dict[str, object], total_tables: int, dq_with_metrics: list, tables: list) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "dq": {
            "avg_score": metrics["dq_avg_score"],
            "below_minimum": sum(1 for table in dq_with_metrics if (table.dq_score or 0.0) < 80),
            "without_metrics": total_tables - len(dq_with_metrics),
            "score_bands": [
                {"key": "0_60", "label": "0-60", "value": int(metrics["score_bands_counter"].get("0_60", 0)), "tone": "risk"},
                {"key": "60_80", "label": "60-80", "value": int(metrics["score_bands_counter"].get("60_80", 0)), "tone": "warning"},
                {"key": "80_90", "label": "80-90", "value": int(metrics["score_bands_counter"].get("80_90", 0)), "tone": "attention"},
                {"key": "90_100", "label": "90-100", "value": int(metrics["score_bands_counter"].get("90_100", 0)), "tone": "success"},
            ],
            "freshness_bands": [
                {"key": "ate_1h", "label": "Até 1h", "value": int(metrics["freshness_bands_counter"].get("ate_1h", 0)), "tone": "success"},
                {"key": "ate_24h", "label": "Até 24h", "value": int(metrics["freshness_bands_counter"].get("ate_24h", 0)), "tone": "attention"},
                {"key": "acima_24h", "label": "> 24h", "value": int(metrics["freshness_bands_counter"].get("acima_24h", 0)), "tone": "risk"},
                {"key": "sem_execucao", "label": "Sem métricas", "value": int(metrics["freshness_bands_counter"].get("sem_execucao", 0)), "tone": "neutral"},
            ],
            "worst_tables": [
                table.to_summary()
                for table in sorted(dq_with_metrics, key=lambda item: (item.dq_score or 0.0, item.table_fqn))[:6]
            ],
            "trend": metrics["trend"],
        },
        "incidents": {
            "total_open": metrics["open_incidents_total"],
            "critical_open": metrics["critical_open_incidents_total"],
            "open_on_certified_assets": metrics["open_on_certified_assets"],
            "avg_open_age_hours": metrics["avg_open_age_hours"],
            "by_status": [
                {
                    "key": key,
                    "label": _INCIDENT_STATUS_LABELS.get(key, key),
                    "value": int(value),
                    "tone": key,
                }
                for key, value in sorted(((row[0], row[1]) for row in metrics["incident_status_rows"]), key=lambda item: item[0])
            ],
            "by_priority": [
                {
                    "key": key,
                    "label": _INCIDENT_SEVERITY_LABELS.get(key, key),
                    "value": int(value),
                    "tone": key,
                }
                for key, value in sorted(((row[0], row[1]) for row in metrics["incident_severity_rows"]), key=lambda item: item[0])
            ],
            "top_items": [
                {
                    "id": incident.id,
                    "title": incident.title,
                    "entity_type": incident.entity_type,
                    "severity": incident.severity,
                    "status": incident.status,
                    "detected_at": incident.detected_at,
                    "table_fqn": incident.table_fqn,
                    "airflow_dag_id": incident.airflow_dag_id,
                }
                for incident in metrics["top_incidents_rows"]
            ],
        },
        "attention": {
            "low_dq": [
                table.to_summary()
                for table in sorted(
                    [table for table in dq_with_metrics if (table.dq_score or 0.0) < 80],
                    key=lambda item: (item.dq_score or 0.0, item.table_fqn),
                )[:6]
            ],
            "no_owner": [
                table.to_summary()
                for table in sorted([table for table in tables if not table.owner_defined], key=lambda item: item.table_fqn)[:6]
            ],
            "no_dictionary": [
                table.to_summary()
                for table in sorted(
                    [table for table in tables if not table.dictionary_complete],
                    key=lambda item: item.table_fqn,
                )[:6]
            ],
            "eligible_not_certified": [
                table.to_summary()
                for table in sorted(
                    [
                        table
                        for table in tables
                        if table.eligible_for_certification
                        and resolve_certification_status_for_profile(table, now=now) != "certified"
                    ],
                    key=lambda item: (-item.readiness_score, item.table_fqn),
                )[:6]
            ],
            "critical_incidents": [
                table.to_summary()
                for table in sorted(
                    [table for table in tables if table.critical_open_incidents > 0],
                    key=lambda item: (-item.critical_open_incidents, item.table_fqn),
                )[:6]
            ],
            "rejected": [
                table.to_summary()
                for table in sorted(
                    [table for table in tables if table.certification_status == "rejected"],
                    key=lambda item: item.table_fqn,
                )[:6]
            ],
            "restricted": [
                table.to_summary()
                for table in sorted(
                    [table for table in tables if "restricted_sensitive" in table.certification_badges],
                    key=lambda item: item.table_fqn,
                )[:6]
            ],
        },
    }
