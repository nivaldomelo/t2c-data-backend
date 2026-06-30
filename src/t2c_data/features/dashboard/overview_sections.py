from __future__ import annotations

from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.support import (
    _BADGE_LABELS,
    _CERTIFICATION_STATUS_LABELS,
    _CRITICALITY_LABELS,
    round_pct,
)


def build_overview_sections(metrics: dict[str, object], total_tables: int, dq_monitored_count: int, tables: list) -> dict[str, object]:
    return {
        "kpis": [
            {
                "key": "datasources",
                "label": "Total de Data Sources",
                "value": metrics["datasources_count"],
                "hint": f"{metrics['active_datasources_count']} ativas",
                "tone": "catalog",
            },
            {
                "key": "databases",
                "label": "Bancos conectados",
                "value": metrics["databases_count"],
                "hint": "Ambientes catalogados",
                "tone": "catalog",
            },
            {
                "key": "assets",
                "label": "Ativos catalogados",
                "value": total_tables,
                "hint": "Tabelas e collections",
                "tone": "catalog",
            },
            {
                "key": "certified",
                "label": "Certificadas",
                "value": metrics["certified_count"],
                "hint": f"{metrics['in_review_count']} em revisão · {metrics['rejected_count']} reprovadas",
                "tone": "quality",
            },
            {
                "key": "owner_pct",
                "label": "% com responsável",
                "value": round_pct(metrics["owner_count"], total_tables),
                "unit": "%",
                "hint": f"{metrics['owner_count']} de {total_tables}",
                "tone": "freshness",
            },
            {
                "key": "dictionary_pct",
                "label": "% com dicionário",
                "value": round_pct(metrics["dictionary_count"], total_tables),
                "unit": "%",
                "hint": f"{metrics['dictionary_count']} de {total_tables}",
                "tone": "glossary",
            },
            {
                "key": "dq_avg",
                "label": "DQ score médio",
                "value": metrics["dq_avg_score"],
                "hint": f"{dq_monitored_count} ativos monitorados",
                "tone": "quality",
            },
            {
                "key": "critical_incidents",
                "label": "Incidentes críticos abertos",
                "value": metrics["critical_open_incidents_total"],
                "hint": f"{metrics['critical_open_assets']} ativos impactados",
                "tone": "risk",
            },
        ],
        "certification": {
            "by_status": [
                {
                    "key": key,
                    "label": _CERTIFICATION_STATUS_LABELS.get(key, key),
                    "value": int(metrics["certification_status_counts"].get(key, 0)),
                    "tone": key,
                }
                for key in ["not_eligible", "eligible", "in_review", "certified", "rejected", "revalidation_pending", "expired"]
                if metrics["certification_status_counts"].get(key, 0) > 0
            ],
            "by_criticality": [
                {
                    "key": key,
                    "label": _CRITICALITY_LABELS.get(key, key),
                    "value": int(metrics["certification_criticality_counts"].get(key, 0)),
                    "tone": key,
                }
                for key in ["low", "medium", "high", "critical"]
                if metrics["certification_criticality_counts"].get(key, 0) > 0
            ],
            "by_badge": [
                {
                    "key": key,
                    "label": _BADGE_LABELS.get(key, key),
                    "value": int(metrics["badge_counts"].get(key, 0)),
                    "tone": key,
                }
                for key in ["internal_use", "official_use", "restricted_sensitive"]
            ],
            "eligible_tables": metrics["eligible_count"],
            "pending_critical": sum(
                1
                for table in tables
                if resolve_certification_status_for_profile(table) != "certified" and table.critical_open_incidents > 0
            ),
        },
        "governance": {
            "coverage": metrics["governance_coverage"],
        },
        "sources": {
            "by_engine": metrics["by_engine"],
            "by_datasource": metrics["by_datasource"],
            "lowest_governance": metrics["lowest_governance"],
            "distribution": metrics["source_distribution"],
        },
        "documentation": {
            "coverage": [
                {
                    "key": "description",
                    "label": "Descrição da tabela",
                    "pct": round_pct(metrics["description_count"], total_tables),
                    "count": metrics["description_count"],
                    "total": total_tables,
                    "tone": "sky",
                },
                {
                    "key": "dictionary",
                    "label": "Dicionário completo",
                    "pct": round_pct(metrics["dictionary_count"], total_tables),
                    "count": metrics["dictionary_count"],
                    "total": total_tables,
                    "tone": "violet",
                },
                {
                    "key": "tags",
                    "label": "Tags aplicadas",
                    "pct": round_pct(metrics["tags_count"], total_tables),
                    "count": metrics["tags_count"],
                    "total": total_tables,
                    "tone": "amber",
                },
                {
                    "key": "terms",
                    "label": "Termos associados",
                    "pct": round_pct(metrics["terms_count"], total_tables),
                    "count": metrics["terms_count"],
                    "total": total_tables,
                    "tone": "cyan",
                },
            ],
            "undocumented_tables": total_tables - metrics["description_count"],
            "most_complete": metrics["most_complete"],
            "least_complete": metrics["least_complete"],
        },
    }
