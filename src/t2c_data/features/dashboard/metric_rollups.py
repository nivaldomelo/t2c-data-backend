from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone

from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.support import TableProfile, _SUPPORTED_ENGINES, engine_label, round_pct


def build_dashboard_rollups(tables: list[TableProfile]) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    total_tables = len(tables)
    owner_count = sum(1 for table in tables if table.owner_defined)
    description_count = sum(1 for table in tables if table.description_complete)
    dictionary_count = sum(1 for table in tables if table.dictionary_complete)
    tags_count = sum(1 for table in tables if table.tags_count > 0)
    terms_count = sum(1 for table in tables if table.terms_count > 0)
    classification_count = sum(1 for table in tables if table.classification_defined)
    review_recent_count = sum(1 for table in tables if table.review_recent)
    eligible_count = sum(1 for table in tables if table.eligible_for_certification)
    certification_statuses = [resolve_certification_status_for_profile(table, now=now) for table in tables]
    certified_count = sum(1 for status in certification_statuses if status == "certified")
    in_review_count = sum(1 for status in certification_statuses if status == "in_review")
    rejected_count = sum(1 for status in certification_statuses if status == "rejected")
    dq_with_metrics = [table for table in tables if table.dq_score is not None]
    dq_avg_score = (
        round(sum(table.dq_score or 0.0 for table in dq_with_metrics) / len(dq_with_metrics), 1)
        if dq_with_metrics
        else 0.0
    )
    critical_open_assets = sum(1 for table in tables if table.critical_open_incidents > 0)

    certification_status_counts = Counter(certification_statuses)
    certification_criticality_counts = Counter(
        table.certification_criticality for table in tables if table.certification_criticality
    )
    badge_counts: Counter[str] = Counter()
    for table in tables:
        badge_counts.update(table.certification_badges)

    governance_coverage = [
        {
            "key": "owner",
            "label": "Owner definido",
            "pct": round_pct(owner_count, total_tables),
            "count": owner_count,
            "total": total_tables,
            "tone": "sky",
        },
        {
            "key": "dictionary",
            "label": "Dicionário completo",
            "pct": round_pct(dictionary_count, total_tables),
            "count": dictionary_count,
            "total": total_tables,
            "tone": "violet",
        },
        {
            "key": "tags",
            "label": "Tags aplicadas",
            "pct": round_pct(tags_count, total_tables),
            "count": tags_count,
            "total": total_tables,
            "tone": "amber",
        },
        {
            "key": "terms",
            "label": "Termos associados",
            "pct": round_pct(terms_count, total_tables),
            "count": terms_count,
            "total": total_tables,
            "tone": "cyan",
        },
        {
            "key": "classification",
            "label": "Classificação definida",
            "pct": round_pct(classification_count, total_tables),
            "count": classification_count,
            "total": total_tables,
            "tone": "rose",
        },
        {
            "key": "review_recent",
            "label": "Revisão recente",
            "pct": round_pct(review_recent_count, total_tables),
            "count": review_recent_count,
            "total": total_tables,
            "tone": "emerald",
        },
        {
            "key": "eligible",
            "label": "Elegíveis para certificação",
            "pct": round_pct(eligible_count, total_tables),
            "count": eligible_count,
            "total": total_tables,
            "tone": "slate",
        },
    ]

    score_bands_counter = Counter()
    for table in dq_with_metrics:
        score = table.dq_score or 0.0
        if score < 60:
            score_bands_counter["0_60"] += 1
        elif score < 80:
            score_bands_counter["60_80"] += 1
        elif score < 90:
            score_bands_counter["80_90"] += 1
        else:
            score_bands_counter["90_100"] += 1

    freshness_bands_counter = Counter()
    for table in tables:
        if table.freshness_seconds is None:
            freshness_bands_counter["sem_execucao"] += 1
        elif table.freshness_seconds <= 3600:
            freshness_bands_counter["ate_1h"] += 1
        elif table.freshness_seconds <= 24 * 3600:
            freshness_bands_counter["ate_24h"] += 1
        else:
            freshness_bands_counter["acima_24h"] += 1

    engine_counter = Counter(table.engine or "other" for table in tables)
    datasource_counter = Counter(table.datasource_name for table in tables)
    datasource_governance: dict[str, list[float]] = defaultdict(list)
    for table in tables:
        coverage_values = [
            1.0 if table.owner_defined else 0.0,
            1.0 if table.dictionary_complete else 0.0,
            1.0 if table.tags_count > 0 else 0.0,
            1.0 if table.terms_count > 0 else 0.0,
        ]
        datasource_governance[table.datasource_name].append(sum(coverage_values) / len(coverage_values))

    by_engine = [
        {
            "key": key,
            "label": engine_label(key),
            "value": int(engine_counter.get(key, 0)),
            "tone": None,
        }
        for key, _ in _SUPPORTED_ENGINES
        if engine_counter.get(key, 0) > 0
    ]
    by_datasource = [
        {"key": key, "label": key, "value": int(value), "tone": None}
        for key, value in datasource_counter.most_common(8)
    ]
    lowest_governance = [
        {
            "key": key,
            "label": key,
            "value": round((sum(values) / max(1, len(values))) * 100.0, 1),
            "tone": None,
        }
        for key, values in sorted(
            datasource_governance.items(),
            key=lambda item: (sum(item[1]) / max(1, len(item[1])), -len(item[1])),
        )[:6]
    ]

    most_complete = [
        table.to_summary()
        for table in sorted(tables, key=lambda item: (-item.documentation_score, item.table_fqn))[:5]
    ]
    least_complete = [
        table.to_summary()
        for table in sorted(tables, key=lambda item: (item.documentation_score, item.table_fqn))[:5]
    ]

    return {
        "total_tables": total_tables,
        "owner_count": owner_count,
        "description_count": description_count,
        "dictionary_count": dictionary_count,
        "tags_count": tags_count,
        "terms_count": terms_count,
        "classification_count": classification_count,
        "review_recent_count": review_recent_count,
        "eligible_count": eligible_count,
        "certified_count": certified_count,
        "in_review_count": in_review_count,
        "rejected_count": rejected_count,
        "dq_with_metrics": dq_with_metrics,
        "dq_avg_score": dq_avg_score,
        "critical_open_assets": critical_open_assets,
        "certification_status_counts": certification_status_counts,
        "certification_criticality_counts": certification_criticality_counts,
        "badge_counts": badge_counts,
        "governance_coverage": governance_coverage,
        "score_bands_counter": score_bands_counter,
        "freshness_bands_counter": freshness_bands_counter,
        "by_engine": by_engine,
        "by_datasource": by_datasource,
        "lowest_governance": lowest_governance,
        "most_complete": most_complete,
        "least_complete": least_complete,
    }
