from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from t2c_data.features.contracts.service import contract_summary, get_current_contract
from t2c_data.features.lineage.table_summary import get_table_summary
from t2c_data.models.dq import DQRule
from t2c_data.models.dq import DQRuleLatestRun
from t2c_data.models.dq import DQRuleRun
from t2c_data.models.catalog import TableEntity
from t2c_data.models.dq import DQRun
from t2c_data.models.incident import Incident

_STATUS_LABELS = {
    "healthy": "Saudável",
    "partial": "Parcial",
    "degraded": "Degradado",
    "critical": "Crítico",
    "no_data": "Sem dados",
    "not_evaluated": "Não avaliado",
    "not_calculable": "Não calculável",
    "stale": "Desatualizado",
}

_TONE_BY_STATUS = {
    "healthy": "success",
    "partial": "warning",
    "degraded": "warning",
    "critical": "danger",
    "no_data": "neutral",
    "not_evaluated": "neutral",
    "not_calculable": "neutral",
    "stale": "warning",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _pct(value: float | None) -> float | None:
    if value is None:
        return None
    return round(max(0.0, min(100.0, value)), 2)


def _status_label(status: str) -> str:
    return _STATUS_LABELS.get(status, status.replace("_", " ").title())


def _status_tone(status: str) -> str:
    return _TONE_BY_STATUS.get(status, "neutral")


def _evaluation_label(*, evidence_level: str, status: str) -> str:
    if status in {"no_data", "not_evaluated", "not_calculable"} or evidence_level == "none":
        return "Não avaliado"
    if status in {"critical", "degraded"}:
        return "Crítico" if status == "critical" else "Atenção"
    if status == "partial" or evidence_level in {"automatic_profiling", "operational_signal", "partial"}:
        return "Parcialmente avaliada"
    if status == "healthy" and evidence_level == "formal_rule":
        return "Saudável"
    if status == "healthy" and evidence_level in {"automatic_profiling", "operational_signal"}:
        return "Parcialmente avaliada"
    return _status_label(status)


def _evaluation_tone(*, evidence_level: str, status: str) -> str:
    if status in {"no_data", "not_evaluated", "not_calculable"} or evidence_level == "none":
        return "neutral"
    if status in {"critical", "degraded"}:
        return "danger"
    if status == "partial" or evidence_level in {"automatic_profiling", "operational_signal", "partial"}:
        return "warning"
    if status == "healthy" and evidence_level == "formal_rule":
        return "success"
    if status == "healthy" and evidence_level in {"automatic_profiling", "operational_signal"}:
        return "warning"
    return _status_tone(status)


def _worst_status(*statuses: str) -> str:
    order = {
        "critical": 5,
        "degraded": 4,
        "stale": 4,
        "partial": 3,
        "no_data": 2,
        "not_evaluated": 2,
        "not_calculable": 2,
        "healthy": 1,
    }
    filtered = [status for status in statuses if status]
    if not filtered:
        return "not_evaluated"
    return max(filtered, key=lambda status: order.get(status, 0))


def _median_or_none(values: Iterable[float | int | None]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return float(median(cleaned))


def _current_columns(snapshot: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not snapshot:
        return []
    return [column for column in snapshot.get("columns", []) if isinstance(column, dict)]


def _build_dimension(
    *,
    key: str,
    label: str,
    status: str,
    value: float | int | None,
    baseline: float | int | None = None,
    delta: float | int | None = None,
    unit: str | None = None,
    detail: str | None = None,
    score: float | None = None,
    applicable: bool = True,
    coverage_type: str = "none",
    coverage_label: str | None = None,
    evidence_level: str = "none",
    rules_count: int = 0,
    configured_rules_count: int | None = None,
    formal_rules_count: int | None = None,
    failed_rules_count: int = 0,
    metric_value: float | int | None = None,
    metric_label: str | None = None,
    trend: dict[str, Any] | None = None,
    summary: str | None = None,
    explanation: str | None = None,
    recommended_action: str | None = None,
) -> dict[str, Any]:
    resolved_configured_rules_count = configured_rules_count if configured_rules_count is not None else rules_count
    resolved_formal_rules_count = formal_rules_count if formal_rules_count is not None else resolved_configured_rules_count
    resolved_trend = trend or {"direction": "unknown", "value": None, "label": "Sem histórico"}
    return {
        "key": key,
        "label": label,
        "status": status,
        "status_label": _status_label(status),
        "tone": _status_tone(status),
        "evaluation_status": status if evidence_level == "formal_rule" else "partial" if evidence_level in {"automatic_profiling", "operational_signal", "partial"} else "not_evaluated" if evidence_level == "none" else status,
        "evaluation_label": _evaluation_label(evidence_level=evidence_level, status=status),
        "evaluation_tone": _evaluation_tone(evidence_level=evidence_level, status=status),
        "value": value,
        "baseline": baseline,
        "delta": delta,
        "unit": unit,
        "detail": detail,
        "score": score,
        "applicable": applicable,
        "coverage_type": coverage_type,
        "coverage_label": coverage_label,
        "evidence_level": evidence_level,
        "rules_count": int(rules_count or 0),
        "configured_rules_count": int(resolved_configured_rules_count or 0),
        "formal_rules_count": int(resolved_formal_rules_count or 0),
        "failed_rules_count": int(failed_rules_count or 0),
        "metric_value": metric_value,
        "metric_label": metric_label,
        "trend": resolved_trend,
        "summary": summary or detail,
        "explanation": explanation,
        "recommended_action": recommended_action,
    }


def _human_duration(seconds: int | float | None) -> str | None:
    if seconds is None:
        return None
    try:
        value = max(0, int(round(float(seconds))))
    except (TypeError, ValueError):
        return None
    if value < 60:
        return f"{value}s"
    minutes = value // 60
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    return f"{days}d"


def _format_sla_label(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    duration = _human_duration(seconds)
    if duration is None:
        return None
    return f"SLA: {duration}"


def _format_trend_label(
    *,
    current_value: float | int | None,
    previous_value: float | int | None,
    higher_is_better: bool = True,
    kind: str = "score",
    stable_threshold: float = 0.5,
) -> dict[str, Any]:
    if current_value is None or previous_value is None:
        return {"direction": "unknown", "value": None, "label": "Sem histórico"}
    try:
        current = float(current_value)
        previous = float(previous_value)
    except (TypeError, ValueError):
        return {"direction": "unknown", "value": None, "label": "Sem histórico"}
    delta = current - previous
    if abs(delta) < stable_threshold:
        return {"direction": "stable", "value": None, "label": "Estável"}
    better = delta > 0 if higher_is_better else delta < 0
    if kind == "freshness":
        if delta < 0:
            return {"direction": "up", "value": None, "label": "Mais recente"}
        return {"direction": "down", "value": None, "label": "Mais atrasado"}
    if kind == "count":
        if delta < 0:
            return {"direction": "up", "value": None, "label": "Reduziu"}
        return {"direction": "down", "value": None, "label": "Aumentou"}
    if kind == "percent":
        return {
            "direction": "up" if better else "down",
            "value": None,
            "label": f"{'Melhorou' if better else 'Piorou'} {abs(delta):.1f} p.p.",
        }
    if kind == "score":
        return {
            "direction": "up" if better else "down",
            "value": None,
            "label": f"{'Melhorou' if better else 'Piorou'} {abs(delta):.1f} pontos",
        }
    return {"direction": "up" if better else "down", "value": None, "label": "Estável"}


def _humanize_metric_value(value: float | int | None, *, kind: str) -> str | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if kind == "percentage":
        return f"{numeric:.1f}%"
    if kind == "freshness":
        duration = _human_duration(numeric)
        return f"Atualizado há {duration}" if duration is not None else None
    if kind == "count":
        rounded = int(round(numeric))
        return f"{rounded} registro(s)"
    if kind == "score":
        return f"{numeric:.1f} pts"
    return str(value)


def _rule_dimension_stats(session: Session, table_id: int) -> dict[str, dict[str, int]]:
    try:
        rows = session.execute(
            select(DQRule, DQRuleLatestRun, DQRuleRun)
            .outerjoin(DQRuleLatestRun, DQRuleLatestRun.rule_id == DQRule.id)
            .outerjoin(DQRuleRun, DQRuleRun.id == DQRuleLatestRun.latest_rule_run_id)
            .where(
                DQRule.table_id == table_id,
                DQRule.is_active.is_(True),
                DQRule.archived.is_(False),
            )
        ).all()
    except Exception:
        return {}
    stats: dict[str, dict[str, int]] = {}
    for rule, _snapshot, latest_run in rows:
        dimension = _dimension_from_rule(rule)
        if not dimension:
            continue
        current = stats.setdefault(dimension, {"rules_count": 0, "failed_rules_count": 0})
        current["rules_count"] += 1
        if latest_run is not None:
            latest_status = str(latest_run.status or "").strip().lower()
            violations_count = _safe_int(getattr(latest_run, "violations_count", None), 0) or 0
            if latest_status == "failed" or violations_count > 0:
                current["failed_rules_count"] += 1
    return stats


def _classify_completeness(*, row_count: int, completeness_pct: float | None) -> tuple[str, str]:
    if row_count <= 0:
        return "no_data", "Sem linhas para avaliar completude"
    if completeness_pct is None:
        return "not_evaluated", "Completude indisponível para a execução atual"
    if completeness_pct >= 97:
        return "healthy", "Completude muito alta"
    if completeness_pct >= 90:
        return "partial", "Completude adequada, mas com campos a revisar"
    return "degraded", "Completude abaixo do esperado"


def _classify_uniqueness(*, row_count: int, duplicates_count: int) -> tuple[str, str, float | None]:
    if row_count <= 0:
        return "no_data", "Sem linhas para avaliar unicidade", None
    duplicate_pct = round((duplicates_count / row_count) * 100.0, 2) if row_count > 0 else None
    if duplicates_count == 0:
        return "healthy", "Nenhuma duplicidade detectada", duplicate_pct
    if duplicate_pct is not None and duplicate_pct <= 1:
        return "partial", "Duplicidade baixa, mas existente", duplicate_pct
    return "degraded", "Duplicidade relevante detectada", duplicate_pct


def _classify_validity(*, row_count: int, failed_rules: int) -> tuple[str, str]:
    if row_count <= 0:
        return "no_data", "Sem linhas para avaliar validade"
    if failed_rules <= 0:
        return "healthy", "Nenhuma falha de regra na última execução"
    if failed_rules == 1:
        return "partial", "Uma regra em falha"
    if failed_rules == 2:
        return "degraded", "Falhas múltiplas em regras"
    return "critical", "Falhas recorrentes em regras"


def _classify_freshness(*, row_count: int, freshness_seconds: int, sla_seconds: int | None) -> tuple[str, str]:
    if row_count <= 0:
        return "no_data", "Sem dados para avaliar freshness"
    if sla_seconds is None:
        if freshness_seconds <= 6 * 3600:
            return "healthy", "Atualização recente"
        if freshness_seconds <= 24 * 3600:
            return "partial", "Atualização em risco"
        return "degraded", "Atualização atrasada"
    if freshness_seconds <= sla_seconds:
        return "healthy", "Dentro do SLA de freshness"
    if freshness_seconds <= int(sla_seconds * 1.25):
        return "partial", "Fora do SLA, porém próximo da janela aceitável"
    return "degraded", "Fora do SLA de freshness"


def _classify_volume(*, row_count: int, baseline: float | None) -> tuple[str, str, float | None]:
    if row_count <= 0:
        return "no_data", "Sem dados no último profiling", None
    if baseline is None or baseline <= 0:
        return "not_evaluated", "Ainda sem baseline suficiente", None
    delta_pct = round(((row_count - baseline) / baseline) * 100.0, 2)
    if abs(delta_pct) <= 15:
        return "healthy", "Volume próximo do baseline", delta_pct
    if abs(delta_pct) <= 35:
        return "partial", "Variação de volume moderada", delta_pct
    return "degraded", "Variação de volume relevante", delta_pct


def _classify_schema(
    *,
    current_columns: list[dict[str, Any]],
    previous_columns: list[dict[str, Any]] | None,
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    if not previous_columns:
        return "not_evaluated", {"added": 0, "removed": 0, "type_changed": 0}, []

    previous_map = {str(column.get("column_name") or ""): column for column in previous_columns}
    current_map = {str(column.get("column_name") or ""): column for column in current_columns}
    added = [name for name in current_map if name and name not in previous_map]
    removed = [name for name in previous_map if name and name not in current_map]
    type_changed: list[dict[str, Any]] = []
    for name, column in current_map.items():
        previous = previous_map.get(name)
        if not previous:
            continue
        current_type = str(column.get("data_type") or "").strip().lower()
        previous_type = str(previous.get("data_type") or "").strip().lower()
        if current_type and previous_type and current_type != previous_type:
            type_changed.append(
                {
                    "column_name": name,
                    "current_type": column.get("data_type"),
                    "previous_type": previous.get("data_type"),
                    "breaking": True,
                }
            )
    if removed or type_changed:
        return "degraded", {"added": len(added), "removed": len(removed), "type_changed": len(type_changed)}, [
            {"kind": "schema_addition", "column_name": name, "breaking": False}
            for name in added
        ] + [
            {"kind": "schema_removal", "column_name": name, "breaking": True}
            for name in removed
        ] + type_changed
    if added:
        return "partial", {"added": len(added), "removed": 0, "type_changed": 0}, [
            {"kind": "schema_addition", "column_name": name, "breaking": False}
            for name in added
        ]
    return "healthy", {"added": 0, "removed": 0, "type_changed": 0}, []


def _build_column_observability(
    *,
    row_count: int,
    current_columns: list[dict[str, Any]],
    previous_columns: list[dict[str, Any]] | None,
    history_by_column: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    previous_map = {str(column.get("column_name") or ""): column for column in (previous_columns or [])}
    column_observability: list[dict[str, Any]] = []
    for column in current_columns:
        name = str(column.get("column_name") or "")
        null_count = _safe_int(column.get("null_count"), 0) or 0
        null_pct = _safe_float(column.get("null_pct"), 0.0) or 0.0
        distinct_count = _safe_int(column.get("distinct_count"), 0) or 0
        distinct_ratio = (distinct_count / row_count * 100.0) if row_count > 0 else None
        uniqueness_pct = max(0.0, 100.0 - ((1.0 - (distinct_count / row_count)) * 100.0)) if row_count > 0 else None
        min_value = column.get("min_value")
        max_value = column.get("max_value")
        prev = previous_map.get(name)
        prev_null_pct = _safe_float(prev.get("null_pct"), None) if prev else None
        prev_distinct_count = _safe_int(prev.get("distinct_count"), None) if prev else None
        if row_count <= 0:
            status = "no_data"
            reason = "Sem linhas para avaliar coluna"
        elif null_pct >= 50:
            status = "degraded"
            reason = "Alta proporção de nulos"
        elif null_pct >= 10:
            status = "partial"
            reason = "Nulos acima do ideal"
        elif distinct_count == 0 and row_count > 0:
            status = "degraded"
            reason = "Sem valores distintos no recorte atual"
        elif distinct_ratio is not None and distinct_ratio <= 5:
            status = "partial"
            reason = "Baixa cardinalidade observada"
        else:
            status = "healthy"
            reason = "Coluna dentro do esperado"
        drift = None
        if prev_null_pct is not None and abs(null_pct - prev_null_pct) >= 10:
            drift = "degraded"
        elif prev_distinct_count is not None and row_count > 0 and abs(distinct_count - prev_distinct_count) >= max(int(row_count * 0.25), 1):
            drift = "partial"
        history = history_by_column.get(name, [])
        column_observability.append(
            {
                "column_name": name,
                "data_type": column.get("data_type"),
                "status": status,
                "status_label": _status_label(status),
                "tone": _status_tone(status),
                "reason": reason,
                "row_count": row_count,
                "null_count": null_count,
                "null_pct": _pct(null_pct),
                "distinct_count": distinct_count,
                "distinct_ratio": _pct(distinct_ratio),
                "uniqueness_pct": _pct(uniqueness_pct),
                "min_value": min_value,
                "max_value": max_value,
                "drift": drift,
                "drift_label": _status_label(drift) if drift else None,
                "previous_null_pct": prev_null_pct,
                "previous_distinct_count": prev_distinct_count,
                "history_points": history[-8:],
            }
        )
    return column_observability


def _build_trend_payload(history: list[dict[str, Any]], current_snapshot: dict[str, Any]) -> dict[str, Any]:
    previous_points = history[:-1] if len(history) > 1 else []
    baseline_row_count = _median_or_none(point.get("row_count") for point in previous_points)
    baseline_dq_score = _median_or_none(point.get("dq_score") for point in previous_points)
    baseline_completeness = _median_or_none(point.get("completeness_pct_avg") for point in previous_points)
    baseline_freshness = _median_or_none(point.get("freshness_seconds") for point in previous_points)
    anomalies: list[dict[str, Any]] = []
    current_row_count = _safe_int(current_snapshot.get("row_count"), 0) or 0
    current_dq_score = _safe_float(current_snapshot.get("dq_score"), 0.0) or 0.0
    current_completeness = _safe_float(current_snapshot.get("completeness_pct_avg"), 0.0) or 0.0
    current_freshness = _safe_int(current_snapshot.get("freshness_seconds"), 0) or 0

    if baseline_row_count and baseline_row_count > 0:
        delta_pct = ((current_row_count - baseline_row_count) / baseline_row_count) * 100.0
        if abs(delta_pct) >= 35:
            anomalies.append(
                {
                    "key": "volume",
                    "severity": "degraded" if abs(delta_pct) >= 50 else "partial",
                    "label": "Anomalia de volume",
                    "detail": f"Volume atual {current_row_count} vs baseline {baseline_row_count:.0f} ({delta_pct:+.1f}%).",
                }
            )
    if baseline_freshness is not None and current_freshness > baseline_freshness * 1.5:
        anomalies.append(
            {
                "key": "freshness",
                "severity": "partial",
                "label": "Freshness acima do baseline",
                "detail": f"Freshness atual {current_freshness}s acima da mediana histórica de {baseline_freshness:.0f}s.",
            }
        )
    if baseline_completeness is not None and current_completeness + 10 < baseline_completeness:
        anomalies.append(
            {
                "key": "completeness",
                "severity": "partial",
                "label": "Completude caiu",
                "detail": f"Completude atual {current_completeness:.1f}pp abaixo do baseline {baseline_completeness:.1f}pp.",
            }
        )
    if baseline_dq_score is not None and current_dq_score + 10 < baseline_dq_score:
        anomalies.append(
            {
                "key": "dq_score",
                "severity": "partial",
                "label": "Score degradou",
                "detail": f"DQ Score atual {current_dq_score:.1f} abaixo da mediana histórica {baseline_dq_score:.1f}.",
            }
        )
    return {
        "points": history[-14:],
        "baseline": {
            "row_count": baseline_row_count,
            "dq_score": baseline_dq_score,
            "completeness_pct_avg": baseline_completeness,
            "freshness_seconds": baseline_freshness,
        },
        "anomalies": anomalies,
    }


def _build_execution_reliability(session: Session, table_id: int) -> dict[str, Any]:
    now = _now()
    windows = {7: now - timedelta(days=7), 30: now - timedelta(days=30)}
    payload: dict[str, Any] = {}
    runs = session.execute(
        select(DQRun.status, DQRun.created_at)
        .where(DQRun.table_id == table_id)
        .order_by(DQRun.created_at.desc())
    ).all()
    latest_valid = None
    latest_failed = None
    for status, created_at in runs:
        if status == "success" and latest_valid is None:
            latest_valid = created_at
        if status == "failed" and latest_failed is None:
            latest_failed = created_at
        if latest_valid and latest_failed:
            break
    for window_days, cutoff in windows.items():
        total = sum(1 for status, created_at in runs if created_at >= cutoff)
        success = sum(1 for status, created_at in runs if created_at >= cutoff and status == "success")
        payload[f"success_rate_{window_days}d"] = round((success / total * 100.0), 1) if total else None
        payload[f"runs_{window_days}d"] = total
    payload["last_valid_run_at"] = latest_valid
    payload["last_failed_run_at"] = latest_failed
    return payload


def _build_incident_state(session: Session, table: TableEntity) -> dict[str, Any]:
    incident = session.scalar(
        select(Incident)
        .where(
            Incident.entity_type == "table",
            Incident.table_fqn == f"{table.schema.name}.{table.name}",
            Incident.status.in_(["open", "investigating", "mitigated"]),
        )
        .order_by(desc(Incident.updated_at))
        .limit(1)
    )
    if incident is None:
        return {
            "status": "closed",
            "status_label": "Sem incidente aberto",
            "severity": None,
            "owner_user_id": None,
            "updated_at": None,
            "incident_id": None,
        }
    return {
        "status": incident.status,
        "status_label": incident.status.replace("_", " ").title(),
        "severity": incident.severity,
        "owner_user_id": incident.owner_user_id,
        "updated_at": incident.updated_at,
        "incident_id": incident.id,
        "title": incident.title,
        "source_type": incident.source_type,
        "occurrences": incident.occurrences,
    }


def _dimension_from_rule(rule: DQRule) -> str | None:
    definition = rule.rule_definition_json if isinstance(rule.rule_definition_json, dict) else {}
    dimension = str(definition.get("dimension") or "").strip().lower()
    if dimension:
        return dimension
    rule_type = str(getattr(rule, "rule_type", "") or "").strip().lower()
    return {
        "nullability": "completude",
        "column_validation": "validade",
        "domain": "validade",
        "uniqueness": "unicidade",
        "freshness": "tempestividade",
        "column_comparison": "consistencia",
        "reconciliation": "acuracia",
    }.get(rule_type)


def _active_rule_dimensions(session: Session, table_id: int) -> set[str]:
    try:
        rules = session.scalars(
            select(DQRule).where(
                DQRule.table_id == table_id,
                DQRule.is_active.is_(True),
                DQRule.archived.is_(False),
            )
        ).all()
    except Exception:
        return set()
    return {dimension for rule in rules if (dimension := _dimension_from_rule(rule))}


def build_dq_observability_payload(
    *,
    session: Session,
    table: TableEntity,
    current_snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any] | None,
    history: list[dict[str, Any]],
    column_history: dict[str, list[dict[str, Any]]],
    current_user=None,
) -> dict[str, Any]:
    current_columns = _current_columns(current_snapshot)
    previous_columns = _current_columns(previous_snapshot)
    row_count = _safe_int(current_snapshot.get("row_count"), 0) or 0
    completeness_pct = _safe_float(current_snapshot.get("completeness_pct_avg"), None)
    dq_score = _safe_float(current_snapshot.get("dq_score"), None)
    effective_dq_score = _safe_float(current_snapshot.get("effective_dq_score"), dq_score)
    failed_rules = _safe_int(current_snapshot.get("failed_rules"), 0) or 0
    duplicates_count = _safe_int(current_snapshot.get("duplicates_count"), 0) or 0
    freshness_seconds = _safe_int(current_snapshot.get("freshness_seconds"), 0) or 0
    contract_payload = contract_summary(session, table_id=table.id)
    current_contract = None
    try:
        current_contract = get_current_contract(session, table_id=table.id)
    except Exception:
        current_contract = None

    contract_columns = len(current_contract.columns) if current_contract and current_contract.columns else 0
    contract_coverage = None
    if current_contract:
        contract_coverage = round(min(100.0, (contract_columns / max(len(current_columns), 1)) * 100.0), 1)
    contract_state = "not_evaluated" if not current_contract else (contract_payload.get("last_validation_status") or "draft")
    contract_label = "Sem contrato formal" if not current_contract else _status_label(str(contract_state).replace("passed", "healthy").replace("failed", "degraded"))

    lineage_summary = None
    try:
        lineage_summary = get_table_summary(session, table.id, current_user=current_user)
    except Exception:
        lineage_summary = None

    baseline_row_count = _median_or_none(point.get("row_count") for point in history[:-1]) if history else None
    freshness_sla_seconds = None
    if current_contract and current_contract.freshness_hours is not None:
        freshness_sla_seconds = int(current_contract.freshness_hours) * 3600

    completeness_status, completeness_detail = _classify_completeness(row_count=row_count, completeness_pct=completeness_pct)
    uniqueness_status, uniqueness_detail, duplicate_pct = _classify_uniqueness(row_count=row_count, duplicates_count=duplicates_count)
    validity_status, validity_detail = _classify_validity(row_count=row_count, failed_rules=failed_rules)
    freshness_status, freshness_detail = _classify_freshness(
        row_count=row_count,
        freshness_seconds=freshness_seconds,
        sla_seconds=freshness_sla_seconds,
    )
    volume_status, volume_detail, volume_delta_pct = _classify_volume(row_count=row_count, baseline=baseline_row_count)
    schema_status, schema_summary, schema_changes = _classify_schema(current_columns=current_columns, previous_columns=previous_columns)
    active_dimensions = _active_rule_dimensions(session, table.id)
    dimension_rule_stats = _rule_dimension_stats(session, table.id)
    accuracy_signal_present = any(
        current_snapshot.get(key) is not None
        for key in ("accuracy_score", "reconciliation_score", "reconciliation_total", "reconciliation_failed", "comparison_delta_pct")
    )

    previous_row_count = _safe_int(previous_snapshot.get("row_count"), 0) if previous_snapshot else None
    previous_completeness_pct = _safe_float(previous_snapshot.get("completeness_pct_avg"), None) if previous_snapshot else None
    previous_failed_rules = _safe_int(previous_snapshot.get("failed_rules"), 0) if previous_snapshot else None
    previous_duplicates_count = _safe_int(previous_snapshot.get("duplicates_count"), 0) if previous_snapshot else None
    previous_freshness_seconds = _safe_int(previous_snapshot.get("freshness_seconds"), 0) if previous_snapshot else None
    previous_duplicate_pct = (
        round((previous_duplicates_count / previous_row_count) * 100.0, 2)
        if previous_row_count and previous_row_count > 0 and previous_duplicates_count is not None
        else None
    )

    def _rule_counts(dimension_key: str) -> tuple[int, int]:
        stats = dimension_rule_stats.get(dimension_key, {})
        return int(stats.get("rules_count", 0) or 0), int(stats.get("failed_rules_count", 0) or 0)

    completeness_rules_count, completeness_failed_rules_count = _rule_counts("completude")
    validity_rules_count, validity_failed_rules_count = _rule_counts("validade")
    consistency_rules_count, consistency_failed_rules_count = _rule_counts("consistencia")
    uniqueness_rules_count, uniqueness_failed_rules_count = _rule_counts("unicidade")
    freshness_rules_count, freshness_failed_rules_count = _rule_counts("tempestividade")
    accuracy_rules_count, accuracy_failed_rules_count = _rule_counts("acuracia")

    completeness_applicable = row_count > 0 and completeness_pct is not None
    uniqueness_applicable = row_count > 0 or "unicidade" in active_dimensions
    validity_applicable = validity_rules_count > 0 or failed_rules > 0
    consistency_applicable = "consistencia" in active_dimensions or previous_snapshot is not None
    freshness_applicable = "tempestividade" in active_dimensions or freshness_sla_seconds is not None or freshness_seconds > 0
    accuracy_applicable = accuracy_rules_count > 0 or accuracy_signal_present
    completeness_evidence_level = "formal_rule" if completeness_rules_count > 0 else "automatic_profiling" if completeness_applicable else "none"
    validity_evidence_level = "formal_rule" if validity_rules_count > 0 else "none"
    consistency_evidence_level = (
        "formal_rule"
        if consistency_rules_count > 0
        else "partial"
        if consistency_applicable
        else "none"
    )
    uniqueness_evidence_level = "formal_rule" if uniqueness_rules_count > 0 else "automatic_profiling" if uniqueness_applicable else "none"
    freshness_evidence_level = (
        "formal_rule"
        if freshness_rules_count > 0 and freshness_sla_seconds is not None
        else "operational_signal"
        if freshness_applicable
        else "none"
    )
    accuracy_evidence_level = "formal_rule" if accuracy_rules_count > 0 else "partial" if accuracy_applicable else "none"
    evidence_dimensions_count = sum(
        1
        for level in (
            completeness_evidence_level,
            validity_evidence_level,
            consistency_evidence_level,
            uniqueness_evidence_level,
            freshness_evidence_level,
            accuracy_evidence_level,
        )
        if level != "none"
    )
    formal_rules_dimensions_count = sum(
        1
        for count in (
            completeness_rules_count,
            validity_rules_count,
            consistency_rules_count,
            uniqueness_rules_count,
            freshness_rules_count,
            accuracy_rules_count,
        )
        if count > 0
    )
    automatic_dimensions_count = sum(1 for level in (completeness_evidence_level, uniqueness_evidence_level) if level == "automatic_profiling")
    operational_dimensions_count = 1 if freshness_evidence_level == "operational_signal" else 0
    partial_dimensions_count = sum(1 for level in (consistency_evidence_level, accuracy_evidence_level) if level == "partial")
    none_dimensions_count = 6 - evidence_dimensions_count
    accuracy_status = (
        "not_evaluated"
        if not accuracy_applicable
        else "no_data"
        if row_count <= 0
        else "healthy"
        if failed_rules == 0
        else "partial"
    )
    accuracy_detail = (
        "Sem regra de acurácia configurada para este ativo."
        if not accuracy_applicable
        else "Sem linhas para avaliar acurácia."
        if row_count <= 0
        else "Reconciliação dentro do esperado."
        if failed_rules == 0
        else "A regra de acurácia foi configurada, mas ainda depende de reconciliação com origem/destino para avaliar o score."
    )

    incident_state = _build_incident_state(session, table)
    execution_reliability = _build_execution_reliability(session, table.id)
    column_observability = _build_column_observability(
        row_count=row_count,
        current_columns=current_columns,
        previous_columns=previous_columns,
        history_by_column=column_history,
    )
    trend_payload = _build_trend_payload(history, current_snapshot)
    try:
        from t2c_data.features.data_quality.observability_store import load_persisted_observability_artifacts

        persisted_artifacts = load_persisted_observability_artifacts(session, table_id=table.id, limit=10)
    except Exception:
        persisted_artifacts = {"baselines": [], "events": [], "evidence_samples": []}
    if persisted_artifacts.get("events"):
        anomaly_entries = [
            {
                "key": f"{event.get('event_type')}:{event.get('metric_key')}",
                "severity": event.get("severity") or "warning",
                "label": event.get("metric_key", "evento"),
                "detail": (
                    f"Observado {event.get('observed_value')} vs baseline {event.get('baseline_value')}"
                    if event.get("baseline_value") is not None
                    else (event.get("details_json") or {}).get("kind", "Evento histórico persistido")
                ),
                "source": "persisted",
            }
            for event in persisted_artifacts.get("events", [])
            if event.get("event_type") in {"anomaly", "drift"}
        ]
        existing_keys = {anomaly["key"] for anomaly in trend_payload["anomalies"]}
        trend_payload["anomalies"].extend(anomaly for anomaly in anomaly_entries if anomaly["key"] not in existing_keys)

    completeness_trend = _format_trend_label(
        current_value=completeness_pct,
        previous_value=previous_completeness_pct,
        higher_is_better=True,
        kind="percent",
    )
    validity_metric_value = max(0.0, 100.0 - min(100.0, failed_rules * 20.0)) if validity_applicable and row_count > 0 else None
    validity_trend = _format_trend_label(
        current_value=validity_failed_rules_count,
        previous_value=previous_failed_rules,
        higher_is_better=False,
        kind="count",
    )
    consistency_metric_value = schema_summary["added"] + schema_summary["removed"] + schema_summary["type_changed"]
    consistency_trend = (
        {"direction": "stable", "value": None, "label": "Estável"}
        if consistency_applicable and consistency_metric_value == 0
        else {"direction": "down", "value": None, "label": "Mudanças estruturais detectadas"}
        if consistency_applicable
        else {"direction": "unknown", "value": None, "label": "Sem histórico"}
    )
    uniqueness_metric_value = _pct(100.0 - (duplicate_pct or 0.0)) if duplicate_pct is not None and uniqueness_applicable else None
    uniqueness_trend = _format_trend_label(
        current_value=duplicate_pct,
        previous_value=previous_duplicate_pct,
        higher_is_better=False,
        kind="percent",
    )
    freshness_metric_value = max(0.0, 100.0 - min(100.0, freshness_seconds / 3600.0 * 5.0)) if freshness_applicable and row_count > 0 else None
    freshness_trend = _format_trend_label(
        current_value=freshness_seconds,
        previous_value=previous_freshness_seconds,
        higher_is_better=False,
        kind="freshness",
        stable_threshold=60.0,
    )
    accuracy_metric_value = _safe_float(
        current_snapshot.get("accuracy_score") or current_snapshot.get("reconciliation_score") or current_snapshot.get("comparison_delta_pct"),
        None,
    )
    accuracy_trend = _format_trend_label(
        current_value=accuracy_metric_value,
        previous_value=None,
        higher_is_better=True,
        kind="score",
    )
    evaluated_dimensions_count = sum(
        1
        for is_applicable in (
            completeness_applicable,
            validity_applicable,
            consistency_applicable,
            uniqueness_applicable,
            freshness_applicable,
            accuracy_applicable,
        )
        if is_applicable
    )
    quality_coverage = {
        "evaluated_dimensions": evidence_dimensions_count,
        "total_dimensions": 6,
        "formal_dimensions": formal_rules_dimensions_count,
        "automatic_profiling_dimensions": automatic_dimensions_count,
        "operational_signal_dimensions": operational_dimensions_count,
        "partial_dimensions": partial_dimensions_count,
        "not_evaluated_dimensions": none_dimensions_count,
        "coverage_pct": round((evidence_dimensions_count / 6.0) * 100.0, 1),
        "summary": f"{evidence_dimensions_count} de 6 dimensões com evidência",
        "formal_summary": f"{formal_rules_dimensions_count} de 6 dimensões com regra formal",
    }

    dimension_rows = [
        _build_dimension(
            key="completude",
            label="Completude",
            status=completeness_status if completeness_applicable else "not_evaluated",
            value=completeness_pct if completeness_applicable else None,
            baseline=trend_payload["baseline"]["completeness_pct_avg"],
            delta=(None if completeness_pct is None or trend_payload["baseline"]["completeness_pct_avg"] is None else round(completeness_pct - float(trend_payload["baseline"]["completeness_pct_avg"]), 2)),
            unit="%",
            detail=completeness_detail if completeness_applicable else "Sem regra de completude configurada para este ativo.",
            score=completeness_pct if completeness_applicable else None,
            applicable=completeness_applicable,
            coverage_type="profiling" if completeness_applicable else "none",
            coverage_label="Avaliada por profiling automático" if completeness_applicable else "Sem regra de completude configurada para este ativo.",
            evidence_level=completeness_evidence_level,
            rules_count=completeness_rules_count,
            configured_rules_count=completeness_rules_count,
            formal_rules_count=completeness_rules_count,
            failed_rules_count=completeness_failed_rules_count,
            metric_value=completeness_pct if completeness_applicable else None,
            metric_label=(
                f"{completeness_pct:.1f}% de preenchimento médio" if completeness_applicable and completeness_pct is not None else None
            ),
            trend=completeness_trend,
            summary=completeness_detail if completeness_applicable else "Sem regra de completude configurada para este ativo.",
            explanation=(
                "O profiling automático não encontrou nulos relevantes nas colunas avaliadas."
                if completeness_applicable and completeness_status == "healthy"
                else "O profiling mostrou campos com preenchimento abaixo do esperado."
                if completeness_applicable
                else "Esta dimensão depende de profiling automático para avaliar nulos e preenchimento."
            ),
            recommended_action=(
                "Formalizar regra de obrigatoriedade para colunas críticas."
                if completeness_applicable and completeness_status in {"partial", "degraded"}
                else "Criar regra de preenchimento obrigatório para CPF/CNPJ, contrato, status ou valor, se aplicável."
                if completeness_applicable and completeness_rules_count <= 0
                else None
            ),
        ),
        _build_dimension(
            key="validade",
            label="Validade",
            status=validity_status if validity_applicable else "not_evaluated",
            value=validity_metric_value,
            baseline=None,
            delta=None,
            unit="%",
            detail=validity_detail if validity_applicable else "Sem regra de validade configurada para este ativo.",
            score=validity_metric_value,
            applicable=validity_applicable,
            coverage_type="rules" if validity_applicable else "none",
            coverage_label="Avaliada por regras configuradas" if validity_applicable else "Sem regra de validade configurada para este ativo.",
            evidence_level=validity_evidence_level,
            rules_count=validity_rules_count,
            configured_rules_count=validity_rules_count,
            formal_rules_count=validity_rules_count,
            failed_rules_count=validity_failed_rules_count,
            metric_value=validity_metric_value,
            metric_label=(
                f"{failed_rules} regra(s) em falha"
                if validity_applicable and failed_rules > 0
                else f"{validity_rules_count} regra(s) configurada(s)"
                if validity_applicable
                else None
            ),
            trend=validity_trend,
            summary=validity_detail if validity_applicable else "Sem regra de validade configurada para este ativo.",
            explanation=(
                "A validade depende de regras como CPF válido, CNPJ válido, e-mail válido, telefone válido, domínio permitido ou datas coerentes."
                if not validity_applicable
                else "As regras configuradas avaliam o formato e os valores permitidos dos campos críticos."
            ),
            recommended_action=(
                "Criar regra de CPF, CNPJ, e-mail, telefone, status permitido ou data coerente."
                if not validity_applicable
                else "Revisar as regras de validade com falha."
                if validity_failed_rules_count > 0
                else None
            ),
        ),
        _build_dimension(
            key="consistencia",
            label="Consistência",
            status=schema_status if consistency_applicable else "not_evaluated",
            value=consistency_metric_value if consistency_applicable else None,
            baseline=None,
            delta=None,
            unit="changes",
            detail=(
                "Sem drift estrutural detectado"
                if consistency_applicable and schema_status == "healthy"
                else (
                    f"{schema_summary['added']} adicionadas · {schema_summary['removed']} removidas · {schema_summary['type_changed']} alteradas"
                    if consistency_applicable
                    else "Sem regra de consistência configurada para este ativo."
                )
            ),
            score=(
                100.0 if schema_status == "healthy" else max(0.0, 100.0 - min(100.0, consistency_metric_value * 25.0))
            )
            if consistency_applicable
            else None,
            applicable=consistency_applicable,
            coverage_type="profiling" if consistency_applicable and consistency_rules_count <= 0 else "rules" if consistency_applicable else "none",
            coverage_label=(
                "Avaliada por profiling estrutural"
                if consistency_applicable and consistency_rules_count <= 0
                else "Avaliada por regras configuradas"
                if consistency_applicable
                else "Sem regra de consistência configurada para este ativo."
            ),
            evidence_level=consistency_evidence_level,
            rules_count=consistency_rules_count,
            configured_rules_count=consistency_rules_count,
            formal_rules_count=consistency_rules_count,
            failed_rules_count=consistency_failed_rules_count,
            metric_value=consistency_metric_value if consistency_applicable else None,
            metric_label=(
                "Sem drift estrutural detectado"
                if consistency_applicable and consistency_metric_value == 0
                else f"{consistency_metric_value} mudança(s) estruturais"
                if consistency_applicable
                else None
            ),
            trend=consistency_trend,
            summary=(
                "Sem drift estrutural detectado"
                if consistency_applicable and consistency_metric_value == 0
                else "Mudanças estruturais detectadas"
                if consistency_applicable
                else "Sem regra de consistência configurada para este ativo."
            ),
            explanation=(
                "A consistência compara estrutura, transições e combinações válidas de campos entre leituras e contratos."
                if consistency_applicable and consistency_rules_count > 0
                else "O profiling estrutural não detectou drift, mas ainda não há regra formal de consistência de negócio."
                if consistency_applicable
                else "Esta dimensão precisa de uma regra de consistência ou de um perfil estrutural para ser avaliada."
            ),
            recommended_action=(
                "Criar uma regra de consistência para este ativo."
                if not consistency_applicable
                else "Criar regra de consistência de negócio, como status coerente, data fim maior que data início ou cota contemplada com data de contemplação."
                if consistency_rules_count <= 0
                else "Revisar o contrato estrutural com os consumidores."
                if consistency_metric_value > 0
                else None
            ),
        ),
        _build_dimension(
            key="unicidade",
            label="Unicidade",
            status=uniqueness_status if uniqueness_applicable else "not_evaluated",
            value=uniqueness_metric_value if uniqueness_applicable else None,
            baseline=None,
            delta=None,
            unit="%",
            detail=uniqueness_detail if uniqueness_applicable else "Sem regra de unicidade configurada para este ativo.",
            score=uniqueness_metric_value,
            applicable=uniqueness_applicable,
            coverage_type="profiling" if uniqueness_applicable and uniqueness_rules_count <= 0 else "rules" if uniqueness_applicable else "none",
            coverage_label=(
                "Avaliada por profiling automático"
                if uniqueness_applicable and uniqueness_rules_count <= 0
                else "Avaliada por regras configuradas"
                if uniqueness_applicable
                else "Sem regra de unicidade configurada para este ativo."
            ),
            evidence_level=uniqueness_evidence_level,
            rules_count=uniqueness_rules_count,
            configured_rules_count=uniqueness_rules_count,
            formal_rules_count=uniqueness_rules_count,
            failed_rules_count=uniqueness_failed_rules_count,
            metric_value=duplicate_pct if uniqueness_applicable else None,
            metric_label=(
                f"{duplicates_count} duplicidade(s) detectada(s)"
                if uniqueness_applicable and duplicates_count > 0
                else "Sem duplicidades detectadas"
                if uniqueness_applicable
                else None
            ),
            trend=uniqueness_trend,
            summary=uniqueness_detail if uniqueness_applicable else "Sem regra de unicidade configurada para este ativo.",
            explanation=(
                "A unicidade é avaliada por profiling de duplicidade e pode ser formalizada com regras de chave única simples ou composta."
                if uniqueness_applicable and uniqueness_rules_count <= 0
                else "A unicidade é avaliada por regras de chave única configuradas."
                if uniqueness_applicable
                else "Esta dimensão precisa de uma regra de unicidade ou de uma leitura de duplicidade para ser avaliada."
            ),
            recommended_action=(
                "Criar uma regra de unicidade para este ativo."
                if not uniqueness_applicable
                else "Criar regra de chave única simples ou composta, como CPF/CNPJ, proposta_id, boleto_id ou cliente + grupo + cota."
                if uniqueness_rules_count <= 0
                else "Revisar chaves de negócio e deduplicação."
                if duplicates_count > 0
                else None
            ),
        ),
        _build_dimension(
            key="tempestividade",
            label="Tempestividade",
            status=freshness_status if freshness_applicable else "not_evaluated",
            value=freshness_metric_value if freshness_applicable else None,
            baseline=trend_payload["baseline"]["freshness_seconds"],
            delta=(
                None
                if trend_payload["baseline"]["freshness_seconds"] is None
                else round(freshness_seconds - float(trend_payload["baseline"]["freshness_seconds"]), 2)
            ),
            unit="s",
            detail=(
                freshness_detail if freshness_applicable and freshness_sla_seconds is not None
                else "Atualização recente, mas sem SLA configurado"
                if freshness_applicable
                else "Sem regra de tempestividade configurada para este ativo."
            ),
            score=freshness_metric_value,
            applicable=freshness_applicable,
            coverage_type="freshness" if freshness_applicable else "none",
            coverage_label=(
                f"Avaliada por freshness · {_format_sla_label(freshness_sla_seconds) or 'sem SLA'}"
                if freshness_applicable
                else "Sem regra de tempestividade configurada para este ativo."
            ),
            evidence_level=freshness_evidence_level,
            rules_count=freshness_rules_count,
            configured_rules_count=freshness_rules_count,
            formal_rules_count=freshness_rules_count,
            failed_rules_count=freshness_failed_rules_count,
            metric_value=freshness_seconds if freshness_applicable else None,
            metric_label=(
                f"Atualizado há {_human_duration(freshness_seconds) or 'N/D'}"
                if freshness_applicable
                else None
            ),
            trend=freshness_trend,
            summary=(
                freshness_detail if freshness_applicable and freshness_sla_seconds is not None
                else "Atualização recente, mas sem SLA configurado"
                if freshness_applicable
                else "Sem regra de tempestividade configurada para este ativo."
            ),
            explanation=(
                "A tempestividade compara a última atualização com o SLA e o baseline histórico."
                if freshness_applicable and freshness_sla_seconds is not None
                else "A tabela está atualizada, mas ainda não existe SLA formal para avaliar atraso com segurança."
                if freshness_applicable
                else "Esta dimensão depende de um SLA ou de um indicador de freshness para ser avaliada."
            ),
            recommended_action=(
                "Configurar freshness/SLA para este ativo."
                if not freshness_applicable
                else "Configurar SLA de freshness, como atualização a cada 15 minutos, 1 hora ou dentro da janela útil."
                if freshness_sla_seconds is None
                else "Revisar a cadência de atualização se o ativo estiver atrasando."
                if freshness_status in {"partial", "degraded"}
                else None
            ),
        ),
        _build_dimension(
            key="acuracia",
            label="Acurácia",
            status=accuracy_status,
            value=accuracy_metric_value,
            baseline=None,
            delta=None,
            unit=None,
            detail=accuracy_detail,
            score=accuracy_metric_value if accuracy_metric_value is not None else None,
            applicable=accuracy_applicable,
            coverage_type="reconciliation" if accuracy_applicable else "none",
            coverage_label="Avaliada por reconciliação" if accuracy_applicable else "Sem regra de acurácia configurada para este ativo.",
            evidence_level=accuracy_evidence_level,
            rules_count=accuracy_rules_count,
            configured_rules_count=accuracy_rules_count,
            formal_rules_count=accuracy_rules_count,
            failed_rules_count=accuracy_failed_rules_count,
            metric_value=accuracy_metric_value,
            metric_label=(
                f"Reconciliação em {accuracy_metric_value:.1f}%" if accuracy_metric_value is not None else None
            ),
            trend=accuracy_trend,
            summary=accuracy_detail if accuracy_applicable else "Sem regra de acurácia configurada para este ativo.",
            explanation=(
                "A acurácia depende de reconciliação entre origem e destino, comparação por chave ou tolerância."
                if accuracy_applicable and accuracy_rules_count > 0
                else "Ainda não há regra formal de reconciliação entre origem e destino para esta tabela."
                if accuracy_applicable
                else "Esta dimensão precisa de uma regra de reconciliação para ser avaliada."
            ),
            recommended_action=(
                "Criar regra de reconciliação, como count origem x destino, soma de valores ou diferença máxima permitida."
                if not accuracy_applicable
                else "Criar regra de reconciliação, como count origem x destino, soma de valores ou diferença máxima permitida."
                if accuracy_rules_count <= 0
                else "Revisar a reconciliação entre origem e destino."
                if accuracy_status in {"partial", "degraded"}
                else None
            ),
        ),
    ]

    table_status = _worst_status(
        completeness_status,
        uniqueness_status,
        validity_status,
        freshness_status,
        volume_status,
        schema_status,
        "critical" if incident_state.get("status") in {"open", "investigating"} else "healthy",
    )
    if row_count <= 0:
        table_status = "no_data"

    failed_checks = [dimension for dimension in dimension_rows if dimension["status"] in {"degraded", "critical"}]
    failed_columns = [column for column in column_observability if column["status"] in {"degraded", "critical"}]
    troubleshooting_actions = []
    if row_count <= 0:
        troubleshooting_actions.append(
            {
                "key": "recheck_pipeline",
                "label": "Revisar pipeline",
                "detail": "A tabela não trouxe linhas no último profiling. Verifique integração, ingestão e janela de carga.",
            }
        )
    if schema_changes:
        troubleshooting_actions.append(
            {
                "key": "review_contract",
                "label": "Validar contrato",
                "detail": "Há sinais de drift de schema. Valide contrato e compatibilidade com consumidores.",
            }
        )
    if failed_rules > 0:
        troubleshooting_actions.append(
            {
                "key": "rerun_rules",
                "label": "Reexecutar regras",
                "detail": "Existem regras em falha no último profiling. Reexecute a bateria e revise os checks com erro.",
            }
        )
    if incident_state.get("incident_id"):
        troubleshooting_actions.append(
            {
                "key": "open_incident",
                "label": "Abrir incidente",
                "detail": "Há incidente operacional associado ao ativo. Use a evidência para acelerar o tratamento.",
            }
        )
    if failed_columns:
        troubleshooting_actions.append(
            {
                "key": "inspect_columns",
                "label": "Inspecionar colunas críticas",
                "detail": "Algumas colunas estão degradadas ou em drift. Priorize o drill-down por coluna.",
            }
        )

    assessment_score = None if table_status in {"no_data", "not_evaluated", "not_calculable"} else effective_dq_score
    if table_status == "healthy" and assessment_score is None:
        assessment_score = dq_score
    assessment_reason = (
        "A execução não produziu linhas para avaliação"
        if table_status == "no_data"
        else "A execução atual ainda não permite cálculo confiável"
        if table_status in {"not_evaluated", "not_calculable"}
        else (
            f"Score {assessment_score:.1f} nas dimensões avaliadas, mas {quality_coverage['summary']} e {quality_coverage['formal_summary']}."
            if assessment_score is not None and (
                quality_coverage["evaluated_dimensions"] < quality_coverage["total_dimensions"]
                or quality_coverage["formal_dimensions"] < quality_coverage["total_dimensions"]
            )
            else "A tabela está dentro do esperado"
            if table_status == "healthy"
            else "Há sinais de degradação operacional ou analítica"
        )
    )

    return {
        "assessment_state": {
            "code": table_status,
            "label": _status_label(table_status),
            "tone": _status_tone(table_status),
            "score": assessment_score,
            "reason": assessment_reason,
        },
        "quality_score": assessment_score,
        "quality_coverage": quality_coverage,
        "dimensions": dimension_rows,
        "table": {
            "status": table_status,
            "status_label": _status_label(table_status),
            "tone": _status_tone(table_status),
            "freshness": {
                "seconds": freshness_seconds,
                "status": freshness_status,
                "status_label": _status_label(freshness_status),
                "detail": freshness_detail,
                "sla_seconds": freshness_sla_seconds,
                "within_sla": freshness_status == "healthy",
            },
            "volume": {
                "row_count": row_count,
                "baseline": baseline_row_count,
                "status": volume_status,
                "status_label": _status_label(volume_status),
                "detail": volume_detail,
                "delta_pct": volume_delta_pct,
            },
            "schema": {
                "status": schema_status,
                "status_label": _status_label(schema_status),
                "added": schema_summary["added"],
                "removed": schema_summary["removed"],
                "type_changed": schema_summary["type_changed"],
                "changes": schema_changes,
            },
            "reliability": execution_reliability,
            "incident": incident_state,
            "contract": {
                "contract_id": contract_payload.get("contract_id"),
                "version": contract_payload.get("version"),
                "status": contract_payload.get("status"),
                "status_label": contract_label,
                "published_at": contract_payload.get("published_at"),
                "last_validation_status": contract_payload.get("last_validation_status"),
                "last_validation_at": contract_payload.get("last_validation_at"),
                "last_validation_issues": contract_payload.get("last_validation_issues"),
                "coverage_pct": contract_coverage,
            },
            "lineage": {
                "downstream_count": getattr(getattr(lineage_summary, "impact", None), "downstream_count", 0) if lineage_summary else 0,
                "dashboard_count": getattr(getattr(lineage_summary, "impact", None), "dashboard_count", 0) if lineage_summary else 0,
                "impact_level": getattr(getattr(lineage_summary, "impact", None), "impact_level", "low") if lineage_summary else "low",
            },
        },
        "trend": trend_payload,
        "columns": column_observability,
        "contract": {
            "contract_id": contract_payload.get("contract_id"),
            "version": contract_payload.get("version"),
            "status": contract_payload.get("status"),
            "status_label": contract_label,
            "published_at": contract_payload.get("published_at"),
            "last_validation_status": contract_payload.get("last_validation_status"),
            "last_validation_at": contract_payload.get("last_validation_at"),
            "last_validation_issues": contract_payload.get("last_validation_issues"),
            "coverage_pct": contract_coverage,
            "column_count": contract_columns,
        },
        "lineage": {
            "downstream_count": getattr(getattr(lineage_summary, "impact", None), "downstream_count", 0) if lineage_summary else 0,
            "dashboard_count": getattr(getattr(lineage_summary, "impact", None), "dashboard_count", 0) if lineage_summary else 0,
            "impact_level": getattr(getattr(lineage_summary, "impact", None), "impact_level", "low") if lineage_summary else "low",
        },
        "troubleshooting": {
            "failed_checks": failed_checks,
            "failed_columns": failed_columns[:8],
            "actions": troubleshooting_actions[:5],
        },
        "historical": persisted_artifacts,
        "schema_changes": schema_changes,
    }


def build_profile_metrics_json(payload: dict[str, Any]) -> dict[str, Any]:
    row_count = _safe_int(payload.get("row_count"), 0) or 0
    profiling_intelligence = payload.get("profiling_intelligence") if isinstance(payload.get("profiling_intelligence"), dict) else {}
    if row_count <= 0:
        completeness_pct_avg = _safe_float(payload.get("completeness_pct_avg"), None)
        dq_score = _safe_float(payload.get("dq_score"), None)
    else:
        completeness_pct_avg = _safe_float(payload.get("completeness_pct_avg"), 100.0) or 100.0
        dq_score = _safe_float(payload.get("dq_score"), 100.0) or 100.0
    duplicates_count = _safe_int(payload.get("duplicates_count"), 0) or 0
    failed_rules = _safe_int(payload.get("failed_rules"), 0) or 0
    columns = [column for column in payload.get("columns", []) if isinstance(column, dict)]
    completeness_status, completeness_detail = _classify_completeness(row_count=row_count, completeness_pct=completeness_pct_avg)
    uniqueness_status, uniqueness_detail, duplicate_pct = _classify_uniqueness(row_count=row_count, duplicates_count=duplicates_count)
    validity_status, validity_detail = _classify_validity(row_count=row_count, failed_rules=failed_rules)
    assessment_status = _worst_status(completeness_status, uniqueness_status, validity_status)
    if row_count <= 0:
        assessment_status = "no_data"
    return {
        "assessment_state": {
            "code": assessment_status,
            "label": _status_label(assessment_status),
            "tone": _status_tone(assessment_status),
            "score": None if assessment_status in {"no_data", "not_evaluated", "not_calculable"} else dq_score,
            "reason": (
                str(payload.get("observation") or "A execução não produziu linhas para avaliação")
                if assessment_status == "no_data"
                else "A execução não conseguiu ser avaliada de forma confiável"
                if assessment_status in {"not_evaluated", "not_calculable"}
                else "Perfilamento executado com qualidade aceitável"
            ),
        },
        "completeness_pct_avg": completeness_pct_avg,
        "dq_score": dq_score,
        "duplicates_count": duplicates_count,
        "failed_rules": failed_rules,
        "profile_summary": {
            "row_count": row_count,
            "column_count": len(columns),
            "estimated_size_bytes": payload.get("estimated_size_bytes"),
            "schema_hash": payload.get("schema_hash"),
            "last_updated_at": payload.get("last_updated_at"),
            "last_loaded_at": payload.get("last_loaded_at"),
            "last_updated_column": payload.get("last_updated_column"),
            "duplicate_business_key_count": _safe_int(payload.get("duplicate_business_key_count"), 0) or 0,
            "freshness_seconds": _safe_int(payload.get("freshness_seconds"), None),
            "volume_change_ratio": _safe_float(payload.get("volume_change_ratio"), None),
        },
        "profiling_intelligence": {
            "weight_profile": profiling_intelligence.get("weight_profile"),
            "observed_score": profiling_intelligence.get("observed_score"),
            "formal_score": profiling_intelligence.get("formal_score"),
            "coverage_score": profiling_intelligence.get("coverage_score"),
            "consolidated_score": profiling_intelligence.get("consolidated_score"),
            "coverage_dimensions": profiling_intelligence.get("coverage_dimensions"),
            "covered_dimensions": profiling_intelligence.get("covered_dimensions"),
            "dimension_scores": profiling_intelligence.get("dimension_scores") or {},
            "rule_suggestions": profiling_intelligence.get("rule_suggestions") or [],
            "quality_message": profiling_intelligence.get("quality_message"),
        },
        "dimensions": [
            _build_dimension(
                key="completeness",
                label="Completeness",
                status=completeness_status,
                value=completeness_pct_avg,
                unit="%",
                detail=completeness_detail,
                score=completeness_pct_avg,
                coverage_type="profiling",
                coverage_label="Avaliada por profiling automático",
                rules_count=0,
                configured_rules_count=0,
                failed_rules_count=0,
                metric_value=completeness_pct_avg,
                metric_label=(
                    f"{completeness_pct_avg:.1f}% de preenchimento médio" if completeness_pct_avg is not None else None
                ),
                trend={"direction": "unknown", "value": None, "label": "Sem histórico"},
                summary=completeness_detail,
                explanation="O profiling automático mede a completude pelas taxas de nulos e preenchimento.",
            ),
            _build_dimension(
                key="uniqueness",
                label="Uniqueness",
                status=uniqueness_status,
                value=_pct(100.0 - (duplicate_pct or 0.0)) if duplicate_pct is not None else None,
                unit="%",
                detail=uniqueness_detail,
                coverage_type="profiling",
                coverage_label="Avaliada por profiling automático",
                rules_count=0,
                configured_rules_count=0,
                failed_rules_count=0,
                metric_value=duplicate_pct,
                metric_label=(
                    f"{duplicates_count} duplicidade(s) detectada(s)"
                    if duplicates_count > 0
                    else "Sem duplicidades detectadas"
                ),
                trend={"direction": "unknown", "value": None, "label": "Sem histórico"},
                summary=uniqueness_detail,
                explanation="A unicidade é estimada por profiling de duplicidade, sem regra formal ainda.",
            ),
            _build_dimension(
                key="validity",
                label="Validity",
                status=validity_status,
                value=dq_score,
                unit="score",
                detail=validity_detail,
                coverage_type="none" if failed_rules <= 0 else "rules",
                coverage_label=(
                    "Sem regra de validade configurada" if failed_rules <= 0 else "Avaliada por regras configuradas"
                ),
                rules_count=0,
                configured_rules_count=0,
                failed_rules_count=int(failed_rules),
                metric_value=dq_score,
                metric_label=(
                    f"{failed_rules} regra(s) em falha" if failed_rules > 0 else "Nenhuma falha de regra"
                ),
                trend={"direction": "unknown", "value": None, "label": "Sem histórico"},
                summary=validity_detail,
                explanation=(
                    "Esta dimensão precisa de regras formais como CPF válido, CNPJ válido, e-mail válido ou domínio permitido."
                    if failed_rules <= 0
                    else "As regras configuradas nesta execução falharam ou ainda dependem de mais contexto para avaliação."
                ),
                recommended_action=(
                    "Criar uma regra de validade para este ativo."
                    if failed_rules <= 0
                    else "Revisar as regras de validade com falha."
                ),
            ),
            _build_dimension(
                key="consistency",
                label="Consistency",
                status="not_evaluated",
                value=None,
                unit="score",
                detail="A reconciliação estrutural é avaliada na visão de observabilidade do ativo.",
                applicable=False,
                coverage_type="none",
                coverage_label="Sem regra de consistência configurada",
                rules_count=0,
                configured_rules_count=0,
                failed_rules_count=0,
                trend={"direction": "unknown", "value": None, "label": "Sem histórico"},
                summary="Sem regra de consistência configurada para este ativo.",
                explanation="Crie regras condicionais ou comparações entre colunas para avaliar a consistência.",
                recommended_action="Criar uma regra de consistência para este ativo.",
            ),
            _build_dimension(
                key="freshness",
                label="Freshness",
                status="not_evaluated",
                value=None,
                unit="score",
                detail="A tempestividade é avaliada na visão de observabilidade do ativo.",
                applicable=False,
                coverage_type="none",
                coverage_label="Sem regra de tempestividade configurada",
                rules_count=0,
                configured_rules_count=0,
                failed_rules_count=0,
                trend={"direction": "unknown", "value": None, "label": "Sem histórico"},
                summary="Sem regra de tempestividade configurada para este ativo.",
                explanation="Crie uma regra de freshness/SLA para monitorar atualização e atraso.",
                recommended_action="Configurar freshness/SLA para este ativo.",
            ),
            _build_dimension(
                key="accuracy",
                label="Accuracy",
                status="not_evaluated",
                value=None,
                unit="score",
                detail="A acurácia depende de regras de reconciliação estruturadas.",
                applicable=False,
                coverage_type="none",
                coverage_label="Sem regra de acurácia configurada",
                rules_count=0,
                configured_rules_count=0,
                failed_rules_count=0,
                trend={"direction": "unknown", "value": None, "label": "Sem histórico"},
                summary="Sem regra de acurácia configurada para este ativo.",
                explanation="Crie regras de reconciliação entre origem e destino para avaliar acurácia.",
                recommended_action="Criar regra de reconciliação entre origem e destino.",
            ),
        ],
        "columns": columns,
    }


__all__ = [
    "build_dq_observability_payload",
    "build_profile_metrics_json",
]
