from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, func, inspect, select

from t2c_data.features.data_quality.observability_store import mask_evidence_rows, persist_evidence_sample, persist_observability_artifacts
from t2c_data.features.data_quality.observability import build_profile_metrics_json
from t2c_data.features.data_quality.latest_runs import sync_latest_snapshot_for_rule_run
from t2c_data.features.governance.column_classification import build_column_classification_map
from t2c_data.models.catalog import ColumnEntity, DataSource, TableEntity
from t2c_data.models.dq import (
    DQColumnMetric,
    DQProfileColumnMetric,
    DQProfileRun,
    DQProfileTableMetric,
    DQRule,
    DQRuleLatestRun,
    DQRuleRun,
    DQRuleSuggestion,
    DQRun,
    DQScoreWeightProfile,
    DQTableMetric,
)

DEFAULT_WEIGHT_PROFILE = {
    "completude": 20.0,
    "validade": 15.0,
    "consistencia": 20.0,
    "unicidade": 15.0,
    "tempestividade": 15.0,
    "acuracia": 10.0,
    "governanca": 5.0,
    "rastreabilidade": 0.0,
    "classificacao_sensivel": 0.0,
}

FINTECH_WEIGHT_PROFILE = {
    "completude": 15.0,
    "validade": 15.0,
    "consistencia": 20.0,
    "unicidade": 10.0,
    "tempestividade": 15.0,
    "acuracia": 10.0,
    "governanca": 5.0,
    "rastreabilidade": 5.0,
    "classificacao_sensivel": 5.0,
}


def validate_profiling_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Profiling payload inválido: esperado objeto JSON")
    columns = payload.get("columns")
    if not isinstance(columns, list):
        raise ValueError("Profiling payload inválido: campo 'columns' ausente ou inválido")
    return payload


def _profiling_status_from_payload(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "success").strip().lower()
    return status if status in {"success", "no_data"} else "success"


def _metric_float(payload: dict[str, Any], key: str, *, default: float) -> float:
    value = payload.get(key)
    if value is None:
        return default
    return float(value)


def _safe_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    intelligence = payload.get("profiling_intelligence")
    if not isinstance(intelligence, dict):
        intelligence = {}
    columns = payload.get("columns") if isinstance(payload.get("columns"), list) else []
    return {
        "columns": columns,
        "profiling_intelligence": intelligence,
    }


def _has_table(session, table_name: str) -> bool:
    try:
        inspector = inspect(session.get_bind())
        return inspector.has_table(table_name, schema="t2c_data") or inspector.has_table(table_name)
    except Exception:
        return False


def _normalize_weights(weights_json: dict | list | None) -> dict[str, float]:
    if isinstance(weights_json, dict):
        source = weights_json
    elif isinstance(weights_json, list):
        source = {str(item.get("dimension") or item.get("key") or ""): item.get("weight") for item in weights_json if isinstance(item, dict)}
    else:
        source = {}
    normalized: dict[str, float] = {}
    for key, default_value in DEFAULT_WEIGHT_PROFILE.items():
        try:
            normalized[key] = float(source.get(key, default_value))
        except Exception:
            normalized[key] = float(default_value)
    total = sum(normalized.values())
    if total <= 0:
        return dict(DEFAULT_WEIGHT_PROFILE)
    if abs(total - 100.0) > 0.01:
        factor = 100.0 / total
        normalized = {key: round(value * factor, 4) for key, value in normalized.items()}
    return normalized


def _ensure_default_weight_profiles(session) -> None:
    if not _has_table(session, "dq_score_weight_profiles"):
        return
    existing = session.scalars(select(DQScoreWeightProfile)).all()
    if existing:
        return
    session.add_all(
        [
            DQScoreWeightProfile(
                name="default",
                is_default=True,
                applies_to_domain=None,
                applies_to_criticality=None,
                weights_json=DEFAULT_WEIGHT_PROFILE,
            ),
            DQScoreWeightProfile(
                name="fintech_consorcios",
                is_default=False,
                applies_to_domain="fintech",
                applies_to_criticality="critical",
                weights_json=FINTECH_WEIGHT_PROFILE,
            ),
        ]
    )
    session.flush()


def _select_weight_profile(session, table: TableEntity, payload_columns: list[dict[str, Any]]) -> tuple[DQScoreWeightProfile | None, dict[str, float]]:
    _ensure_default_weight_profiles(session)
    table_tokens = f"{table.name} {getattr(getattr(table, 'schema', None), 'name', '')}".lower()
    fintech_like = any(
        token in table_tokens
        for token in ("finance", "consor", "cota", "boleto", "proposta", "contrato", "pagamento", "cliente")
    ) or bool(getattr(table, "has_personal_data", False)) or bool(getattr(table, "has_sensitive_personal_data", False))
    if fintech_like:
        profile = session.scalar(
            select(DQScoreWeightProfile)
            .where(
                DQScoreWeightProfile.applies_to_domain == "fintech",
            )
            .order_by(DQScoreWeightProfile.is_default.desc(), DQScoreWeightProfile.id.asc())
            .limit(1)
        )
        if profile is not None:
            return profile, _normalize_weights(profile.weights_json)
    profile = session.scalar(
        select(DQScoreWeightProfile)
        .where(DQScoreWeightProfile.is_default.is_(True))
        .order_by(DQScoreWeightProfile.id.asc())
        .limit(1)
    )
    if profile is None:
        profile = session.scalar(select(DQScoreWeightProfile).order_by(DQScoreWeightProfile.id.asc()).limit(1))
    return profile, _normalize_weights(profile.weights_json if profile is not None else DEFAULT_WEIGHT_PROFILE)


def _rule_dimension_from_rule_type(rule_type: str | None) -> str | None:
    mapping = {
        "nullability": "completude",
        "column_validation": "validade",
        "domain": "validade",
        "uniqueness": "unicidade",
        "freshness": "tempestividade",
        "column_comparison": "consistencia",
        "reconciliation": "acuracia",
    }
    return mapping.get(str(rule_type or "").strip().lower())


def _formal_score_for_table(session, table_id: int) -> tuple[float | None, dict[str, Any]]:
    if not (_has_table(session, "dq_rules") and _has_table(session, "dq_rule_latest_runs") and _has_table(session, "dq_rule_runs")):
        return None, {"active_rules": 0, "passed_rules": 0, "failed_rules": 0, "dimensions": {}}
    rows = session.execute(
        select(DQRule.id, DQRule.rule_type, DQRuleLatestRun.latest_rule_run_id, DQRuleRun.status)
        .select_from(DQRule)
        .outerjoin(DQRuleLatestRun, DQRuleLatestRun.rule_id == DQRule.id)
        .outerjoin(DQRuleRun, DQRuleRun.id == DQRuleLatestRun.latest_rule_run_id)
        .where(DQRule.table_id == table_id, DQRule.is_active.is_(True), DQRule.archived.is_(False))
    ).all()
    if not rows:
        return None, {"active_rules": 0, "passed_rules": 0, "failed_rules": 0, "dimensions": {}}
    passed = 0
    failed = 0
    dimensions: dict[str, dict[str, int]] = {}
    for rule_id, rule_type, latest_rule_run_id, status in rows:
        dimension = _rule_dimension_from_rule_type(rule_type) or "governanca"
        bucket = dimensions.setdefault(dimension, {"total": 0, "passed": 0, "failed": 0})
        bucket["total"] += 1
        if str(status or "").lower() == "pass":
            bucket["passed"] += 1
            passed += 1
        elif latest_rule_run_id is not None:
            bucket["failed"] += 1
            failed += 1
    total = passed + failed
    score = round((passed / total) * 100.0, 4) if total > 0 else None
    return score, {"active_rules": total, "passed_rules": passed, "failed_rules": failed, "dimensions": dimensions}


def _merge_metric_payload(payload: dict[str, Any], intelligence: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload)
    merged["profiling_intelligence"] = intelligence
    if intelligence.get("consolidated_score") is not None:
        merged["dq_score"] = intelligence.get("consolidated_score")
    return merged


def _score_breakdown_json(
    *,
    payload: dict[str, Any],
    intelligence: dict[str, Any],
    formal_score: float | None,
    formal_breakdown: dict[str, Any],
    weights: dict[str, float],
) -> dict[str, Any]:
    return {
        "weight_profile": intelligence.get("weight_profile") or "default",
        "weights": weights,
        "observed_score": intelligence.get("observed_score"),
        "formal_score": formal_score,
        "coverage_score": intelligence.get("coverage_score"),
        "consolidated_score": intelligence.get("consolidated_score"),
        "dimension_scores": intelligence.get("dimension_scores") or {},
        "formal_breakdown": formal_breakdown,
        "quality_message": intelligence.get("quality_message"),
        "row_count": int(payload.get("row_count") or 0),
    }


def _persist_profile_artifacts(
    session,
    *,
    dq_run: DQRun,
    table: TableEntity,
    datasource: DataSource | None,
    schema_name: str,
    payload: dict[str, Any],
    created_by_user_id: int | None,
    job_id: int | None,
    trigger_type: str,
) -> tuple[DQProfileRun | None, DQProfileTableMetric | None, list[DQProfileColumnMetric], list[DQRuleSuggestion], dict[str, Any]]:
    profile_payload = _safe_profile_payload(payload)
    columns = [column for column in profile_payload["columns"] if isinstance(column, dict)]
    intelligence = dict(profile_payload["profiling_intelligence"] or {})
    if not (_has_table(session, "dq_profile_runs") and _has_table(session, "dq_profile_table_metrics") and _has_table(session, "dq_profile_column_metrics") and _has_table(session, "dq_rule_suggestions")):
        merged_payload = _merge_metric_payload(payload, intelligence)
        return None, None, [], [], merged_payload  # type: ignore[return-value]
    weight_profile, weights = _select_weight_profile(session, table, columns)
    formal_score, formal_breakdown = _formal_score_for_table(session, table.id)
    observed_score = intelligence.get("observed_score")
    if observed_score is None:
        observed_score = _metric_float(payload, "dq_score", default=0.0 if int(payload.get("row_count") or 0) <= 0 else 100.0)
    coverage_score = intelligence.get("coverage_score")
    if coverage_score is None:
        covered = sum(1 for value in (intelligence.get("dimension_scores") or {}).values() if value is not None)
        coverage_score = round((covered / 9.0) * 100.0, 4) if covered else 0.0
    if formal_score is None and formal_breakdown["active_rules"] > 0:
        formal_score = round((formal_breakdown["passed_rules"] / max(formal_breakdown["active_rules"], 1)) * 100.0, 4)
    if formal_score is None:
        consolidated_score = round(float(observed_score), 4) if observed_score is not None else None
    else:
        consolidated_score = round(
            max(0.0, min(100.0, float(observed_score) * 0.55 + float(formal_score) * 0.35 + float(coverage_score) * 0.10)),
            4,
        )
    intelligence = {
        **intelligence,
        "weight_profile": weight_profile.name if weight_profile is not None else "default",
        "observed_score": observed_score,
        "formal_score": formal_score,
        "coverage_score": coverage_score,
        "consolidated_score": consolidated_score,
        "quality_message": intelligence.get("quality_message")
        or (
            "Boa qualidade observada, mas baixa cobertura formal."
            if formal_score is None
            else "Qualidade consolidada com base em profiling e regras formais."
        ),
    }
    merged_payload = _merge_metric_payload(payload, intelligence)
    profile_run = session.scalar(select(DQProfileRun).where(DQProfileRun.dq_run_id == dq_run.id).limit(1))
    if profile_run is None:
        profile_run = DQProfileRun(
            dq_run_id=dq_run.id,
            job_id=job_id,
            table_id=table.id,
            datasource_id=datasource.id if datasource is not None else None,
            schema_name=schema_name,
            table_name=table.name,
            status=dq_run.status,
            started_at=dq_run.started_at,
            finished_at=dq_run.finished_at,
            duration_seconds=int(round((dq_run.duration_ms or 0) / 1000)) if dq_run.duration_ms is not None else None,
            row_count=int(payload.get("row_count") or 0),
            column_count=len(columns),
            sampled=bool(payload.get("sampled") or False),
            sample_ratio=payload.get("sample_ratio"),
            execution_engine="spark",
            error_message=dq_run.error_message,
            created_by_user_id=created_by_user_id,
            trigger_type=trigger_type,
            profile_summary_json=merged_payload,
        )
        session.add(profile_run)
        session.flush()
    else:
        profile_run.job_id = job_id
        profile_run.table_id = table.id
        profile_run.datasource_id = datasource.id if datasource is not None else None
        profile_run.schema_name = schema_name
        profile_run.table_name = table.name
        profile_run.status = dq_run.status
        profile_run.started_at = dq_run.started_at
        profile_run.finished_at = dq_run.finished_at
        profile_run.duration_seconds = int(round((dq_run.duration_ms or 0) / 1000)) if dq_run.duration_ms is not None else None
        profile_run.row_count = int(payload.get("row_count") or 0)
        profile_run.column_count = len(columns)
        profile_run.sampled = bool(payload.get("sampled") or False)
        profile_run.sample_ratio = payload.get("sample_ratio")
        profile_run.execution_engine = "spark"
        profile_run.error_message = dq_run.error_message
        profile_run.created_by_user_id = created_by_user_id
        profile_run.trigger_type = trigger_type
        profile_run.profile_summary_json = merged_payload
        session.execute(delete(DQProfileTableMetric).where(DQProfileTableMetric.profile_run_id == profile_run.id))
        session.execute(delete(DQProfileColumnMetric).where(DQProfileColumnMetric.profile_run_id == profile_run.id))
        session.execute(delete(DQRuleSuggestion).where(DQRuleSuggestion.profile_run_id == profile_run.id))
        session.flush()

    table_metric = DQProfileTableMetric(
        profile_run_id=profile_run.id,
        table_id=table.id,
        row_count=int(payload.get("row_count") or 0),
        column_count=len(columns),
        duplicate_rows_count=int(payload.get("duplicates_count") or 0),
        duplicate_business_key_count=int(payload.get("duplicate_business_key_count") or 0),
        schema_hash=payload.get("schema_hash"),
        schema_drift_detected=bool(intelligence.get("dimension_scores", {}).get("consistencia") is not None and payload.get("schema_hash") is not None and getattr(table, "schema_hash", None) not in {None, payload.get("schema_hash")}),
        freshness_seconds=int(payload.get("freshness_seconds")) if payload.get("freshness_seconds") is not None else None,
        volume_change_ratio=float(payload.get("volume_change_ratio")) if payload.get("volume_change_ratio") is not None else None,
        quality_score=float(consolidated_score or 0.0),
        observed_score=float(observed_score) if observed_score is not None else None,
        formal_score=float(formal_score) if formal_score is not None else None,
        coverage_score=float(coverage_score) if coverage_score is not None else None,
        score_breakdown_json=_score_breakdown_json(
            payload=payload,
            intelligence=intelligence,
            formal_score=formal_score,
            formal_breakdown=formal_breakdown,
            weights=weights,
        ),
    )
    session.add(table_metric)
    session.flush()

    catalog_columns = {
        c.name: c
        for c in session.scalars(select(ColumnEntity).where(ColumnEntity.table_id == table.id)).all()
    }
    column_metrics: list[DQProfileColumnMetric] = []
    for column in columns:
        column_name = str(column.get("column_name") or "")
        catalog_col = catalog_columns.get(column_name)
        column_metric = DQProfileColumnMetric(
            profile_run_id=profile_run.id,
            table_id=table.id,
            column_id=catalog_col.id if catalog_col else None,
            column_name=column_name,
            data_type=str(column.get("data_type") or (catalog_col.data_type if catalog_col else "string")),
            inferred_type=column.get("inferred_type"),
            expected_type=column.get("expected_type"),
            type_mismatch=bool(column.get("type_mismatch") or False),
            null_count=int(column.get("null_count") or 0),
            null_ratio=float(column.get("null_ratio") or 0.0),
            fill_ratio=float(column.get("fill_ratio") or 0.0),
            distinct_count=int(column.get("distinct_count") or 0),
            distinct_ratio=float(column.get("distinct_ratio") or 0.0),
            cardinality_level=column.get("cardinality_level"),
            min_value_masked=column.get("min_value_masked"),
            max_value_masked=column.get("max_value_masked"),
            mean_value=column.get("mean_value"),
            median_value=column.get("median_value"),
            stddev_value=column.get("stddev_value"),
            top_values_json_masked=column.get("top_values_json_masked"),
            pattern_type=column.get("pattern_type"),
            pattern_confidence=column.get("pattern_confidence"),
            outlier_count=int(column.get("outlier_count") or 0),
            duplicate_count=int(column.get("duplicate_count") or 0),
            sensitive_guess=column.get("sensitive_guess"),
            examples_masked_json=column.get("examples_masked_json"),
            created_by_user_id=created_by_user_id,
        )
        session.add(column_metric)
        column_metrics.append(column_metric)
    session.flush()

    suggestion_rows: list[DQRuleSuggestion] = []
    for suggestion in intelligence.get("rule_suggestions", []) or []:
        if not isinstance(suggestion, dict):
            continue
        dimension = str(suggestion.get("dimension") or "").strip()
        suggested_rule_type = str(suggestion.get("suggested_rule_type") or "").strip()
        if not dimension or not suggested_rule_type:
            continue
        definition = suggestion.get("rule_definition_json")
        column_name = None
        if isinstance(definition, dict):
            column_name = definition.get("target_column")
            conditions = definition.get("conditions")
            if isinstance(conditions, list) and conditions:
                first_condition = conditions[0]
                if isinstance(first_condition, dict):
                    column_name = str(first_condition.get("column") or column_name or "")
        rule_suggestion = DQRuleSuggestion(
            profile_run_id=profile_run.id,
            table_id=table.id,
            column_id=catalog_columns.get(column_name).id if column_name and catalog_columns.get(column_name) else None,
            column_name=column_name,
            dimension=dimension,
            suggested_rule_type=suggested_rule_type,
            rule_definition_json=definition if isinstance(definition, (dict, list)) else None,
            confidence_score=float(suggestion.get("confidence_score") or 0.0),
            reason=str(suggestion.get("reason") or ""),
            status="suggested",
            created_by_user_id=created_by_user_id,
        )
        session.add(rule_suggestion)
        suggestion_rows.append(rule_suggestion)
    session.flush()
    return profile_run, table_metric, column_metrics, suggestion_rows, merged_payload


def build_profile_storage_payload(
    *,
    payload: dict[str, Any],
    datasource: DataSource,
    schema_name: str,
    table_name: str,
    profiling_status: str,
    profile_timestamp: datetime,
) -> dict[str, Any]:
    columns = payload.get("columns") if isinstance(payload.get("columns"), list) else []
    return {
        "source_name": datasource.name,
        "schema_name": schema_name,
        "table_name": table_name,
        "profile_timestamp": profile_timestamp.isoformat(),
        "row_count": int(payload.get("row_count") or 0),
        "column_count": len(columns),
        "profiling_status": profiling_status,
        "observation": None if payload.get("observation") is None else str(payload.get("observation")),
        "metrics_json": {
            "completeness_pct_avg": (None if payload.get("completeness_pct_avg") is None else float(payload.get("completeness_pct_avg"))),
            "dq_score": (None if payload.get("dq_score") is None else float(payload.get("dq_score"))),
            "duplicates_count": int(payload.get("duplicates_count") or 0),
            "failed_rules": int(payload.get("failed_rules") or 0),
            "columns": columns,
            **build_profile_metrics_json(payload),
        },
    }


def persist_profiling_output(
    session,
    table: TableEntity,
    datasource: DataSource,
    schema_name: str,
    payload: dict[str, Any],
    *,
    job_id: int | None = None,
    created_by_user_id: int | None = None,
    trigger_type: str = "system",
) -> DQRun:
    payload = validate_profiling_payload(payload)
    profiling_status = _profiling_status_from_payload(payload)
    profile_timestamp = datetime.now(timezone.utc)
    storage_payload = build_profile_storage_payload(
        payload=payload,
        datasource=datasource,
        schema_name=schema_name,
        table_name=table.name,
        profiling_status=profiling_status,
        profile_timestamp=profile_timestamp,
    )
    dq_run = DQRun(datasource_id=datasource.id, table_id=table.id, status=profiling_status, execution_engine="spark")
    dq_run.schema_name = schema_name
    dq_run.profile_payload_json = storage_payload
    session.add(dq_run)
    session.flush()

    _profile_artifacts = _persist_profile_artifacts(
        session,
        dq_run=dq_run,
        table=table,
        datasource=datasource,
        schema_name=schema_name,
        payload=payload,
        created_by_user_id=created_by_user_id,
        job_id=job_id,
        trigger_type=trigger_type,
    )
    _profile_run, _profile_table_metric, _profile_column_metrics, _rule_suggestions, merged_payload = _profile_artifacts
    payload = merged_payload
    storage_payload["metrics_json"] = build_profile_metrics_json(merged_payload)
    dq_run.profile_payload_json = storage_payload
    _persist_table_metric(session, dq_run=dq_run, table=table, payload=payload, metrics_json=storage_payload["metrics_json"])
    table_metric = session.scalar(
        select(DQTableMetric)
        .where(DQTableMetric.run_id == dq_run.id, DQTableMetric.table_id == table.id)
        .order_by(DQTableMetric.id.desc())
        .limit(1)
    )
    if table_metric is not None:
        current_columns = session.scalars(
            select(DQColumnMetric).where(DQColumnMetric.table_metric_id == table_metric.id).order_by(DQColumnMetric.column_name.asc())
        ).all()
        persist_observability_artifacts(
            session,
            dq_run=dq_run,
            table=table,
            table_metric=table_metric,
            current_columns=current_columns,
        )
    return dq_run


def persist_profiling_output_into_existing_run(
    session,
    *,
    dq_run: DQRun,
    table: TableEntity,
    datasource: DataSource,
    schema_name: str,
    payload: dict[str, Any],
    job_id: int | None = None,
    created_by_user_id: int | None = None,
    trigger_type: str = "system",
) -> DQRun:
    payload = validate_profiling_payload(payload)
    profiling_status = _profiling_status_from_payload(payload)
    profile_timestamp = datetime.now(timezone.utc)
    storage_payload = build_profile_storage_payload(
        payload=payload,
        datasource=datasource,
        schema_name=schema_name,
        table_name=table.name,
        profiling_status=profiling_status,
        profile_timestamp=profile_timestamp,
    )
    dq_run.status = profiling_status
    dq_run.execution_engine = "spark"
    dq_run.error_message = None
    dq_run.schema_name = schema_name
    dq_run.profile_payload_json = storage_payload
    session.add(dq_run)
    session.flush()

    _profile_artifacts = _persist_profile_artifacts(
        session,
        dq_run=dq_run,
        table=table,
        datasource=datasource,
        schema_name=schema_name,
        payload=payload,
        created_by_user_id=created_by_user_id,
        job_id=job_id,
        trigger_type=trigger_type,
    )
    _profile_run, _profile_table_metric, _profile_column_metrics, _rule_suggestions, merged_payload = _profile_artifacts
    payload = merged_payload
    storage_payload["metrics_json"] = build_profile_metrics_json(merged_payload)
    dq_run.profile_payload_json = storage_payload
    _persist_table_metric(session, dq_run=dq_run, table=table, payload=payload, metrics_json=storage_payload["metrics_json"])
    table_metric = session.scalar(
        select(DQTableMetric)
        .where(DQTableMetric.run_id == dq_run.id, DQTableMetric.table_id == table.id)
        .order_by(DQTableMetric.id.desc())
        .limit(1)
    )
    if table_metric is not None:
        current_columns = session.scalars(
            select(DQColumnMetric).where(DQColumnMetric.table_metric_id == table_metric.id).order_by(DQColumnMetric.column_name.asc())
        ).all()
        persist_observability_artifacts(
            session,
            dq_run=dq_run,
            table=table,
            table_metric=table_metric,
            current_columns=current_columns,
        )
    return dq_run


def persist_rules_output(
    session,
    *,
    table: TableEntity,
    rules: list[DQRule],
    payload: dict[str, Any],
    reporter_user_id: int | None,
) -> list[DQRuleRun]:
    rules_by_id = {rule.id: rule for rule in rules}
    created: list[DQRuleRun] = []
    for item in payload.get("rules", []):
        rule_id = int(item.get("rule_id"))
        rule = rules_by_id.get(rule_id)
        if not rule:
            continue
        status_value = str(item.get("status") or "error")
        violations_count = int(item.get("violations_count") or 0)
        preview_rows = item.get("preview_rows") if isinstance(item.get("preview_rows"), list) else None
        column_classifications = (
            build_column_classification_map(session, table_id=table.id, key_by="name") if preview_rows else None
        )
        masked_preview_rows, masked_fields = (
            mask_evidence_rows(preview_rows, table=table, column_classifications=column_classifications)
            if preview_rows
            else (None, [])
        )
        run = DQRuleRun(
            rule_id=rule.id,
            status=status_value if status_value in {"pass", "fail", "error"} else "error",
            execution_engine="spark",
            violations_count=violations_count,
            sample_rows_json=masked_preview_rows,
            error_message=None if item.get("error_message") is None else str(item.get("error_message")),
        )
        session.add(run)
        session.flush()
        created.append(run)
        sync_latest_snapshot_for_rule_run(session, rule_run=run, rule=rule)
        if run.status == "fail" and violations_count > 0 and rule.is_active:
            from t2c_data.features.data_quality.rules import upsert_incident_for_dq_rule

            upsert_incident_for_dq_rule(
                session,
                rule,
                violations_count=violations_count,
                preview_rows=masked_preview_rows or [],
                run_id=run.id,
                reporter_user_id=reporter_user_id,
            )
            try:
                from t2c_data.features.data_quality.notifications import notify_dq_rule_violation

                notify_dq_rule_violation(
                    session,
                    rule=rule,
                    table=table,
                    violations_count=violations_count,
                    preview_rows=masked_preview_rows or [],
                    run_id=run.id,
                    reporter_user_id=reporter_user_id,
                )
            except Exception:
                pass
            persist_evidence_sample(
                session,
                dq_run=None,
                rule_run_id=run.id,
                table=table,
                sample_rows=preview_rows or [],
                evidence_type="rule_violation",
                origin="dq_rule",
                status="masked",
                rule_id=rule.id,
                affected_rows_count=violations_count,
                details_json={"rule_name": rule.name, "severity": rule.severity, "masked_fields": masked_fields},
            )
    return created


def _persist_table_metric(
    session,
    *,
    dq_run: DQRun,
    table: TableEntity,
    payload: dict[str, Any],
    metrics_json: dict[str, Any],
) -> None:
    row_count = int(payload.get("row_count") or 0)
    table_metric = DQTableMetric(
        run_id=dq_run.id,
        table_id=table.id,
        row_count=row_count,
        column_count=len(payload.get("columns") or []),
        completeness_pct_avg=_metric_float(payload, "completeness_pct_avg", default=0.0 if row_count <= 0 else 100.0),
        dq_score=_metric_float(payload, "dq_score", default=0.0 if row_count <= 0 else 100.0),
        duplicates_count=int(payload.get("duplicates_count") or 0),
        failed_rules=int(payload.get("failed_rules") or 0),
        metrics_json=metrics_json,
    )
    session.add(table_metric)
    session.flush()

    catalog_columns = {
        c.name: c
        for c in session.scalars(select(ColumnEntity).where(ColumnEntity.table_id == table.id)).all()
    }
    if _has_table(session, "dq_column_metrics"):
        for col in payload.get("columns", []):
            catalog_col = catalog_columns.get(str(col.get("column_name")))
            session.add(
                DQColumnMetric(
                    run_id=dq_run.id,
                    table_metric_id=table_metric.id,
                    column_id=catalog_col.id if catalog_col else None,
                    column_name=str(col.get("column_name") or ""),
                    data_type=str(col.get("data_type") or (catalog_col.data_type if catalog_col else "string")),
                    null_count=int(col.get("null_count") or 0),
                    distinct_count=int(col.get("distinct_count") or 0),
                    null_pct=float(col.get("null_pct") or 0.0),
                    min_value=None if col.get("min_value") is None else str(col.get("min_value")),
                    max_value=None if col.get("max_value") is None else str(col.get("max_value")),
                )
            )
        session.flush()


__all__ = [
    "build_profile_storage_payload",
    "persist_profiling_output",
    "persist_profiling_output_into_existing_run",
    "persist_rules_output",
    "validate_profiling_payload",
]
