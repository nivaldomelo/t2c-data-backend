from __future__ import annotations

import os
from importlib import util
from types import SimpleNamespace
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")


def _load_module(module_name: str, relative_path: str):
    module_path = Path(__file__).resolve().parents[1] / relative_path
    spec = util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
      raise RuntimeError(f"Unable to load {module_name}")
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


observability_module = _load_module(
    "test_dq_observability_module",
    "app/features/data_quality/observability.py",
)
policy_module = _load_module(
    "test_dq_operational_policy_module",
    "app/features/data_quality/operational_policy.py",
)

build_dq_observability_payload = observability_module.build_dq_observability_payload
build_profile_metrics_json = observability_module.build_profile_metrics_json
apply_operational_dq_policy = policy_module.apply_operational_dq_policy


def test_profile_metrics_mark_zero_volume_as_no_data() -> None:
    payload = build_profile_metrics_json(
        {
            "row_count": 0,
            "completeness_pct_avg": 100.0,
            "dq_score": 100.0,
            "duplicates_count": 0,
            "failed_rules": 0,
            "columns": [],
        }
    )

    assert payload["assessment_state"]["code"] == "no_data"
    assert payload["assessment_state"]["score"] is None
    assert payload["dimensions"][0]["status"] == "no_data"


def test_operational_policy_suspends_score_for_zero_volume() -> None:
    payload = {
        "dq_score": 97.5,
        "current": {"row_count": 0, "dq_score": 97.5, "columns": []},
        "observability": {"table": {}},
    }

    result = apply_operational_dq_policy(SimpleNamespace(), table=SimpleNamespace(), payload=payload)

    assert result["effective_dq_score"] is None
    assert result["assessment_state"]["code"] == "no_data"
    assert result["observability"]["table"]["status"] == "no_data"


def test_table_observability_detects_schema_drift_and_traces_actions(monkeypatch) -> None:
    monkeypatch.setattr(
        observability_module,
        "contract_summary",
        lambda *_args, **_kwargs: {
            "contract_id": None,
            "version": None,
            "status": None,
            "published_at": None,
            "last_validation_status": None,
            "last_validation_at": None,
            "last_validation_issues": None,
        },
    )
    monkeypatch.setattr(observability_module, "get_current_contract", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(observability_module, "get_table_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        observability_module,
        "_build_execution_reliability",
        lambda *_args, **_kwargs: {"success_rate_7d": None, "success_rate_30d": None, "runs_7d": 0, "runs_30d": 0},
    )
    monkeypatch.setattr(
        observability_module,
        "_build_incident_state",
        lambda *_args, **_kwargs: {"status": "closed", "status_label": "Sem incidente aberto", "incident_id": None},
    )

    table = SimpleNamespace(id=10, name="sales", schema=SimpleNamespace(name="gold"))
    current_snapshot = {
        "row_count": 0,
        "completeness_pct_avg": 100.0,
        "dq_score": 97.5,
        "effective_dq_score": 97.5,
        "duplicates_count": 0,
        "failed_rules": 0,
        "freshness_seconds": 7200,
        "columns": [
            {"column_name": "id", "data_type": "integer", "null_count": 0, "null_pct": 0.0, "distinct_count": 0, "min_value": None, "max_value": None},
            {"column_name": "amount", "data_type": "numeric", "null_count": 0, "null_pct": 0.0, "distinct_count": 0, "min_value": None, "max_value": None},
        ],
    }
    previous_snapshot = {
        "row_count": 10,
        "completeness_pct_avg": 98.0,
        "dq_score": 98.0,
        "freshness_seconds": 3600,
        "columns": [
            {"column_name": "id", "data_type": "integer", "null_count": 0, "null_pct": 0.0, "distinct_count": 10, "min_value": "1", "max_value": "10"},
            {"column_name": "amount", "data_type": "text", "null_count": 0, "null_pct": 0.0, "distinct_count": 10, "min_value": None, "max_value": None},
        ],
    }

    observability = build_dq_observability_payload(
        session=SimpleNamespace(),
        table=table,
        current_snapshot=current_snapshot,
        previous_snapshot=previous_snapshot,
        history=[
            {"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "dq_score": 98.0, "completeness_pct_avg": 98.0, "row_count": 10, "freshness_seconds": 3600},
            {"run_id": 2, "run_at": "2026-04-13T10:00:00Z", "dq_score": 97.5, "completeness_pct_avg": 100.0, "row_count": 0, "freshness_seconds": 7200},
        ],
        column_history={
            "id": [{"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "null_count": 0, "null_pct": 0.0, "distinct_count": 10, "min_value": "1", "max_value": "10"}],
            "amount": [{"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "null_count": 0, "null_pct": 0.0, "distinct_count": 10, "min_value": None, "max_value": None}],
        },
        current_user=None,
    )

    assert observability["assessment_state"]["code"] == "no_data"
    assert observability["table"]["schema"]["status"] == "degraded"
    assert observability["table"]["schema"]["type_changed"] == 1
    assert observability["troubleshooting"]["actions"][0]["key"] == "recheck_pipeline"


def test_observability_payload_includes_persisted_history(monkeypatch) -> None:
    monkeypatch.setattr(
        observability_module,
        "contract_summary",
        lambda *_args, **_kwargs: {
            "contract_id": None,
            "version": None,
            "status": None,
            "published_at": None,
            "last_validation_status": None,
            "last_validation_at": None,
            "last_validation_issues": None,
        },
    )
    monkeypatch.setattr(observability_module, "get_current_contract", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(observability_module, "get_table_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        observability_module,
        "_build_execution_reliability",
        lambda *_args, **_kwargs: {"success_rate_7d": None, "success_rate_30d": None, "runs_7d": 0, "runs_30d": 0},
    )
    monkeypatch.setattr(
        observability_module,
        "_build_incident_state",
        lambda *_args, **_kwargs: {"status": "closed", "status_label": "Sem incidente aberto", "incident_id": None},
    )
    monkeypatch.setattr(
        "t2c_data.features.data_quality.observability_store.load_persisted_observability_artifacts",
        lambda *_args, **_kwargs: {
            "baselines": [{"metric_key": "volume", "baseline_value": 10.0, "current_value": 0.0}],
            "events": [{"event_type": "anomaly", "metric_key": "volume", "severity": "warning", "observed_value": 0.0, "baseline_value": 10.0}],
            "evidence_samples": [{"id": 1, "dq_run_id": 7, "rule_run_id": None, "rule_id": None, "column_name": None, "evidence_type": "rule_violation", "origin": "dq_rule", "status": "masked", "sample_size": 1, "affected_rows_count": 1, "masked_fields_json": ["email"], "sample_rows_json": [{"email": {"value": "[masked]", "redacted": True, "visibility": "masked", "reason": "sensitive_field"}}], "evidence_json": {}, "created_at": "2026-04-13T10:00:00Z"}],
        },
    )

    table = SimpleNamespace(id=10, name="sales", schema=SimpleNamespace(name="gold"))
    observability = build_dq_observability_payload(
        session=SimpleNamespace(),
        table=table,
        current_snapshot={
            "row_count": 0,
            "completeness_pct_avg": 100.0,
            "dq_score": 97.5,
            "effective_dq_score": 97.5,
            "duplicates_count": 0,
            "failed_rules": 0,
            "freshness_seconds": 7200,
            "columns": [],
        },
        previous_snapshot=None,
        history=[],
        column_history={},
        current_user=None,
    )

    assert observability["historical"]["baselines"]
    assert observability["historical"]["evidence_samples"][0]["sample_rows_json"][0]["email"]["redacted"] is True
