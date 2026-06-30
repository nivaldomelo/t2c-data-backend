from __future__ import annotations

import os
import sys
from importlib import util
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark-jobs"))

from t2c_data.features.data_quality.rule_builder import build_rule_definition, builder_options_payload


def _load_module(module_name: str, relative_path: str):
    module_path = Path(__file__).resolve().parents[1] / relative_path
    spec = util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {module_name}")
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


observability_module = _load_module(
    "test_dq_dimensions_observability_module",
    "app/features/data_quality/observability.py",
)

try:
    dq_rules_job = _load_module(
        "test_dq_dimensions_dq_rules_job",
        "spark-jobs/dq_rules_job.py",
    )
except ModuleNotFoundError:
    dq_rules_job = None


def test_builder_options_expose_dimensions_templates_and_reconciliation() -> None:
    payload = builder_options_payload()

    assert {item["value"] for item in payload["category_options"]} == {
        "technical",
        "business",
        "operational",
    }
    assert {item["value"] for item in payload["dimension_options"]} == {
        "completude",
        "validade",
        "consistencia",
        "unicidade",
        "tempestividade",
        "acuracia",
    }
    assert any(item["value"] == "reconciliation" for item in payload["rule_types"])
    assert any(item["key"] == "acuracia.contagem" for item in payload["templates"])
    assert any(item["key"] == "unicidade.chave_composta" for item in payload["templates"])


def test_build_rule_definition_supports_composite_unique_and_reconciliation() -> None:
    datasource = SimpleNamespace(id=7, name="warehouse")
    schema = SimpleNamespace(name="gold")
    table = SimpleNamespace(id=12, name="propostas")
    columns_by_name = {
        "cliente_id": SimpleNamespace(id=1, name="cliente_id", data_type="integer"),
        "grupo_id": SimpleNamespace(id=2, name="grupo_id", data_type="integer"),
        "cota_id": SimpleNamespace(id=3, name="cota_id", data_type="integer"),
    }

    uniqueness_definition = build_rule_definition(
        datasource=datasource,
        schema=schema,
        table=table,
        rule_type="uniqueness",
        logic="AND",
        conditions=[],
        columns_by_name=columns_by_name,
        quality_dimension="unicidade",
        rule_category="technical",
        template_key="unicidade.chave_composta",
        unique_columns=["cliente_id", "grupo_id", "cota_id"],
    )

    reconciliation_definition = build_rule_definition(
        datasource=datasource,
        schema=schema,
        table=table,
        rule_type="reconciliation",
        logic="AND",
        conditions=[],
        columns_by_name=columns_by_name,
        quality_dimension="acuracia",
        rule_category="business",
        template_key="acuracia.contagem",
        comparison_target={
            "table_fqn": "dw.financeiro.boletos",
            "metric": "count",
            "tolerance_abs": 2,
            "tolerance_pct": 5,
        },
    )

    assert uniqueness_definition["dimension"] == "unicidade"
    assert uniqueness_definition["category"] == "technical"
    assert uniqueness_definition["template_key"] == "unicidade.chave_composta"
    assert uniqueness_definition["unique_columns"] == ["cliente_id", "grupo_id", "cota_id"]
    assert uniqueness_definition["conditions"] == []
    assert reconciliation_definition["dimension"] == "acuracia"
    assert reconciliation_definition["category"] == "business"
    assert reconciliation_definition["type"] == "reconciliation"
    assert reconciliation_definition["comparison"]["table_fqn"] == "dw.financeiro.boletos"
    assert reconciliation_definition["comparison"]["metric"] == "count"


def test_observability_payload_exposes_six_dimensions(monkeypatch) -> None:
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
        observability_module,
        "_active_rule_dimensions",
        lambda *_args, **_kwargs: {"completude", "validade", "consistencia", "unicidade", "tempestividade", "acuracia"},
    )
    monkeypatch.setattr(
        observability_module,
        "_rule_dimension_stats",
        lambda *_args, **_kwargs: {},
    )

    table = SimpleNamespace(id=10, name="propostas", schema=SimpleNamespace(name="gold"))
    current_snapshot = {
        "row_count": 100,
        "completeness_pct_avg": 98.0,
        "dq_score": 96.0,
        "effective_dq_score": 96.0,
        "duplicates_count": 0,
        "failed_rules": 0,
        "freshness_seconds": 1800,
        "columns": [
            {"column_name": "id", "data_type": "integer", "null_count": 0, "null_pct": 0.0, "distinct_count": 100, "min_value": "1", "max_value": "100"},
            {"column_name": "status", "data_type": "text", "null_count": 0, "null_pct": 0.0, "distinct_count": 3, "min_value": None, "max_value": None},
        ],
    }

    observability = observability_module.build_dq_observability_payload(
        session=SimpleNamespace(),
        table=table,
        current_snapshot=current_snapshot,
        previous_snapshot=current_snapshot,
        history=[
            {"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "dq_score": 96.0, "completeness_pct_avg": 98.0, "row_count": 100, "freshness_seconds": 1800},
            {"run_id": 2, "run_at": "2026-04-13T10:00:00Z", "dq_score": 96.0, "completeness_pct_avg": 98.0, "row_count": 100, "freshness_seconds": 1800},
        ],
        column_history={
            "id": [{"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "null_count": 0, "null_pct": 0.0, "distinct_count": 100, "min_value": "1", "max_value": "100"}],
            "status": [{"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "null_count": 0, "null_pct": 0.0, "distinct_count": 3, "min_value": None, "max_value": None}],
        },
        current_user=None,
    )

    dimensions = {item["key"]: item for item in observability["dimensions"]}
    assert list(dimensions) == ["completude", "validade", "consistencia", "unicidade", "tempestividade", "acuracia"]
    assert dimensions["completude"]["status"] == "healthy"
    assert dimensions["completude"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["consistencia"]["status"] == "healthy"
    assert dimensions["consistencia"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["unicidade"]["status"] == "healthy"
    assert dimensions["unicidade"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["tempestividade"]["status"] == "healthy"
    assert dimensions["tempestividade"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["tempestividade"]["summary"] == "Atualização recente, mas sem SLA configurado"
    assert dimensions["validade"]["status"] == "not_evaluated"
    assert dimensions["validade"]["evaluation_label"] == "Não avaliado"
    assert dimensions["acuracia"]["status"] == "not_evaluated"
    assert dimensions["acuracia"]["evaluation_label"] == "Não avaliado"
    assert observability["quality_coverage"]["evaluated_dimensions"] == 4
    assert observability["quality_coverage"]["formal_dimensions"] == 0
    assert observability["quality_coverage"]["summary"] == "4 de 6 dimensões com evidência"
    assert observability["quality_coverage"]["formal_summary"] == "0 de 6 dimensões com regra formal"


def test_observability_payload_explains_not_evaluated_dimensions(monkeypatch) -> None:
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
    monkeypatch.setattr(observability_module, "_active_rule_dimensions", lambda *_args, **_kwargs: set())

    table = SimpleNamespace(id=11, name="clientes", schema=SimpleNamespace(name="gold"))
    current_snapshot = {
        "row_count": 100,
        "completeness_pct_avg": 98.0,
        "dq_score": 96.0,
        "effective_dq_score": 96.0,
        "duplicates_count": 0,
        "failed_rules": 0,
        "freshness_seconds": 1800,
        "columns": [
            {"column_name": "id", "data_type": "integer", "null_count": 0, "null_pct": 0.0, "distinct_count": 100, "min_value": "1", "max_value": "100"},
        ],
    }

    observability = observability_module.build_dq_observability_payload(
        session=SimpleNamespace(),
        table=table,
        current_snapshot=current_snapshot,
        previous_snapshot=current_snapshot,
        history=[
            {"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "dq_score": 96.0, "completeness_pct_avg": 98.0, "row_count": 100, "freshness_seconds": 1800},
            {"run_id": 2, "run_at": "2026-04-13T10:00:00Z", "dq_score": 96.0, "completeness_pct_avg": 98.0, "row_count": 100, "freshness_seconds": 1800},
        ],
        column_history={
            "id": [{"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "null_count": 0, "null_pct": 0.0, "distinct_count": 100, "min_value": "1", "max_value": "100"}],
        },
        current_user=None,
    )

    dimensions = {item["key"]: item for item in observability["dimensions"]}
    assert observability["quality_coverage"]["evaluated_dimensions"] == 4
    assert observability["quality_coverage"]["summary"] == "4 de 6 dimensões com evidência"
    assert dimensions["completude"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["consistencia"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["unicidade"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["tempestividade"]["evaluation_label"] == "Parcialmente avaliada"
    assert dimensions["validade"]["status"] == "not_evaluated"
    assert dimensions["validade"]["evaluation_label"] == "Não avaliado"
    assert dimensions["validade"]["coverage_label"] == "Sem regra de validade configurada para este ativo."
    assert dimensions["validade"]["explanation"]
    assert dimensions["validade"]["recommended_action"] == "Criar regra de CPF, CNPJ, e-mail, telefone, status permitido ou data coerente."
    assert dimensions["acuracia"]["status"] == "not_evaluated"
    assert dimensions["acuracia"]["evaluation_label"] == "Não avaliado"
    assert dimensions["acuracia"]["trend"]["label"] == "Sem histórico"
    assert dimensions["acuracia"]["recommended_action"] == "Criar regra de reconciliação, como count origem x destino, soma de valores ou diferença máxima permitida."
    assert "4 de 6 dimensões com evidência" in observability["assessment_state"]["reason"]


def test_observability_payload_marks_formal_rules_as_healthy(monkeypatch) -> None:
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
    monkeypatch.setattr(observability_module, "get_current_contract", lambda *_args, **_kwargs: SimpleNamespace(columns=[SimpleNamespace(name="id")], freshness_hours=1))
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
        observability_module,
        "_active_rule_dimensions",
        lambda *_args, **_kwargs: {"completude", "validade", "consistencia", "unicidade", "tempestividade", "acuracia"},
    )
    monkeypatch.setattr(
        observability_module,
        "_rule_dimension_stats",
        lambda *_args, **_kwargs: {
            "completude": {"rules_count": 1, "failed_rules_count": 0},
            "validade": {"rules_count": 1, "failed_rules_count": 0},
            "consistencia": {"rules_count": 1, "failed_rules_count": 0},
            "unicidade": {"rules_count": 1, "failed_rules_count": 0},
            "tempestividade": {"rules_count": 1, "failed_rules_count": 0},
            "acuracia": {"rules_count": 1, "failed_rules_count": 0},
        },
    )

    table = SimpleNamespace(id=12, name="contratos", schema=SimpleNamespace(name="gold"))
    current_snapshot = {
        "row_count": 100,
        "completeness_pct_avg": 98.0,
        "dq_score": 96.0,
        "effective_dq_score": 96.0,
        "duplicates_count": 0,
        "failed_rules": 0,
        "freshness_seconds": 1800,
        "columns": [
            {"column_name": "id", "data_type": "integer", "null_count": 0, "null_pct": 0.0, "distinct_count": 100, "min_value": "1", "max_value": "100"},
        ],
    }

    observability = observability_module.build_dq_observability_payload(
        session=SimpleNamespace(),
        table=table,
        current_snapshot=current_snapshot,
        previous_snapshot=current_snapshot,
        history=[
            {"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "dq_score": 96.0, "completeness_pct_avg": 98.0, "row_count": 100, "freshness_seconds": 1800},
            {"run_id": 2, "run_at": "2026-04-13T10:00:00Z", "dq_score": 96.0, "completeness_pct_avg": 98.0, "row_count": 100, "freshness_seconds": 1800},
        ],
        column_history={
            "id": [{"run_id": 1, "run_at": "2026-04-12T10:00:00Z", "null_count": 0, "null_pct": 0.0, "distinct_count": 100, "min_value": "1", "max_value": "100"}],
        },
        current_user=None,
    )

    dimensions = {item["key"]: item for item in observability["dimensions"]}
    assert observability["quality_coverage"]["evaluated_dimensions"] == 6
    assert observability["quality_coverage"]["formal_dimensions"] == 6
    assert observability["quality_coverage"]["summary"] == "6 de 6 dimensões com evidência"
    assert observability["quality_coverage"]["formal_summary"] == "6 de 6 dimensões com regra formal"
    assert dimensions["completude"]["evaluation_label"] == "Saudável"
    assert dimensions["consistencia"]["evaluation_label"] == "Saudável"
    assert dimensions["unicidade"]["evaluation_label"] == "Saudável"
    assert dimensions["tempestividade"]["evaluation_label"] == "Saudável"
    assert dimensions["acuracia"]["evaluation_label"] == "Saudável"


def test_spark_helpers_support_regex_not_future_and_reconciliation() -> None:
    if dq_rules_job is None:
        pytest.skip("pyspark não está disponível neste ambiente de teste.")

    regex_expr = dq_rules_job._build_condition_expr(
        SimpleNamespace(columns=["documento"]),
        {"column": "documento", "operator": "matches_regex", "value": "^\\d{3}$"},
    )
    not_future_expr = dq_rules_job._build_condition_expr(
        SimpleNamespace(columns=["data_emissao"]),
        {"column": "data_emissao", "operator": "not_future"},
    )

    assert regex_expr is not None
    assert not_future_expr is not None

    class _FakeAggResult:
        def __init__(self, value):
            self._value = value

        def collect(self):
            return [{"value": self._value}]

    class _FakeCountResult:
        def __init__(self, count_value: int):
            self._count_value = count_value

        def collect(self):
            return [{"value": self._count_value}]

    class _FakeDataFrame:
        def __init__(self, *, count_value: int, sum_value: float, columns: list[str]):
            self._count_value = count_value
            self._sum_value = sum_value
            self.columns = columns

        def count(self):
            return self._count_value

        def select(self, *_args, **_kwargs):
            return _FakeAggResult(self._sum_value)

    count_result = dq_rules_job._reconciliation_result(
        _FakeDataFrame(count_value=10, sum_value=0.0, columns=["id"]),
        _FakeDataFrame(count_value=10, sum_value=0.0, columns=["id"]),
        {"comparison": {"metric": "count", "table_fqn": "dw.financeiro.boletos", "key_columns": ["id"]}},
    )
    sum_result = dq_rules_job._reconciliation_result(
        _FakeDataFrame(count_value=10, sum_value=100.0, columns=["id", "valor"]),
        _FakeDataFrame(count_value=10, sum_value=101.5, columns=["id", "valor"]),
        {"comparison": {"metric": "sum", "table_fqn": "dw.financeiro.boletos", "column": "valor", "tolerance_abs": 2}},
    )

    assert count_result[0] == 0
    assert count_result[2] is None
    assert sum_result[0] == 0
    assert sum_result[2] is None
