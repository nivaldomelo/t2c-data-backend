from __future__ import annotations

from datetime import datetime, timezone

from t2c_data.features.integrations import airflow_read_models as airflow_contract
from t2c_data.services.integrations.status_resolver import (
    dimension_available,
    dimension_delayed,
    dimension_down,
    dimension_healthy,
    dimension_idle,
    dimension_partial,
    dimension_unknown,
    resolve_status_contract,
    resolve_status_contract_v2,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_resolve_status_contract_returns_healthy_for_fully_ok_integration() -> None:
    contract = resolve_status_contract(
        source_name="metabase",
        connectivity=dimension_healthy(message="ok", checked_at=_now(), reason_code="health_ok"),
        operation=dimension_idle(message="sync ok", checked_at=_now(), reason_code="sync_ok"),
        consumption=dimension_available(message="available", checked_at=_now(), reason_code="consumption_available"),
        checked_at=_now(),
        contract_version="v1",
    )

    assert contract.contract_version == "v1"
    assert contract.overall_status == "healthy"
    assert contract.connectivity.status == "healthy"
    assert contract.operation.status == "idle"
    assert contract.consumption.status == "available"


def test_resolve_status_contract_returns_critical_when_connectivity_down() -> None:
    contract = resolve_status_contract(
        source_name="airflow",
        connectivity=dimension_down(message="down", checked_at=_now(), reason_code="source_unreachable"),
        operation=dimension_idle(message="idle", checked_at=_now(), reason_code="idle"),
        consumption=dimension_available(message="available", checked_at=_now(), reason_code="consumption_available"),
        checked_at=_now(),
        contract_version="v1",
    )

    assert contract.overall_status == "critical"
    assert contract.connectivity.status == "down"


def test_resolve_status_contract_returns_warning_for_delayed_operation() -> None:
    contract = resolve_status_contract(
        source_name="metabase",
        connectivity=dimension_healthy(message="ok", checked_at=_now(), reason_code="health_ok"),
        operation=dimension_delayed(message="delayed", checked_at=_now(), reason_code="sync_partial"),
        consumption=dimension_partial(message="partial", checked_at=_now(), reason_code="partial_consumption"),
        checked_at=_now(),
        contract_version="v1",
    )

    assert contract.overall_status == "warning"
    assert contract.operation.status == "delayed"
    assert contract.consumption.status == "partial"


def test_resolve_status_contract_returns_unknown_when_dimension_is_unknown() -> None:
    contract = resolve_status_contract(
        source_name="airflow",
        connectivity=dimension_unknown(message="unknown", checked_at=_now(), reason_code="health_unknown"),
        operation=dimension_idle(message="idle", checked_at=_now(), reason_code="idle"),
        consumption=dimension_available(message="available", checked_at=_now(), reason_code="consumption_available"),
        checked_at=_now(),
        contract_version="v1",
    )

    assert contract.overall_status == "unknown"
    assert contract.connectivity.status == "unknown"


def test_resolve_status_contract_v2_keeps_payload_shape_without_breaking_v1() -> None:
    contract = resolve_status_contract_v2(
        source_name="airflow",
        connectivity=dimension_healthy(message="ok", checked_at=_now(), reason_code="health_ok"),
        operation=dimension_idle(message="idle", checked_at=_now(), reason_code="idle"),
        consumption=dimension_available(message="available", checked_at=_now(), reason_code="consumption_available"),
        checked_at=_now(),
    )

    assert contract.contract_version == "v2"
    assert contract.overall_status == "healthy"
    assert contract.connectivity.reason_code == "health_ok"


def test_inspect_airflow_operational_contract_reports_missing_views(monkeypatch) -> None:
    monkeypatch.setattr(airflow_contract.settings, "airflow_source_schema", "airflow_meta")
    monkeypatch.setattr(airflow_contract.settings, "airflow_metadata_contract_version", "v7")

    def fake_schema_exists(executor, schema_name: str) -> bool:  # noqa: ANN001
        return schema_name == "airflow_meta"

    def fake_table_exists(executor, table_name: str) -> bool:  # noqa: ANN001
        return table_name in {"dag_run", "dag", "task_instance", "dag_tag", "task_fail", "log"}

    def fake_relation_exists(executor, schema_name: str, relation_name: str) -> bool:  # noqa: ANN001
        return relation_name in {airflow_contract.AIRFLOW_DAG_RUNS_VIEW, airflow_contract.AIRFLOW_DAGS_VIEW}

    monkeypatch.setattr(airflow_contract, "_schema_exists", fake_schema_exists)
    monkeypatch.setattr(airflow_contract, "_table_exists", fake_table_exists)
    monkeypatch.setattr(airflow_contract, "_relation_exists", fake_relation_exists)

    snapshot = airflow_contract.inspect_airflow_operational_contract(object())

    assert snapshot.contract_version == "v7"
    assert snapshot.ready is False
    assert airflow_contract.AIRFLOW_FAILURES_VIEW in snapshot.missing_views
    assert airflow_contract.AIRFLOW_OPERATIONAL_VIEW in snapshot.missing_views
