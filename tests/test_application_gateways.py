from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.data_quality.application import (
    launch_single_rule_run,
    launch_spark_batch_profiling_run,
    launch_spark_profiling_run,
)
from t2c_data.features.platform.jobs import run_platform_job
from t2c_data.models.catalog import DataSource
from t2c_data.models.dq import DQRule
from t2c_data.features.lineage.application import run_lineage_source_sync, run_lineage_table_sync
from t2c_data.schemas.dq import DQSparkProfilingRunRequest
from t2c_data.schemas.dq import DQSparkBatchProfilingRunRequest
from t2c_data.schemas.lineage import LineageSourceConfigOut, LineageSourceSyncOut
from t2c_data.schemas.platform import IntegrationSyncJobRunIn


class _FakeDB:
    def __init__(self, *, execute_result=None, get_map: dict[tuple[type, int], object] | None = None) -> None:
        self.commit_calls = 0
        self.execute_result = execute_result or []
        self.get_map = get_map or {}

    def commit(self) -> None:
        self.commit_calls += 1

    def execute(self, _query):
        return SimpleNamespace(all=lambda: list(self.execute_result))

    def get(self, model, key):
        return self.get_map.get((model, key))


class _FakeLineageGateway:
    def __init__(self, result: LineageSourceSyncOut) -> None:
        self.result = result
        self.source_calls: list[dict] = []
        self.table_calls: list[dict] = []

    def sync_source(self, **kwargs) -> LineageSourceSyncOut:
        self.source_calls.append(kwargs)
        return self.result

    def sync_table(self, **kwargs) -> LineageSourceSyncOut:
        self.table_calls.append(kwargs)
        return self.result


class _FakeDQGateway:
    def __init__(self) -> None:
        self.table_run_calls: list[dict] = []
        self.schema_run_calls: list[dict] = []
        self.profiling_calls: list[dict] = []
        self.rule_calls: list[dict] = []
        self.schema_profiling_calls: list[dict] = []

    def create_table_run(self, *, table_id: int | None, table_fqn: str | None, **_kwargs):
        self.table_run_calls.append({"table_id": table_id, "table_fqn": table_fqn})
        return SimpleNamespace(id=701)

    def create_schema_run(self, *, datasource_id: int | None, schema_name: str, **_kwargs):
        self.schema_run_calls.append({"datasource_id": datasource_id, "schema_name": schema_name})
        return SimpleNamespace(id=801)

    def create_batch_run(self, *, datasource_id: int | None, scope: str, schema_name: str | None = None, **_kwargs):
        self.schema_run_calls.append({"datasource_id": datasource_id, "schema_name": schema_name, "scope": scope})
        return SimpleNamespace(id=851)

    def enqueue_profiling(
        self,
        *,
        table_id: int | None,
        table_fqn: str | None,
        columns: list[str],
        sample_fraction: float | None,
        requested_by_user_id: int | None,
        dq_run_id: int | None = None,
        **_kwargs,
    ):
        self.profiling_calls.append(
            {
                "table_id": table_id,
                "table_fqn": table_fqn,
                "columns": columns,
                "sample_fraction": sample_fraction,
                "requested_by_user_id": requested_by_user_id,
                "dq_run_id": dq_run_id,
            }
        )
        return SimpleNamespace(id=702)

    def enqueue_rules(self, **kwargs):
        self.rule_calls.append(kwargs)
        return SimpleNamespace(id=703)

    def enqueue_schema_profiling(
        self,
        *,
        parent_run_id: int,
        table_targets: list[dict],
        requested_by_user_id: int | None,
        concurrency: int,
        sample_fraction: float | None = None,
        columns: list[str] | None = None,
        **_kwargs,
    ) -> None:
        self.schema_profiling_calls.append(
            {
                "parent_run_id": parent_run_id,
                "table_targets": table_targets,
                "requested_by_user_id": requested_by_user_id,
                "concurrency": concurrency,
                "sample_fraction": sample_fraction,
                "columns": columns,
            }
        )


def _build_lineage_result() -> LineageSourceSyncOut:
    return LineageSourceSyncOut(
        source=LineageSourceConfigOut(
            id=9,
            name="OpenLineage",
            source_type="openlineage",
            base_url="http://openlineage:5000",
            default_namespace="demo",
            auth_type="none",
            auth_username=None,
            configured_auth=False,
            enabled=True,
            last_sync_at=None,
            last_sync_status=None,
            last_sync_message=None,
            created_at="2026-03-26T10:00:00Z",
            updated_at="2026-03-26T10:00:00Z",
        ),
        namespace="demo",
        node_id="dataset:demo:gold.sales",
        depth=2,
        datasets_synced=3,
        jobs_synced=1,
        runs_synced=2,
        relations_created=4,
        relations_updated=1,
    )


def _build_source_config() -> SimpleNamespace:
    return SimpleNamespace(
        id=9,
        base_url="http://openlineage:5000",
        default_namespace="demo",
        auth_type="none",
        auth_username=None,
        auth_secret=None,
        enabled=True,
        last_sync_at=None,
        last_sync_status=None,
        last_sync_message=None,
        created_at="2026-03-26T10:00:00Z",
        updated_at="2026-03-26T10:00:00Z",
    )


def test_run_lineage_source_sync_uses_injected_gateway(monkeypatch) -> None:
    db = _FakeDB()
    source = _build_source_config()
    user = SimpleNamespace(id=21)
    audit_calls: list[dict] = []
    gateway = _FakeLineageGateway(_build_lineage_result())

    monkeypatch.setattr("t2c_data.features.lineage.sync_actions.get_source_config", lambda _db, _source_id: source)
    monkeypatch.setattr("t2c_data.features.lineage.sync_actions.add_audit_log", lambda **kwargs: audit_calls.append(kwargs))

    result = run_lineage_source_sync(
        db=db,
        source_id=9,
        namespace="demo",
        node_id="dataset:demo:gold.sales",
        depth=2,
        table_id=101,
        user=user,
        sync_gateway=gateway,
    )

    assert result.datasets_synced == 3
    assert db.commit_calls == 1
    assert gateway.source_calls == [
        {
            "db": db,
            "source": source,
            "namespace": "demo",
            "node_id": "dataset:demo:gold.sales",
            "depth": 2,
            "table_id": 101,
        }
    ]
    assert audit_calls and audit_calls[0]["action"] == "lineage.source.sync"


def test_run_lineage_table_sync_uses_injected_gateway(monkeypatch) -> None:
    db = _FakeDB()
    user = SimpleNamespace(id=22)
    audit_calls: list[dict] = []
    gateway = _FakeLineageGateway(_build_lineage_result())
    source = _build_source_config()

    monkeypatch.setattr("t2c_data.features.lineage.sync_actions.get_source_config", lambda _db, _source_id: source)
    monkeypatch.setattr("t2c_data.features.lineage.sync_actions.add_audit_log", lambda **kwargs: audit_calls.append(kwargs))

    result = run_lineage_table_sync(db=db, table_id=55, depth=3, user=user, sync_gateway=gateway)

    assert result.relations_created == 4
    assert db.commit_calls == 1
    assert gateway.table_calls == [{"db": db, "table_id": 55, "depth": 3}]
    assert audit_calls and audit_calls[0]["action"] == "lineage.table.sync"


def test_launch_spark_profiling_run_table_scope_uses_injected_gateway(monkeypatch) -> None:
    db = _FakeDB()
    user = SimpleNamespace(id=31)
    audit_calls: list[dict] = []
    gateway = _FakeDQGateway()
    payload = DQSparkProfilingRunRequest(
        scope="table",
        table_id=77,
        table_fqn="silver.orders",
        columns=["customer_id"],
        sample_fraction=0.25,
    )

    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.write_audit_log_sync", lambda *_args, **kwargs: audit_calls.append(kwargs))

    result = launch_spark_profiling_run(
        db=db,
        payload=payload,
        current_user=user,
        audit_kwargs={"actor_user_id": user.id},
        execution_gateway=gateway,
    )

    assert result.run_id == 701
    assert result.job_run_id == 702
    assert result.scope == "table"
    assert db.commit_calls == 1
    assert gateway.table_run_calls == [{"table_id": 77, "table_fqn": "silver.orders"}]
    assert gateway.profiling_calls == [
        {
            "table_id": 77,
            "table_fqn": "silver.orders",
            "columns": ["customer_id"],
            "sample_fraction": 0.25,
            "requested_by_user_id": 31,
            "dq_run_id": 701,
        }
    ]
    assert audit_calls and audit_calls[0]["action"] == "dq.profiling.run.start"


def test_launch_spark_profiling_run_schema_scope_uses_injected_gateway(monkeypatch) -> None:
    rows = [
        (
            SimpleNamespace(id=10, name="orders", table_type="table"),
            SimpleNamespace(name="silver"),
            SimpleNamespace(datasource_id=7),
        ),
        (
            SimpleNamespace(id=11, name="customers", table_type="table"),
            SimpleNamespace(name="silver"),
            SimpleNamespace(datasource_id=7),
        ),
    ]
    db = _FakeDB(execute_result=rows)
    user = SimpleNamespace(id=32)
    audit_calls: list[dict] = []
    gateway = _FakeDQGateway()
    payload = DQSparkProfilingRunRequest(
        scope="schema",
        schema="silver",
        datasource_id=7,
        concurrency=4,
        limit=10,
        include_tables=["orders", "customers"],
        sample_fraction=0.5,
        columns=["id"],
    )

    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.write_audit_log_sync", lambda *_args, **kwargs: audit_calls.append(kwargs))

    result = launch_spark_profiling_run(
        db=db,
        payload=payload,
        current_user=user,
        audit_kwargs={"actor_user_id": user.id},
        execution_gateway=gateway,
    )

    assert result.run_id == 801
    assert result.scope == "schema"
    assert result.tables_total == 2
    assert db.commit_calls == 1
    assert gateway.schema_run_calls == [{"datasource_id": 7, "schema_name": "silver"}]
    assert len(gateway.schema_profiling_calls) == 1
    schema_call = gateway.schema_profiling_calls[0]
    assert schema_call["parent_run_id"] == 801
    assert schema_call["requested_by_user_id"] == 32
    assert schema_call["concurrency"] == 4
    assert schema_call["sample_fraction"] == 0.5
    assert schema_call["columns"] == ["id"]
    assert schema_call["table_targets"] == [
        {"table_id": 10, "table_fqn": "silver.orders", "schema_name": "silver", "datasource_id": 7},
        {"table_id": 11, "table_fqn": "silver.customers", "schema_name": "silver", "datasource_id": 7},
    ]
    assert audit_calls and audit_calls[0]["action"] == "dq.profiling.schema_run.start"


def test_launch_spark_batch_profiling_run_datasource_scope_uses_injected_gateway(monkeypatch) -> None:
    rows = [
        (
            SimpleNamespace(id=10, name="orders", table_type="table"),
            SimpleNamespace(name="bronze"),
            SimpleNamespace(datasource_id=7),
        ),
        (
            SimpleNamespace(id=11, name="customers", table_type="table"),
            SimpleNamespace(name="silver"),
            SimpleNamespace(datasource_id=7),
        ),
    ]
    db = _FakeDB(execute_result=rows)
    user = SimpleNamespace(id=34)
    audit_calls: list[dict] = []
    gateway = _FakeDQGateway()
    payload = DQSparkBatchProfilingRunRequest(
        scope_type="datasource",
        datasource_id=7,
        concurrency=3,
        limit=200,
    )

    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.write_audit_log_sync", lambda *_args, **kwargs: audit_calls.append(kwargs))
    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.update_dq_run_fields", lambda *_args, **_kwargs: None)

    result = launch_spark_batch_profiling_run(
        db=db,
        payload=payload,
        current_user=user,
        audit_kwargs={"actor_user_id": user.id},
        execution_gateway=gateway,
    )

    assert result.run_id == 851
    assert result.scope == "datasource"
    assert result.tables_total == 2
    assert db.commit_calls == 1
    assert gateway.schema_run_calls == [{"datasource_id": 7, "schema_name": None, "scope": "datasource"}]
    assert len(gateway.schema_profiling_calls) == 1
    assert gateway.schema_profiling_calls[0]["table_targets"] == [
        {"table_id": 10, "table_fqn": "bronze.orders", "schema_name": "bronze", "datasource_id": 7},
        {"table_id": 11, "table_fqn": "silver.customers", "schema_name": "silver", "datasource_id": 7},
    ]
    assert audit_calls and audit_calls[0]["action"] == "dq.profiling.batch_run.start"


def test_launch_spark_batch_profiling_run_tables_scope_uses_injected_gateway(monkeypatch) -> None:
    rows = [
        (
            SimpleNamespace(id=20, name="payments", table_type="table"),
            SimpleNamespace(name="gold"),
            SimpleNamespace(datasource_id=8),
        ),
    ]
    db = _FakeDB(execute_result=rows)
    user = SimpleNamespace(id=35)
    audit_calls: list[dict] = []
    gateway = _FakeDQGateway()
    payload = DQSparkBatchProfilingRunRequest(
        scope_type="tables",
        datasource_id=8,
        schema="gold",
        table_ids=[20],
        concurrency=2,
        limit=50,
    )

    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.write_audit_log_sync", lambda *_args, **kwargs: audit_calls.append(kwargs))
    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.update_dq_run_fields", lambda *_args, **_kwargs: None)

    result = launch_spark_batch_profiling_run(
        db=db,
        payload=payload,
        current_user=user,
        audit_kwargs={"actor_user_id": user.id},
        execution_gateway=gateway,
    )

    assert result.run_id == 851
    assert result.scope == "tables"
    assert result.tables_total == 1
    assert gateway.schema_run_calls == [{"datasource_id": 8, "schema_name": "gold", "scope": "tables"}]
    assert gateway.schema_profiling_calls[0]["table_targets"] == [
        {"table_id": 20, "table_fqn": "gold.payments", "schema_name": "gold", "datasource_id": 8},
    ]
    assert audit_calls and audit_calls[0]["metadata"]["scope_type"] == "tables"


def test_launch_spark_batch_profiling_run_schema_scope_without_tables_returns_clear_message(monkeypatch) -> None:
    db = _FakeDB(execute_result=[])
    user = SimpleNamespace(id=36)
    gateway = _FakeDQGateway()
    payload = DQSparkBatchProfilingRunRequest(
        scope_type="schema",
        datasource_id=9,
        schema="analytics",
        concurrency=4,
        limit=200,
    )

    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.write_audit_log_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.update_dq_run_fields", lambda *_args, **_kwargs: None)

    with pytest.raises(HTTPException) as exc_info:
        launch_spark_batch_profiling_run(
            db=db,
            payload=payload,
            current_user=user,
            audit_kwargs={"actor_user_id": user.id},
            execution_gateway=gateway,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Nenhuma tabela elegível encontrada para o schema analytics."


def test_launch_single_rule_run_uses_spark_gateway_and_never_local_engine(monkeypatch) -> None:
    rule = DQRule(
        id=91,
        table_id=77,
        table_fqn="warehouse.silver.orders",
        name="price > 0",
        severity="high",
        rule_type="column_validation",
        execution_engine="python",
        is_active=True,
        rule_definition_json={
            "version": 1,
            "type": "column_validation",
            "logic": "AND",
            "conditions": [{"column": "price", "operator": "greater_than", "value": 0, "value_type": "number"}],
        },
    )
    db = _FakeDB(get_map={(DQRule, 91): rule})
    user = SimpleNamespace(id=33)
    audit_calls: list[dict] = []
    gateway = _FakeDQGateway()

    monkeypatch.setattr("t2c_data.features.data_quality.spark_launch_commands.write_audit_log_sync", lambda *_args, **kwargs: audit_calls.append(kwargs))
    monkeypatch.setattr(
        "t2c_data.features.data_quality.spark_launch_commands.build_dq_job_out",
        lambda job, _db: SimpleNamespace(id=job.id, status="queued", execution_engine="spark"),
    )

    result = launch_single_rule_run(
        db=db,
        rule_id=91,
        current_user=user,
        audit_kwargs={"actor_user_id": user.id},
        execution_gateway=gateway,
    )

    assert result.id == 703
    assert db.commit_calls == 1
    assert gateway.table_run_calls == [{"table_id": 77, "table_fqn": "warehouse.silver.orders"}]
    assert gateway.rule_calls == [
        {
            "table_id": 77,
            "table_fqn": "warehouse.silver.orders",
            "rule_ids": [91],
            "requested_by_user_id": 33,
            "dq_run_id": 701,
            "execution_engine": "spark",
        }
    ]
    assert audit_calls and audit_calls[0]["action"] == "dq_rule.run.start"


def test_run_platform_job_datasource_scan_enqueues_instead_of_running_inline(monkeypatch) -> None:
    datasource = SimpleNamespace(id=41, name="warehouse", db_type="postgres")
    db = _FakeDB(get_map={(DataSource, 41): datasource})
    user = SimpleNamespace(id=44)
    enqueue_calls: list[dict] = []

    def _enqueue(_session, *, datasource, started_by, trigger_mode, schedule_id=None):
        enqueue_calls.append(
            {
                "datasource_id": datasource.id,
                "started_by": started_by,
                "trigger_mode": trigger_mode,
                "schedule_id": schedule_id,
            }
        )
        scan_run = SimpleNamespace(id=501, status="queued")
        job = SimpleNamespace(
            id=801,
            job_key="datasource:scan:datasource:41",
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=41,
            target_name="warehouse",
            trigger_mode=trigger_mode,
            status="queued",
            queued_at="2026-05-25T00:00:00Z",
            started_at="2026-05-25T00:00:00Z",
            finished_at=None,
            next_expected_run_at=None,
            records_processed=None,
            progress_pct=0.0,
            correlation_id="corr-1",
            requested_by_user_id=44,
            error=None,
            context_json={"datasource_id": 41},
            result_summary_json=None,
            created_at=None,
            updated_at=None,
        )
        return scan_run, job

    monkeypatch.setattr("t2c_data.features.scanner.application.enqueue_datasource_scan", _enqueue)

    result = run_platform_job(
        session=db,
        payload=IntegrationSyncJobRunIn(source="datasource", job_type="scan", target_id=41, target_type="datasource"),
        current_user=user,
        audit_kwargs={"actor_user_id": user.id},
    )

    assert result.id == 801
    assert result.status == "queued"
    assert enqueue_calls == [{"datasource_id": 41, "started_by": 44, "trigger_mode": "manual", "schedule_id": None}]
