from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

import pytest

from t2c_data.features.data_quality.guardrails import (
    ensure_python_execution_allowed,
    python_execution_allowed,
    sanitize_execution_error,
)
from t2c_data.features.data_quality.profiling import profile_table
from t2c_data.features.data_quality.python_workers import (
    enqueue_python_profiling_job,
    enqueue_python_rules_job,
    enqueue_python_schema_profiling_run,
)
from t2c_data.features.data_quality.rules import run_dq_rule


def test_sanitize_execution_error_redacts_sensitive_tokens() -> None:
    error = RuntimeError("connection failed for postgresql://user:secret@db.local/catalog")
    message = sanitize_execution_error(error, default_message="Falha generica.")
    assert message == "Falha generica."


def test_python_execution_is_removed_regardless_of_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("t2c_data.features.data_quality.guardrails.settings.env", "test")
    assert python_execution_allowed() is False
    with pytest.raises(RuntimeError, match="Execucao local de Data Quality foi removida"):
        ensure_python_execution_allowed()


def test_python_worker_entrypoints_always_fail_closed() -> None:
    with pytest.raises(RuntimeError, match="Execucao local de Data Quality foi removida"):
        enqueue_python_profiling_job(
            table_id=1,
            table_fqn="warehouse.bronze.categories",
            columns=[],
            sample_fraction=None,
            requested_by_user_id=1,
            dq_run_id=1,
        )
    with pytest.raises(RuntimeError, match="Execucao local de Data Quality foi removida"):
        enqueue_python_rules_job(
            table_id=1,
            table_fqn="warehouse.bronze.categories",
            rule_ids=[1],
            requested_by_user_id=1,
            dq_run_id=1,
        )
    with pytest.raises(RuntimeError, match="Execucao local de Data Quality foi removida"):
        enqueue_python_schema_profiling_run(
            parent_run_id=1,
            table_targets=[],
            requested_by_user_id=1,
            concurrency=1,
            sample_fraction=None,
            columns=[],
        )


def test_rule_execution_is_blocked_in_backend() -> None:
    with pytest.raises(RuntimeError, match="executadas exclusivamente no cluster Spark"):
        run_dq_rule(None, None)  # type: ignore[arg-type]


def test_profiling_execution_is_blocked_in_backend() -> None:
    with pytest.raises(RuntimeError, match="executadas exclusivamente no cluster Spark"):
        profile_table(None, None)  # type: ignore[arg-type]
