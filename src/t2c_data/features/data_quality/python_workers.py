from __future__ import annotations

from typing import Any, NoReturn

from t2c_data.services.data_quality import local_execution_disabled_message


def _raise_removed() -> NoReturn:
    raise RuntimeError(local_execution_disabled_message())


def enqueue_python_profiling_job(
    *,
    table_id: int | None,
    table_fqn: str | None,
    columns: list[str],
    sample_fraction: float | None,
    requested_by_user_id: int | None,
    dq_run_id: int | None = None,
):
    _raise_removed()


def enqueue_python_rules_job(
    *,
    table_id: int | None,
    table_fqn: str | None,
    rule_ids: list[int],
    requested_by_user_id: int | None,
    dq_run_id: int | None = None,
):
    _raise_removed()


def enqueue_python_schema_profiling_run(
    *,
    parent_run_id: int,
    table_targets: list[dict[str, Any]],
    requested_by_user_id: int | None,
    concurrency: int,
    sample_fraction: float | None = None,
    columns: list[str] | None = None,
) -> None:
    _raise_removed()


def execute_python_profiling_job(
    job_run_id: int,
    *,
    table_id: int | None,
    table_fqn: str | None,
    columns: list[str],
    sample_fraction: float | None,
    user_id: int | None,
    dq_run_id: int | None = None,
) -> None:
    _raise_removed()


def execute_python_rules_job(
    job_run_id: int,
    *,
    table_id: int | None,
    table_fqn: str | None,
    rule_ids: list[int],
    user_id: int | None,
    dq_run_id: int | None = None,
) -> None:
    _raise_removed()


__all__ = [
    "enqueue_python_profiling_job",
    "enqueue_python_rules_job",
    "enqueue_python_schema_profiling_run",
    "execute_python_profiling_job",
    "execute_python_rules_job",
]
