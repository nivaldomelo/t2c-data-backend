from __future__ import annotations

from typing import Protocol

from t2c_data.features.data_quality.spark_runs import (
    create_spark_batch_dq_run,
    create_spark_dq_run,
    create_spark_schema_dq_run,
)
from t2c_data.services.data_quality import ensure_spark_execution_engine


class DQExecutionGateway(Protocol):
    def create_table_run(
        self,
        *,
        table_id: int | None,
        table_fqn: str | None,
        profiling_schedule_id: int | None = None,
        execution_engine: str | None = None,
    ): ...

    def create_schema_run(
        self,
        *,
        datasource_id: int | None,
        schema_name: str,
        profiling_schedule_id: int | None = None,
        execution_engine: str | None = None,
    ): ...

    def create_batch_run(
        self,
        *,
        datasource_id: int | None,
        scope: str,
        schema_name: str | None = None,
        profiling_schedule_id: int | None = None,
        execution_engine: str | None = None,
    ): ...

    def enqueue_profiling(
        self,
        *,
        table_id: int | None,
        table_fqn: str | None,
        columns: list[str],
        sample_fraction: float | None,
        requested_by_user_id: int | None,
        dq_run_id: int | None = None,
        execution_engine: str | None = None,
    ): ...

    def enqueue_rules(
        self,
        *,
        table_id: int | None,
        table_fqn: str | None,
        rule_ids: list[int],
        requested_by_user_id: int | None,
        dq_run_id: int | None = None,
        execution_engine: str | None = None,
    ): ...

    def enqueue_schema_profiling(
        self,
        *,
        parent_run_id: int,
        table_targets: list[dict],
        requested_by_user_id: int | None,
        concurrency: int,
        sample_fraction: float | None = None,
        columns: list[str] | None = None,
        execution_engine: str | None = None,
    ) -> None: ...


class DefaultDQExecutionGateway:
    def create_table_run(
        self,
        *,
        table_id: int | None,
        table_fqn: str | None,
        profiling_schedule_id: int | None = None,
        execution_engine: str | None = None,
    ):
        engine = ensure_spark_execution_engine(execution_engine)
        return create_spark_dq_run(
            table_id=table_id,
            table_fqn=table_fqn,
            profiling_schedule_id=profiling_schedule_id,
            execution_engine=engine,
        )

    def create_schema_run(
        self,
        *,
        datasource_id: int | None,
        schema_name: str,
        profiling_schedule_id: int | None = None,
        execution_engine: str | None = None,
    ):
        engine = ensure_spark_execution_engine(execution_engine)
        return create_spark_schema_dq_run(
            datasource_id=datasource_id,
            schema_name=schema_name,
            profiling_schedule_id=profiling_schedule_id,
            execution_engine=engine,
        )

    def create_batch_run(
        self,
        *,
        datasource_id: int | None,
        scope: str,
        schema_name: str | None = None,
        profiling_schedule_id: int | None = None,
        execution_engine: str | None = None,
    ):
        engine = ensure_spark_execution_engine(execution_engine)
        return create_spark_batch_dq_run(
            datasource_id=datasource_id,
            scope=scope,
            schema_name=schema_name,
            profiling_schedule_id=profiling_schedule_id,
            execution_engine=engine,
        )

    def enqueue_profiling(
        self,
        *,
        table_id: int | None,
        table_fqn: str | None,
        columns: list[str],
        sample_fraction: float | None,
        requested_by_user_id: int | None,
        dq_run_id: int | None = None,
        execution_engine: str | None = None,
    ):
        ensure_spark_execution_engine(execution_engine)
        from t2c_data.services.dq_spark import enqueue_profiling_job

        return enqueue_profiling_job(
            table_id=table_id,
            table_fqn=table_fqn,
            columns=columns,
            sample_fraction=sample_fraction,
            requested_by_user_id=requested_by_user_id,
            dq_run_id=dq_run_id,
        )

    def enqueue_rules(
        self,
        *,
        table_id: int | None,
        table_fqn: str | None,
        rule_ids: list[int],
        requested_by_user_id: int | None,
        dq_run_id: int | None = None,
        execution_engine: str | None = None,
    ):
        ensure_spark_execution_engine(execution_engine)
        from t2c_data.services.dq_spark import enqueue_rules_job

        return enqueue_rules_job(
            table_id=table_id,
            table_fqn=table_fqn,
            rule_ids=rule_ids,
            requested_by_user_id=requested_by_user_id,
            dq_run_id=dq_run_id,
        )

    def enqueue_schema_profiling(
        self,
        *,
        parent_run_id: int,
        table_targets: list[dict],
        requested_by_user_id: int | None,
        concurrency: int,
        sample_fraction: float | None = None,
        columns: list[str] | None = None,
        execution_engine: str | None = None,
    ) -> None:
        ensure_spark_execution_engine(execution_engine)
        from t2c_data.services.dq_spark import enqueue_schema_profiling_run

        enqueue_schema_profiling_run(
            parent_run_id=parent_run_id,
            table_targets=table_targets,
            requested_by_user_id=requested_by_user_id,
            concurrency=concurrency,
            sample_fraction=sample_fraction,
            columns=columns,
        )
