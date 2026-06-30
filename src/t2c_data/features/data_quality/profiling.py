from __future__ import annotations

from datetime import datetime, timezone

import psycopg
from psycopg import sql
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from t2c_data.features.data_quality.guardrails import (
    apply_postgres_read_only_guardrails,
    sanitize_execution_error,
)
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQColumnMetric, DQRun, DQTableMetric
from t2c_data.features.data_quality.observability import build_profile_metrics_json
from t2c_data.services.data_quality import spark_only_execution_message


def _connection_url(datasource: DataSource) -> URL:
    if datasource.db_type != "postgres":
        raise ValueError("DQ profiling supports only postgres in MVP")
    return URL.create(
        "postgresql+psycopg",
        username=datasource.username,
        password=datasource.password,
        host=datasource.host,
        port=datasource.port,
        database=datasource.database,
    )


def _is_min_max_type(data_type: str) -> bool:
    lower = data_type.lower()
    return any(
        token in lower
        for token in (
            "int",
            "numeric",
            "decimal",
            "double",
            "real",
            "date",
            "time",
            "timestamp",
        )
    )


def profile_table(
    session: Session,
    table: TableEntity,
    *,
    execution_engine: str = "python",
    profiling_schedule_id: int | None = None,
    dq_run: DQRun | None = None,
) -> DQRun:
    raise RuntimeError(spark_only_execution_message())


__all__ = ["profile_table"]
