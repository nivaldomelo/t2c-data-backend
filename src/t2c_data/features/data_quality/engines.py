from __future__ import annotations

from typing import Literal

ExecutionEngine = Literal["python", "spark"]

DEFAULT_DQ_EXECUTION_ENGINE: ExecutionEngine = "spark"


def normalize_execution_engine(value: str | None) -> ExecutionEngine:
    normalized = (value or DEFAULT_DQ_EXECUTION_ENGINE).strip().lower()
    if normalized in {"spark", "spark_cluster"}:
        return "spark"
    return "python"


def execution_engine_label(value: str | None) -> str:
    engine = normalize_execution_engine(value)
    return "Spark cluster" if engine == "spark" else "Execucao legada anterior ao Spark"


def is_spark_engine(value: str | None) -> bool:
    return normalize_execution_engine(value) == "spark"


def is_python_engine(value: str | None) -> bool:
    return normalize_execution_engine(value) == "python"
