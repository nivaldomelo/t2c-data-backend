from __future__ import annotations

from t2c_data.core.config import settings
from t2c_data.features.data_quality.engines import normalize_execution_engine

CONFIGURED_EXECUTION_ENGINE = "spark"
DEFAULT_DQ_EXECUTION_MODE = "spark_only"
_LOCAL_DISABLED_MODES = {"spark_only", "local_disabled"}
SPARK_ONLY_EXECUTION_MESSAGE = (
    "As regras de Data Quality sao executadas exclusivamente no cluster Spark. "
    "O backend apenas orquestra e persiste resultados."
)
LOCAL_EXECUTION_DISABLED_MESSAGE = "Execucao local de Data Quality foi removida. Use execucao Spark-only."


def configured_execution_engine(_value: str | None = None) -> str:
    return CONFIGURED_EXECUTION_ENGINE


def configured_execution_mode(_value: str | None = None) -> str:
    normalized = (_value or settings.dq_execution_mode or DEFAULT_DQ_EXECUTION_MODE).strip().lower()
    if normalized in _LOCAL_DISABLED_MODES:
        return normalized
    return DEFAULT_DQ_EXECUTION_MODE


def dq_local_execution_disabled() -> bool:
    return configured_execution_mode() in _LOCAL_DISABLED_MODES


def spark_only_execution_message() -> str:
    return SPARK_ONLY_EXECUTION_MESSAGE


def local_execution_disabled_message() -> str:
    return LOCAL_EXECUTION_DISABLED_MESSAGE


def ensure_spark_execution_engine(value: str | None = None) -> str:
    normalized = normalize_execution_engine(value)
    if normalized != "spark":
        return CONFIGURED_EXECUTION_ENGINE
    return normalized


__all__ = [
    "CONFIGURED_EXECUTION_ENGINE",
    "DEFAULT_DQ_EXECUTION_MODE",
    "configured_execution_engine",
    "configured_execution_mode",
    "dq_local_execution_disabled",
    "ensure_spark_execution_engine",
    "local_execution_disabled_message",
    "spark_only_execution_message",
]
