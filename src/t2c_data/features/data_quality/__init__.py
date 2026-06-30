from __future__ import annotations

from importlib import import_module
from typing import Any, Callable

__all__ = ["execute_schema_profiling_orchestration"]


def execute_schema_profiling_orchestration(*args: Any, **kwargs: Any) -> Any:
    """Lazy wrapper that keeps the package import chain lightweight."""
    module = import_module("t2c_data.features.data_quality.spark_schema")
    return module.execute_schema_profiling_orchestration(*args, **kwargs)


def __getattr__(name: str) -> Any:
    if name == "execute_schema_profiling_orchestration":
        return execute_schema_profiling_orchestration
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
