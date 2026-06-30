from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace
from typing import Any
from uuid import UUID


def make_json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: make_json_safe(val) for key, val in asdict(value).items()}
    if hasattr(value, "model_dump"):
        return make_json_safe(value.model_dump(mode="json"))
    if isinstance(value, SimpleNamespace):
        return make_json_safe(vars(value))
    if isinstance(value, dict):
        return {str(key): make_json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(item) for item in value]
    return str(value)


def to_jsonable(value: Any) -> Any:
    return make_json_safe(value)


__all__ = ["make_json_safe", "to_jsonable"]
