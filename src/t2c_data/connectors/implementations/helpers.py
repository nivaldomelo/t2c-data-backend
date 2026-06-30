from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from t2c_data.connectors.base import ConnectorError


def require_value(connection: dict[str, Any], key: str, label: str | None = None) -> str:
    value = connection.get(key)
    if value is None or not str(value).strip():
        raise ConnectorError(
            f"Campo obrigatório ausente: {label or key}",
            detail=f"Missing required field: {key}",
            code="invalid_config",
        )
    return str(value).strip()


def require_secret(secrets: dict[str, str], key: str, label: str | None = None) -> str:
    value = secrets.get(key)
    if value is None or not str(value).strip():
        raise ConnectorError(
            f"Segredo obrigatório ausente: {label or key}",
            detail=f"Missing required secret: {key}",
            code="invalid_config",
        )
    return str(value).strip()


def optional_int(connection: dict[str, Any], key: str, default: int) -> int:
    raw = connection.get(key)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ConnectorError(
            f"Campo numérico inválido: {key}",
            detail=str(exc),
            code="invalid_config",
        ) from exc


def parse_mongodb_uri(uri: str) -> dict[str, Any]:
    parsed = urlparse(uri)
    return {
        "host": parsed.hostname or "mongodb",
        "port": int(parsed.port or 27017),
        "database": parsed.path.strip("/") or "admin",
        "username": parsed.username or "mongodb",
    }
