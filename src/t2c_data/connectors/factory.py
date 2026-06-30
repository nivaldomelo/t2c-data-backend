from __future__ import annotations

from typing import Any

from t2c_data.connectors.base import BaseConnector, UnsupportedConnectorError
from t2c_data.connectors.registry import CONNECTOR_DEFINITIONS


def create_connector(engine: str, connection: dict[str, Any] | None = None, secrets: dict[str, str] | None = None) -> BaseConnector:
    definition = CONNECTOR_DEFINITIONS.get((engine or "").strip().lower())
    if not definition:
        raise UnsupportedConnectorError(engine)
    return definition.connector_cls(connection=connection or {}, secrets=secrets or {})
