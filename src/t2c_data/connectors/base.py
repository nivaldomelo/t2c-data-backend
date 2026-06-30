from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ConnectorCapabilities:
    test_connection: bool = True
    list_schemas: bool = False
    list_tables: bool = False
    get_database_info: bool = False

    def as_dict(self) -> dict[str, bool]:
        return {
            "test_connection": self.test_connection,
            "list_schemas": self.list_schemas,
            "list_tables": self.list_tables,
            "get_database_info": self.get_database_info,
        }


class ConnectorError(Exception):
    def __init__(self, message: str, *, detail: str | None = None, code: str = "connector_error") -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or message
        self.code = code


class UnsupportedConnectorError(ConnectorError):
    def __init__(self, engine: str) -> None:
        super().__init__(
            f"O conector '{engine}' ainda não está suportado.",
            detail=f"Connector not supported: {engine}",
            code="unsupported_connector",
        )


class MissingDriverError(ConnectorError):
    def __init__(self, engine: str, package_name: str) -> None:
        super().__init__(
            f"O driver do conector '{engine}' não está instalado no servidor.",
            detail=f"Missing dependency '{package_name}' for connector '{engine}'",
            code="missing_driver",
        )


class BaseConnector(ABC):
    engine: str = "unknown"
    capabilities = ConnectorCapabilities()

    def __init__(self, connection: dict[str, Any] | None = None, secrets: dict[str, str] | None = None) -> None:
        self.connection = connection or {}
        self.secrets = secrets or {}

    @classmethod
    def summarize_connection(cls, connection: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
        return {
            "host": str(connection.get("host") or connection.get("account") or connection.get("server_hostname") or connection.get("file_path") or connection.get("project_id") or "custom"),
            "port": int(connection.get("port") or 0),
            "database": str(connection.get("database") or connection.get("project_id") or connection.get("service_name") or connection.get("catalog") or connection.get("file_path") or "default"),
            "username": str(connection.get("username") or connection.get("user") or ("token" if secrets.get("access_token") else "system")),
        }

    def close(self) -> None:
        return None

    @abstractmethod
    def test_connection(self) -> dict[str, Any]:
        raise NotImplementedError

    def get_database_info(self) -> dict[str, Any]:
        raise ConnectorError("Este conector ainda não implementa leitura de metadados básicos.", code="capability_not_available")

    def list_schemas(self) -> list[str]:
        raise ConnectorError("Este conector ainda não implementa listagem de schemas.", code="capability_not_available")

    def list_tables(self, schema: str | None = None) -> list[str]:
        raise ConnectorError("Este conector ainda não implementa listagem de tabelas.", code="capability_not_available")
