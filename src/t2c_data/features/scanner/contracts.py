from __future__ import annotations

from typing import Any, Protocol
from urllib.parse import quote_plus

from t2c_data.connectors.base import ConnectorError
from t2c_data.features.scanner.mongodb import scan_mongodb
from t2c_data.models.catalog import DataSource
from t2c_data.features.scanner.postgres import scan_postgres
from t2c_data.features.scanner.types import ScanPayload
from t2c_data.core.config import settings


def _resolved_connection(datasource: DataSource) -> dict[str, Any]:
    connection = dict(datasource.connection_config or {})
    if datasource.host and "host" not in connection:
        connection["host"] = datasource.host
    if datasource.port and "port" not in connection:
        connection["port"] = datasource.port
    if datasource.database and "database" not in connection:
        connection["database"] = datasource.database
    if datasource.username and "username" not in connection:
        connection["username"] = datasource.username
    return connection


def _normalize_name_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def _normalize_int(value: Any, default: int) -> int:
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return default


class MetadataScanGateway(Protocol):
    def scan(self, datasource: DataSource) -> ScanPayload: ...


class DefaultMetadataScanGateway:
    def scan(self, datasource: DataSource) -> ScanPayload:
        if datasource.db_type == "postgres":
            connection = _resolved_connection(datasource)
            connect_timeout_seconds = _normalize_int(
                connection.get("scan_connect_timeout_seconds"),
                settings.datasource_scan_connect_timeout_seconds,
            )
            statement_timeout_ms = _normalize_int(
                connection.get("scan_statement_timeout_ms"),
                settings.datasource_scan_statement_timeout_ms,
            )
            connection_uri = datasource.get_secret("connection_uri")
            if not connection_uri:
                password = datasource.get_secret("password") or datasource.password
                if not password:
                    raise ConnectorError("Senha não configurada para o datasource.", code="invalid_config")
                connection_uri = (
                    "postgresql://"
                    f"{quote_plus(str(connection['username']))}:{quote_plus(password)}"
                    f"@{connection['host']}:{connection['port']}/{quote_plus(str(connection['database']))}"
                )
            return scan_postgres(
                connection_uri=connection_uri,
                include_schemas=datasource.include_schemas or [],
                exclude_schemas=datasource.exclude_schemas or [],
                connect_timeout_seconds=connect_timeout_seconds,
                statement_timeout_ms=statement_timeout_ms,
            )

        if datasource.db_type == "mongodb":
            connection = _resolved_connection(datasource)
            uri = datasource.get_secret("uri")
            database_name = str(connection.get("database") or datasource.database or "").strip()
            include_collections = _normalize_name_list(connection.get("include_collections"))
            exclude_collections = _normalize_name_list(connection.get("exclude_collections"))
            if not uri:
                raise ConnectorError("URI MongoDB não configurada para o datasource.", code="invalid_config")
            if not database_name:
                raise ConnectorError("Database MongoDB não configurado para o datasource.", code="invalid_config")
            return scan_mongodb(
                uri=uri,
                database_name=database_name,
                include_collections=include_collections,
                exclude_collections=exclude_collections,
            )

        raise ConnectorError(f"Scan para {datasource.db_type} ainda não está implementado no MVP.", code="not_implemented")
