from __future__ import annotations

import logging
from time import perf_counter
from typing import Any

from t2c_data.connectors.base import ConnectorError
from t2c_data.connectors.registry import CONNECTOR_DEFINITIONS, CONNECTOR_METADATA
from t2c_data.features.datasource.contracts import DefaultDatasourceConnectorGateway
from t2c_data.models.catalog import DataSource
from t2c_data.schemas.datasource import (
    ConnectorCapabilitiesOut,
    ConnectorDefinitionOut,
    DataSourceConnectionTestOut,
    DataSourceDetail,
    DataSourceOut,
)

logger = logging.getLogger(__name__)
CONNECTOR_GATEWAY = DefaultDatasourceConnectorGateway()


def normalize_schema_list(values: list[str] | None) -> list[str]:
    if not values:
        return []
    normalized: list[str] = []
    for item in values:
        clean = item.strip()
        if clean and clean not in normalized:
            normalized.append(clean)
    return normalized


def normalize_connection(values: dict[str, Any] | None) -> dict[str, Any]:
    if not values:
        return {}
    normalized: dict[str, Any] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                continue
            normalized[key] = stripped
        else:
            normalized[key] = value
    return normalized


def normalize_secrets(values: dict[str, Any] | None) -> dict[str, str]:
    if not values:
        return {}
    normalized: dict[str, str] = {}
    for key, value in values.items():
        if value is None:
            continue
        if hasattr(value, "get_secret_value"):
            plain = value.get_secret_value()
        else:
            plain = str(value)
        if plain.strip():
            normalized[key] = plain.strip()
    return normalized


def legacy_connection(entity: DataSource) -> dict[str, Any]:
    return {
        "host": entity.host,
        "port": entity.port,
        "database": entity.database,
        "username": entity.username,
    }


def resolved_connection(entity: DataSource) -> dict[str, Any]:
    connection = normalize_connection(entity.connection_config or {})
    legacy = legacy_connection(entity)
    for key, value in legacy.items():
        if value not in (None, "") and key not in connection:
            connection[key] = value
    return connection


def capabilities_out(engine: str) -> ConnectorCapabilitiesOut:
    definition = CONNECTOR_DEFINITIONS[engine]
    return ConnectorCapabilitiesOut(**definition.capabilities)


def connector_definitions_out() -> list[ConnectorDefinitionOut]:
    ordered = sorted(CONNECTOR_METADATA.values(), key=lambda item: item["order"])
    return [ConnectorDefinitionOut(**item) for item in ordered]


def mask_host(host: str | None) -> str | None:
    if not host:
        return host
    if len(host) <= 6:
        return "***"
    return f"{host[:3]}***{host[-2:]}"


def sanitize_error_message(exc: Exception) -> tuple[str, str, str]:
    if isinstance(exc, ConnectorError):
        return exc.message, exc.detail, exc.code
    detail = str(exc).strip() or exc.__class__.__name__
    lowered = detail.lower()
    if "password authentication failed" in lowered or "access denied" in lowered or "invalid username/password" in lowered:
        return "Falha de autenticação.", detail[:300], "invalid_credentials"
    if "does not exist" in lowered and "database" in lowered:
        return "Banco de dados não encontrado.", detail[:300], "database_not_found"
    if "timeout" in lowered or "timed out" in lowered:
        return "Tempo limite excedido ao conectar.", detail[:300], "timeout"
    if "connection refused" in lowered:
        return "Conexão recusada pelo servidor.", detail[:300], "connection_refused"
    if "ssl" in lowered and "required" in lowered:
        return "O servidor exige SSL/TLS para conexão.", detail[:300], "ssl_required"
    if "could not translate host name" in lowered or "name or service not known" in lowered or "getaddrinfo failed" in lowered:
        return "Host não encontrado.", detail[:300], "invalid_host"
    return "Falha ao conectar na fonte de dados.", detail[:300], "connection_failed"


def apply_connection_summary(entity: DataSource, engine: str, connection: dict[str, Any], secrets: dict[str, str]) -> None:
    summary = CONNECTOR_GATEWAY.summarize_connection(engine=engine, connection=connection, secrets=secrets)
    entity.db_type = engine
    entity.host = str(summary.get("host") or "custom")
    entity.port = int(summary.get("port") or 0)
    entity.database = str(summary.get("database") or "default")
    entity.username = str(summary.get("username") or "system")
    entity.connection_config = connection or None


def datasource_out(entity: DataSource) -> DataSourceOut:
    return DataSourceOut(
        id=entity.id,
        name=entity.name,
        db_type=entity.db_type,
        host=entity.host,
        port=entity.port,
        database=entity.database,
        username=entity.username,
        detected_schemas=entity.detected_schemas,
        include_schemas=entity.include_schemas,
        exclude_schemas=entity.exclude_schemas,
        is_active=entity.is_active,
        created_at=entity.created_at,
        updated_at=entity.updated_at,
        capabilities=capabilities_out(entity.db_type),
    )


def datasource_detail(entity: DataSource) -> DataSourceDetail:
    return DataSourceDetail(
        id=entity.id,
        name=entity.name,
        db_type=entity.db_type,
        host=entity.host,
        port=entity.port,
        database=entity.database,
        username=entity.username,
        connection=resolved_connection(entity),
        configured_secrets=sorted(entity.secret_values.keys()),
        detected_schemas=entity.detected_schemas,
        include_schemas=entity.include_schemas,
        exclude_schemas=entity.exclude_schemas,
        is_active=entity.is_active,
        created_at=entity.created_at,
        updated_at=entity.updated_at,
        capabilities=capabilities_out(entity.db_type),
    )


def run_connection_test(
    engine: str,
    connection: dict[str, Any],
    secrets: dict[str, str],
    connector_gateway=CONNECTOR_GATEWAY,
) -> DataSourceConnectionTestOut:
    capabilities = capabilities_out(engine)
    summary = connector_gateway.summarize_connection(engine=engine, connection=connection, secrets=secrets)
    started_at = perf_counter()
    try:
        details = connector_gateway.test_connection(engine=engine, connection=connection, secrets=secrets)
        capabilities_dict = CONNECTOR_DEFINITIONS[engine].capabilities
        schemas: list[str] | None = None
        warning: str | None = None
        if capabilities_dict.get("list_schemas", False):
            try:
                schemas = connector_gateway.list_schemas(engine=engine, connection=connection, secrets=secrets)
            except Exception as schema_exc:  # noqa: BLE001
                schema_message, schema_detail, schema_code = sanitize_error_message(schema_exc)
                warning = (
                    "A conexão foi validada, mas não foi possível listar os schemas. "
                    "Verifique permissões no PostgreSQL ou informe um schema padrão válido."
                )
                details = {
                    **details,
                    "schema_listing": {
                        "success": False,
                        "message": schema_message,
                        "detail": schema_detail,
                        "code": schema_code,
                    },
                }
                schemas = []
            if schemas is not None and len(schemas) == 0:
                if engine == "postgres":
                    default_schema = str(connection.get("default_schema") or "public").strip() or "public"
                    warning = (
                        f"Conexão realizada, mas nenhum schema foi encontrado para este usuário. "
                        f"Se fizer sentido para o banco, use o schema padrão informado: {default_schema}."
                    )
                else:
                    warning = "Conexão realizada, mas nenhum schema foi encontrado para este usuário."
        latency_ms = int((perf_counter() - started_at) * 1000)
        logger.info(
            "datasource.test.success engine=%s host=%s database=%s latency_ms=%s",
            engine,
            mask_host(str(summary.get("host") or "")),
            summary.get("database"),
            latency_ms,
        )
        return DataSourceConnectionTestOut(
            success=True,
            message=warning or "Conexão validada com sucesso.",
            engine=engine,
            host=str(summary.get("host") or "custom"),
            port=int(summary.get("port") or 0),
            database=str(summary.get("database") or "default"),
            default_schema=str(connection.get("default_schema") or "public") if engine == "postgres" else None,
            latency_ms=latency_ms,
            details=details,
            capabilities=capabilities,
            schemas=schemas,
            warning=warning,
        )
    except Exception as exc:  # noqa: BLE001
        latency_ms = int((perf_counter() - started_at) * 1000)
        message, detail, code = sanitize_error_message(exc)
        logger.warning(
            "datasource.test.failed engine=%s host=%s database=%s latency_ms=%s code=%s detail=%s",
            engine,
            mask_host(str(summary.get("host") or "")),
            summary.get("database"),
            latency_ms,
            code,
            detail,
        )
        return DataSourceConnectionTestOut(
            success=False,
            message=message,
            engine=engine,
            host=str(summary.get("host") or "custom"),
            port=int(summary.get("port") or 0),
            database=str(summary.get("database") or "default"),
            default_schema=str(connection.get("default_schema") or "public") if engine == "postgres" else None,
            latency_ms=latency_ms,
            details={"code": code, "detail": detail},
            capabilities=capabilities,
            schemas=None,
            warning=None,
        )
