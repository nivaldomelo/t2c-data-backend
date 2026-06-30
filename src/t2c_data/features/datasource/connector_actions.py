from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from t2c_data.features.datasource.contracts import DefaultDatasourceConnectorGateway, DatasourceConnectorGateway
from t2c_data.models.catalog import DataSource


def test_datasource_connection(
    *,
    payload,
    normalize_connection,
    normalize_secrets,
    run_connection_test,
    connector_gateway: DatasourceConnectorGateway | None = None,
):
    connection = normalize_connection(payload.connection)
    secrets = normalize_secrets(payload.secrets)
    return run_connection_test(payload.db_type, connection, secrets, connector_gateway or DefaultDatasourceConnectorGateway())


test_datasource_connection.__test__ = False


def retest_saved_datasource_connection(
    *,
    db: Session,
    datasource_id: int,
    resolved_connection,
    run_connection_test,
    connector_gateway: DatasourceConnectorGateway | None = None,
):
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")
    result = run_connection_test(
        datasource.db_type,
        resolved_connection(datasource),
        datasource.secret_values,
        connector_gateway or DefaultDatasourceConnectorGateway(),
    )
    result_success = getattr(result, "success", None)
    if result_success is None and isinstance(result, dict):
        result_success = bool(result.get("success"))
    result_schemas = getattr(result, "schemas", None)
    if result_schemas is None and isinstance(result, dict):
        result_schemas = result.get("schemas")
    if result_success and result_schemas is not None:
        datasource.detected_schemas = [schema for schema in result_schemas if str(schema).strip()] or None
        db.add(datasource)
        db.commit()
        db.refresh(datasource)
    return result


def list_datasource_schemas_via_connector(
    *,
    db: Session,
    datasource_id: int,
    resolved_connection,
    capabilities_out,
    sanitize_error_message,
    connector_gateway: DatasourceConnectorGateway | None = None,
):
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")

    gateway = connector_gateway or DefaultDatasourceConnectorGateway()
    try:
        schemas = gateway.list_schemas(
            engine=datasource.db_type,
            connection=resolved_connection(datasource),
            secrets=datasource.secret_values,
        )
    except Exception as exc:  # noqa: BLE001
        message, detail, code = sanitize_error_message(exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": message, "code": code, "detail": detail},
        ) from exc
    return {
        "engine": datasource.db_type,
        "schemas": schemas,
        "capabilities": capabilities_out(datasource.db_type),
    }


def list_datasource_tables_via_connector(
    *,
    db: Session,
    datasource_id: int,
    schema: str | None,
    resolved_connection,
    capabilities_out,
    sanitize_error_message,
    connector_gateway: DatasourceConnectorGateway | None = None,
):
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")

    gateway = connector_gateway or DefaultDatasourceConnectorGateway()
    try:
        tables = gateway.list_tables(
            engine=datasource.db_type,
            connection=resolved_connection(datasource),
            secrets=datasource.secret_values,
            schema=schema,
        )
    except Exception as exc:  # noqa: BLE001
        message, detail, code = sanitize_error_message(exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": message, "code": code, "detail": detail},
        ) from exc
    return {
        "engine": datasource.db_type,
        "schema": schema,
        "tables": tables,
        "capabilities": capabilities_out(datasource.db_type),
    }


__all__ = [
    "list_datasource_schemas_via_connector",
    "list_datasource_tables_via_connector",
    "retest_saved_datasource_connection",
    "test_datasource_connection",
]
