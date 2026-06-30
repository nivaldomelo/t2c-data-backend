from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.datasource.contracts import DefaultDatasourceConnectorGateway, DatasourceConnectorGateway
from t2c_data.features.datasource.persistence import hard_delete_datasource
from t2c_data.models.catalog import DataSource
from t2c_data.services.audit import add_audit_log, serialize_model, write_audit_log_sync


def create_datasource_with_audit(
    *,
    db: Session,
    payload,
    normalize_connection,
    normalize_schema_list,
    normalize_secrets,
    to_out,
    audit_kwargs: dict,
    connector_gateway: DatasourceConnectorGateway | None = None,
):
    existing = db.scalar(select(DataSource).where(DataSource.name == payload.name))
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Datasource name already exists")

    connection = normalize_connection(payload.connection)
    secrets = normalize_secrets(payload.secrets)

    datasource = DataSource(
        name=payload.name,
        db_type=payload.db_type,
        host="pending",
        port=0,
        database="pending",
        username="pending",
        detected_schemas=normalize_schema_list(payload.detected_schemas) or None,
        include_schemas=normalize_schema_list(payload.include_schemas) or None,
        exclude_schemas=normalize_schema_list(payload.exclude_schemas) or None,
        is_active=payload.is_active,
    )
    gateway = connector_gateway or DefaultDatasourceConnectorGateway()
    summary = gateway.summarize_connection(engine=payload.db_type, connection=connection, secrets=secrets)
    datasource.db_type = payload.db_type
    datasource.host = str(summary.get("host") or "custom")
    datasource.port = int(summary.get("port") or 0)
    datasource.database = str(summary.get("database") or "default")
    datasource.username = str(summary.get("username") or "system")
    datasource.connection_config = connection or None
    datasource.set_secret_values(secrets)
    db.add(datasource)
    db.commit()
    db.refresh(datasource)
    write_audit_log_sync(
        db,
        action="datasource.create",
        entity_type="datasource",
        entity_id=datasource.id,
        after=serialize_model(datasource),
        **audit_kwargs,
    )
    db.commit()
    return to_out(datasource)


def list_datasources_out(*, db: Session, to_out) -> list:
    return [to_out(d) for d in db.scalars(select(DataSource).order_by(DataSource.id.desc())).all()]


def get_datasource_detail(*, db: Session, datasource_id: int, to_detail):
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")
    return to_detail(datasource)


def update_datasource_with_audit(
    *,
    db: Session,
    datasource_id: int,
    payload,
    normalize_connection,
    normalize_schema_list,
    normalize_secrets,
    resolved_connection,
    to_out,
    audit_kwargs: dict,
    connector_gateway: DatasourceConnectorGateway | None = None,
):
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")
    before = serialize_model(datasource)

    updates = payload.model_dump(exclude_unset=True, exclude_none=False)

    if "name" in updates and updates["name"] and updates["name"] != datasource.name:
        existing = db.scalar(select(DataSource).where(DataSource.name == updates["name"], DataSource.id != datasource_id))
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Datasource name already exists")

    next_engine = updates.get("db_type") or datasource.db_type
    next_connection = resolved_connection(datasource)
    if "connection" in updates and updates["connection"] is not None:
        next_connection = normalize_connection(updates["connection"])

    next_secrets = datasource.secret_values
    if "secrets" in updates and updates["secrets"] is not None:
        incoming_secrets = normalize_secrets(updates["secrets"])
        next_secrets = {**next_secrets, **incoming_secrets}

    if "include_schemas" in updates:
        datasource.include_schemas = normalize_schema_list(updates["include_schemas"]) or None
    if "exclude_schemas" in updates:
        datasource.exclude_schemas = normalize_schema_list(updates["exclude_schemas"]) or None
    if "detected_schemas" in updates:
        datasource.detected_schemas = normalize_schema_list(updates["detected_schemas"]) or None
    if "is_active" in updates and updates["is_active"] is not None:
        datasource.is_active = bool(updates["is_active"])
    if "name" in updates and updates["name"]:
        datasource.name = updates["name"]

    gateway = connector_gateway or DefaultDatasourceConnectorGateway()
    summary = gateway.summarize_connection(engine=next_engine, connection=next_connection, secrets=next_secrets)
    datasource.db_type = next_engine
    datasource.host = str(summary.get("host") or "custom")
    datasource.port = int(summary.get("port") or 0)
    datasource.database = str(summary.get("database") or "default")
    datasource.username = str(summary.get("username") or "system")
    datasource.connection_config = next_connection or None
    datasource.set_secret_values(next_secrets)

    db.add(datasource)
    db.commit()
    db.refresh(datasource)
    write_audit_log_sync(
        db,
        action="datasource.update",
        entity_type="datasource",
        entity_id=datasource.id,
        before=before,
        after=serialize_model(datasource),
        **audit_kwargs,
    )
    db.commit()
    return to_out(datasource)


def delete_datasource_with_audit(*, db: Session, datasource_id: int, user, audit_kwargs: dict) -> Response:
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")

    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="datasource.delete",
        entity_type="datasource",
        entity_id=datasource_id,
        message="Datasource hard deleted",
        changes={"name": datasource.name, "db_type": datasource.db_type},
    )
    db.flush()
    write_audit_log_sync(
        db,
        action="datasource.delete.request",
        entity_type="datasource",
        entity_id=datasource_id,
        before=serialize_model(datasource),
        **audit_kwargs,
    )
    db.flush()

    hard_delete_datasource(session=db, datasource_id=datasource_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


__all__ = [
    "create_datasource_with_audit",
    "delete_datasource_with_audit",
    "get_datasource_detail",
    "list_datasources_out",
    "update_datasource_with_audit",
]
