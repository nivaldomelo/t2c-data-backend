from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from pymongo import MongoClient
from pymongo.errors import ConfigurationError, InvalidURI, OperationFailure, PyMongoError, ServerSelectionTimeoutError

from t2c_data.connectors.base import ConnectorError
from t2c_data.features.scanner.types import ScanPayload, ScannedColumn, ScannedTable

_DEFAULT_SCHEMA_NAME = "default"
_MAX_SAMPLE_DOCUMENTS = 25


@dataclass
class _FieldProfile:
    types: set[str]
    presence_count: int = 0
    saw_null: bool = False


def _mongo_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, Decimal):
        return "decimal"
    if isinstance(value, str):
        return "string"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, list):
        if not value:
            return "array"
        nested = sorted({_mongo_type_name(item) for item in value[:5]})
        return f"array<{ '|'.join(nested) }>"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__.lower()


def _collect_profiles(document: dict[str, Any], prefix: str, profiles: dict[str, _FieldProfile]) -> None:
    for key, value in document.items():
        path = f"{prefix}.{key}" if prefix else key
        profile = profiles.setdefault(path, _FieldProfile(types=set()))
        profile.presence_count += 1
        type_name = _mongo_type_name(value)
        if type_name == "null":
            profile.saw_null = True
        else:
            profile.types.add(type_name)
        if isinstance(value, dict):
            _collect_profiles(value, path, profiles)


def _build_columns(sample_documents: Iterable[dict[str, Any]]) -> list[ScannedColumn]:
    documents = list(sample_documents)
    if not documents:
        return []

    profiles: dict[str, _FieldProfile] = {}
    for document in documents:
        if isinstance(document, dict):
            _collect_profiles(document, "", profiles)

    columns: list[ScannedColumn] = []
    for ordinal, field_name in enumerate(sorted(profiles.keys()), start=1):
        profile = profiles[field_name]
        data_type = " | ".join(sorted(profile.types)) if profile.types else "unknown"
        if profile.saw_null and "null" not in data_type:
            data_type = f"{data_type} | null" if data_type else "null"
        columns.append(
            ScannedColumn(
                name=field_name,
                data_type=data_type or "unknown",
                is_primary_key=field_name == "_id",
                is_nullable=profile.saw_null or profile.presence_count < len(documents),
                ordinal_position=ordinal,
                comment=None,
            )
        )
    return columns


def _sanitize_mongo_error(exc: Exception) -> ConnectorError:
    detail = str(exc).strip() or exc.__class__.__name__
    lowered = detail.lower()

    if isinstance(exc, (InvalidURI, ConfigurationError)):
        return ConnectorError("URI MongoDB inválida.", detail=detail[:300], code="invalid_uri")
    if isinstance(exc, ServerSelectionTimeoutError):
        if "getaddrinfo" in lowered or "name or service not known" in lowered or "nodename nor servname provided" in lowered:
            return ConnectorError("Host MongoDB não encontrado.", detail=detail[:300], code="invalid_host")
        return ConnectorError("Tempo limite excedido ao conectar no MongoDB.", detail=detail[:300], code="timeout")
    if isinstance(exc, OperationFailure):
        if "not authorized" in lowered or "unauthorized" in lowered:
            return ConnectorError("Sem permissão para listar collections no MongoDB.", detail=detail[:300], code="permission_denied")
        if "authentication failed" in lowered or "auth failed" in lowered:
            return ConnectorError("Credenciais MongoDB inválidas.", detail=detail[:300], code="invalid_credentials")
        return ConnectorError("Falha de operação no MongoDB.", detail=detail[:300], code="operation_failed")
    if isinstance(exc, PyMongoError):
        return ConnectorError("Falha ao conectar ou consultar o MongoDB.", detail=detail[:300], code="mongo_error")
    return ConnectorError("Falha ao executar o scan no MongoDB.", detail=detail[:300], code="scan_failed")


def scan_mongodb(
    *,
    uri: str,
    database_name: str,
    include_collections: list[str] | None = None,
    exclude_collections: list[str] | None = None,
) -> ScanPayload:
    include_collections = include_collections or []
    exclude_collections = exclude_collections or []
    client: MongoClient | None = None

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        database = client[database_name]
        collection_names = sorted(database.list_collection_names())

        if include_collections:
            allowed = set(include_collections)
            collection_names = [name for name in collection_names if name in allowed]
        if exclude_collections:
            blocked = set(exclude_collections)
            collection_names = [name for name in collection_names if name not in blocked]

        tables: list[ScannedTable] = []
        for collection_name in collection_names:
            collection = database[collection_name]
            try:
                document_count = int(collection.estimated_document_count())
            except Exception:
                document_count = 0
            sample_documents = list(collection.find({}, limit=_MAX_SAMPLE_DOCUMENTS))
            columns = _build_columns(sample_documents)
            comment = f"MongoDB collection with {document_count} documents"
            if document_count == 0:
                comment = "MongoDB collection is empty"
            tables.append(
                ScannedTable(
                    schema_name=_DEFAULT_SCHEMA_NAME,
                    table_name=collection_name,
                    table_type="collection",
                    comment=comment,
                    columns=columns,
                )
            )

        return ScanPayload(database_name=database_name, tables=tables)
    except Exception as exc:  # noqa: BLE001
        raise _sanitize_mongo_error(exc) from exc
    finally:
        try:
            if client is not None:
                client.close()
        except Exception:
            pass
