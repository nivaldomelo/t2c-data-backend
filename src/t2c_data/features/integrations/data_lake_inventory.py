from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from typing import Any
from xml.etree import ElementTree as ET

from fastapi import HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from t2c_data.features.integrations.data_lake import (
    _aws_credentials_for_connection,
    _clear_sensitive_credentials,
    _aws_request,
    _extract_error_code,
    _extract_error_message,
    S3ListObjectsError,
    _request_sts_get_caller_identity,
    get_data_lake_connection_or_404,
)
from t2c_data.features.integrations.data_lake_s3 import is_parquet_key, list_s3_objects_recursive, parse_data_lake_object_key, parse_prefix_list, parse_xml_response
from t2c_data.features.integrations.data_lake_quality import (
    build_data_lake_observation_payload,
    calculate_data_lake_table_quality,
    resolve_data_lake_table_freshness_sla_hours,
)
from t2c_data.features.platform.jobs import enqueue_integration_job
from t2c_data.models.auth import User
from t2c_data.models.platform import DataLakeConnection, DataLakeInventoryScanRun, DataLakeInventoryTable, DataLakeTableObservation
from t2c_data.schemas.integrations import (
    DataLakeCatalogPageOut,
    DataLakeCatalogSummaryOut,
    DataLakeCatalogTableOut,
    DataLakeInventoryPageOut,
    DataLakeInventoryScanOut,
    DataLakeInventoryScanRunOut,
    DataLakeInventorySummaryOut,
    DataLakeInventoryTableOut,
    DataLakeTableFreshnessSlaIn,
)
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import write_audit_log_sync

logger = logging.getLogger(__name__)

IGNORED_FOLDER_NAMES = {
    "_temporary",
    "_temporarydata",
    "_temporaryfiles",
    "_temporary0",
    "_committed",
    "_started",
    "_metadata",
    "_logs",
    "_log",
    "_spark_metadata",
    "_spark_metadata",
    "_success",
    "_SUCCESS",
    "_delta_log",
    "archive",
    "archives",
    "log",
    "logs",
    "tmp",
    "temp",
}
IGNORED_FILE_NAMES = {"_success", "_SUCCESS", ".ds_store"}


@dataclass(slots=True)
class _S3ObjectEntry:
    key: str
    size: int
    last_modified: datetime | None


@dataclass(slots=True)
class _S3PrefixScanResult:
    contents: list[_S3ObjectEntry]
    common_prefixes: list[str]
    is_truncated: bool
    next_token: str | None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _join_prefix(*parts: str | None) -> str:
    cleaned: list[str] = []
    for part in parts:
        if part is None:
            continue
        value = part.strip("/")
        if value:
            cleaned.append(value)
    if not cleaned:
        return ""
    return "/".join(cleaned) + "/"


def _looks_like_partition_segment(segment: str) -> bool:
    lowered = segment.lower()
    if "=" in segment:
        return True
    if len(segment) == 10 and lowered[4] == "-" and lowered[7] == "-" and segment[:4].isdigit() and segment[5:7].isdigit() and segment[8:].isdigit():
        return True
    if len(segment) == 8 and segment.isdigit():
        return True
    if len(segment) == 4 and segment.isdigit():
        return True
    return False


def _is_ignored_name(name: str) -> bool:
    normalized = name.strip().lower()
    if not normalized:
        return True
    if normalized in IGNORED_FOLDER_NAMES or normalized in IGNORED_FILE_NAMES:
        return True
    return normalized.startswith("_") or normalized.startswith(".")


def _parse_s3_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_list_objects_response(body: str) -> _S3PrefixScanResult:
    root = parse_xml_response(body)
    if root is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid S3 response")
    contents: list[_S3ObjectEntry] = []
    common_prefixes: list[str] = []
    for content in root.findall(".//Contents"):
        key = (content.findtext("Key") or "").strip()
        if not key:
            continue
        size_text = (content.findtext("Size") or "0").strip()
        try:
            size = int(size_text)
        except ValueError:
            size = 0
        contents.append(
            _S3ObjectEntry(
                key=key,
                size=size,
                last_modified=_parse_s3_datetime(content.findtext("LastModified")),
            )
        )
    for prefix in root.findall(".//CommonPrefixes"):
        value = (prefix.findtext("Prefix") or "").strip()
        if value:
            common_prefixes.append(value)
    is_truncated = (root.findtext(".//IsTruncated") or "false").strip().lower() == "true"
    next_token = (root.findtext(".//NextContinuationToken") or "").strip() or None
    return _S3PrefixScanResult(contents=contents, common_prefixes=common_prefixes, is_truncated=is_truncated, next_token=next_token)


def _list_objects_v2(
    *,
    bucket: str,
    region: str,
    prefix: str,
    credentials: dict[str, str],
    request_runner=_aws_request,
    delimiter: str = "/",
    continuation_token: str | None = None,
) -> _S3PrefixScanResult:
    query_params: dict[str, Any] = {
        "list-type": "2",
        "delimiter": delimiter,
        "prefix": prefix,
        "max-keys": "1000",
    }
    if continuation_token:
        query_params["continuation-token"] = continuation_token
    response = request_runner(
        method="GET",
        url=f"https://s3.{region}.amazonaws.com/{bucket}",
        region=region,
        service="s3",
        credentials=credentials,
        query_params=query_params,
    )
    if response.status_code != 200:
        code = _extract_error_code(response.body) or "list_objects_failed"
        message = _extract_error_message(response.body) or "ListObjectsV2 failed"
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"{code}: {message}")
    return _parse_list_objects_response(response.body)


def _is_parquet_key(key: str) -> bool:
    return key.lower().endswith(".parquet")


def _scan_table_prefix(
    *,
    bucket: str,
    region: str,
    credentials: dict[str, str],
    layer: str,
    table_prefix: str,
    request_runner=_aws_request,
) -> dict[str, Any]:
    root_prefix = table_prefix.rstrip("/") + "/"
    queue: deque[str] = deque([root_prefix])
    visited_prefixes: set[str] = set()
    file_count = 0
    parquet_count = 0
    non_parquet_count = 0
    total_bytes = 0
    latest_modified: datetime | None = None
    has_partitions = False
    partition_patterns: set[str] = set()
    sample_parquet_files: list[dict[str, Any]] = []

    while queue:
        current_prefix = queue.popleft()
        if current_prefix in visited_prefixes:
            continue
        visited_prefixes.add(current_prefix)
        continuation_token: str | None = None
        while True:
            page = _list_objects_v2(
                bucket=bucket,
                region=region,
                prefix=current_prefix,
                credentials=credentials,
                request_runner=request_runner,
                delimiter="/",
                continuation_token=continuation_token,
            )
            for child_prefix in page.common_prefixes:
                child_name = child_prefix.rstrip("/").split("/")[-1]
                if _is_ignored_name(child_name):
                    continue
                queue.append(child_prefix)
                if child_prefix != root_prefix:
                    has_partitions = True
                    relative = child_prefix[len(root_prefix) :].strip("/")
                    if relative:
                        for segment in relative.split("/"):
                            if _looks_like_partition_segment(segment):
                                if "=" in segment:
                                    partition_patterns.add("key_value")
                                elif len(segment) == 10 and segment.count("-") == 2:
                                    partition_patterns.add("date_path")
                                else:
                                    partition_patterns.add("partitioned")
            for entry in page.contents:
                file_name = entry.key.rstrip("/").split("/")[-1]
                if _is_ignored_name(file_name):
                    continue
                file_count += 1
                if _is_parquet_key(entry.key):
                    parquet_count += 1
                    total_bytes += max(entry.size, 0)
                    if len(sample_parquet_files) < 5:
                        sample_parquet_files.append(
                            {
                                "key": entry.key,
                                "size": max(entry.size, 0),
                                "last_modified": entry.last_modified.isoformat() if entry.last_modified else None,
                            }
                        )
                else:
                    non_parquet_count += 1
                if entry.last_modified and (latest_modified is None or entry.last_modified > latest_modified):
                    latest_modified = entry.last_modified
            if not page.is_truncated:
                break
            continuation_token = page.next_token
            if not continuation_token:
                break

    status_scan = "scanned" if parquet_count > 0 else ("no_parquet" if file_count > 0 or has_partitions else "empty")
    return {
        "layer": layer,
        "table_name": root_prefix.rstrip("/").split("/")[-1],
        "path_base": root_prefix.rstrip("/"),
        "files_count": file_count,
        "parquet_files_count": parquet_count,
        "non_parquet_files_count": non_parquet_count,
        "size_total_bytes": total_bytes,
        "last_modified_at": latest_modified,
        "has_partitions": has_partitions,
        "partition_pattern_detected": ",".join(sorted(partition_patterns)) if partition_patterns else None,
        "status_scan": status_scan,
        "data_last_scan_at": _now(),
        "sample_parquet_files": sample_parquet_files,
        "error_message": None,
    }


def serialize_data_lake_inventory_table(item: DataLakeInventoryTable) -> dict[str, Any]:
    return {
        "id": item.id,
        "connection_id": item.connection_id,
        "layer": item.layer,
        "table_name": item.table_name,
        "path_base": item.path_base,
        "files_count": item.files_count,
        "parquet_files_count": item.parquet_files_count,
        "non_parquet_files_count": item.non_parquet_files_count,
        "size_total_bytes": item.size_total_bytes,
        "last_modified_at": item.last_modified_at,
        "has_partitions": item.has_partitions,
        "partition_pattern_detected": item.partition_pattern_detected,
        "status_scan": item.status_scan,
        "data_last_scan_at": item.data_last_scan_at,
        "freshness_sla_hours_override": item.freshness_sla_hours_override,
        "last_quality_score": item.last_quality_score,
        "last_quality_evaluated_at": item.last_quality_evaluated_at,
        "data_owner_id": item.data_owner_id,
        "domain_name": item.domain_name,
        "description": item.description,
        "classification": item.classification,
        "criticality": item.criticality,
        "is_monitored": bool(item.is_monitored),
        "governance_last_updated_at": item.governance_last_updated_at,
        "catalog_ready": bool(item.data_owner_id or item.domain_name or item.description or item.classification or item.criticality or item.is_monitored),
        "governance_status": (
            "ready"
            if item.data_owner_id and item.domain_name and item.description and item.classification
            else "partial"
            if any((item.data_owner_id, item.domain_name, item.description, item.classification, item.criticality, item.is_monitored))
            else "unlinked"
        ),
        "scan_run_id": item.scan_run_id,
        "error_message": item.error_message,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def serialize_data_lake_catalog_table(connection: DataLakeConnection, item: DataLakeInventoryTable) -> dict[str, Any]:
    payload = serialize_data_lake_inventory_table(item)
    payload.update(
        {
            "connection_name": connection.name,
            "bucket": connection.bucket,
            "region": connection.region,
            "prefix": connection.prefix,
        }
    )
    return payload


def serialize_data_lake_scan_run(item: DataLakeInventoryScanRun) -> dict[str, Any]:
    return {
        "id": item.id,
        "connection_id": item.connection_id,
        "status": item.status,
        "scanned_layers_count": item.scanned_layers_count,
        "discovered_tables_count": item.discovered_tables_count,
        "discovered_parquet_files_count": item.discovered_parquet_files_count,
        "total_bytes": item.total_bytes,
        "trigger_mode": item.trigger_mode,
        "schedule_id": item.schedule_id,
        "error_message": item.error_message,
        "started_at": item.started_at,
        "finished_at": item.finished_at,
        "scanned_by_user_id": item.scanned_by_user_id,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def _latest_scan_run(session: Session, connection_id: int) -> DataLakeInventoryScanRun | None:
    return session.scalar(
        select(DataLakeInventoryScanRun)
        .where(DataLakeInventoryScanRun.connection_id == connection_id)
        .order_by(DataLakeInventoryScanRun.created_at.desc(), DataLakeInventoryScanRun.id.desc())
    )


def _inventory_rows_query(
    session: Session,
    *,
    connection_id: int,
    layer: str | None = None,
    name: str | None = None,
    status: str | None = None,
    has_partitions: bool | None = None,
    freshness_state: str | None = None,
    freshness_days: int = 7,
):
    stmt = select(DataLakeInventoryTable).where(DataLakeInventoryTable.connection_id == connection_id)
    if layer:
        stmt = stmt.where(DataLakeInventoryTable.layer == layer)
    if name:
        pattern = f"%{name.strip()}%"
        stmt = stmt.where(
            DataLakeInventoryTable.table_name.ilike(pattern) | DataLakeInventoryTable.path_base.ilike(pattern)
        )
    if status:
        stmt = stmt.where(DataLakeInventoryTable.status_scan == status)
    if has_partitions is not None:
        stmt = stmt.where(DataLakeInventoryTable.has_partitions.is_(has_partitions))
    return stmt.order_by(
        DataLakeInventoryTable.layer.asc(),
        DataLakeInventoryTable.table_name.asc(),
        DataLakeInventoryTable.path_base.asc(),
        DataLakeInventoryTable.id.asc(),
    )


def _inventory_summary(session: Session, connection: DataLakeConnection) -> DataLakeInventorySummaryOut:
    rows = session.scalars(
        select(DataLakeInventoryTable).where(DataLakeInventoryTable.connection_id == connection.id)
    ).all()
    latest_scan = _latest_scan_run(session, connection.id)
    total_tables = len(rows)
    bronze_tables = sum(1 for item in rows if item.layer == "bronze")
    silver_tables = sum(1 for item in rows if item.layer == "silver")
    gold_tables = sum(1 for item in rows if item.layer == "gold")
    total_parquet_files = sum(item.parquet_files_count for item in rows)
    total_bytes = sum(item.size_total_bytes for item in rows)
    tables_without_parquet = sum(1 for item in rows if item.parquet_files_count == 0)
    tables_without_recent_update = sum(
        1
        for item in rows
        if (
            _as_utc(item.last_modified_at) is None
            or _as_utc(item.last_modified_at)
            < _now() - timedelta(hours=resolve_data_lake_table_freshness_sla_hours(connection, item))
        )
    )
    layers_detected = sorted({item.layer for item in rows})
    return DataLakeInventorySummaryOut(
        connection_id=connection.id,
        connection_name=connection.name,
        total_tables=total_tables,
        bronze_tables=bronze_tables,
        silver_tables=silver_tables,
        gold_tables=gold_tables,
        total_parquet_files=total_parquet_files,
        total_bytes=total_bytes,
        tables_without_parquet=tables_without_parquet,
        tables_without_recent_update=tables_without_recent_update,
        layers_detected=layers_detected,
        last_scan_at=latest_scan.finished_at if latest_scan else None,
        latest_scan_status=latest_scan.status if latest_scan else None,
        latest_scan_message=latest_scan.error_message if latest_scan else None,
        latest_scan_run_id=latest_scan.id if latest_scan else None,
    )


def _catalog_summary(session: Session, rows: list[tuple[DataLakeInventoryTable, DataLakeConnection]]) -> DataLakeCatalogSummaryOut:
    latest_scan = session.scalars(
        select(DataLakeInventoryScanRun).order_by(DataLakeInventoryScanRun.created_at.desc(), DataLakeInventoryScanRun.id.desc())
    ).first()
    connections = {connection.id: connection for _item, connection in rows}
    return DataLakeCatalogSummaryOut(
        total_tables=len(rows),
        bronze_tables=sum(1 for item, _connection in rows if item.layer == "bronze"),
        silver_tables=sum(1 for item, _connection in rows if item.layer == "silver"),
        gold_tables=sum(1 for item, _connection in rows if item.layer == "gold"),
        total_parquet_files=sum(item.parquet_files_count for item, _connection in rows),
        total_bytes=sum(item.size_total_bytes for item, _connection in rows),
        tables_without_parquet=sum(1 for item, _connection in rows if item.parquet_files_count == 0),
        tables_without_recent_update=sum(
            1
            for item, connection in rows
            if (
                _as_utc(item.last_modified_at) is None
                or _as_utc(item.last_modified_at)
                < _now() - timedelta(hours=resolve_data_lake_table_freshness_sla_hours(connection, item))
            )
        ),
        active_connections=sum(1 for connection in connections.values() if connection.is_active),
        total_connections=len(connections),
        layers_detected=sorted({item.layer for item, _connection in rows}),
        last_scan_at=latest_scan.finished_at if latest_scan else None,
        latest_scan_status=latest_scan.status if latest_scan else None,
        latest_scan_message=latest_scan.error_message if latest_scan else None,
        latest_scan_run_id=latest_scan.id if latest_scan else None,
    )


def _catalog_sort_key(item: DataLakeInventoryTable, connection: DataLakeConnection, sort_by: str) -> tuple[Any, ...]:
    if sort_by == "last_modified":
        modified = _as_utc(item.last_modified_at) or datetime.min.replace(tzinfo=timezone.utc)
        return (modified, item.layer, item.table_name, item.path_base, item.id)
    if sort_by == "volume":
        return (item.size_total_bytes, item.layer, item.table_name, item.path_base, item.id)
    if sort_by == "files_count":
        return (item.files_count, item.layer, item.table_name, item.path_base, item.id)
    if sort_by == "layer":
        return (item.layer, item.table_name, item.path_base, item.id)
    return (item.table_name, item.layer, item.path_base, item.id)


def get_data_lake_catalog_page(
    session: Session,
    *,
    page: int = 1,
    page_size: int = 25,
    connection_id: int | None = None,
    bucket: str | None = None,
    layer: str | None = None,
    status: str | None = None,
    has_partitions: bool | None = None,
    has_parquet: bool | None = None,
    freshness_state: str | None = None,
    search: str | None = None,
    sort_by: str = "last_modified",
    sort_dir: str = "desc",
) -> DataLakeCatalogPageOut:
    rows = session.execute(
        select(DataLakeInventoryTable, DataLakeConnection).join(
            DataLakeConnection,
            DataLakeConnection.id == DataLakeInventoryTable.connection_id,
        )
    ).all()
    filtered: list[tuple[DataLakeInventoryTable, DataLakeConnection]] = []
    normalized_search = (search or "").strip().lower()
    normalized_bucket = (bucket or "").strip().lower()
    normalized_sort_by = (sort_by or "last_modified").strip().lower()
    normalized_sort_dir = (sort_dir or "desc").strip().lower()

    for item, connection in rows:
        if connection_id is not None and item.connection_id != connection_id:
            continue
        if normalized_bucket and normalized_bucket != connection.bucket.lower():
            continue
        if layer and item.layer != layer:
            continue
        if status and item.status_scan != status:
            continue
        if has_partitions is not None and bool(item.has_partitions) is not has_partitions:
            continue
        if has_parquet is not None and (item.parquet_files_count > 0) is not has_parquet:
            continue
        if freshness_state is not None and _inventory_row_freshness_state(connection, item) != freshness_state:
            continue
        if normalized_search:
            haystack = " ".join(
                filter(
                    None,
                    [
                        item.table_name,
                        item.path_base,
                        connection.name,
                        connection.bucket,
                        connection.prefix or "",
                        item.partition_pattern_detected or "",
                    ],
                )
            ).lower()
            if normalized_search not in haystack:
                continue
        filtered.append((item, connection))

    reverse = normalized_sort_dir != "asc"
    filtered.sort(key=lambda pair: _catalog_sort_key(pair[0], pair[1], normalized_sort_by), reverse=reverse)
    normalized_page = max(1, page)
    normalized_page_size = max(1, min(page_size, 100))
    total = len(filtered)
    start = (normalized_page - 1) * normalized_page_size
    end = start + normalized_page_size
    page_items = filtered[start:end]

    return DataLakeCatalogPageOut(
        summary=_catalog_summary(session, filtered),
        items=[
            DataLakeCatalogTableOut.model_validate(
                serialize_data_lake_catalog_table(connection, item)
            )
            for item, connection in page_items
        ],
        total=total,
        page=normalized_page,
        page_size=normalized_page_size,
        has_more=end < total,
    )


def _inventory_row_freshness_state(connection: DataLakeConnection, item: DataLakeInventoryTable) -> str:
    modified_at = _as_utc(item.last_modified_at)
    if modified_at is None:
        return "stale"
    threshold_hours = resolve_data_lake_table_freshness_sla_hours(connection, item)
    if modified_at >= _now() - timedelta(hours=threshold_hours):
        return "recent"
    return "stale"


def get_data_lake_inventory_page(
    session: Session,
    connection_id: int,
    *,
    page: int = 1,
    page_size: int = 25,
    layer: str | None = None,
    name: str | None = None,
    status: str | None = None,
    has_partitions: bool | None = None,
    freshness_state: str | None = None,
) -> DataLakeInventoryPageOut:
    connection = get_data_lake_connection_or_404(session, connection_id)
    normalized_page = max(1, page)
    normalized_page_size = max(1, min(page_size, 100))
    stmt = _inventory_rows_query(
        session,
        connection_id=connection.id,
        layer=layer,
        name=name,
        status=status,
        has_partitions=has_partitions,
        freshness_state=freshness_state,
    )
    rows = session.scalars(stmt).all()
    if freshness_state == "recent":
        rows = [item for item in rows if _inventory_row_freshness_state(connection, item) == "recent"]
    elif freshness_state == "stale":
        rows = [item for item in rows if _inventory_row_freshness_state(connection, item) == "stale"]
    total = len(rows)
    start = (normalized_page - 1) * normalized_page_size
    end = start + normalized_page_size
    items = rows[start:end]
    latest_scan = _latest_scan_run(session, connection.id)
    return DataLakeInventoryPageOut(
        summary=_inventory_summary(session, connection),
        latest_scan=DataLakeInventoryScanRunOut.model_validate(serialize_data_lake_scan_run(latest_scan)) if latest_scan else None,
        items=[DataLakeInventoryTableOut.model_validate(serialize_data_lake_inventory_table(item)) for item in items],
        total=total,
        page=normalized_page,
        page_size=normalized_page_size,
        has_more=end < total,
    )


def list_data_lake_inventory_scans(
    session: Session,
    connection_id: int,
    *,
    page: int = 1,
    page_size: int = 25,
) -> PageOut[DataLakeInventoryScanRunOut]:
    connection = get_data_lake_connection_or_404(session, connection_id)
    normalized_page = max(1, page)
    normalized_page_size = max(1, min(page_size, 100))
    scans = session.scalars(
        select(DataLakeInventoryScanRun)
        .where(DataLakeInventoryScanRun.connection_id == connection.id)
        .order_by(DataLakeInventoryScanRun.created_at.desc(), DataLakeInventoryScanRun.id.desc())
    ).all()
    total = len(scans)
    start = (normalized_page - 1) * normalized_page_size
    end = start + normalized_page_size
    items = scans[start:end]
    return PageOut[DataLakeInventoryScanRunOut](
        items=[DataLakeInventoryScanRunOut.model_validate(serialize_data_lake_scan_run(item)) for item in items],
        total=total,
        page=normalized_page,
        page_size=normalized_page_size,
        has_more=end < total,
    )


def _run_data_lake_inventory_scan(
    session: Session,
    connection_id: int,
    *,
    current_user: User | None,
    audit_kwargs: dict[str, Any] | None = None,
    trigger_mode: str = "manual",
    schedule_id: int | None = None,
    request_runner=_aws_request,
    scan_run: DataLakeInventoryScanRun | None = None,
) -> DataLakeInventoryScanOut:
    connection = get_data_lake_connection_or_404(session, connection_id)
    if scan_run is None:
        scan_run = DataLakeInventoryScanRun(
            connection_id=connection.id,
            status="running",
            started_at=_now(),
            scanned_by_user_id=current_user.id if current_user is not None else None,
            trigger_mode=trigger_mode,
            schedule_id=schedule_id,
        )
        session.add(scan_run)
        session.commit()
        session.refresh(scan_run)
    else:
        scan_run.status = "running"
        scan_run.started_at = scan_run.started_at or _now()
        if current_user is not None and scan_run.scanned_by_user_id is None:
            scan_run.scanned_by_user_id = current_user.id
        scan_run.trigger_mode = trigger_mode
        scan_run.schedule_id = schedule_id
        session.add(scan_run)
        session.commit()
        session.refresh(scan_run)

    try:
        credentials, _mode = _aws_credentials_for_connection(connection)
        caller_identity = _request_sts_get_caller_identity(region=connection.region, credentials=credentials, request_runner=request_runner)
        prefix_values = parse_prefix_list(connection.prefix)
        scan_roots = prefix_values or [None]
        discovered_rows_map: dict[tuple[str, str, str], dict[str, Any]] = {}
        persisted_tables: list[tuple[DataLakeInventoryTable, dict[str, Any]]] = []
        discovered_layers_set: set[str] = set()
        seen_objects: dict[str, Any] = {}
        discovered_tables = 0
        discovered_parquet_files = 0
        total_bytes = 0

        for root_prefix in scan_roots:
            try:
                for entry in list_s3_objects_recursive(
                    bucket=connection.bucket,
                    region=connection.region,
                    prefix=root_prefix,
                    credentials=credentials,
                    request_runner=request_runner,
                ):
                    seen_objects[entry.key] = entry
            except S3ListObjectsError as exc:
                logger.warning(
                    "data lake inventory scan prefix listing failed",
                    extra={
                        "connection_id": connection.id,
                        "bucket": connection.bucket,
                        "region": connection.region,
                        "prefix": root_prefix,
                        "auth_mode": connection.auth_type,
                        "error_code": exc.code,
                        "error_message": exc.message,
                        "error_detail": exc.detail,
                        "status_code": exc.status_code,
                        "response_body": exc.response_body,
                    },
                )
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"{exc.code}: {exc.message}") from exc
        for entry in seen_objects.values():
            parsed = parse_data_lake_object_key(entry.key)
            if parsed is None:
                continue
            discovered_layers_set.add(parsed.layer)
            key = (parsed.layer, parsed.table_name, parsed.path_base)
            table_row = discovered_rows_map.setdefault(
                key,
                {
                    "layer": parsed.layer,
                    "table_name": parsed.table_name,
                    "path_base": parsed.path_base,
                    "files_count": 0,
                    "parquet_files_count": 0,
                    "non_parquet_files_count": 0,
                    "size_total_bytes": 0,
                    "last_modified_at": None,
                    "has_partitions": False,
                    "partition_pattern_detected": None,
                    "sample_parquet_files": [],
                    "status_scan": "empty",
                    "data_last_scan_at": _now(),
                    "error_message": None,
                },
            )
            table_row["files_count"] += 1
            if is_parquet_key(entry.key):
                table_row["parquet_files_count"] += 1
                discovered_parquet_files += 1
                size = max(entry.size, 0)
                table_row["size_total_bytes"] += size
                total_bytes += size
                if len(table_row["sample_parquet_files"]) < 5:
                    table_row["sample_parquet_files"].append(
                        {
                            "key": entry.key,
                            "size": size,
                            "last_modified": entry.last_modified.isoformat() if entry.last_modified else None,
                        }
                    )
            else:
                table_row["non_parquet_files_count"] += 1
            if entry.last_modified and (table_row["last_modified_at"] is None or entry.last_modified > table_row["last_modified_at"]):
                table_row["last_modified_at"] = entry.last_modified
            if parsed.partition_segments:
                table_row["has_partitions"] = True
                patterns: set[str] = set(
                    str(table_row["partition_pattern_detected"]).split(",") if table_row["partition_pattern_detected"] else []
                )
                for segment in parsed.partition_segments:
                    if "=" in segment:
                        patterns.add("key_value")
                    elif len(segment) == 10 and segment.count("-") == 2:
                        patterns.add("date_path")
                    else:
                        patterns.add("partitioned")
                patterns.discard("")
                table_row["partition_pattern_detected"] = ",".join(sorted(patterns)) if patterns else None

        for table_row in discovered_rows_map.values():
            table_row["status_scan"] = "scanned" if table_row["parquet_files_count"] > 0 else "no_parquet" if table_row["files_count"] > 0 else "empty"
            discovered_tables += 1 if table_row["files_count"] > 0 else 0

        session.execute(delete(DataLakeInventoryTable).where(DataLakeInventoryTable.connection_id == connection.id))
        for row in sorted(discovered_rows_map.values(), key=lambda item: (item["layer"], item["table_name"], item["path_base"])):
            table = DataLakeInventoryTable(
                connection_id=connection.id,
                layer=row["layer"],
                table_name=row["table_name"],
                path_base=row["path_base"],
                files_count=row["files_count"],
                parquet_files_count=row["parquet_files_count"],
                non_parquet_files_count=row["non_parquet_files_count"],
                size_total_bytes=row["size_total_bytes"],
                last_modified_at=row["last_modified_at"],
                has_partitions=row["has_partitions"],
                partition_pattern_detected=row["partition_pattern_detected"],
                status_scan=row["status_scan"],
                data_last_scan_at=row["data_last_scan_at"],
                sample_parquet_files_json=row.get("sample_parquet_files"),
                scan_run_id=scan_run.id,
                error_message=None,
            )
            session.add(table)
            persisted_tables.append((table, row))
        session.flush()
        for table, row in persisted_tables:
            quality_snapshot = calculate_data_lake_table_quality(
                connection=connection,
                inventory=table,
                sample_entries=list(row.get("sample_parquet_files") or []),
                columns=[],
                parquet_metadata=[],
                errors=[],
                exact_coverage=False,
                row_count=None,
                row_count_method="unavailable",
                row_count_confidence="unknown",
            )
            session.add(
                DataLakeTableObservation(
                    **build_data_lake_observation_payload(
                        connection=connection,
                        inventory=table,
                        quality_snapshot=quality_snapshot,
                        row_count=None,
                        row_count_method="unavailable",
                        row_count_confidence="unknown",
                        size_total_bytes=row["size_total_bytes"],
                    )
                )
            )

        scan_run.status = "success"
        scan_run.scanned_layers_count = len(discovered_layers_set)
        scan_run.discovered_tables_count = discovered_tables
        scan_run.discovered_parquet_files_count = discovered_parquet_files
        scan_run.total_bytes = total_bytes
        scan_run.error_message = None
        scan_run.finished_at = _now()
        session.add(scan_run)
        session.commit()
        write_audit_log_sync(
            session,
            action="integrations.data_lake.inventory_scan",
            entity_type="data_lake_connection",
            entity_id=connection.id,
            metadata={
                "name": connection.name,
                "bucket": connection.bucket,
                "region": connection.region,
                "tables": discovered_tables,
                "parquet_files": discovered_parquet_files,
                "layers": len(discovered_layers_set),
            },
            **(audit_kwargs or {}),
        )
        session.commit()
        logger.debug(
            "data lake inventory scan completed",
            extra={
                "connection_id": connection.id,
                "bucket": connection.bucket,
                "prefixes": prefix_values,
                "auth_mode": connection.auth_type,
                "caller_identity_arn": (caller_identity or {}).get("arn"),
                "caller_identity_account": (caller_identity or {}).get("account"),
                "objects_seen": len(seen_objects),
                "tables": discovered_tables,
                "parquet_files": discovered_parquet_files,
                "layers": len(discovered_layers_set),
            },
        )
        summary = _inventory_summary(session, connection)
        result = DataLakeInventoryScanOut(
            scan_run=DataLakeInventoryScanRunOut.model_validate(serialize_data_lake_scan_run(scan_run)),
            summary=summary,
        )
        _clear_sensitive_credentials(credentials)
        return result
    except Exception as exc:
        session.rollback()
        scan_run.status = "error"
        scan_run.error_message = str(exc)
        scan_run.finished_at = _now()
        session.add(scan_run)
        session.commit()
        write_audit_log_sync(
            session,
            action="integrations.data_lake.inventory_scan_error",
            entity_type="data_lake_connection",
            entity_id=connection.id,
            metadata={
                "name": connection.name,
                "bucket": connection.bucket,
                "region": connection.region,
                "error": str(exc),
            },
            **(audit_kwargs or {}),
        )
        session.commit()
        _clear_sensitive_credentials(locals().get("credentials") if "credentials" in locals() else None)
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


def scan_data_lake_inventory(
    session: Session,
    connection_id: int,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
    correlation_id: str | None = None,
) -> DataLakeInventoryScanOut:
    return enqueue_data_lake_inventory_scan(
        session,
        connection_id,
        current_user=current_user,
        audit_kwargs=audit_kwargs,
        trigger_mode="manual",
        schedule_id=None,
        correlation_id=correlation_id,
    )


def enqueue_data_lake_inventory_scan(
    session: Session,
    connection_id: int,
    *,
    current_user: User | None,
    audit_kwargs: dict[str, Any] | None = None,
    trigger_mode: str = "manual",
    schedule_id: int | None = None,
    correlation_id: str | None = None,
) -> DataLakeInventoryScanOut:
    connection = get_data_lake_connection_or_404(session, connection_id)
    scan_run = DataLakeInventoryScanRun(
        connection_id=connection.id,
        status="queued",
        started_at=None,
        finished_at=None,
        scanned_by_user_id=current_user.id if current_user is not None else None,
        trigger_mode=trigger_mode,
        schedule_id=schedule_id,
        error_message=None,
    )
    session.add(scan_run)
    session.flush()

    try:
        job = enqueue_integration_job(
            session,
            source="s3",
            job_type="inventory_scan",
            target_type="data_lake_connection",
            target_id=connection.id,
            target_name=connection.name,
            trigger_mode=trigger_mode,
            requested_by_user_id=current_user.id if current_user is not None else None,
            correlation_id=correlation_id,
            payload_json={
                "connection_id": connection.id,
                "scan_run_id": scan_run.id,
                "requested_by_user_id": current_user.id if current_user is not None else None,
                "trigger_mode": trigger_mode,
                "schedule_id": schedule_id,
            },
            context_json={
                "connection_id": connection.id,
                "connection_name": connection.name,
                "bucket": connection.bucket,
                "region": connection.region,
                "trigger_mode": trigger_mode,
                "schedule_id": schedule_id,
            },
        )
    except Exception:
        session.rollback()
        raise

    if job is None:
        session.rollback()
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Fila de jobs indisponível para o scan do Data Lake.")

    write_audit_log_sync(
        session,
        action="integrations.data_lake.inventory_scan_queued",
        entity_type="data_lake_connection",
        entity_id=connection.id,
        metadata={
            "name": connection.name,
            "bucket": connection.bucket,
            "region": connection.region,
            "job_id": job.id,
            "scan_run_id": scan_run.id,
            "trigger_mode": trigger_mode,
            "schedule_id": schedule_id,
        },
        **(audit_kwargs or {}),
    )
    session.commit()
    session.refresh(scan_run)
    summary = _inventory_summary(session, connection)
    result = DataLakeInventoryScanOut(
        scan_run=DataLakeInventoryScanRunOut.model_validate(serialize_data_lake_scan_run(scan_run)),
        summary=summary,
        job_id=job.id,
        job_status=job.status,
        correlation_id=job.correlation_id,
    )
    _clear_sensitive_credentials(locals().get("credentials") if "credentials" in locals() else None)
    return result


def update_data_lake_inventory_table_freshness_sla(
    session: Session,
    connection_id: int,
    table_id: int,
    payload: DataLakeTableFreshnessSlaIn,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
) -> DataLakeInventoryTableOut:
    connection = get_data_lake_connection_or_404(session, connection_id)
    table = _inventory_table_query(session, connection_id, table_id)
    before = serialize_data_lake_inventory_table(table)
    table.freshness_sla_hours_override = payload.freshness_sla_hours_override
    session.add(table)
    session.commit()
    session.refresh(table)
    write_audit_log_sync(
        session,
        action="integrations.data_lake.inventory_table_freshness_sla_update",
        entity_type="data_lake_inventory_table",
        entity_id=table.id,
        before=before,
        after=serialize_data_lake_inventory_table(table),
        metadata={
            "connection_id": connection.id,
            "connection_name": connection.name,
            "table_name": table.table_name,
            "freshness_sla_hours_override": table.freshness_sla_hours_override,
        },
        **audit_kwargs,
    )
    session.commit()
    return DataLakeInventoryTableOut.model_validate(serialize_data_lake_inventory_table(table))


__all__ = [
    "enqueue_data_lake_inventory_scan",
    "get_data_lake_inventory_page",
    "list_data_lake_inventory_scans",
    "update_data_lake_inventory_table_freshness_sla",
    "_run_data_lake_inventory_scan",
    "scan_data_lake_inventory",
    "serialize_data_lake_inventory_table",
    "serialize_data_lake_scan_run",
]
