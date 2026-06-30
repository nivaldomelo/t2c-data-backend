from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
import time
from dataclasses import replace
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.integrations.data_lake import (
    _aws_credentials_for_connection,
    _clear_sensitive_credentials,
    _aws_request,
    _aws_sign_headers,
    _extract_error_code,
    _extract_error_message,
    get_data_lake_connection_or_404,
)
from t2c_data.features.integrations.data_lake_inventory import _is_ignored_name, _scan_table_prefix, serialize_data_lake_inventory_table
from t2c_data.features.integrations.data_lake_s3 import S3ListObjectsError, is_parquet_key, list_s3_objects_recursive
from t2c_data.features.integrations.data_lake_quality import build_data_lake_observation_payload, calculate_data_lake_table_quality
from t2c_data.features.platform.jobs import record_asset_row_count_snapshot
from t2c_data.models.platform import DataLakeInventoryTable, DataLakeTableObservation
from t2c_data.schemas.integrations import (
    DataLakeTableDetailColumnOut,
    DataLakeTableDetailErrorOut,
    DataLakeTableDetailFileOut,
    DataLakeTableDetailHistoryOut,
    DataLakeTableDetailOut,
    DataLakeTableDetailScoreOut,
    DataLakeTableDetailSignalOut,
    DataLakeInventoryTableOut,
    DataLakeTableFileOut,
    DataLakeTableFilesPageOut,
)

logger = logging.getLogger(__name__)


_PARQUET_MAGIC = b"PAR1"
_DETAIL_SAMPLE_LIMIT = 5
_EXACT_ROW_COUNT_THRESHOLD = 10
_PYARROW_FOOTER_MIN_BYTES = 32
_HTTP_422_STATUS = getattr(status, "HTTP_422_UNPROCESSABLE_CONTENT", None) or status.HTTP_422_UNPROCESSABLE_ENTITY
_S3_ACCESS_ERROR_CODES = {
    "AccessDenied",
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "ExpiredToken",
    "InvalidToken",
}
_S3_REGION_ERROR_CODES = {
    "PermanentRedirect",
    "AuthorizationHeaderMalformed",
}
_S3_NOT_FOUND_ERROR_CODES = {
    "NoSuchKey",
    "NoSuchBucket",
}


@dataclass(slots=True)
class _ParquetFileMetadata:
    num_rows: int | None
    columns: list[dict[str, Any]]
    schema_signature: str
    schema_hash: str
    row_group_count: int
    column_stats: dict[str, dict[str, Any]]


class _S3ObjectReadError(RuntimeError):
    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        key: str,
        operation: str,
        status_code: int,
        code: str,
        message: str,
        detail: str | None,
        response_body: str | None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.bucket = bucket
        self.region = region
        self.key = key
        self.operation = operation
        self.status_code = status_code
        self.code = code
        self.message = message
        self.detail = detail
        self.response_body = response_body

    @property
    def category(self) -> str:
        if self.code in _S3_REGION_ERROR_CODES or self.status_code in {301, 302, 307}:
            return "s3_region"
        if self.code in _S3_NOT_FOUND_ERROR_CODES or self.status_code == 404:
            return "s3_not_found"
        if self.code in _S3_ACCESS_ERROR_CODES or self.status_code in {401, 403}:
            return "s3_access"
        if "footer" in self.message.lower() or "parquet" in self.message.lower():
            return "parquet_read"
        return "s3_unknown"

    def note(self) -> str:
        location = f"{self.bucket}/{self.key}"
        detail = self.detail or self.response_body or self.message
        return f"{location}: {self.category}:{self.operation}: {detail}"

    def as_payload(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "region": self.region,
            "key": self.key,
            "operation": self.operation,
            "category": self.category,
            "status_code": self.status_code,
            "code": self.code,
            "message": self.message,
            "detail": self.detail,
            "response_body": self.response_body,
        }


class _CompactReader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._index = 0
        self._last_field_id = 0

    def _read_byte(self) -> int:
        if self._index >= len(self._data):
            raise ValueError("Unexpected end of compact thrift payload")
        value = self._data[self._index]
        self._index += 1
        return value

    def _read_varint(self) -> int:
        shift = 0
        value = 0
        while True:
            byte = self._read_byte()
            value |= (byte & 0x7F) << shift
            if byte & 0x80 == 0:
                return value
            shift += 7
            if shift > 63:
                raise ValueError("Invalid compact thrift varint")

    def _read_zigzag(self) -> int:
        raw = self._read_varint()
        return (raw >> 1) ^ -(raw & 1)

    def _read_bytes(self, length: int) -> bytes:
        if length < 0 or self._index + length > len(self._data):
            raise ValueError("Unexpected end of compact thrift payload")
        value = self._data[self._index : self._index + length]
        self._index += length
        return value

    def read_field_header(self) -> tuple[int, int]:
        header = self._read_byte()
        field_type = header & 0x0F
        if field_type == 0:
            self._last_field_id = 0
            return field_type, 0
        delta = header >> 4
        if delta:
            self._last_field_id += delta
            return field_type, self._last_field_id
        self._last_field_id = self._read_zigzag()
        return field_type, self._last_field_id

    def read_bool(self, field_type: int | None = None) -> bool:
        if field_type is None:
            return bool(self._read_byte())
        return field_type == 1

    def read_i16(self) -> int:
        return int(self._read_zigzag())

    def read_i32(self) -> int:
        return int(self._read_zigzag())

    def read_i64(self) -> int:
        return int(self._read_zigzag())

    def read_double(self) -> float:
        import struct

        return struct.unpack("<d", self._read_bytes(8))[0]

    def read_binary(self) -> bytes:
        length = self._read_varint()
        return self._read_bytes(length)

    def read_string(self) -> str:
        return self.read_binary().decode("utf-8", errors="replace")

    def read_list_header(self) -> tuple[int, int]:
        header = self._read_byte()
        size = header >> 4
        elem_type = header & 0x0F
        if size == 15:
            size = self._read_varint()
        return size, elem_type

    def skip(self, field_type: int) -> None:
        if field_type in {1, 2}:  # bool true/false
            return
        if field_type == 3:  # byte
            self._read_byte()
            return
        if field_type == 4:
            self.read_i16()
            return
        if field_type == 5:
            self.read_i32()
            return
        if field_type == 6:
            self.read_i64()
            return
        if field_type == 7:
            self.read_double()
            return
        if field_type == 8:
            self.read_binary()
            return
        if field_type in {9, 10}:  # list / set
            size, elem_type = self.read_list_header()
            for _ in range(size):
                self.skip(elem_type)
            return
        if field_type == 11:  # map
            size = self._read_varint()
            if size == 0:
                return
            type_header = self._read_byte()
            key_type = type_header >> 4
            value_type = type_header & 0x0F
            for _ in range(size):
                self.skip(key_type)
                self.skip(value_type)
            return
        if field_type == 12:  # struct
            last_field_id = self._last_field_id
            self._last_field_id = 0
            while True:
                nested_type, _nested_id = self.read_field_header()
                if nested_type == 0:
                    break
                self.skip(nested_type)
            self._last_field_id = last_field_id
            return
        raise ValueError(f"Unsupported compact thrift type: {field_type}")


def _parquet_type_label(value: int | None) -> str | None:
    if value is None:
        return None
    return {
        0: "BOOLEAN",
        1: "INT32",
        2: "INT64",
        3: "INT96",
        4: "FLOAT",
        5: "DOUBLE",
        6: "BYTE_ARRAY",
        7: "FIXED_LEN_BYTE_ARRAY",
    }.get(value, f"type_{value}")


def _parquet_repetition_label(value: int | None) -> str | None:
    if value is None:
        return None
    return {
        0: "REQUIRED",
        1: "OPTIONAL",
        2: "REPEATED",
    }.get(value, f"repetition_{value}")


def _is_suspicious_column_name(name: str) -> bool:
    normalized = name.strip().lower()
    return not normalized or normalized.startswith("_col") or normalized in {"col", "column", "field"}


def _is_parquet_key(key: str) -> bool:
    return key.lower().endswith(".parquet")


def _parse_schema_element(reader: _CompactReader) -> dict[str, Any]:
    item: dict[str, Any] = {}
    while True:
        field_type, field_id = reader.read_field_header()
        if field_type == 0:
            break
        if field_id == 1:
            item["type"] = reader.read_i32()
        elif field_id == 2:
            item["type_length"] = reader.read_i32()
        elif field_id == 3:
            item["repetition_type"] = reader.read_i32()
        elif field_id == 4:
            item["name"] = reader.read_string()
        elif field_id == 5:
            item["num_children"] = reader.read_i32()
        elif field_id == 6:
            item["converted_type"] = reader.read_i32()
        elif field_id == 7:
            item["scale"] = reader.read_i32()
        elif field_id == 8:
            item["precision"] = reader.read_i32()
        elif field_id == 9:
            item["field_id"] = reader.read_i32()
        else:
            reader.skip(field_type)
    return item


def _parse_statistics(reader: _CompactReader) -> dict[str, Any]:
    data: dict[str, Any] = {}
    while True:
        field_type, field_id = reader.read_field_header()
        if field_type == 0:
            break
        if field_id == 1:
            data["max"] = reader.read_binary()
        elif field_id == 2:
            data["min"] = reader.read_binary()
        elif field_id == 3:
            data["null_count"] = reader.read_i64()
        elif field_id == 4:
            data["distinct_count"] = reader.read_i64()
        elif field_id == 5:
            data["max_value"] = reader.read_binary()
        elif field_id == 6:
            data["min_value"] = reader.read_binary()
        elif field_id == 7:
            data["is_max_value_set"] = reader.read_bool()
        elif field_id == 8:
            data["is_min_value_set"] = reader.read_bool()
        else:
            reader.skip(field_type)
    return data


def _parse_column_meta_data(reader: _CompactReader) -> dict[str, Any]:
    data: dict[str, Any] = {}
    while True:
        field_type, field_id = reader.read_field_header()
        if field_type == 0:
            break
        if field_id == 1:
            data["type"] = reader.read_i32()
        elif field_id == 2:
            size, elem_type = reader.read_list_header()
            encodings: list[int] = []
            for _ in range(size):
                if elem_type == 5:
                    encodings.append(reader.read_i32())
                else:
                    reader.skip(elem_type)
            data["encodings"] = encodings
        elif field_id == 3:
            size, elem_type = reader.read_list_header()
            path: list[str] = []
            for _ in range(size):
                if elem_type == 8:
                    path.append(reader.read_string())
                else:
                    reader.skip(elem_type)
            data["path_in_schema"] = path
        elif field_id == 4:
            data["codec"] = reader.read_i32()
        elif field_id == 5:
            data["num_values"] = reader.read_i64()
        elif field_id == 6:
            data["total_uncompressed_size"] = reader.read_i64()
        elif field_id == 7:
            data["total_compressed_size"] = reader.read_i64()
        elif field_id == 8:
            reader.skip(field_type)
        elif field_id == 9:
            data["data_page_offset"] = reader.read_i64()
        elif field_id == 10:
            data["index_page_offset"] = reader.read_i64()
        elif field_id == 11:
            data["dictionary_page_offset"] = reader.read_i64()
        elif field_id == 12:
            data["statistics"] = _parse_statistics(reader)
        elif field_id == 13:
            reader.skip(field_type)
        elif field_id == 14:
            reader.skip(field_type)
        else:
            reader.skip(field_type)
    return data


def _parse_column_chunk(reader: _CompactReader) -> dict[str, Any]:
    data: dict[str, Any] = {}
    while True:
        field_type, field_id = reader.read_field_header()
        if field_type == 0:
            break
        if field_id == 1:
            data["file_path"] = reader.read_string()
        elif field_id == 2:
            data["file_offset"] = reader.read_i64()
        elif field_id == 3:
            data["meta_data"] = _parse_column_meta_data(reader)
        elif field_id == 4:
            data["offset_index_offset"] = reader.read_i64()
        elif field_id == 5:
            data["offset_index_length"] = reader.read_i32()
        elif field_id == 6:
            data["column_index_offset"] = reader.read_i64()
        elif field_id == 7:
            data["column_index_length"] = reader.read_i32()
        elif field_id == 8:
            data["encrypted_column_metadata"] = reader.read_binary()
        else:
            reader.skip(field_type)
    return data


def _parse_row_group(reader: _CompactReader) -> dict[str, Any]:
    data: dict[str, Any] = {"columns": []}
    while True:
        field_type, field_id = reader.read_field_header()
        if field_type == 0:
            break
        if field_id == 1:
            size, elem_type = reader.read_list_header()
            columns: list[dict[str, Any]] = []
            for _ in range(size):
                if elem_type == 12:
                    columns.append(_parse_column_chunk(reader))
                else:
                    reader.skip(elem_type)
            data["columns"] = columns
        elif field_id == 2:
            data["total_byte_size"] = reader.read_i64()
        elif field_id == 3:
            data["num_rows"] = reader.read_i64()
        elif field_id == 4:
            size, elem_type = reader.read_list_header()
            for _ in range(size):
                reader.skip(elem_type)
        elif field_id == 5:
            data["file_offset"] = reader.read_i64()
        elif field_id == 6:
            data["total_compressed_size"] = reader.read_i64()
        else:
            reader.skip(field_type)
    return data


def _parse_file_metadata(payload: bytes) -> dict[str, Any]:
    reader = _CompactReader(payload)
    data: dict[str, Any] = {}
    while True:
        field_type, field_id = reader.read_field_header()
        if field_type == 0:
            break
        if field_id == 1:
            data["version"] = reader.read_i32()
        elif field_id == 2:
            size, elem_type = reader.read_list_header()
            schema: list[dict[str, Any]] = []
            for _ in range(size):
                if elem_type != 12:
                    reader.skip(elem_type)
                    continue
                schema.append(_parse_schema_element(reader))
            data["schema"] = schema
        elif field_id == 3:
            data["num_rows"] = reader.read_i64()
        elif field_id == 4:
            size, elem_type = reader.read_list_header()
            row_groups: list[dict[str, Any]] = []
            for _ in range(size):
                if elem_type == 12:
                    row_groups.append(_parse_row_group(reader))
                else:
                    reader.skip(elem_type)
            data["row_groups"] = row_groups
        elif field_id == 5:
            size, elem_type = reader.read_list_header()
            key_values: list[dict[str, Any]] = []
            for _ in range(size):
                if elem_type == 12:
                    key_values.append({})
                    while True:
                        nested_type, nested_id = reader.read_field_header()
                        if nested_type == 0:
                            break
                        if nested_id == 1:
                            key_values[-1]["key"] = reader.read_string()
                        elif nested_id == 2:
                            key_values[-1]["value"] = reader.read_string()
                        else:
                            reader.skip(nested_type)
                else:
                    reader.skip(elem_type)
            data["key_value_metadata"] = key_values
        else:
            reader.skip(field_type)
    return data


def _flatten_schema(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not elements:
        return []
    if len(elements) == 1:
        return []

    def _walk(index: int, prefix: tuple[str, ...]) -> tuple[list[dict[str, Any]], int]:
        element = elements[index]
        index += 1
        name = str(element.get("name") or f"field_{index}")
        repetition_type = _parquet_repetition_label(element.get("repetition_type"))
        physical_type = _parquet_type_label(element.get("type"))
        logical_type = None
        nullable = repetition_type != "REQUIRED"
        children = int(element.get("num_children") or 0)
        path = ".".join((*prefix, name))
        columns: list[dict[str, Any]] = []
        if children > 0:
            for _ in range(children):
                child_columns, index = _walk(index, (*prefix, name))
                columns.extend(child_columns)
        else:
            columns.append(
                {
                    "path": path,
                    "name": name,
                    "physical_type": physical_type,
                    "logical_type": logical_type,
                    "repetition_type": repetition_type,
                    "nullable": nullable,
                    "is_suspicious": _is_suspicious_column_name(name),
                }
            )
        return columns, index

    columns: list[dict[str, Any]] = []
    index = 1
    root_children = int(elements[0].get("num_children") or 0)
    for _ in range(root_children):
        child_columns, index = _walk(index, ())
        columns.extend(child_columns)
    return columns


def _schema_signature(columns: list[dict[str, Any]]) -> str:
    return "|".join(
        f"{column['path']}:{column.get('physical_type') or 'unknown'}:{column.get('repetition_type') or 'unknown'}:{int(bool(column.get('nullable', True)))}"
        for column in columns
    )


def _aws_request_bytes(
    *,
    method: str,
    url: str,
    region: str,
    service: str,
    credentials: dict[str, str],
    body: bytes = b"",
    query_params: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    parsed = httpx.URL(url)
    canonical_querystring, signed_headers = _aws_sign_headers(
        method=method,
        service=service,
        region=region,
        host=parsed.host or "",
        canonical_uri=parsed.raw_path.decode("utf-8") or "/",
        query_params=query_params,
        body=body,
        credentials=credentials,
        extra_headers=extra_headers,
    )
    final_url = str(parsed.copy_with(query=canonical_querystring.encode("utf-8"))) if canonical_querystring else str(parsed)
    response = httpx.request(method, final_url, headers=signed_headers, content=body, timeout=15.0, follow_redirects=False)
    return response.status_code, {key.lower(): value for key, value in response.headers.items()}, response.content


def _s3_object_url(*, bucket: str, region: str, key: str) -> str:
    encoded_bucket = quote(bucket, safe="")
    encoded_key = quote(key.lstrip("/"), safe="/-_.~=")
    return f"https://{encoded_bucket}.s3.{region}.amazonaws.com/{encoded_key}"


def _content_length_from_headers(headers: dict[str, str]) -> int | None:
    content_length = headers.get("content-length")
    if not content_length:
        return None
    try:
        parsed = int(str(content_length).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _total_length_from_content_range(headers: dict[str, str]) -> int | None:
    content_range = headers.get("content-range")
    if not content_range:
        return None
    match = re.search(r"/(\d+)$", content_range.strip())
    if not match:
        return None
    try:
        total = int(match.group(1))
    except ValueError:
        return None
    return total if total >= 0 else None


def _classify_s3_object_error(
    *,
    bucket: str,
    region: str,
    key: str,
    operation: str,
    status_code: int,
    body: str | None,
) -> _S3ObjectReadError:
    code = _extract_error_code(body) or f"s3_{operation}_failed"
    message = _extract_error_message(body) or "Object read failed"
    return _S3ObjectReadError(
        bucket=bucket,
        region=region,
        key=key,
        operation=operation,
        status_code=status_code,
        code=code,
        message=message,
        detail=f"{code}: {message}",
        response_body=body,
    )


def _extract_parquet_footer_bytes(payload: bytes) -> bytes:
    if len(payload) < 8 or payload[-4:] != _PARQUET_MAGIC:
        raise ValueError("Invalid parquet footer")
    footer_length = int.from_bytes(payload[-8:-4], "little", signed=False)
    footer_start = len(payload) - 8 - footer_length
    footer_end = len(payload) - 9
    if footer_start < 0 or footer_end < footer_start:
        raise ValueError("Invalid parquet footer length")
    return payload[footer_start : footer_end + 1]


def _build_parquet_file_metadata_from_pyarrow(
    file_metadata: Any,
    *,
    bucket: str = "",
    region: str = "",
    key: str = "",
) -> _ParquetFileMetadata:
    columns: list[dict[str, Any]] = []
    column_count = max(0, int(getattr(file_metadata, "num_columns", 0) or 0))
    row_group_count = max(0, int(getattr(file_metadata, "num_row_groups", 0) or 0))
    for index in range(column_count):
        column = file_metadata.schema.column(index)
        repetition_type = (
            "REPEATED"
            if int(getattr(column, "max_repetition_level", 0) or 0) > 0
            else "OPTIONAL"
            if int(getattr(column, "max_definition_level", 0) or 0) > 0
            else "REQUIRED"
        )
        columns.append(
            {
                "path": getattr(column, "path", None) or getattr(column, "name", None) or f"field_{index}",
                "name": getattr(column, "name", None) or getattr(column, "path", None) or f"field_{index}",
                "physical_type": str(getattr(column, "physical_type", None) or "unknown"),
                "logical_type": str(getattr(column, "logical_type", None)) if getattr(column, "logical_type", None) is not None else None,
                "repetition_type": repetition_type,
                "nullable": repetition_type != "REQUIRED",
                "is_suspicious": _is_suspicious_column_name(str(getattr(column, "name", None) or getattr(column, "path", None) or f"field_{index}")),
            }
        )
    row_groups: list[dict[str, Any]] = []
    column_stats: dict[str, dict[str, Any]] = {}
    for row_group_index in range(row_group_count):
        row_group = file_metadata.row_group(row_group_index)
        row_group_dict = {"num_rows": int(row_group.num_rows or 0), "columns": []}
        for column_index in range(int(row_group.num_columns or 0)):
            column = row_group.column(column_index)
            path = ".".join(str(part) for part in list(column.path_in_schema or []) if part)
            stats_obj = getattr(column, "statistics", None)
            stats = {
                "null_count": int(getattr(stats_obj, "null_count", 0) or 0) if stats_obj is not None else 0,
                "distinct_count": int(getattr(stats_obj, "distinct_count", 0) or 0) if stats_obj is not None and getattr(stats_obj, "distinct_count", None) is not None else None,
            }
            if path:
                current = column_stats.setdefault(
                    path,
                    {
                        "num_values": 0,
                        "null_count": 0,
                        "distinct_count": None,
                        "files_present": 0,
                        "row_groups_present": 0,
                    },
                )
                current["row_groups_present"] += 1
                current["files_present"] = 1
                if getattr(column, "num_values", None) is not None:
                    current["num_values"] += max(0, int(column.num_values or 0))
                current["null_count"] += max(0, int(stats["null_count"] or 0))
                if stats["distinct_count"] is not None:
                    distinct_count = max(0, int(stats["distinct_count"] or 0))
                    previous = current.get("distinct_count")
                    if previous is None:
                        current["distinct_count"] = distinct_count
                    else:
                        current["distinct_count"] = min(int(previous), distinct_count)
            row_group_dict["columns"].append(
                {
                    "meta_data": {
                        "path_in_schema": list(column.path_in_schema or []),
                        "num_values": int(getattr(column, "num_values", 0) or 0),
                        "statistics": stats,
                    }
                }
            )
        row_groups.append(row_group_dict)
    row_count = max(0, int(getattr(file_metadata, "num_rows", 0) or 0))
    if row_count <= 0 and row_groups:
        row_count = sum(max(0, int(row_group.get("num_rows") or 0)) for row_group in row_groups)
    if row_count <= 0 and column_stats:
        fallback_counts = [max(0, int(values.get("num_values") or 0)) for values in column_stats.values() if values.get("num_values") is not None]
        if fallback_counts:
            row_count = max(fallback_counts)
    schema_signature = _schema_signature(columns)
    return _ParquetFileMetadata(
        num_rows=row_count,
        columns=columns,
        schema_signature=schema_signature,
        schema_hash=schema_signature[:64],
        row_group_count=len(row_groups),
        column_stats=column_stats,
    )


def _metadata_has_structural_value(metadata: _ParquetFileMetadata | None) -> bool:
    if metadata is None:
        return False
    return bool(metadata.columns) or bool(metadata.num_rows is not None and metadata.num_rows > 0)


def _build_parquet_file_metadata_from_footer(
    footer_bytes: bytes,
    *,
    bucket: str = "",
    region: str = "",
    key: str = "",
) -> _ParquetFileMetadata:
    try:
        import pyarrow as pa
        import pyarrow._parquet as _pq
    except Exception:  # pragma: no cover - optional dependency guard
        pa = None
        _pq = None

    if pa is not None and _pq is not None and len(footer_bytes) >= _PYARROW_FOOTER_MIN_BYTES:
        try:
            file_metadata = _pq._reconstruct_filemetadata(pa.py_buffer(footer_bytes))
            metadata = _build_parquet_file_metadata_from_pyarrow(file_metadata, bucket=bucket, region=region, key=key)
            if _metadata_has_structural_value(metadata):
                return metadata
        except Exception:
            pass

    try:
        parsed = _parse_file_metadata(footer_bytes)
    except Exception as exc:  # noqa: BLE001
        raise _S3ObjectReadError(
            bucket=bucket,
            region=region,
            key=key,
            operation="read_footer",
            status_code=_HTTP_422_STATUS,
            code="parquet_metadata_parse_failed",
            message="Failed to parse parquet footer metadata",
            detail="Failed to parse parquet footer metadata",
            response_body=None,
        ) from exc
    columns = _flatten_schema(list(parsed.get("schema") or []))
    signature = _schema_signature(columns)
    column_stats: dict[str, dict[str, Any]] = {}
    row_groups = list(parsed.get("row_groups") or [])
    for row_group in list(parsed.get("row_groups") or []):
        for column_chunk in list(row_group.get("columns") or []):
            meta_data = dict(column_chunk.get("meta_data") or {})
            path = ".".join(str(part) for part in list(meta_data.get("path_in_schema") or []) if part)
            if not path:
                continue
            stats = dict(meta_data.get("statistics") or {})
            current = column_stats.setdefault(
                path,
                {
                    "num_values": 0,
                    "null_count": 0,
                    "distinct_count": None,
                    "files_present": 0,
                    "row_groups_present": 0,
                },
            )
            current["row_groups_present"] += 1
            current["files_present"] = 1
            if meta_data.get("num_values") is not None:
                current["num_values"] += max(0, int(meta_data.get("num_values") or 0))
            if stats.get("null_count") is not None:
                current["null_count"] += max(0, int(stats.get("null_count") or 0))
            if stats.get("distinct_count") is not None:
                distinct_count = max(0, int(stats.get("distinct_count") or 0))
                previous = current.get("distinct_count")
                if previous is None:
                    current["distinct_count"] = distinct_count
                else:
                    current["distinct_count"] = min(int(previous), distinct_count)
    row_count = max(0, int(parsed.get("num_rows") or 0))
    if row_count <= 0 and row_groups:
        row_count = sum(max(0, int(row_group.get("num_rows") or 0)) for row_group in row_groups)
    if row_count <= 0 and column_stats:
        fallback_counts = [max(0, int(values.get("num_values") or 0)) for values in column_stats.values() if values.get("num_values") is not None]
        if fallback_counts:
            row_count = max(fallback_counts)
    return _ParquetFileMetadata(
        num_rows=row_count,
        columns=columns,
        schema_signature=signature,
        schema_hash=signature[:64],
        row_group_count=len(row_groups),
        column_stats=column_stats,
    )


def _pyarrow_s3_parquet_metadata(
    *,
    bucket: str,
    region: str,
    key: str,
    credentials: dict[str, str],
) -> _ParquetFileMetadata | None:
    try:
        import pyarrow.fs as pa_fs
        import pyarrow.parquet as pq
    except Exception:  # pragma: no cover - optional dependency guard
        return None

    access_key_id = credentials.get("aws_access_key_id") or None
    secret_access_key = credentials.get("aws_secret_access_key") or None
    session_token = credentials.get("aws_session_token") or None
    if not access_key_id or not secret_access_key:
        return None
    try:
        filesystem = pa_fs.S3FileSystem(
            access_key=access_key_id,
            secret_key=secret_access_key,
            session_token=session_token,
            region=region or None,
            force_virtual_addressing=True,
        )
        object_path = f"{bucket}/{key.lstrip('/')}"
        with filesystem.open_input_file(object_path) as file_obj:
            parquet_file = pq.ParquetFile(file_obj)
            return _build_parquet_file_metadata_from_pyarrow(parquet_file.metadata, bucket=bucket, region=region, key=key)
    except Exception:  # noqa: BLE001
        return None


def _pyarrow_parquet_metadata_from_bytes(
    payload: bytes,
    *,
    bucket: str = "",
    region: str = "",
    key: str = "",
) -> _ParquetFileMetadata | None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception:  # pragma: no cover - optional dependency guard
        return None
    try:
        parquet_file = pq.ParquetFile(pa.BufferReader(payload))
        metadata = _build_parquet_file_metadata_from_pyarrow(parquet_file.metadata, bucket=bucket, region=region, key=key)
        return metadata if _metadata_has_structural_value(metadata) else None
    except Exception:  # noqa: BLE001
        return None


def _pyarrow_parquet_row_count(payload: bytes) -> int | None:
    metadata = _pyarrow_parquet_metadata_from_bytes(payload)
    return metadata.num_rows if metadata is not None else None


def _pyarrow_s3_parquet_row_count(
    *,
    bucket: str,
    region: str,
    key: str,
    credentials: dict[str, str],
) -> int | None:
    try:
        import pyarrow.fs as pa_fs
        import pyarrow.parquet as pq
    except Exception:  # pragma: no cover - optional dependency guard
        return None

    access_key_id = credentials.get("aws_access_key_id") or None
    secret_access_key = credentials.get("aws_secret_access_key") or None
    session_token = credentials.get("aws_session_token") or None
    if not access_key_id or not secret_access_key:
        return None
    try:
        filesystem = pa_fs.S3FileSystem(
            access_key=access_key_id,
            secret_key=secret_access_key,
            session_token=session_token,
            region=region or None,
            force_virtual_addressing=True,
        )
        object_path = f"{bucket}/{key.lstrip('/')}"
        with filesystem.open_input_file(object_path) as file_obj:
            parquet_file = pq.ParquetFile(file_obj)
            return int(getattr(parquet_file.metadata, "num_rows", 0) or 0)
    except Exception:  # noqa: BLE001
        return None


def _recover_parquet_row_count_with_pyarrow(
    metadata: _ParquetFileMetadata,
    *,
    bucket: str,
    region: str,
    key: str,
    credentials: dict[str, str],
    request_runner=_aws_request_bytes,
    started_at: float | None = None,
) -> _ParquetFileMetadata:
    if metadata.columns and metadata.num_rows > 0:
        return metadata
    try:
        _, full_body = _read_s3_object_bytes(
            bucket=bucket,
            region=region,
            key=key,
            credentials=credentials,
            request_runner=request_runner,
        )
    except _S3ObjectReadError:
        return metadata
    except HTTPException:
        return metadata
    pyarrow_metadata = _pyarrow_parquet_metadata_from_bytes(full_body, bucket=bucket, region=region, key=key)
    if pyarrow_metadata is not None and _metadata_has_structural_value(pyarrow_metadata):
        logger.info(
            "Data Lake parquet metadata recovered via pyarrow fallback",
            extra={
                "bucket": bucket,
                "region": region,
                "key": key,
                "operation": "get_object",
                "fallback_used": True,
                "duration_ms": int((time.perf_counter() - started_at) * 1000) if started_at is not None else None,
                "row_count": pyarrow_metadata.num_rows,
                "column_count": len(pyarrow_metadata.columns),
            },
        )
        return pyarrow_metadata
    pyarrow_s3_row_count = _pyarrow_s3_parquet_row_count(
        bucket=bucket,
        region=region,
        key=key,
        credentials=credentials,
    )
    if pyarrow_s3_row_count and pyarrow_s3_row_count > 0:
        logger.info(
            "Data Lake parquet row count recovered via pyarrow s3fs fallback",
            extra={
                "bucket": bucket,
                "region": region,
                "key": key,
                "operation": "get_object",
                "fallback_used": True,
                "duration_ms": int((time.perf_counter() - started_at) * 1000) if started_at is not None else None,
                "row_count": pyarrow_s3_row_count,
            },
        )
        return replace(metadata, num_rows=pyarrow_s3_row_count)
    return metadata


def _recover_table_row_count_from_samples(
    *,
    sample_entries: list[dict[str, Any]],
    bucket: str,
    region: str,
    credentials: dict[str, str],
    request_runner=_aws_request_bytes,
) -> int | None:
    total = 0
    recovered = False
    for sample in sample_entries:
        key = str(sample.get("key") or "")
        if not key or not is_parquet_key(key):
            continue
        try:
            _, full_body = _read_s3_object_bytes(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
                request_runner=request_runner,
            )
        except _S3ObjectReadError:
            continue
        except HTTPException:
            continue
        row_count = _pyarrow_parquet_row_count(full_body)
        if row_count is None or row_count <= 0:
            row_count = _pyarrow_s3_parquet_row_count(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
            )
        if row_count is None or row_count <= 0:
            continue
        total += int(row_count)
        recovered = True
    return total if recovered and total > 0 else None


def _read_s3_object_head(
    *,
    bucket: str,
    region: str,
    key: str,
    credentials: dict[str, str],
    request_runner=_aws_request,
) -> tuple[dict[str, str], str]:
    url = _s3_object_url(bucket=bucket, region=region, key=key)
    response = request_runner(method="HEAD", url=url, region=region, service="s3", credentials=credentials)
    if response.status_code not in {200, 206}:
        body = response.body if isinstance(response.body, str) else response.body.decode("utf-8", errors="replace")
        error = _classify_s3_object_error(
            bucket=bucket,
            region=region,
            key=key,
            operation="head_object",
            status_code=response.status_code,
            body=body,
        )
        logger.warning(
            "Data Lake parquet head_object failed",
            extra={
                "bucket": bucket,
                "region": region,
                "key": key,
                "operation": "head_object",
                "status_code": response.status_code,
                "error_category": error.category,
                "error_code": error.code,
                "error_message": error.message,
                "response_body": body,
            },
        )
        raise error
    return response.headers, response.body


def _read_s3_object_bytes(
    *,
    bucket: str,
    region: str,
    key: str,
    credentials: dict[str, str],
    request_runner=_aws_request_bytes,
    byte_range: tuple[int, int] | None = None,
    suffix_length: int | None = None,
) -> tuple[dict[str, str], bytes]:
    url = _s3_object_url(bucket=bucket, region=region, key=key)
    extra_headers = None
    if byte_range is not None:
        extra_headers = {"range": f"bytes={byte_range[0]}-{byte_range[1]}"}
    elif suffix_length is not None and suffix_length > 0:
        extra_headers = {"range": f"bytes=-{suffix_length}"}
    status_code, headers, content = request_runner(
        method="GET",
        url=url,
        region=region,
        service="s3",
        credentials=credentials,
        extra_headers=extra_headers,
    )
    if status_code not in {200, 206}:
        body = content.decode("utf-8", errors="replace")
        error = _classify_s3_object_error(
            bucket=bucket,
            region=region,
            key=key,
            operation="get_object",
            status_code=status_code,
            body=body,
        )
        logger.warning(
            "Data Lake parquet get_object failed",
            extra={
                "bucket": bucket,
                "region": region,
                "key": key,
                "operation": "get_object",
                "byte_range": byte_range,
                "suffix_length": suffix_length,
                "status_code": status_code,
                "error_category": error.category,
                "error_code": error.code,
                "error_message": error.message,
                "response_body": body,
            },
        )
        raise error
    return headers, content


def _read_parquet_footer_metadata(
    *,
    bucket: str,
    region: str,
    key: str,
    credentials: dict[str, str],
    request_runner=_aws_request_bytes,
) -> _ParquetFileMetadata:
    started_at = time.perf_counter()
    head_issue: _S3ObjectReadError | None = None
    tail_issue: _S3ObjectReadError | None = None
    full_issue: _S3ObjectReadError | None = None
    full_body: bytes | None = None
    content_length: int | None = None

    try:
        head_headers, _head_body = _read_s3_object_head(
            bucket=bucket,
            region=region,
            key=key,
            credentials=credentials,
            request_runner=_aws_request,
        )
        content_length = _content_length_from_headers(head_headers)
    except _S3ObjectReadError as exc:
        head_issue = exc
    except HTTPException as exc:  # pragma: no cover - defensive compatibility with tests/monkeypatches
        detail = str(exc.detail) if exc.detail is not None else str(exc)
        head_issue = _S3ObjectReadError(
            bucket=bucket,
            region=region,
            key=key,
            operation="head_object",
            status_code=int(exc.status_code or _HTTP_422_STATUS),
            code=(detail.split(":", 1)[0].strip() if ":" in detail else "s3_object_head_failed"),
            message=(detail.split(":", 1)[1].strip() if ":" in detail else detail),
            detail=detail,
            response_body=detail,
        )
    if content_length is not None and content_length < 8:
        full_issue = _S3ObjectReadError(
            bucket=bucket,
            region=region,
            key=key,
            operation="get_object",
            status_code=_HTTP_422_STATUS,
            code="parquet_too_small",
            message="Parquet file is too small",
            detail="Parquet file is too small",
            response_body=None,
        )
    if content_length is None or content_length < 8:
        try:
            footer_headers, footer_tail = _read_s3_object_bytes(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
                request_runner=request_runner,
                suffix_length=8,
            )
            content_length = _total_length_from_content_range(footer_headers) or _content_length_from_headers(footer_headers) or len(footer_tail)
            if len(footer_tail) >= 8 and footer_tail[-4:] == _PARQUET_MAGIC:
                if content_length is not None and content_length > len(footer_tail):
                    footer_length = int.from_bytes(footer_tail[:4], "little", signed=False)
                    footer_start = content_length - 8 - footer_length
                    footer_end = content_length - 9
                    if footer_start < 0 or footer_end < footer_start:
                        raise _S3ObjectReadError(
                            bucket=bucket,
                            region=region,
                            key=key,
                            operation="get_object",
                            status_code=_HTTP_422_STATUS,
                            code="parquet_footer_invalid_length",
                            message="Invalid parquet footer length",
                            detail="Invalid parquet footer length",
                            response_body=None,
                        )
                    _, footer_bytes = _read_s3_object_bytes(
                        bucket=bucket,
                        region=region,
                        key=key,
                        credentials=credentials,
                        request_runner=request_runner,
                        byte_range=(footer_start, footer_end),
                    )
                    metadata = _build_parquet_file_metadata_from_footer(footer_bytes, bucket=bucket, region=region, key=key)
                    return _recover_parquet_row_count_with_pyarrow(
                        metadata,
                        bucket=bucket,
                        region=region,
                        key=key,
                        credentials=credentials,
                        request_runner=request_runner,
                        started_at=started_at,
                    )
                if content_length is not None and content_length == len(footer_tail):
                    full_body = footer_tail
                else:
                    raise _S3ObjectReadError(
                        bucket=bucket,
                        region=region,
                        key=key,
                        operation="get_object",
                        status_code=_HTTP_422_STATUS,
                        code="parquet_footer_invalid_length",
                        message="Invalid parquet footer length",
                        detail="Invalid parquet footer length",
                        response_body=None,
                    )
                if full_body is None:
                    raise _S3ObjectReadError(
                        bucket=bucket,
                        region=region,
                        key=key,
                        operation="get_object",
                        status_code=_HTTP_422_STATUS,
                        code="parquet_footer_invalid_length",
                        message="Invalid parquet footer length",
                        detail="Invalid parquet footer length",
                        response_body=None,
                    )
                footer_bytes = _extract_parquet_footer_bytes(full_body)
                metadata = _build_parquet_file_metadata_from_footer(footer_bytes, bucket=bucket, region=region, key=key)
                metadata = _recover_parquet_row_count_with_pyarrow(
                    metadata,
                    bucket=bucket,
                    region=region,
                    key=key,
                    credentials=credentials,
                    request_runner=request_runner,
                    started_at=started_at,
                )
                logger.info(
                    "Data Lake parquet footer read via suffix range fallback",
                    extra={
                        "bucket": bucket,
                        "region": region,
                        "key": key,
                        "operation": "get_object",
                        "fallback_used": True,
                        "head_failed": head_issue is not None,
                        "duration_ms": int((time.perf_counter() - started_at) * 1000),
                        "content_length": content_length,
                    },
                )
                return metadata
        except _S3ObjectReadError as exc:
            tail_issue = exc
        except HTTPException as exc:  # pragma: no cover - defensive compatibility with tests/monkeypatches
            detail = str(exc.detail) if exc.detail is not None else str(exc)
            tail_issue = _S3ObjectReadError(
                bucket=bucket,
                region=region,
                key=key,
                operation="get_object",
                status_code=int(exc.status_code or _HTTP_422_STATUS),
                code=(detail.split(":", 1)[0].strip() if ":" in detail else "s3_object_read_failed"),
                message=(detail.split(":", 1)[1].strip() if ":" in detail else detail),
                detail=detail,
                response_body=detail,
            )

    if content_length is not None and content_length >= 8:
        try:
            footer_headers, footer_tail = _read_s3_object_bytes(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
                request_runner=request_runner,
                byte_range=(content_length - 8, content_length - 1),
            )
            if len(footer_tail) < 8 or footer_tail[-4:] != _PARQUET_MAGIC:
                raise _S3ObjectReadError(
                    bucket=bucket,
                    region=region,
                    key=key,
                    operation="get_object",
                    status_code=_HTTP_422_STATUS,
                    code="parquet_footer_invalid",
                    message="Invalid parquet footer",
                    detail="Invalid parquet footer",
                    response_body=footer_tail.decode("utf-8", errors="replace"),
                )
            footer_length = int.from_bytes(footer_tail[:4], "little", signed=False)
            footer_start = content_length - 8 - footer_length
            footer_end = content_length - 9
            if footer_start < 0 or footer_end < footer_start:
                raise _S3ObjectReadError(
                    bucket=bucket,
                    region=region,
                    key=key,
                    operation="get_object",
                    status_code=_HTTP_422_STATUS,
                    code="parquet_footer_invalid_length",
                    message="Invalid parquet footer length",
                    detail="Invalid parquet footer length",
                    response_body=None,
                )
            _, footer_bytes = _read_s3_object_bytes(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
                request_runner=request_runner,
                byte_range=(footer_start, footer_end),
            )
            metadata = _build_parquet_file_metadata_from_footer(footer_bytes, bucket=bucket, region=region, key=key)
            metadata = _recover_parquet_row_count_with_pyarrow(
                metadata,
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
                request_runner=request_runner,
                started_at=started_at,
            )
            logger.info(
                "Data Lake parquet footer read successfully",
                extra={
                    "bucket": bucket,
                    "region": region,
                    "key": key,
                    "operation": "get_object",
                    "fallback_used": head_issue is not None,
                    "head_failed": head_issue is not None,
                    "duration_ms": int((time.perf_counter() - started_at) * 1000),
                    "content_length": content_length,
                },
            )
            return metadata
        except _S3ObjectReadError as exc:
            tail_issue = exc
            pyarrow_metadata = _pyarrow_s3_parquet_metadata(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
            )
            if pyarrow_metadata is not None and pyarrow_metadata.num_rows > 0:
                logger.info(
                    "Data Lake parquet footer recovered via pyarrow s3 fallback after range failure",
                    extra={
                        "bucket": bucket,
                        "region": region,
                        "key": key,
                        "operation": "get_object",
                        "fallback_used": True,
                        "head_failed": head_issue is not None,
                        "duration_ms": int((time.perf_counter() - started_at) * 1000),
                        "content_length": content_length,
                        "row_count": pyarrow_metadata.num_rows,
                    },
                )
                return pyarrow_metadata
        except HTTPException as exc:  # pragma: no cover - defensive compatibility with tests/monkeypatches
            detail = str(exc.detail) if exc.detail is not None else str(exc)
            tail_issue = _S3ObjectReadError(
                bucket=bucket,
                region=region,
                key=key,
                operation="get_object",
                status_code=int(exc.status_code or _HTTP_422_STATUS),
                code=(detail.split(":", 1)[0].strip() if ":" in detail else "s3_object_read_failed"),
                message=(detail.split(":", 1)[1].strip() if ":" in detail else detail),
                detail=detail,
                response_body=detail,
            )

    if tail_issue is not None and tail_issue.category == "parquet_read":
        logger.warning(
            "Data Lake parquet footer read failed during footer parse",
            extra={
                "bucket": bucket,
                "region": region,
                "key": key,
                "operation": tail_issue.operation,
                "status_code": tail_issue.status_code,
                "error_category": tail_issue.category,
                "error_code": tail_issue.code,
                "error_message": tail_issue.message,
                "response_body": tail_issue.response_body,
                "fallback_used": head_issue is not None,
                "duration_ms": int((time.perf_counter() - started_at) * 1000),
            },
        )
        raise tail_issue

    if full_body is None:
        try:
            full_headers, full_body = _read_s3_object_bytes(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
                request_runner=request_runner,
            )
            content_length = _content_length_from_headers(full_headers) or len(full_body)
        except _S3ObjectReadError as exc:
            full_issue = exc
        except HTTPException as exc:  # pragma: no cover - defensive compatibility with tests/monkeypatches
            detail = str(exc.detail) if exc.detail is not None else str(exc)
            full_issue = _S3ObjectReadError(
                bucket=bucket,
                region=region,
                key=key,
                operation="get_object",
                status_code=int(exc.status_code or _HTTP_422_STATUS),
                code=(detail.split(":", 1)[0].strip() if ":" in detail else "s3_object_read_failed"),
                message=(detail.split(":", 1)[1].strip() if ":" in detail else detail),
                detail=detail,
                response_body=detail,
            )
    if full_body is not None:
        if len(full_body) < 8 or full_body[-4:] != _PARQUET_MAGIC:
            error = _S3ObjectReadError(
                bucket=bucket,
                region=region,
                key=key,
                operation="get_object",
                status_code=_HTTP_422_STATUS,
                code="parquet_footer_invalid",
                message="Invalid parquet footer",
                detail="Invalid parquet footer",
                response_body=full_body[-128:].decode("utf-8", errors="replace"),
            )
            logger.warning(
                "Data Lake parquet footer invalid after full object read",
                extra={
                    "bucket": bucket,
                    "region": region,
                    "key": key,
                    "operation": "get_object",
                    "fallback_used": head_issue is not None,
                    "status_code": error.status_code,
                    "error_category": error.category,
                    "error_code": error.code,
                    "error_message": error.message,
                },
            )
            raise error
        footer_bytes = _extract_parquet_footer_bytes(full_body)
        try:
            metadata = _build_parquet_file_metadata_from_footer(footer_bytes, bucket=bucket, region=region, key=key)
        except _S3ObjectReadError:
            metadata = _pyarrow_s3_parquet_metadata(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
            )
            if metadata is None:
                raise
        if metadata is None:
            metadata = _pyarrow_s3_parquet_metadata(
                bucket=bucket,
                region=region,
                key=key,
                credentials=credentials,
            )
            if metadata is None:
                raise _S3ObjectReadError(
                    bucket=bucket,
                    region=region,
                    key=key,
                    operation="get_object",
                    status_code=_HTTP_422_STATUS,
                    code="parquet_metadata_parse_failed",
                    message="Failed to parse parquet footer metadata",
                    detail="Failed to parse parquet footer metadata",
                    response_body=None,
                )
        if not _metadata_has_structural_value(metadata):
            pyarrow_metadata = _pyarrow_parquet_metadata_from_bytes(full_body, bucket=bucket, region=region, key=key)
            if pyarrow_metadata is not None and _metadata_has_structural_value(pyarrow_metadata):
                metadata = pyarrow_metadata
        logger.info(
            "Data Lake parquet footer read via full object fallback",
            extra={
                "bucket": bucket,
                "region": region,
                "key": key,
                "operation": "get_object",
                "fallback_used": head_issue is not None or tail_issue is not None,
                "head_failed": head_issue is not None,
                "duration_ms": int((time.perf_counter() - started_at) * 1000),
                "content_length": content_length,
            },
        )
        return metadata

    best_issue = tail_issue or full_issue or head_issue
    if best_issue is not None:
        pyarrow_metadata = _pyarrow_s3_parquet_metadata(
            bucket=bucket,
            region=region,
            key=key,
            credentials=credentials,
        )
        if pyarrow_metadata is not None and pyarrow_metadata.num_rows > 0:
            logger.info(
                "Data Lake parquet footer recovered via pyarrow s3 fallback after final failure",
                extra={
                    "bucket": bucket,
                    "region": region,
                    "key": key,
                    "operation": "get_object",
                    "fallback_used": True,
                    "head_failed": head_issue is not None,
                    "duration_ms": int((time.perf_counter() - started_at) * 1000),
                    "row_count": pyarrow_metadata.num_rows,
                },
            )
            return pyarrow_metadata
        logger.warning(
            "Data Lake parquet footer read failed",
            extra={
                "bucket": bucket,
                "region": region,
                "key": key,
                "operation": best_issue.operation,
                "status_code": best_issue.status_code,
                "error_category": best_issue.category,
                "error_code": best_issue.code,
                "error_message": best_issue.message,
                "response_body": best_issue.response_body,
                "fallback_used": head_issue is not None,
                "duration_ms": int((time.perf_counter() - started_at) * 1000),
            },
        )
        raise best_issue


def _freshness_payload(last_modified_at: datetime | None) -> tuple[str, str | None]:
    if last_modified_at is None:
        return "unknown", "Nenhum arquivo parquet com data de atualização foi identificado."
    if last_modified_at.tzinfo is None:
        last_modified_at = last_modified_at.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - last_modified_at
    if age <= timedelta(days=1):
        return "fresh", "Atualização observada nas últimas 24h."
    if age <= timedelta(days=7):
        return "recent", "Atualização observada na última semana."
    return "stale", "A tabela não recebeu atualização recente."


def _load_table_history(session: Session, connection_id: int, table_id: int, *, limit: int = 6) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(DataLakeTableObservation)
        .where(
            DataLakeTableObservation.connection_id == connection_id,
            DataLakeTableObservation.table_id == table_id,
        )
        .order_by(DataLakeTableObservation.created_at.desc(), DataLakeTableObservation.id.desc())
        .limit(max(1, limit))
    ).all()
    history: list[dict[str, Any]] = []
    for row in rows:
        history.append(
            {
                "observed_at": row.observed_at or row.created_at,
                "source_kind": row.source_kind,
                "freshness_status": row.freshness_status,
                "freshness_age_seconds": row.freshness_age_seconds,
                "freshness_sla_hours": row.freshness_sla_hours,
                "row_count": row.row_count,
                "row_count_method": row.row_count_method,
                "row_count_confidence": row.row_count_confidence,
                "size_total_bytes": row.size_total_bytes,
                "quality_score": row.quality_score,
                "schema_variants_count": row.schema_variants_count,
                "drift_detected": row.drift_detected,
            }
        )
    return history


def _inventory_table_query(session: Session, connection_id: int, table_id: int) -> DataLakeInventoryTable:
    row = session.scalar(
        select(DataLakeInventoryTable).where(
            DataLakeInventoryTable.connection_id == connection_id,
            DataLakeInventoryTable.id == table_id,
        )
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Lake table not found")
    return row


def _inventory_table_query_by_id(session: Session, table_id: int) -> DataLakeInventoryTable:
    row = session.scalar(
        select(DataLakeInventoryTable).where(
            DataLakeInventoryTable.id == table_id,
        )
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Lake table not found")
    return row


def get_data_lake_table_detail(
    session: Session,
    connection_id: int,
    table_id: int,
    *,
    request_runner=_aws_request_bytes,
) -> DataLakeTableDetailOut:
    connection = get_data_lake_connection_or_404(session, connection_id)
    inventory = _inventory_table_query(session, connection_id, table_id)
    inventory_payload = DataLakeInventoryTableOut.model_validate(serialize_data_lake_inventory_table(inventory))
    credentials, _mode = _aws_credentials_for_connection(connection)
    table_prefix = inventory.path_base.rstrip("/") + "/"
    sample_files = list(inventory.sample_parquet_files_json or [])
    sample_entries: list[dict[str, Any]] = []
    if sample_files:
        sample_entries = [item for item in sample_files if isinstance(item, dict)]

    if not sample_entries:
        # Fallback: re-read the prefix inventory to discover sample parquet files.
        prefix_result = _scan_table_prefix(
            bucket=connection.bucket,
            region=connection.region,
            credentials=credentials,
            layer=inventory.layer,
            table_prefix=table_prefix,
            request_runner=_aws_request,
        )
        sample_entries = list(prefix_result.get("sample_parquet_files") or [])

    parquet_metadata: list[_ParquetFileMetadata] = []
    file_items: list[DataLakeTableDetailFileOut] = []
    errors: list[str] = []
    technical_errors: list[DataLakeTableDetailErrorOut] = []
    for sample in sample_entries[:_DETAIL_SAMPLE_LIMIT]:
        key = str(sample.get("key") or "")
        if not key or not _is_parquet_key(key):
            continue
        size_bytes = int(sample.get("size") or 0)
        last_modified = sample.get("last_modified")
        try:
            metadata = _read_parquet_footer_metadata(
                bucket=connection.bucket,
                region=connection.region,
                key=key,
                credentials=credentials,
                request_runner=request_runner,
            )
            parquet_metadata.append(metadata)
            logger.info(
                "Data Lake parquet detail metadata parsed",
                extra={
                    "bucket": connection.bucket,
                    "region": connection.region,
                    "key": key,
                    "operation": "read_footer",
                    "schema_read_ok": bool(metadata.columns),
                    "row_count_read_ok": bool(metadata.num_rows and metadata.num_rows > 0),
                    "metadata_num_rows": metadata.num_rows,
                    "row_group_count": getattr(metadata, "row_group_count", None),
                    "column_count": len(metadata.columns),
                },
            )
            file_items.append(
                DataLakeTableDetailFileOut(
                    key=key,
                    size_bytes=size_bytes,
                    last_modified_at=last_modified,
                    row_count=metadata.num_rows,
                    schema_signature=metadata.schema_signature,
                    is_sample=True,
                )
            )
        except _S3ObjectReadError as exc:
            errors.append(exc.note())
            technical_errors.append(DataLakeTableDetailErrorOut.model_validate(exc.as_payload()))
            file_items.append(
                DataLakeTableDetailFileOut(
                    key=key,
                    size_bytes=size_bytes,
                    last_modified_at=last_modified,
                    row_count=None,
                    schema_signature=None,
                    is_sample=True,
                )
            )
        except HTTPException as exc:  # pragma: no cover - defensive compatibility with tests/monkeypatches
            detail = str(exc.detail) if exc.detail is not None else str(exc)
            errors.append(f"{key}: {detail}")
            technical_errors.append(
                DataLakeTableDetailErrorOut.model_validate(
                    {
                        "bucket": connection.bucket,
                        "region": connection.region,
                        "key": key,
                        "operation": "get_object",
                        "category": "unknown",
                        "status_code": int(exc.status_code or _HTTP_422_STATUS),
                        "code": (detail.split(":", 1)[0].strip() if ":" in detail else None),
                        "message": (detail.split(":", 1)[1].strip() if ":" in detail else detail),
                        "detail": detail,
                        "response_body": detail,
                    }
                )
            )
            file_items.append(
                DataLakeTableDetailFileOut(
                    key=key,
                    size_bytes=size_bytes,
                    last_modified_at=last_modified,
                    row_count=None,
                    schema_signature=None,
                    is_sample=True,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{key}: {exc}")
            technical_errors.append(
                DataLakeTableDetailErrorOut.model_validate(
                    {
                        "bucket": connection.bucket,
                        "region": connection.region,
                        "key": key,
                        "operation": "get_object",
                        "category": "unknown",
                        "status_code": None,
                        "code": "unexpected_error",
                        "message": str(exc),
                        "detail": str(exc),
                        "response_body": None,
                    }
                )
            )
            file_items.append(
                DataLakeTableDetailFileOut(
                    key=key,
                    size_bytes=size_bytes,
                    last_modified_at=last_modified,
                    row_count=None,
                    schema_signature=None,
                    is_sample=True,
                )
            )

    signatures = {item.schema_signature for item in parquet_metadata if item.schema_signature}
    exact_coverage = bool(parquet_metadata) and inventory.parquet_files_count <= len(sample_entries)
    row_count_available = False
    if parquet_metadata:
        primary = parquet_metadata[0]
        columns = [DataLakeTableDetailColumnOut.model_validate(column) for column in primary.columns]
        if exact_coverage:
            row_count = sum(item.num_rows or 0 for item in parquet_metadata)
            row_count_method = "exact"
            row_count_confidence = "exact"
        else:
            sample_total = sum(item.num_rows or 0 for item in parquet_metadata)
            average_rows = round(sample_total / max(1, len(parquet_metadata)))
            row_count = max(0, int(average_rows * max(1, inventory.parquet_files_count)))
            row_count_method = "estimated"
            row_count_confidence = "estimated"
        if (row_count is None or row_count <= 0) and sample_entries:
            recovered_row_count = _recover_table_row_count_from_samples(
                sample_entries=sample_entries,
                bucket=connection.bucket,
                region=connection.region,
                credentials=credentials,
                request_runner=request_runner,
            )
            if recovered_row_count is not None and recovered_row_count > 0:
                row_count = recovered_row_count
                row_count_method = "exact"
                row_count_confidence = "exact"
            else:
                row_count = None
                row_count_method = "unavailable"
                row_count_confidence = "unknown"
        row_count_available = row_count is not None and row_count > 0
        if not row_count_available:
            row_count_method = "unavailable"
            row_count_confidence = "unknown"
        schema_status = "exact" if exact_coverage and len(signatures) == 1 else ("variant" if len(signatures) > 1 else "estimated")
        schema_message = (
            "Schema consolidado a partir dos arquivos amostrados."
            if len(signatures) == 1
            else "Foram detectadas variações no schema dos arquivos amostrados."
            if len(signatures) > 1
            else "Schema não pôde ser consolidado com segurança."
        )
    else:
        columns = []
        row_count = None
        row_count_method = "unavailable"
        row_count_confidence = "unknown"
        schema_status = "unavailable"
        schema_message = "Nenhum arquivo parquet válido pôde ser lido."

    quality_snapshot = calculate_data_lake_table_quality(
        connection=connection,
        inventory=inventory,
        sample_entries=sample_entries,
        columns=[column.model_dump() for column in columns],
        parquet_metadata=parquet_metadata,
        errors=errors,
        exact_coverage=exact_coverage,
        row_count=row_count,
        row_count_method=row_count_method,
        row_count_confidence=row_count_confidence,
    )
    freshness_status = str(quality_snapshot["freshness_status"])
    freshness_detail = quality_snapshot["freshness_detail"]
    technical_notes = list(errors)
    if parquet_metadata:
        if exact_coverage and row_count_available:
            technical_notes.append("A tabela possuía um número pequeno de arquivos parquet, então a leitura foi tratada como exata.")
        else:
            technical_notes.append("A leitura foi amostrada para reduzir o custo no S3; row count e schema são consolidados a partir de arquivos de referência.")
        if len(signatures) > 1:
            technical_notes.append("Há divergência de schema entre os arquivos parquet amostrados.")
        if not row_count_available:
            technical_notes.append("A volumetria não pôde ser consolidada com segurança a partir dos footers parquet lidos.")
    else:
        technical_notes.append("Nenhum arquivo parquet válido pôde ser lido para consolidar schema e volumetria.")

    history_payload = build_data_lake_observation_payload(
        connection=connection,
        inventory=inventory,
        quality_snapshot=quality_snapshot,
        row_count=row_count,
        row_count_method=row_count_method,
        row_count_confidence=row_count_confidence,
        size_total_bytes=inventory.size_total_bytes,
    )
    try:
        session.add(DataLakeTableObservation(**history_payload))
        record_asset_row_count_snapshot(
            session,
            asset_type="datalake_table",
            asset_id=inventory.id,
            asset_name=inventory.table_name,
            asset_fqn=f"{connection.bucket}/{inventory.path_base}",
            source="s3",
            row_count=row_count,
            row_count_method=row_count_method,
            row_count_confidence=row_count_confidence,
            context_json={
                "connection_id": connection.id,
                "connection_name": connection.name,
                "bucket": connection.bucket,
                "region": connection.region,
                "path_base": inventory.path_base,
                "schema_status": schema_status,
                "exact_coverage": exact_coverage,
                "row_count_available": row_count_available,
            },
        )
        inventory.last_quality_score = quality_snapshot["quality_score"]
        inventory.last_quality_evaluated_at = history_payload["observed_at"]
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()

    history_items = _load_table_history(session, connection.id, inventory.id)
    quality_signals = [DataLakeTableDetailSignalOut.model_validate(item) for item in quality_snapshot["quality_signals"]]
    operational_signals = [DataLakeTableDetailSignalOut.model_validate(item) for item in quality_snapshot["operational_signals"]]
    quality_breakdown = [DataLakeTableDetailScoreOut.model_validate(item) for item in quality_snapshot["quality_breakdown"]]

    result = DataLakeTableDetailOut(
        inventory=DataLakeInventoryTableOut.model_validate(serialize_data_lake_inventory_table(inventory)),
        connection_id=connection.id,
        connection_name=connection.name,
        bucket=connection.bucket,
        region=connection.region,
        prefix=connection.prefix,
        sample_files=file_items,
        schema_status=schema_status,
        schema_message=schema_message,
        schema_variants_count=len(signatures),
        row_count=row_count,
        row_count_method=row_count_method,
        row_count_confidence=row_count_confidence,
        row_count_source_files=len(parquet_metadata),
        column_count=len(columns),
        columns=columns,
        partitions=[part for part in (inventory.partition_pattern_detected or "").split(",") if part],
        last_modified_at=inventory.last_modified_at,
        freshness_age_seconds=quality_snapshot["freshness_age_seconds"],
        freshness_age_hours=quality_snapshot["freshness_age_hours"],
        freshness_sla_hours=quality_snapshot["freshness_sla_hours"],
        freshness_status=freshness_status,
        freshness_detail=freshness_detail,
        quality_score=quality_snapshot["quality_score"],
        quality_breakdown=quality_breakdown,
        quality_signals=quality_signals,
        operational_signals=operational_signals,
        history=[DataLakeTableDetailHistoryOut.model_validate(item) for item in history_items],
        technical_errors=technical_errors,
        technical_notes=technical_notes,
    )
    _clear_sensitive_credentials(credentials)
    return result


def get_data_lake_table_detail_by_id(
    session: Session,
    table_id: int,
    *,
    request_runner=_aws_request_bytes,
) -> DataLakeTableDetailOut:
    inventory = _inventory_table_query_by_id(session, table_id)
    return get_data_lake_table_detail(
        session,
        inventory.connection_id,
        table_id,
        request_runner=request_runner,
    )


def list_data_lake_table_files(
    session: Session,
    table_id: int,
    *,
    page: int = 1,
    page_size: int = 25,
    request_runner=_aws_request,
) -> DataLakeTableFilesPageOut:
    inventory = _inventory_table_query_by_id(session, table_id)
    connection = get_data_lake_connection_or_404(session, inventory.connection_id)
    credentials, _mode = _aws_credentials_for_connection(connection)
    table_prefix = inventory.path_base.rstrip("/") + "/"

    try:
        entries = list_s3_objects_recursive(
            bucket=connection.bucket,
            region=connection.region,
            prefix=table_prefix,
            credentials=credentials,
            request_runner=request_runner,
        )
    except S3ListObjectsError as exc:
        _clear_sensitive_credentials(credentials)
        raise HTTPException(status_code=_HTTP_422_STATUS, detail=f"{exc.code}: {exc.message}") from exc

    file_rows: list[DataLakeTableFileOut] = []
    for entry in entries:
        file_name = entry.key.rstrip("/").split("/")[-1]
        if _is_ignored_name(file_name):
            continue
        is_parquet = is_parquet_key(entry.key)
        relative_path = entry.key[len(table_prefix) :] if entry.key.startswith(table_prefix) else entry.key
        suffix = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "arquivo"
        file_rows.append(
            DataLakeTableFileOut(
                key=entry.key,
                size_bytes=max(entry.size, 0),
                last_modified_at=entry.last_modified,
                is_parquet=is_parquet,
                file_type="parquet" if is_parquet else suffix,
                relative_path=relative_path,
            )
        )

    file_rows.sort(
        key=lambda item: (
            item.last_modified_at or datetime.min.replace(tzinfo=timezone.utc),
            item.key,
        ),
        reverse=True,
    )
    normalized_page = max(1, page)
    normalized_page_size = max(1, min(page_size, 100))
    total = len(file_rows)
    start = (normalized_page - 1) * normalized_page_size
    end = start + normalized_page_size
    items = file_rows[start:end]

    result = DataLakeTableFilesPageOut(
        items=items,
        total=total,
        page=normalized_page,
        page_size=normalized_page_size,
        has_more=end < total,
    )
    _clear_sensitive_credentials(credentials)
    return result


__all__ = ["get_data_lake_table_detail", "get_data_lake_table_detail_by_id", "list_data_lake_table_files"]
