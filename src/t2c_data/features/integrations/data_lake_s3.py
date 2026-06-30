from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from xml.etree import ElementTree as ET

try:  # Prefer defusedxml to harden against XML entity-expansion attacks from S3-compatible endpoints.
    from defusedxml.ElementTree import fromstring as _xml_fromstring
except ImportError:  # pragma: no cover - falls back to stdlib (no external-entity resolution by default)
    from xml.etree.ElementTree import fromstring as _xml_fromstring


DATA_LAKE_LAYERS = ("bronze", "silver", "gold")
_MULTI_PREFIX_SPLIT_RE = re.compile(r"[,\n;]+")
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class S3ObjectEntry:
    key: str
    size: int
    last_modified: datetime | None


@dataclass(slots=True)
class DataLakeObjectPath:
    key: str
    layer: str
    table_name: str
    path_base: str
    partition_segments: tuple[str, ...]


class S3ListObjectsError(RuntimeError):
    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        prefix: str | None,
        status_code: int,
        code: str,
        message: str,
        detail: str | None,
        response_body: str | None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.bucket = bucket
        self.region = region
        self.prefix = prefix
        self.status_code = status_code
        self.code = code
        self.message = message
        self.detail = detail
        self.response_body = response_body


def normalize_prefix(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped.strip("/")


def parse_prefix_list(value: str | None) -> list[str]:
    normalized = normalize_prefix(value)
    if not normalized:
        return []
    parts = [normalize_prefix(part) for part in _MULTI_PREFIX_SPLIT_RE.split(value or "")]
    cleaned = [part for part in parts if part]
    if cleaned:
        deduped: list[str] = []
        seen: set[str] = set()
        for item in cleaned:
            if item not in seen:
                seen.add(item)
                deduped.append(item)
        return deduped
    return [normalized]


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


def parse_xml_response(body: str | None) -> ET.Element | None:
    if not body:
        return None
    try:
        root = _xml_fromstring(body)
    except Exception:  # noqa: BLE001 - ParseError / defusedxml EntitiesForbidden etc.
        return None
    for element in root.iter():
        if isinstance(element.tag, str) and "}" in element.tag:
            element.tag = element.tag.rsplit("}", 1)[1]
    return root


def classify_s3_list_error(*, response_status_code: int, response_body: str | None, region: str) -> tuple[str, str, str | None]:
    root = parse_xml_response(response_body)
    code = None if root is None else root.findtext(".//Code")
    message = None if root is None else root.findtext(".//Message")
    bucket_region = None
    if root is not None:
        bucket_region = root.findtext(".//Region")
        if bucket_region is None:
            bucket_region = root.findtext(".//BucketRegion")
    normalized_code = (code or "").strip()
    normalized_message = (message or "").strip() or None
    normalized_bucket_region = (bucket_region or "").strip() or None
    if normalized_bucket_region and normalized_bucket_region != region:
        return "wrong_region", "A região informada não corresponde ao bucket.", normalized_message or normalized_code
    if response_status_code in {301, 302, 307} or normalized_code in {"PermanentRedirect", "AuthorizationHeaderMalformed"}:
        return "wrong_region", "A região informada não corresponde ao bucket.", normalized_message or normalized_code
    if response_status_code == 404 or normalized_code == "NoSuchBucket":
        return "bucket_not_found", "Bucket inexistente ou inacessível.", normalized_message or normalized_code
    if response_status_code in {401, 403}:
        if normalized_code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken", "AccessDenied"}:
            if normalized_code == "AccessDenied":
                return "access_denied", "A credencial não possui acesso ao bucket.", normalized_message or normalized_code
            return "invalid_credentials", "Credencial AWS inválida.", normalized_message or normalized_code
        return "access_denied", "A credencial não possui acesso ao bucket.", normalized_message or normalized_code
    if normalized_code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken"}:
        return "invalid_credentials", "Credencial AWS inválida.", normalized_message or normalized_code
    return "unexpected_error", "Erro inesperado ao validar a conexão.", normalized_message or normalized_code


def is_parquet_key(key: str) -> bool:
    return key.lower().endswith(".parquet")


def looks_like_partition_segment(segment: str) -> bool:
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


def parse_data_lake_object_key(key: str) -> DataLakeObjectPath | None:
    normalized = (key or "").strip().strip("/")
    if not normalized:
        return None
    segments = [segment for segment in normalized.split("/") if segment]
    if len(segments) < 2:
        return None
    layer = segments[0].lower()
    if layer not in DATA_LAKE_LAYERS:
        return None
    if len(segments) < 3:
        return None
    directories = segments[1:-1]
    if not directories:
        return None

    partition_start = next((index for index, segment in enumerate(directories) if looks_like_partition_segment(segment)), None)
    if partition_start is None:
        path_directories = directories
        table_name = directories[-1]
    else:
        path_directories = directories[:partition_start]
        if not path_directories:
            return None
        table_name = path_directories[-1]

    partition_segments = tuple(segment for segment in directories[partition_start:] if partition_start is not None) if partition_start is not None else ()
    path_base = "/".join((layer, *path_directories))
    return DataLakeObjectPath(
        key=normalized,
        layer=layer,
        table_name=table_name,
        path_base=path_base,
        partition_segments=partition_segments,
    )


def list_s3_objects_recursive(
    *,
    bucket: str,
    region: str,
    credentials: dict[str, str],
    request_runner,
    prefix: str | None = None,
    max_keys: int = 1000,
) -> list[S3ObjectEntry]:
    query_params: dict[str, Any] = {
        "list-type": "2",
        "max-keys": str(max_keys),
    }
    normalized_prefix = normalize_prefix(prefix)
    if normalized_prefix:
        query_params["prefix"] = f"{normalized_prefix}/"

    continuation_token: str | None = None
    objects: list[S3ObjectEntry] = []
    while True:
        if continuation_token:
            query_params["continuation-token"] = continuation_token
        else:
            query_params.pop("continuation-token", None)
        response = request_runner(
            method="GET",
            url=f"https://s3.{region}.amazonaws.com/{bucket}",
            region=region,
            service="s3",
            credentials=credentials,
            query_params=query_params,
        )
        if response.status_code != 200:
            code, message, detail = classify_s3_list_error(
                response_status_code=response.status_code,
                response_body=response.body,
                region=region,
            )
            logger.warning(
                "S3 list_objects_v2 returned non-200 response",
                extra={
                    "bucket": bucket,
                    "region": region,
                    "prefix": normalized_prefix,
                    "status_code": response.status_code,
                    "error_code": code,
                    "error_message": message,
                    "error_detail": detail,
                    "response_body": response.body,
                },
            )
            raise S3ListObjectsError(
                bucket=bucket,
                region=region,
                prefix=normalized_prefix,
                status_code=response.status_code,
                code=code,
                message=message,
                detail=detail,
                response_body=response.body,
            )
        root = parse_xml_response(response.body)
        if root is None:
            logger.warning(
                "S3 list_objects_v2 returned unparsable XML",
                extra={
                    "bucket": bucket,
                    "region": region,
                    "prefix": normalized_prefix,
                    "response_body": response.body,
                },
            )
            raise S3ListObjectsError(
                bucket=bucket,
                region=region,
                prefix=normalized_prefix,
                status_code=response.status_code,
                code="unexpected_error",
                message="Erro inesperado ao interpretar a resposta do S3.",
                detail=response.body,
                response_body=response.body,
            )
        page_objects: list[S3ObjectEntry] = []
        for content in root.findall(".//Contents"):
            key = (content.findtext("Key") or "").strip()
            if not key:
                continue
            size_text = (content.findtext("Size") or "0").strip()
            try:
                size = int(size_text)
            except ValueError:
                size = 0
            objects.append(
                S3ObjectEntry(
                    key=key,
                    size=size,
                    last_modified=_parse_s3_datetime(content.findtext("LastModified")),
                )
            )
            page_objects.append(objects[-1])
        declared_key_count = root.findtext(".//KeyCount")
        is_truncated = (root.findtext(".//IsTruncated") or "false").strip().lower() == "true"
        next_token = (root.findtext(".//NextContinuationToken") or "").strip() or None
        logger.debug(
            "S3 list_objects_v2 page parsed",
            extra={
                "bucket": bucket,
                "region": region,
                "prefix": normalized_prefix,
                "declared_key_count": declared_key_count,
                "key_count": len(page_objects),
                "is_truncated": is_truncated,
                "next_token": next_token,
                "first_keys": [entry.key for entry in page_objects[:5]],
                "status_code": response.status_code,
            },
        )
        if not is_truncated or not next_token:
            break
        continuation_token = next_token
    return objects
