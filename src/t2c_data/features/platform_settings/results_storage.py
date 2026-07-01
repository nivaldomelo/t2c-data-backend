from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def is_remote_results_uri(path: str | None) -> bool:
    """True when the results/logs location is an S3 URI (`s3://` or Hadoop's `s3a://`)."""
    p = (path or "").strip().lower()
    return p.startswith("s3://") or p.startswith("s3a://")


def _parse_s3(uri: str) -> tuple[str, str]:
    normalized = uri.replace("s3a://", "s3://", 1) if uri.lower().startswith("s3a://") else uri
    parsed = urlparse(normalized)
    return parsed.netloc, parsed.path.lstrip("/")


def _s3_client():
    import boto3  # lazy: only needed when an S3 results dir is configured

    return boto3.client("s3")


def write_results_text(results_dir: str, filename: str, content: str) -> str:
    """Write a results/log file under `results_dir`, returning the full path/URI written.

    Uses S3 (boto3) when `results_dir` is an s3://|s3a:// URI so that logs are durable and
    readable by any pod (API + workers); otherwise writes to the local filesystem. AWS
    credentials come from the environment (AWS_ACCESS_KEY_ID/SECRET/REGION)."""
    if is_remote_results_uri(results_dir):
        uri = f"{results_dir.rstrip('/')}/{filename}"
        bucket, key = _parse_s3(uri)
        _s3_client().put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
        return uri
    directory = Path(results_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_text(content, encoding="utf-8")
    return str(path)


def read_results_text(path_or_uri: str) -> str:
    """Read a results/log file previously written by write_results_text (local or S3)."""
    if is_remote_results_uri(path_or_uri):
        bucket, key = _parse_s3(path_or_uri)
        obj = _s3_client().get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    return Path(path_or_uri).read_text(encoding="utf-8")
