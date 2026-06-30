from __future__ import annotations

import difflib
import hashlib
import hmac
import logging
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlencode

import httpx
from fastapi import HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.models.auth import User
from t2c_data.models.platform import DataLakeConnection
from t2c_data.features.integrations.data_lake_s3 import S3ListObjectsError, is_parquet_key, list_s3_objects_recursive, parse_data_lake_object_key, parse_prefix_list, parse_xml_response
from t2c_data.schemas.integrations import DataLakeConnectionIn, DataLakeConnectionOut, DataLakeConnectionTestOut
from t2c_data.services.audit import write_audit_log_sync

logger = logging.getLogger(__name__)

DATA_LAKE_AUTH_TYPES = {
    "access_key_secret_key",
    "access_key_secret_key_session_token",
    "role_arn",
    "default_environment",
}

DATA_LAKE_TEST_STATUSES = {
    "success",
    "access_denied",
    "bucket_not_found",
    "invalid_credentials",
    "wrong_region",
    "unexpected_error",
}


@dataclass(slots=True)
class AwsHttpResponse:
    status_code: int
    headers: dict[str, str]
    body: str


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _normalize_prefix(value: str | None) -> str | None:
    prefixes = parse_prefix_list(value)
    if not prefixes:
        return None
    return ", ".join(prefixes)


def _parse_prefix_values(value: str | None) -> list[str]:
    return parse_prefix_list(value)


def _normalize_auth_type(value: str | None) -> str:
    normalized = (value or "default_environment").strip().lower()
    return normalized if normalized in DATA_LAKE_AUTH_TYPES else "default_environment"


def _secret_values_from_connection(connection: DataLakeConnection) -> dict[str, str]:
    return connection.secret_values


def _clear_sensitive_credentials(credentials: dict[str, str] | None) -> None:
    if credentials is not None:
        credentials.clear()


def serialize_data_lake_connection(connection: DataLakeConnection) -> dict[str, Any]:
    secrets = _secret_values_from_connection(connection)
    return {
        "id": connection.id,
        "name": connection.name,
        "description": connection.description,
        "bucket": connection.bucket,
        "region": connection.region,
        "prefix": connection.prefix,
        "auth_type": connection.auth_type,
        "freshness_sla_hours_default": connection.freshness_sla_hours_default,
        "freshness_sla_hours_bronze": connection.freshness_sla_hours_bronze,
        "freshness_sla_hours_silver": connection.freshness_sla_hours_silver,
        "freshness_sla_hours_gold": connection.freshness_sla_hours_gold,
        "aws_access_key_id": connection.access_key_id,
        "role_arn": connection.role_arn,
        "aws_secret_access_key_configured": bool(secrets.get("aws_secret_access_key")),
        "aws_session_token_configured": bool(secrets.get("aws_session_token")),
        "credentials_configured": bool(
            connection.auth_type == "default_environment"
            or connection.access_key_id
            or connection.role_arn
            or secrets
        ),
        "last_test_status": connection.last_test_status,
        "last_test_message": connection.last_test_message,
        "last_test_at": connection.last_test_at,
        "is_active": connection.is_active,
        "created_by_user_id": connection.created_by_user_id,
        "created_at": connection.created_at,
        "updated_at": connection.updated_at,
    }


def get_data_lake_connection_or_404(session: Session, connection_id: int) -> DataLakeConnection:
    connection = session.get(DataLakeConnection, connection_id)
    if connection is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Lake connection not found")
    return connection


def list_data_lake_connections(session: Session) -> list[dict[str, Any]]:
    items = session.scalars(select(DataLakeConnection).order_by(DataLakeConnection.created_at.desc(), DataLakeConnection.id.desc())).all()
    return [serialize_data_lake_connection(item) for item in items]


def _apply_payload(connection: DataLakeConnection, payload: DataLakeConnectionIn) -> None:
    normalized_name = _normalize_text(payload.name)
    normalized_bucket = _normalize_text(payload.bucket)
    normalized_region = _normalize_text(payload.region)
    normalized_prefix = _normalize_prefix(payload.prefix)
    auth_type = _normalize_auth_type(payload.auth_type)

    if not normalized_name:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Connection name is required")
    if not normalized_bucket:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Bucket is required")
    if not normalized_region:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Region is required")

    connection.name = normalized_name
    connection.description = _normalize_text(payload.description)
    connection.bucket = normalized_bucket
    connection.region = normalized_region
    connection.prefix = normalized_prefix
    connection.auth_type = auth_type
    connection.freshness_sla_hours_default = payload.freshness_sla_hours_default
    connection.freshness_sla_hours_bronze = payload.freshness_sla_hours_bronze
    connection.freshness_sla_hours_silver = payload.freshness_sla_hours_silver
    connection.freshness_sla_hours_gold = payload.freshness_sla_hours_gold
    connection.is_active = bool(payload.is_active)

    if auth_type == "default_environment":
        connection.access_key_id = None
        connection.role_arn = None
        connection.set_secret_values({})
        return

    connection.access_key_id = _normalize_text(payload.aws_access_key_id)
    connection.role_arn = _normalize_text(payload.role_arn) if auth_type == "role_arn" else None
    secrets = {}
    normalized_secret = _normalize_text(payload.aws_secret_access_key)
    if normalized_secret:
        secrets["aws_secret_access_key"] = normalized_secret
    normalized_token = _normalize_text(payload.aws_session_token)
    if normalized_token:
        secrets["aws_session_token"] = normalized_token
    connection.set_secret_values(secrets)


def _prepare_connection_payload(connection: DataLakeConnection) -> DataLakeConnectionIn:
    return DataLakeConnectionIn(
        name=connection.name,
        description=connection.description,
        bucket=connection.bucket,
        region=connection.region,
        prefix=connection.prefix,
        auth_type=connection.auth_type,  # type: ignore[arg-type]
        freshness_sla_hours_default=connection.freshness_sla_hours_default,
        freshness_sla_hours_bronze=connection.freshness_sla_hours_bronze,
        freshness_sla_hours_silver=connection.freshness_sla_hours_silver,
        freshness_sla_hours_gold=connection.freshness_sla_hours_gold,
        aws_access_key_id=connection.access_key_id,
        role_arn=connection.role_arn,
        is_active=connection.is_active,
    )


def create_data_lake_connection(
    session: Session,
    payload: DataLakeConnectionIn,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
) -> dict[str, Any]:
    existing = session.scalar(select(DataLakeConnection).where(DataLakeConnection.name == payload.name))
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Data Lake connection name already exists")

    connection = DataLakeConnection(created_by_user_id=current_user.id)
    _apply_payload(connection, payload)
    session.add(connection)
    session.commit()
    session.refresh(connection)
    write_audit_log_sync(
        session,
        action="integrations.data_lake.create",
        entity_type="data_lake_connection",
        entity_id=connection.id,
        after=serialize_data_lake_connection(connection),
        metadata={
            "name": connection.name,
            "bucket": connection.bucket,
            "region": connection.region,
            "auth_type": connection.auth_type,
            "secret_fields_configured": sorted(_secret_values_from_connection(connection).keys()),
        },
        **audit_kwargs,
    )
    session.commit()
    return serialize_data_lake_connection(connection)


def update_data_lake_connection(
    session: Session,
    connection_id: int,
    payload: DataLakeConnectionIn,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
) -> dict[str, Any]:
    connection = get_data_lake_connection_or_404(session, connection_id)
    before = serialize_data_lake_connection(connection)
    existing_secret_values = connection.secret_values
    existing_access_key_id = connection.access_key_id
    existing_role_arn = connection.role_arn
    incoming = payload.model_dump()
    if not incoming.get("aws_secret_access_key"):
        incoming["aws_secret_access_key"] = existing_secret_values.get("aws_secret_access_key")
    if not incoming.get("aws_session_token"):
        incoming["aws_session_token"] = existing_secret_values.get("aws_session_token")
    if not incoming.get("aws_access_key_id"):
        incoming["aws_access_key_id"] = existing_access_key_id
    if not incoming.get("role_arn"):
        incoming["role_arn"] = existing_role_arn
    _apply_payload(connection, DataLakeConnectionIn.model_validate(incoming))
    session.add(connection)
    session.commit()
    session.refresh(connection)
    write_audit_log_sync(
        session,
        action="integrations.data_lake.update",
        entity_type="data_lake_connection",
        entity_id=connection.id,
        before=before,
        after=serialize_data_lake_connection(connection),
        metadata={
            "name": connection.name,
            "bucket": connection.bucket,
            "region": connection.region,
            "auth_type": connection.auth_type,
            "secret_fields_configured": sorted(_secret_values_from_connection(connection).keys()),
        },
        **audit_kwargs,
    )
    session.commit()
    return serialize_data_lake_connection(connection)


def delete_data_lake_connection(
    session: Session,
    connection_id: int,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
) -> Response:
    connection = get_data_lake_connection_or_404(session, connection_id)
    before = serialize_data_lake_connection(connection)
    session.delete(connection)
    session.commit()
    write_audit_log_sync(
        session,
        action="integrations.data_lake.delete",
        entity_type="data_lake_connection",
        entity_id=connection_id,
        before=before,
        metadata={"name": connection.name, "bucket": connection.bucket, "region": connection.region},
        **audit_kwargs,
    )
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _env_credential(name: str) -> str:
    """Read an AWS_* credential from the server environment ONLY when explicitly enabled.

    Using the host's ambient IAM credentials lets an app-admin act with the server's cloud
    identity, so it is opt-in via DATALAKE_ALLOW_DEFAULT_ENV_CREDENTIALS (default off)."""
    if not settings.datalake_allow_default_env_credentials:
        return ""
    return _normalize_text(os.getenv(name))


def _aws_credentials_for_connection(connection: DataLakeConnection) -> tuple[dict[str, str], str]:
    secrets = _secret_values_from_connection(connection)
    auth_type = _normalize_auth_type(connection.auth_type)
    if auth_type == "default_environment":
        if not settings.datalake_allow_default_env_credentials:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Modo 'default_environment' desabilitado. Habilite "
                    "DATALAKE_ALLOW_DEFAULT_ENV_CREDENTIALS ou informe credenciais explícitas."
                ),
            )
        env_access_key_id = _normalize_text(os.getenv("AWS_ACCESS_KEY_ID"))
        env_secret_access_key = _normalize_text(os.getenv("AWS_SECRET_ACCESS_KEY"))
        env_session_token = _normalize_text(os.getenv("AWS_SESSION_TOKEN"))
        if not env_access_key_id or not env_secret_access_key:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="AWS default environment credentials are not available in the current runtime",
            )
        return (
            {
                "aws_access_key_id": env_access_key_id,
                "aws_secret_access_key": env_secret_access_key,
                "aws_session_token": env_session_token,
            },
            "default_environment",
        )

    access_key_id = _normalize_text(connection.access_key_id) or _env_credential("AWS_ACCESS_KEY_ID")
    secret_access_key = _normalize_text(secrets.get("aws_secret_access_key")) or _env_credential("AWS_SECRET_ACCESS_KEY")
    session_token = _normalize_text(secrets.get("aws_session_token")) or _env_credential("AWS_SESSION_TOKEN")

    if auth_type == "role_arn":
        if not _normalize_text(connection.role_arn):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Role ARN is required for this authentication mode")
        if not access_key_id or not secret_access_key:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Role assumption requires source AWS credentials",
            )
        return (
            {
                "aws_access_key_id": access_key_id,
                "aws_secret_access_key": secret_access_key,
                "aws_session_token": session_token,
            },
            "role_arn",
        )

    if not access_key_id or not secret_access_key:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="AWS access key id and secret access key are required for this authentication mode",
        )
    return (
        {
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_access_key,
            "aws_session_token": session_token,
        },
        "access_key",
    )


def _aws_canonical_uri(path: str) -> str:
    if not path:
        return "/"
    segments = path.split("/")
    encoded_segments = [quote(segment, safe="-_.~") for segment in segments]
    canonical_uri = "/".join(encoded_segments)
    return canonical_uri if canonical_uri.startswith("/") else f"/{canonical_uri}"


def _aws_sign_headers(
    *,
    method: str,
    service: str,
    region: str,
    host: str,
    canonical_uri: str,
    query_params: dict[str, Any] | None,
    body: bytes,
    credentials: dict[str, str],
    extra_headers: dict[str, str] | None = None,
) -> tuple[str, dict[str, str]]:
    amz_date = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_uri = _aws_canonical_uri(canonical_uri)
    canonical_querystring = ""
    if query_params:
        canonical_querystring = "&".join(
            f"{quote(str(key), safe='-_.~')}={quote(str(value), safe='-_.~')}"
            for key, value in sorted(query_params.items(), key=lambda item: str(item[0]))
            if value is not None and str(value) != ""
        )
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if extra_headers:
        for key, value in extra_headers.items():
            normalized_key = key.strip().lower()
            if normalized_key and value is not None:
                headers[normalized_key] = str(value).strip()
    session_token = credentials.get("aws_session_token")
    if session_token:
        headers["x-amz-security-token"] = session_token
    canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
    signed_headers = ";".join(sorted(headers))
    canonical_request = "\n".join(
        [
            method.upper(),
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signing_key = _aws_signing_key(credentials["aws_secret_access_key"], date_stamp, region, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"{algorithm} Credential={credentials['aws_access_key_id']}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    signed = {
        "Authorization": authorization,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if extra_headers:
        for key, value in extra_headers.items():
            normalized_key = key.strip().lower()
            if normalized_key and value is not None:
                signed[normalized_key] = str(value).strip()
    if session_token:
        signed["x-amz-security-token"] = session_token
    return canonical_querystring, signed


def _aws_signing_key(secret_key: str, date_stamp: str, region_name: str, service_name: str) -> bytes:
    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = _sign(f"AWS4{secret_key}".encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region_name)
    k_service = _sign(k_region, service_name)
    return _sign(k_service, "aws4_request")


def _aws_request(
    *,
    method: str,
    url: str,
    region: str,
    service: str,
    credentials: dict[str, str],
    body: bytes = b"",
    query_params: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> AwsHttpResponse:
    parsed = httpx.URL(url)
    host = parsed.host or ""
    canonical_uri = parsed.raw_path.decode("utf-8") or "/"
    canonical_querystring, signed_headers = _aws_sign_headers(
        method=method,
        service=service,
        region=region,
        host=host,
        canonical_uri=canonical_uri,
        query_params=query_params,
        body=body,
        credentials=credentials,
        extra_headers=extra_headers,
    )
    final_url = parsed.copy_with(query=canonical_querystring.encode("utf-8")) if canonical_querystring else parsed
    response = httpx.request(method, str(final_url), headers=signed_headers, content=body, timeout=15.0, follow_redirects=False)
    return AwsHttpResponse(status_code=response.status_code, headers={key.lower(): value for key, value in response.headers.items()}, body=response.text)


def _extract_error_code(body: str | None) -> str | None:
    root = parse_xml_response(body)
    if root is None:
        return None
    code = root.findtext(".//Code")
    return code.strip() if isinstance(code, str) and code.strip() else None


def _extract_error_message(body: str | None) -> str | None:
    root = parse_xml_response(body)
    if root is None:
        return None
    message = root.findtext(".//Message")
    return message.strip() if isinstance(message, str) and message.strip() else None


def _assume_role_credentials(
    *,
    region: str,
    role_arn: str,
    source_credentials: dict[str, str],
    request_runner=_aws_request,
) -> dict[str, str]:
    body = urlencode(
        {
            "Action": "AssumeRole",
            "Version": "2011-06-15",
            "RoleArn": role_arn,
            "RoleSessionName": "t2c-data-lake-test",
        }
    ).encode("utf-8")
    response = request_runner(
        method="POST",
        url=f"https://sts.{region}.amazonaws.com/",
        region=region,
        service="sts",
        credentials=source_credentials,
        body=body,
    )
    if response.status_code != 200:
        code = _extract_error_code(response.body) or "assume_role_failed"
        message = _extract_error_message(response.body) or "Role assumption failed"
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"{code}: {message}")

    root = parse_xml_response(response.body)
    if root is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid STS response")
    credentials = root.find(".//Credentials")
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="STS response did not include credentials")
    access_key_id = credentials.findtext("AccessKeyId")
    secret_access_key = credentials.findtext("SecretAccessKey")
    session_token = credentials.findtext("SessionToken")
    if not access_key_id or not secret_access_key:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="STS response missing credentials")
    return {
        "aws_access_key_id": access_key_id.strip(),
        "aws_secret_access_key": secret_access_key.strip(),
        "aws_session_token": session_token.strip() if session_token else None,
    }


def _request_sts_get_caller_identity(
    *,
    region: str,
    credentials: dict[str, str],
    request_runner=_aws_request,
) -> dict[str, str] | None:
    body = urlencode({"Action": "GetCallerIdentity", "Version": "2011-06-15"}).encode("utf-8")
    response = request_runner(
        method="POST",
        url=f"https://sts.{region}.amazonaws.com/",
        region=region,
        service="sts",
        credentials=credentials,
        body=body,
    )
    if response.status_code != 200:
        logger.warning(
            "STS GetCallerIdentity failed",
            extra={
                "region": region,
                "status_code": response.status_code,
                "response_body": response.body,
            },
        )
        return None
    root = parse_xml_response(response.body)
    if root is None:
        logger.warning(
            "STS GetCallerIdentity returned unparsable XML",
            extra={
                "region": region,
                "response_body": response.body,
            },
        )
        return None
    arn = root.findtext(".//Arn") or ""
    account = root.findtext(".//Account") or ""
    user_id = root.findtext(".//UserId") or ""
    return {
        "arn": arn.strip(),
        "account": account.strip(),
        "user_id": user_id.strip(),
    }


def _request_s3_head_bucket(
    *,
    bucket: str,
    region: str,
    credentials: dict[str, str],
    request_runner=_aws_request,
) -> AwsHttpResponse:
    url = f"https://s3.{region}.amazonaws.com/{quote(bucket, safe='')}"
    return request_runner(method="HEAD", url=url, region=region, service="s3", credentials=credentials)


def _request_s3_list_prefix(
    *,
    bucket: str,
    region: str,
    prefix: str | None,
    credentials: dict[str, str],
    request_runner=_aws_request,
    max_keys: int = 1000,
) -> AwsHttpResponse:
    query_params: dict[str, Any] = {"list-type": "2", "max-keys": str(max_keys)}
    if prefix:
        query_params["prefix"] = prefix
    url = f"https://s3.{region}.amazonaws.com/{quote(bucket, safe='')}"
    return request_runner(
        method="GET",
        url=url,
        region=region,
        service="s3",
        credentials=credentials,
        query_params=query_params,
    )


def _parse_prefix_preview(response: AwsHttpResponse, *, parent_prefix: str | None = None) -> list[dict[str, Any]]:
    root = parse_xml_response(response.body)
    if root is None:
        return []
    parent = (parent_prefix or "").strip("/")
    contents: dict[str, dict[str, Any]] = {}
    for content in root.findall(".//Contents"):
        key = (content.findtext("Key") or "").strip()
        if not key:
            continue
        if parent:
            relative = key[len(parent) + 1 :] if key.startswith(f"{parent}/") else key
        else:
            relative = key
        if "/" not in relative.rstrip("/"):
            entry = contents.setdefault(
                key.rstrip("/").split("/")[-1],
                {"prefix": key.rstrip("/"), "parquet_files_count": 0, "subfolders_count": 0, "object_count": 0},
            )
            entry["object_count"] += 1
            if is_parquet_key(key):
                entry["parquet_files_count"] += 1
    for prefix in root.findall(".//CommonPrefixes"):
        value = (prefix.findtext("Prefix") or "").strip()
        if not value:
            continue
        normalized = value.rstrip("/")
        name = normalized.split("/")[-1]
        entry = contents.setdefault(
            name,
            {"prefix": normalized, "parquet_files_count": 0, "subfolders_count": 0, "object_count": 0},
        )
        entry["subfolders_count"] += 1
    return sorted(contents.values(), key=lambda item: item["prefix"])


def _suggest_prefix(prefix: str | None, available: list[dict[str, Any]]) -> str | None:
    normalized = _normalize_prefix(prefix)
    if not normalized:
        return None
    candidates = [str(item.get("prefix") or "").rstrip("/").split("/")[-1] for item in available if item.get("prefix")]
    if not candidates:
        return None
    first_segment = normalized.split("/")[0]
    matches = difflib.get_close_matches(first_segment, candidates, n=1, cutoff=0.6)
    if not matches:
        return None
    return matches[0]


def _classify_s3_error(response: AwsHttpResponse, *, region: str) -> tuple[str, str, str | None]:
    code = _extract_error_code(response.body)
    message = _extract_error_message(response.body)
    bucket_region = response.headers.get("x-amz-bucket-region")
    if bucket_region and bucket_region.strip() and bucket_region.strip() != region:
        return "wrong_region", "A região informada não corresponde ao bucket.", message or code
    if response.status_code in {301, 302, 307} or code in {"PermanentRedirect", "AuthorizationHeaderMalformed"}:
        return "wrong_region", "A região informada não corresponde ao bucket.", message or code
    if response.status_code == 404 or code == "NoSuchBucket":
        return "bucket_not_found", "Bucket inexistente ou inacessível.", message or code
    if response.status_code in {401, 403}:
        if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken", "AccessDenied"}:
            if code == "AccessDenied":
                return "access_denied", "A credencial não possui acesso ao bucket.", message or code
            return "invalid_credentials", "Credencial AWS inválida.", message or code
        return "access_denied", "A credencial não possui acesso ao bucket.", message or code
    if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken"}:
        return "invalid_credentials", "Credencial AWS inválida.", message or code
    return "unexpected_error", "Erro inesperado ao validar a conexão.", message or code


def test_data_lake_connection_payload(
    payload: DataLakeConnectionIn,
    *,
    secret_values: dict[str, str] | None = None,
    request_runner=_aws_request,
) -> dict[str, Any]:
    auth_type = _normalize_auth_type(payload.auth_type)
    bucket = _normalize_text(payload.bucket)
    region = _normalize_text(payload.region)
    prefix = _normalize_prefix(payload.prefix)
    if not bucket or not region:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Bucket and region are required")

    credentials: dict[str, str]
    credentials_mode = auth_type
    role_arn_used: str | None = None
    caller_identity: dict[str, str] | None = None
    if auth_type == "default_environment":
        env_access_key_id = _normalize_text(os.getenv("AWS_ACCESS_KEY_ID"))
        env_secret_access_key = _normalize_text(os.getenv("AWS_SECRET_ACCESS_KEY"))
        env_session_token = _normalize_text(os.getenv("AWS_SESSION_TOKEN"))
        if not env_access_key_id or not env_secret_access_key:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="AWS default environment credentials are not available in the current runtime",
            )
        credentials = {
            "aws_access_key_id": env_access_key_id,
            "aws_secret_access_key": env_secret_access_key,
            "aws_session_token": env_session_token,
        }
    else:
        source_access_key_id = _normalize_text(payload.aws_access_key_id) or _normalize_text(os.getenv("AWS_ACCESS_KEY_ID"))
        source_secret_access_key = (
            _normalize_text(payload.aws_secret_access_key)
            or _normalize_text((secret_values or {}).get("aws_secret_access_key"))
            or _normalize_text(os.getenv("AWS_SECRET_ACCESS_KEY"))
        )
        source_session_token = _normalize_text(payload.aws_session_token) or _normalize_text(os.getenv("AWS_SESSION_TOKEN"))
        if not source_access_key_id or not source_secret_access_key:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="AWS access key id and secret access key are required for this authentication mode",
            )
        credentials = {
            "aws_access_key_id": source_access_key_id,
            "aws_secret_access_key": source_secret_access_key,
            "aws_session_token": source_session_token,
        }
        if auth_type == "role_arn":
            role_arn_used = _normalize_text(payload.role_arn)
            if not role_arn_used:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Role ARN is required for this authentication mode")
            credentials = _assume_role_credentials(region=region, role_arn=role_arn_used, source_credentials=credentials, request_runner=request_runner)

    caller_identity = _request_sts_get_caller_identity(region=region, credentials=credentials, request_runner=request_runner)

    started_at = datetime.now(timezone.utc)
    import time

    clock = time.perf_counter()
    head_response = _request_s3_head_bucket(bucket=bucket, region=region, credentials=credentials, request_runner=request_runner)
    latency_ms = int((time.perf_counter() - clock) * 1000)
    if head_response.status_code != 200:
        status_key, message, detail = _classify_s3_error(head_response, region=region)
        return {
            "ok": False,
            "status": status_key,
            "message": message,
            "detail": detail,
            "bucket": bucket,
            "region": region,
            "prefix": prefix,
            "latency_ms": latency_ms,
            "tested_at": started_at,
            "bucket_accessible": False,
            "prefix_accessible": False,
            "prefix_object_count": 0,
            "credentials_mode": credentials_mode,
            "role_arn_used": role_arn_used,
            "caller_identity_arn": (caller_identity or {}).get("arn"),
            "caller_identity_account": (caller_identity or {}).get("account"),
            "caller_identity_userid": (caller_identity or {}).get("user_id"),
        }

    prefix_accessible = True
    prefix_object_count = 0
    discovered_parquet_files_count = 0
    bucket_prefixes: list[dict[str, Any]] = []
    prefix_candidates: list[str] = []
    prefix_suggestion: str | None = None
    prefix_diagnostics: list[str] = []
    table_candidates: list[dict[str, Any]] = []
    example_paths: list[str] = []

    root_response = _request_s3_list_prefix(bucket=bucket, region=region, prefix=None, credentials=credentials, request_runner=request_runner)
    if root_response.status_code == 200:
        bucket_prefixes = _parse_prefix_preview(root_response)
        prefix_candidates = [str(item.get("prefix") or "") for item in bucket_prefixes if item.get("prefix")]
        if not bucket_prefixes:
            prefix_diagnostics.append("Nenhuma pasta compatível encontrada no bucket.")
    else:
        _, root_message, root_detail = _classify_s3_error(root_response, region=region)
        prefix_diagnostics.append(root_message)
        if root_detail:
            prefix_diagnostics.append(root_detail)

    prefix_values = _parse_prefix_values(prefix)
    if prefix and len(prefix_values) > 1:
        prefix_diagnostics.append("Múltiplos prefixos detectados: " + ", ".join(f"{item}/" for item in prefix_values))
    elif prefix_values:
        prefix_diagnostics.append(f"Busca recursiva iniciada a partir de {prefix_values[0]}/.")

    if prefix_values:
        all_objects: dict[str, Any] = {}
        try:
            for prefix_value in prefix_values:
                prefix_objects = list_s3_objects_recursive(
                    bucket=bucket,
                    region=region,
                    prefix=prefix_value,
                    credentials=credentials,
                    request_runner=request_runner,
                )
                for entry in prefix_objects:
                    all_objects[entry.key] = entry
        except S3ListObjectsError as exc:
            prefix_diagnostics.append(f"{exc.code}: {exc.message}")
            if exc.detail:
                prefix_diagnostics.append(exc.detail)
            logger.debug(
                "data_lake test prefix listing failed",
                extra={
                    "bucket": bucket,
                    "region": region,
                    "prefix": prefix,
                    "auth_mode": auth_type,
                    "role_arn_used": role_arn_used,
                    "caller_identity_arn": (caller_identity or {}).get("arn"),
                    "caller_identity_account": (caller_identity or {}).get("account"),
                    "error_code": exc.code,
                    "error_message": exc.message,
                    "error_detail": exc.detail,
                    "status_code": exc.status_code,
                    "response_body": exc.response_body,
                },
            )
            return {
                "ok": False,
                "status": exc.code,
                "message": exc.message,
                "detail": exc.detail or exc.response_body,
                "bucket": bucket,
                "region": region,
                "prefix": prefix,
                "latency_ms": latency_ms,
                "tested_at": started_at,
                "bucket_accessible": True,
                "prefix_accessible": False,
                "prefix_object_count": 0,
                "parquet_files_count": 0,
                "bucket_prefixes": bucket_prefixes,
                "prefix_candidates": prefix_candidates,
                "prefix_suggestion": prefix_suggestion,
                "prefix_diagnostics": prefix_diagnostics,
                "table_candidates": table_candidates,
                "example_paths": example_paths[:5],
                "credentials_mode": credentials_mode,
                "role_arn_used": role_arn_used,
                "caller_identity_arn": (caller_identity or {}).get("arn"),
                "caller_identity_account": (caller_identity or {}).get("account"),
                "caller_identity_userid": (caller_identity or {}).get("user_id"),
            }
        prefix_object_count = len(all_objects)
        discovered_parquet_files_count = sum(1 for entry in all_objects.values() if is_parquet_key(entry.key))
        candidate_rows: dict[str, dict[str, Any]] = {}
        for entry in all_objects.values():
            parsed = parse_data_lake_object_key(entry.key)
            if parsed is None:
                continue
            candidate = candidate_rows.setdefault(
                parsed.path_base,
                {
                    "layer": parsed.layer,
                    "table_name": parsed.table_name,
                    "path_base": parsed.path_base,
                    "files_count": 0,
                    "parquet_files_count": 0,
                    "size_total_bytes": 0,
                    "last_modified_at": None,
                    "has_partitions": False,
                    "partition_pattern_detected": None,
                    "example_path": parsed.key,
                },
            )
            candidate["files_count"] += 1
            if is_parquet_key(entry.key):
                candidate["parquet_files_count"] += 1
                candidate["size_total_bytes"] += max(entry.size, 0)
            if entry.last_modified and (
                candidate["last_modified_at"] is None or entry.last_modified > candidate["last_modified_at"]
            ):
                candidate["last_modified_at"] = entry.last_modified
            if parsed.partition_segments:
                candidate["has_partitions"] = True
                patterns: set[str] = set(str(candidate["partition_pattern_detected"] or "").split(",") if candidate["partition_pattern_detected"] else [])
                for segment in parsed.partition_segments:
                    if "=" in segment:
                        patterns.add("key_value")
                    elif len(segment) == 10 and segment.count("-") == 2:
                        patterns.add("date_path")
                    else:
                        patterns.add("partitioned")
                patterns.discard("")
                candidate["partition_pattern_detected"] = ",".join(sorted(patterns)) if patterns else candidate["partition_pattern_detected"]
            if candidate["example_path"] == parsed.key and parsed.key not in example_paths:
                example_paths.append(parsed.key)
        table_candidates = sorted(candidate_rows.values(), key=lambda item: (item["layer"], item["table_name"], item["path_base"]))
        if prefix_object_count > 0:
            prefix_diagnostics.append(f"Objetos encontrados na busca recursiva: {prefix_object_count}.")
            prefix_diagnostics.append(f"Arquivos parquet encontrados: {discovered_parquet_files_count}.")
        if table_candidates:
            prefix_diagnostics.append(
                "Tabelas candidatas detectadas: " + ", ".join(f"{item['layer']}/{item['table_name']}" for item in table_candidates[:10])
            )
            prefix_diagnostics.append(f"Exemplo de caminho encontrado: {table_candidates[0]['example_path']}")
        if prefix_object_count <= 0:
            prefix_diagnostics.append("Nenhum objeto encontrado no prefixo informado.")
            prefix_diagnostics.append("Nenhum arquivo parquet encontrado no prefixo informado.")
        elif discovered_parquet_files_count <= 0:
            prefix_diagnostics.append("Foram encontrados objetos, mas nenhum arquivo parquet válido no prefixo informado.")
        if prefix and bucket_prefixes:
            prefix_suggestion = _suggest_prefix(prefix, bucket_prefixes)
            if not prefix_suggestion and prefix_object_count <= 0:
                detected_prefixes = ", ".join(f"{str(item.get('prefix') or '').rstrip('/')}/" for item in bucket_prefixes[:5])
                prefix_diagnostics.append(f"Prefixos encontrados no bucket: {detected_prefixes}.")
                prefix_diagnostics.append("Nenhuma pasta compatível encontrada para o prefixo informado.")
            if prefix_suggestion:
                prefix_diagnostics.append(f"Você quis dizer {prefix_suggestion}/?")
    elif prefix is not None:
        prefix_diagnostics.append("O prefixo informado não pôde ser normalizado.")
    if prefix_object_count <= 0 and not prefix_diagnostics:
        prefix_diagnostics.append("Nenhum arquivo parquet encontrado no prefixo informado.")
    logger.debug(
        "data_lake test diagnostics",
        extra={
            "bucket": bucket,
            "region": region,
            "prefix": prefix,
            "auth_mode": auth_type,
            "role_arn_used": role_arn_used,
            "caller_identity_arn": (caller_identity or {}).get("arn"),
            "caller_identity_account": (caller_identity or {}).get("account"),
            "prefix_object_count": prefix_object_count,
            "parquet_files_count": discovered_parquet_files_count,
            "table_candidates_count": len(table_candidates),
            "example_paths": example_paths[:5],
        },
    )
    return {
        "ok": True,
        "status": "success",
        "message": "Conexão validada com sucesso.",
        "detail": None,
        "bucket": bucket,
        "region": region,
        "prefix": prefix,
        "latency_ms": latency_ms,
        "tested_at": started_at,
        "bucket_accessible": True,
        "prefix_accessible": prefix_accessible,
        "prefix_object_count": prefix_object_count,
        "parquet_files_count": discovered_parquet_files_count,
        "bucket_prefixes": bucket_prefixes,
        "prefix_candidates": prefix_candidates,
        "prefix_suggestion": prefix_suggestion,
        "prefix_diagnostics": prefix_diagnostics,
        "table_candidates": table_candidates,
        "example_paths": example_paths[:5],
        "credentials_mode": credentials_mode,
        "role_arn_used": role_arn_used,
        "caller_identity_arn": (caller_identity or {}).get("arn"),
        "caller_identity_account": (caller_identity or {}).get("account"),
        "caller_identity_userid": (caller_identity or {}).get("user_id"),
    }


def test_data_lake_connection(
    session: Session,
    connection_id: int,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
    request_runner=_aws_request,
) -> dict[str, Any]:
    connection = get_data_lake_connection_or_404(session, connection_id)
    payload = _prepare_connection_payload(connection)
    try:
        result = test_data_lake_connection_payload(payload, secret_values=connection.secret_values, request_runner=request_runner)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive fallback for unexpected transport bugs
        logger.exception(
            "Unexpected error while testing Data Lake connection",
            extra={
                "connection_id": connection.id,
                "bucket": connection.bucket,
                "region": connection.region,
                "prefix": connection.prefix,
                "auth_type": connection.auth_type,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro inesperado ao validar a conexão com o Data Lake. Verifique credenciais, bucket e região.",
        ) from exc
    connection.last_test_status = str(result["status"])
    connection.last_test_message = str(result["message"])
    connection.last_test_at = result["tested_at"]
    session.add(connection)
    session.commit()
    write_audit_log_sync(
        session,
        action="integrations.data_lake.test",
        entity_type="data_lake_connection",
        entity_id=connection.id,
        metadata={
            "name": connection.name,
            "bucket": connection.bucket,
            "region": connection.region,
            "status": result["status"],
            "message": result["message"],
            "prefix": connection.prefix,
        },
        **audit_kwargs,
    )
    session.commit()
    return result


def delete_data_lake_connection_safe(
    session: Session,
    connection_id: int,
    *,
    current_user: User,
    audit_kwargs: dict[str, Any],
) -> Response:
    return delete_data_lake_connection(session, connection_id, current_user=current_user, audit_kwargs=audit_kwargs)


__all__ = [
    "AwsHttpResponse",
    "create_data_lake_connection",
    "delete_data_lake_connection_safe",
    "get_data_lake_connection_or_404",
    "list_data_lake_connections",
    "serialize_data_lake_connection",
    "test_data_lake_connection",
    "test_data_lake_connection_payload",
    "update_data_lake_connection",
]
