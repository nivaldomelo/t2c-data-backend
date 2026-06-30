from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.integrations.data_lake import (
    AwsHttpResponse,
    create_data_lake_connection,
    list_data_lake_connections,
    _aws_canonical_uri,
    _aws_request,
    test_data_lake_connection as run_data_lake_connection,
    test_data_lake_connection_payload as run_data_lake_connection_test_payload,
    update_data_lake_connection,
)
from fastapi import HTTPException
from t2c_data.models import Base, DataLakeConnection
from t2c_data.schemas.integrations import DataLakeConnectionIn

if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def test_create_list_and_update_data_lake_connection_preserves_secrets(monkeypatch) -> None:
    db = _build_session()
    user = SimpleNamespace(id=11)
    audit_calls: list[dict] = []
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: audit_calls.append(kwargs))

    created = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-bronze",
            description="Conexão principal",
            bucket="catalog-bronze",
            region="sa-east-1",
            prefix="bronze",
            auth_type="access_key_secret_key",
            aws_access_key_id="AKIA_TEST",
            aws_secret_access_key="secret-one",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    assert created["name"] == "lake-bronze"
    assert created["aws_secret_access_key_configured"] is True
    assert created["credentials_configured"] is True
    assert "aws_secret_access_key" not in created

    update_data_lake_connection(
        db,
        created["id"],
        DataLakeConnectionIn(
            name="lake-bronze",
            description="Conexão principal atualizada",
            bucket="catalog-bronze",
            region="sa-east-1",
            prefix="bronze",
            auth_type="access_key_secret_key",
            aws_access_key_id="AKIA_TEST",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    connection = db.scalar(select(DataLakeConnection))
    assert connection is not None
    assert connection.secret_values["aws_secret_access_key"] == "secret-one"

    items = list_data_lake_connections(db)
    assert len(items) == 1
    assert items[0]["last_test_status"] is None
    assert len(audit_calls) >= 2


def test_data_lake_connection_test_uses_persisted_secret(monkeypatch) -> None:
    db = _build_session()
    user = SimpleNamespace(id=12)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    created = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-bronze-test",
            description=None,
            bucket="catalog-bronze",
            region="sa-east-1",
            prefix="bronze",
            auth_type="access_key_secret_key",
            aws_access_key_id="AKIA_TEST",
            aws_secret_access_key="secret-one",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    requests: list[dict] = []

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        body_text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        requests.append(
            {
                "method": method,
                "service": service,
                "region": region,
                "url": url,
                "credentials": dict(credentials),
                "query_params": dict(query_params or {}),
            }
        )
        if service == "sts" and "GetCallerIdentity" in body_text:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">
                      <GetCallerIdentityResult>
                        <Arn>arn:aws:iam::123456789012:user/t2c-data-lake</Arn>
                        <UserId>AIDATEST</UserId>
                        <Account>123456789012</Account>
                      </GetCallerIdentityResult>
                    </GetCallerIdentityResponse>
                """,
            )
        if method == "HEAD":
            return AwsHttpResponse(status_code=200, headers={}, body="")
        if service == "s3":
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>bronze/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                        <Size>100</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        raise AssertionError(f"Unexpected request: {service} {method}")

    result = run_data_lake_connection(
        db,
        created["id"],
        current_user=user,
        audit_kwargs={"user_id": user.id},
        request_runner=fake_runner,
    )

    assert result["ok"] is True
    assert any(request["service"] == "sts" for request in requests)
    assert any(request["service"] == "s3" for request in requests)
    s3_request = next(request for request in requests if request["service"] == "s3")
    assert s3_request["credentials"]["aws_access_key_id"] == "AKIA_TEST"
    assert s3_request["credentials"]["aws_secret_access_key"] == "secret-one"


def test_aws_request_handles_empty_and_filled_query(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_httpx_request(method, url, headers, content, timeout, follow_redirects):  # noqa: ANN001
        calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "content": content,
                "timeout": timeout,
                "follow_redirects": follow_redirects,
            }
        )
        return SimpleNamespace(status_code=200, headers={"X-Test": "ok"}, text="done")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake.httpx.request", fake_httpx_request)

    empty = _aws_request(
        method="GET",
        url="https://s3.sa-east-1.amazonaws.com/catalog-bronze",
        region="sa-east-1",
        service="s3",
        credentials={"aws_access_key_id": "AKIA_TEST", "aws_secret_access_key": "secret-one"},
    )
    filled = _aws_request(
        method="GET",
        url="https://s3.sa-east-1.amazonaws.com/catalog-bronze",
        region="sa-east-1",
        service="s3",
        credentials={"aws_access_key_id": "AKIA_TEST", "aws_secret_access_key": "secret-one"},
        query_params={"list-type": "2", "prefix": "bronze/orders", "delimiter": "/"},
    )

    assert empty.status_code == 200
    assert filled.status_code == 200
    assert calls[0]["url"] == "https://s3.sa-east-1.amazonaws.com/catalog-bronze"
    assert "prefix=bronze%2Forders" in calls[1]["url"]
    assert "list-type=2" in calls[1]["url"]


def test_aws_canonical_uri_encodes_reserved_path_characters() -> None:
    assert _aws_canonical_uri("/bronze/eventos_status/ano=2026/mes=04/dia=22/arquivo.parquet") == "/bronze/eventos_status/ano%3D2026/mes%3D04/dia%3D22/arquivo.parquet"


def test_data_lake_test_payload_supports_role_arn_and_prefix(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "ENV_SESSION")

    requests: list[dict] = []

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        body_text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        requests.append(
            {
                "method": method,
                "service": service,
                "region": region,
                "url": url,
                "credentials": dict(credentials),
                "query_params": dict(query_params or {}),
            }
        )
        prefix = (query_params or {}).get("prefix")
        if service == "sts" and "AssumeRole" in body_text:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <AssumeRoleResponse>
                      <AssumeRoleResult>
                        <Credentials>
                          <AccessKeyId>TEMP_AKIA</AccessKeyId>
                          <SecretAccessKey>TEMP_SECRET</SecretAccessKey>
                          <SessionToken>TEMP_TOKEN</SessionToken>
                        </Credentials>
                      </AssumeRoleResult>
                </AssumeRoleResponse>
                """,
            )
        if service == "sts" and "GetCallerIdentity" in body_text:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">
                      <GetCallerIdentityResult>
                        <Arn>arn:aws:sts::123456789012:assumed-role/T2CDataLake/t2c-data-lake-test</Arn>
                        <UserId>AIDATEMP</UserId>
                        <Account>123456789012</Account>
                      </GetCallerIdentityResult>
                    </GetCallerIdentityResponse>
                """,
            )
        if method == "HEAD":
            return AwsHttpResponse(status_code=200, headers={}, body="")
        if prefix is None:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <CommonPrefixes><Prefix>silver/</Prefix></CommonPrefixes>
                    </ListBucketResult>
                """,
            )
        if prefix == "silver/":
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>silver/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                        <Size>90</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        return AwsHttpResponse(status_code=200, headers={}, body="<ListBucketResult><KeyCount>1</KeyCount></ListBucketResult>")

    result = run_data_lake_connection_test_payload(
        DataLakeConnectionIn(
            name="lake-silver",
            description=None,
            bucket="catalog-silver",
            region="sa-east-1",
            prefix="silver",
            auth_type="role_arn",
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
            role_arn="arn:aws:iam::123456789012:role/T2CDataLake",
            is_active=True,
        ),
        request_runner=fake_runner,
    )

    assert result["ok"] is True
    assert result["status"] == "success"
    assert result["prefix_object_count"] == 1
    assert result["parquet_files_count"] == 1
    assert result["table_candidates"][0]["table_name"] == "clientes"
    assert result["example_paths"][0].endswith(".parquet")
    assert result["credentials_mode"] == "role_arn"
    assert result["role_arn_used"] == "arn:aws:iam::123456789012:role/T2CDataLake"
    assert result["caller_identity_arn"] == "arn:aws:sts::123456789012:assumed-role/T2CDataLake/t2c-data-lake-test"
    assert result["caller_identity_account"] == "123456789012"
    assert any(request["service"] == "sts" for request in requests)
    assert any(request["service"] == "s3" for request in requests)
    first_s3_request = next(request for request in requests if request["service"] == "s3")
    assert first_s3_request["credentials"]["aws_access_key_id"] == "TEMP_AKIA"


def test_data_lake_test_payload_uses_persisted_secret_fallback(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")

    requests: list[dict] = []

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        body_text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        requests.append(
            {
                "method": method,
                "service": service,
                "region": region,
                "url": url,
                "credentials": dict(credentials),
                "query_params": dict(query_params or {}),
            }
        )
        if service == "sts" and "GetCallerIdentity" in body_text:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">
                      <GetCallerIdentityResult>
                        <Arn>arn:aws:iam::123456789012:user/t2c-data-lake</Arn>
                        <UserId>AIDATEST</UserId>
                        <Account>123456789012</Account>
                      </GetCallerIdentityResult>
                    </GetCallerIdentityResponse>
                """,
            )
        if method == "HEAD":
            return AwsHttpResponse(status_code=200, headers={}, body="")
        return AwsHttpResponse(
            status_code=200,
            headers={},
            body="""
                <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                  <IsTruncated>false</IsTruncated>
                  <Contents>
                    <Key>bronze/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                    <Size>100</Size>
                    <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                  </Contents>
                </ListBucketResult>
            """,
        )

    result = run_data_lake_connection_test_payload(
        DataLakeConnectionIn(
            name="lake-saved",
            description=None,
            bucket="catalog-saved",
            region="sa-east-1",
            prefix="bronze",
            auth_type="access_key_secret_key",
            aws_access_key_id="AKIA_TEST",
            aws_secret_access_key=None,
            aws_session_token=None,
            role_arn=None,
            is_active=True,
        ),
        secret_values={"aws_secret_access_key": "secret-one"},
        request_runner=fake_runner,
    )

    assert result["ok"] is True
    assert requests[0]["credentials"]["aws_secret_access_key"] == "secret-one"
    assert result["caller_identity_account"] == "123456789012"


def test_data_lake_test_payload_surfaces_prefix_suggestions(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        body_text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        prefix = (query_params or {}).get("prefix")
        if service == "sts" and "GetCallerIdentity" in body_text:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">
                      <GetCallerIdentityResult>
                        <Arn>arn:aws:iam::123456789012:user/t2c-data-lake</Arn>
                        <UserId>AIDATEST</UserId>
                        <Account>123456789012</Account>
                      </GetCallerIdentityResult>
                    </GetCallerIdentityResponse>
                """,
            )
        if method == "HEAD":
            return AwsHttpResponse(status_code=200, headers={}, body="")
        if service == "s3" and not prefix:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <KeyCount>0</KeyCount>
                      <CommonPrefixes><Prefix>bronze/</Prefix></CommonPrefixes>
                      <CommonPrefixes><Prefix>silver/</Prefix></CommonPrefixes>
                      <CommonPrefixes><Prefix>gold/</Prefix></CommonPrefixes>
                    </ListBucketResult>
                """,
            )
        if service == "s3" and prefix in {"bonze", "bonze/"}:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                    </ListBucketResult>
                """,
            )
        raise AssertionError(f"Unexpected request: {service} {method} {prefix}")

    result = run_data_lake_connection_test_payload(
        DataLakeConnectionIn(
            name="lake-bronze",
            description=None,
            bucket="catalog-bronze",
            region="sa-east-1",
            prefix="bonze",
            auth_type="default_environment",
            is_active=True,
        ),
        request_runner=fake_runner,
    )

    assert result["ok"] is True
    assert result["prefix_object_count"] == 0
    assert result["prefix_suggestion"] == "bronze"
    assert "bronze" in result["prefix_candidates"]
    assert result["credentials_mode"] == "default_environment"
    assert result["caller_identity_arn"] == "arn:aws:iam::123456789012:user/t2c-data-lake"
    assert any("Nenhum arquivo parquet encontrado" in item for item in result["prefix_diagnostics"])
    assert any("Você quis dizer bronze/" in item for item in result["prefix_diagnostics"])
    assert any(item["prefix"] == "bronze" for item in result["bucket_prefixes"])


def test_data_lake_test_payload_surfaces_access_denied_on_recursive_listing(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        body_text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        prefix = (query_params or {}).get("prefix")
        if service == "sts" and "GetCallerIdentity" in body_text:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">
                      <GetCallerIdentityResult>
                        <Arn>arn:aws:iam::123456789012:user/t2c-data-lake</Arn>
                        <UserId>AIDATEST</UserId>
                        <Account>123456789012</Account>
                      </GetCallerIdentityResult>
                    </GetCallerIdentityResponse>
                """,
            )
        if method == "HEAD":
            return AwsHttpResponse(status_code=200, headers={}, body="")
        if service == "s3" and not prefix:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <CommonPrefixes><Prefix>bronze/</Prefix></CommonPrefixes>
                    </ListBucketResult>
                """,
            )
        if service == "s3" and prefix == "bronze/":
            return AwsHttpResponse(
                status_code=403,
                headers={},
                body="""
                    <Error xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <Code>AccessDenied</Code>
                      <Message>Access Denied</Message>
                    </Error>
                """,
            )
        raise AssertionError(f"Unexpected request: {service} {method} {prefix}")

    result = run_data_lake_connection_test_payload(
        DataLakeConnectionIn(
            name="lake-denied",
            description=None,
            bucket="catalog-denied",
            region="sa-east-1",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        request_runner=fake_runner,
    )

    assert result["ok"] is False
    assert result["status"] == "access_denied"
    assert result["prefix_accessible"] is False
    assert result["prefix_object_count"] == 0
    assert result["caller_identity_account"] == "123456789012"
    assert any("A credencial não possui acesso ao bucket" in item or "AccessDenied" in item for item in result["prefix_diagnostics"])


def test_data_lake_test_payload_ignores_get_caller_identity_failure(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")

    requests: list[dict] = []

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        body_text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        prefix = (query_params or {}).get("prefix")
        requests.append(
            {
                "method": method,
                "service": service,
                "body": body_text,
                "prefix": prefix,
            }
        )
        if method == "HEAD":
            return AwsHttpResponse(status_code=200, headers={}, body="")
        if service == "sts" and "GetCallerIdentity" in body_text:
            return AwsHttpResponse(
                status_code=403,
                headers={},
                body="<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>",
            )
        if service == "s3" and prefix is None:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <CommonPrefixes><Prefix>bronze/</Prefix></CommonPrefixes>
                    </ListBucketResult>
                """,
            )
        if service == "s3" and prefix == "bronze/":
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>bronze/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                        <Size>100</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        raise AssertionError(f"Unexpected request: {service} {method} {prefix}")

    result = run_data_lake_connection_test_payload(
        DataLakeConnectionIn(
            name="lake-get-caller-identity-failure",
            description=None,
            bucket="catalog-bronze",
            region="sa-east-1",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        request_runner=fake_runner,
    )

    assert result["ok"] is True
    assert result["status"] == "success"
    assert result["prefix_object_count"] == 1
    assert result["parquet_files_count"] == 1
    assert result["table_candidates"][0]["table_name"] == "clientes"
    assert result["caller_identity_arn"] is None
    assert any(request["service"] == "sts" for request in requests)
    assert any(request["service"] == "s3" and request["prefix"] == "bronze/" for request in requests)


def test_data_lake_test_payload_supports_multiple_explicit_prefixes(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")

    requests: list[dict] = []

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        body_text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        prefix = (query_params or {}).get("prefix")
        requests.append(
            {
                "method": method,
                "service": service,
                "prefix": prefix,
            }
        )
        if service == "sts" and "GetCallerIdentity" in body_text:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">
                      <GetCallerIdentityResult>
                        <Arn>arn:aws:iam::123456789012:user/t2c-data-lake</Arn>
                        <UserId>AIDATEST</UserId>
                        <Account>123456789012</Account>
                      </GetCallerIdentityResult>
                    </GetCallerIdentityResponse>
                """,
            )
        if method == "HEAD":
            return AwsHttpResponse(status_code=200, headers={}, body="")
        if service == "s3" and not prefix:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                    </ListBucketResult>
                """,
            )
        if service == "s3" and prefix == "bronze/":
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>bronze/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                        <Size>100</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        if service == "s3" and prefix == "silver/":
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>silver/pedidos/part-000.parquet</Key>
                        <Size>90</Size>
                        <LastModified>2026-04-19T10:00:00.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        return AwsHttpResponse(status_code=200, headers={}, body="<ListBucketResult><KeyCount>1</KeyCount></ListBucketResult>")

    result = run_data_lake_connection_test_payload(
        DataLakeConnectionIn(
            name="lake-multi-prefix",
            description=None,
            bucket="catalog-multi",
            region="sa-east-1",
            prefix="bronze, silver",
            auth_type="default_environment",
            is_active=True,
        ),
        request_runner=fake_runner,
    )

    assert result["ok"] is True
    assert result["prefix_object_count"] == 2
    assert result["parquet_files_count"] == 2
    assert {item["table_name"] for item in result["table_candidates"]} == {"clientes", "pedidos"}
    assert any(request["prefix"] == "bronze/" for request in requests)
    assert any(request["prefix"] == "silver/" for request in requests)


def test_data_lake_connection_endpoint_returns_friendly_error_on_unexpected_bug(monkeypatch) -> None:
    db = _build_session()
    user = SimpleNamespace(id=13)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.test_data_lake_connection_payload", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("boom")))

    created = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-buggy",
            description=None,
            bucket="catalog-buggy",
            region="sa-east-1",
            prefix="bronze",
            auth_type="access_key_secret_key",
            aws_access_key_id="AKIA_TEST",
            aws_secret_access_key="secret-one",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    try:
        run_data_lake_connection(
            db,
            created["id"],
            current_user=user,
            audit_kwargs={"user_id": user.id},
            request_runner=lambda **_kwargs: AwsHttpResponse(status_code=200, headers={}, body=""),
        )
    except HTTPException as exc:
        assert exc.status_code == 502
        assert "Erro inesperado ao validar a conexão" in str(exc.detail)
    else:
        raise AssertionError("Expected HTTPException to be raised")


def test_data_lake_test_payload_maps_region_errors(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")

    def fake_runner(*, method, url, region, service, credentials, body=b"", query_params=None):  # noqa: ANN001
        return AwsHttpResponse(
            status_code=301,
            headers={"x-amz-bucket-region": "us-east-1"},
            body="<Error><Code>PermanentRedirect</Code><Message>wrong region</Message></Error>",
        )

    result = run_data_lake_connection_test_payload(
        DataLakeConnectionIn(
            name="lake-gold",
            description=None,
            bucket="catalog-gold",
            region="sa-east-1",
            prefix=None,
            auth_type="default_environment",
            is_active=True,
        ),
        request_runner=fake_runner,
    )

    assert result["ok"] is False
    assert result["status"] == "wrong_region"
    assert result["bucket_accessible"] is False
