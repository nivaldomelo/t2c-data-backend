from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.integrations.data_lake import AwsHttpResponse, create_data_lake_connection
from t2c_data.features.integrations.data_lake_detail import list_data_lake_table_files
from t2c_data.features.integrations.data_lake_inventory import _run_data_lake_inventory_scan, get_data_lake_catalog_page, get_data_lake_inventory_page, scan_data_lake_inventory
from t2c_data.features.integrations.data_lake_s3 import parse_data_lake_object_key
from t2c_data.models import Base, DataLakeInventoryScanRun, DataLakeInventoryTable, IntegrationSyncJob
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


def test_data_lake_inventory_scan_discovers_layers_and_tables(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=21)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory.write_audit_log_sync", lambda *_args, **kwargs: None)

    created = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-inventory",
            description="Inventário principal",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix=None,
            auth_type="default_environment",
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
                "url": url,
                "service": service,
                "region": region,
                "query_params": dict(query_params or {}),
            }
        )
        prefix = (query_params or {}).get("prefix", "")
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
        if method == "GET" and service == "s3" and prefix == "":
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult>
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>bronze/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                        <Size>100</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                      <Contents>
                        <Key>silver/pedidos/part-000.parquet</Key>
                        <Size>90</Size>
                        <LastModified>2026-04-19T10:00:00.000Z</LastModified>
                      </Contents>
                      <Contents>
                        <Key>gold/dashboard/part-001.parquet</Key>
                        <Size>80</Size>
                        <LastModified>2026-04-18T09:00:00.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        raise AssertionError(f"Unexpected request: {service} {method} {prefix}")

    result = _run_data_lake_inventory_scan(
        db,
        created["id"],
        current_user=user,
        audit_kwargs={"user_id": user.id},
        request_runner=fake_runner,
    )

    assert result.scan_run.status == "success"
    assert result.summary.total_tables == 3
    assert result.summary.bronze_tables == 1
    assert result.summary.silver_tables == 1
    assert result.summary.gold_tables == 1
    assert result.summary.tables_without_parquet == 0

    items = db.scalars(select(DataLakeInventoryTable).order_by(DataLakeInventoryTable.layer, DataLakeInventoryTable.table_name)).all()
    assert len(items) == 3
    assert any(item.table_name == "clientes" and item.parquet_files_count == 1 and item.has_partitions for item in items)
    assert any(item.table_name == "pedidos" and item.layer == "silver" for item in items)
    assert any(item.table_name == "dashboard" and item.layer == "gold" for item in items)

    page = get_data_lake_inventory_page(db, created["id"], page=1, page_size=2)
    assert page.total == 3
    assert len(page.items) == 2
    assert page.summary.connection_id == created["id"]

    scan_runs = db.scalars(select(DataLakeInventoryScanRun)).all()
    assert len(scan_runs) == 1
    assert scan_runs[0].status == "success"
    assert scan_runs[0].discovered_tables_count == 3

    assert all("delimiter" not in request["query_params"] for request in requests)


def test_data_lake_inventory_scan_with_bronze_prefix_discovers_clientes(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=22)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory.write_audit_log_sync", lambda *_args, **kwargs: None)

    created = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-bronze-prefix",
            description="Inventário bronze",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

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
        if method == "GET" and service == "s3" and prefix in {"bronze/", "bronze/clientes/"}:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>bronze/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                        <Size>2048</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        raise AssertionError(f"Unexpected request: {service} {method} {prefix}")

    result = _run_data_lake_inventory_scan(
        db,
        created["id"],
        current_user=user,
        audit_kwargs={"user_id": user.id},
        request_runner=fake_runner,
    )

    assert result.summary.total_tables == 1
    row = db.scalar(select(DataLakeInventoryTable))
    assert row is not None
    assert row.layer == "bronze"
    assert row.table_name == "clientes"
    assert row.path_base == "bronze/clientes"
    assert row.has_partitions is True
    assert row.parquet_files_count == 1
    assert row.size_total_bytes == 2048
    assert row.last_modified_at is not None


def test_data_lake_inventory_scan_manual_only_queues_job(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=29)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory.write_audit_log_sync", lambda *_args, **_kwargs: None)

    created = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-queued",
            description="Scan assíncrono",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    def _unexpected_run(*_args, **_kwargs):
        raise AssertionError("manual web flow must not execute the heavy Data Lake scan directly")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory._run_data_lake_inventory_scan", _unexpected_run)

    result = scan_data_lake_inventory(
        db,
        created["id"],
        current_user=user,
        audit_kwargs={"user_id": user.id},
        correlation_id="corr-data-lake-queued",
    )

    assert result.scan_run.status == "queued"
    assert result.job_id is not None
    assert result.job_status == "queued"
    assert result.correlation_id == "corr-data-lake-queued"
    assert db.scalar(select(DataLakeInventoryTable).where(DataLakeInventoryTable.connection_id == created["id"])) is None

    job = db.get(IntegrationSyncJob, result.job_id)
    assert job is not None
    assert job.status == "queued"
    assert job.payload_json["connection_id"] == created["id"]
    assert job.payload_json["scan_run_id"] == result.scan_run.id


def test_data_lake_catalog_page_and_table_files_are_populated(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=23)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory.write_audit_log_sync", lambda *_args, **kwargs: None)

    created = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-catalog",
            description="Catálogo global",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

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
        if method == "GET" and service == "s3" and prefix in {"bronze/", "bronze/clientes/"}:
            return AwsHttpResponse(
                status_code=200,
                headers={},
                body="""
                    <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
                      <IsTruncated>false</IsTruncated>
                      <Contents>
                        <Key>bronze/clientes/ano=2026/mes=04/dia=20/clientes_20260420T224632_200.parquet</Key>
                        <Size>2048</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                      <Contents>
                        <Key>bronze/clientes/ano=2026/mes=04/dia=20/_SUCCESS</Key>
                        <Size>0</Size>
                        <LastModified>2026-04-20T22:46:32.000Z</LastModified>
                      </Contents>
                    </ListBucketResult>
                """,
            )
        raise AssertionError(f"Unexpected request: {service} {method} {prefix}")

    _run_data_lake_inventory_scan(
        db,
        created["id"],
        current_user=user,
        audit_kwargs={"user_id": user.id},
        request_runner=fake_runner,
    )

    catalog = get_data_lake_catalog_page(db, page=1, page_size=25, search="clientes")
    assert catalog.total == 1
    assert catalog.items[0].connection_name == "lake-catalog"
    assert catalog.items[0].bucket == "catalog-datalake"
    assert catalog.items[0].path_base == "bronze/clientes"

    table = db.scalar(select(DataLakeInventoryTable))
    assert table is not None
    files_page = list_data_lake_table_files(db, table.id, page=1, page_size=25, request_runner=fake_runner)
    assert files_page.total == 1
    assert files_page.items[0].is_parquet is True
    assert files_page.items[0].file_type == "parquet"
    assert files_page.items[0].relative_path.endswith("clientes_20260420T224632_200.parquet")


def test_parse_data_lake_object_key_supports_bronze_raw_and_silver_domain_tables() -> None:
    bronze = parse_data_lake_object_key("bronze/clientes/ano=2026/mes=04/dia=20/clientes.parquet")
    raw = parse_data_lake_object_key("bronze/raw/clientes/ano=2026/mes=04/dia=20/clientes.parquet")
    silver = parse_data_lake_object_key("silver/financeiro/clientes/ano=2026/mes=04/dia=20/clientes.parquet")

    assert bronze is not None and bronze.layer == "bronze" and bronze.table_name == "clientes"
    assert bronze.path_base == "bronze/clientes"
    assert bronze.partition_segments == ("ano=2026", "mes=04", "dia=20")

    assert raw is not None and raw.layer == "bronze" and raw.table_name == "clientes"
    assert raw.path_base == "bronze/raw/clientes"
    assert raw.partition_segments == ("ano=2026", "mes=04", "dia=20")

    assert silver is not None and silver.layer == "silver" and silver.table_name == "clientes"
    assert silver.path_base == "silver/financeiro/clientes"
    assert silver.partition_segments == ("ano=2026", "mes=04", "dia=20")
