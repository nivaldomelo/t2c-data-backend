from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import HTTPException
import pyarrow as pa
import pyarrow.parquet as pq
os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.integrations.data_lake import create_data_lake_connection
from t2c_data.features.integrations.data_lake_detail import _aws_request_bytes, _build_parquet_file_metadata_from_footer, _read_s3_object_bytes, _read_s3_object_head, _s3_object_url, get_data_lake_table_detail
from t2c_data.models import Base, DataLakeInventoryTable
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


def _create_detail_inventory(db: Session, connection: dict[str, object]) -> DataLakeInventoryTable:
    observed_at = datetime.now(timezone.utc) - timedelta(hours=2)
    scanned_at = observed_at + timedelta(minutes=10)

    inventory = DataLakeInventoryTable(
        connection_id=int(connection["id"]),
        layer="bronze",
        table_name="clientes",
        path_base="bronze/clientes",
        files_count=1,
        parquet_files_count=1,
        non_parquet_files_count=0,
        size_total_bytes=1024,
        last_modified_at=observed_at,
        has_partitions=True,
        partition_pattern_detected="key_value",
        status_scan="scanned",
        data_last_scan_at=scanned_at,
        sample_parquet_files_json=[
            {
                "key": "bronze/clientes/ano=2026/mes=04/dia=22/clientes_20260422T195219_200.parquet",
                "size": 1024,
                "last_modified": observed_at.isoformat(),
            }
        ],
        scan_run_id=None,
        error_message=None,
    )
    db.add(inventory)
    db.commit()
    db.refresh(inventory)
    return inventory


def _fake_parquet_metadata(num_rows: int = 11) -> dict[str, object]:
    return {
        "num_rows": num_rows,
        "schema": [
            {"num_children": 1},
            {"name": "id", "type": 1, "repetition_type": 0, "num_children": 0},
        ],
        "row_groups": [
            {
                "num_rows": num_rows,
                "columns": [
                    {
                        "meta_data": {
                            "path_in_schema": ["id"],
                            "num_values": num_rows,
                            "statistics": {"null_count": 0},
                        }
                    }
                ],
            }
        ],
    }


def test_data_lake_table_detail_uses_sample_metadata(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=31)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-detail",
            description="Conexão para detalhe",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix=None,
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    observed_at = datetime.now(timezone.utc) - timedelta(hours=2)
    scanned_at = observed_at + timedelta(minutes=10)

    inventory = DataLakeInventoryTable(
        connection_id=connection["id"],
        layer="bronze",
        table_name="orders",
        path_base="bronze/orders",
        files_count=2,
        parquet_files_count=2,
        non_parquet_files_count=0,
        size_total_bytes=2048,
        last_modified_at=observed_at,
        has_partitions=True,
        partition_pattern_detected="key_value",
        status_scan="scanned",
        data_last_scan_at=scanned_at,
        sample_parquet_files_json=[
            {
                "key": "bronze/orders/ano=2026/mes=04/file-1.parquet",
                "size": 1024,
                "last_modified": observed_at.isoformat(),
            },
            {
                "key": "bronze/orders/ano=2026/mes=04/file-2.parquet",
                "size": 1024,
                "last_modified": (observed_at + timedelta(minutes=5)).isoformat(),
            },
        ],
        scan_run_id=None,
        error_message=None,
    )
    db.add(inventory)
    db.commit()
    db.refresh(inventory)

    class DummyMetadata:
        def __init__(self, num_rows: int, columns: list[dict[str, object]], signature: str) -> None:
            self.num_rows = num_rows
            self.columns = columns
            self.schema_signature = signature

    def fake_metadata(*, bucket, region, key, credentials, request_runner=None):  # noqa: ANN001
        if key.endswith("file-1.parquet"):
            return DummyMetadata(
                120,
                [
                    {
                        "path": "order_id",
                        "name": "order_id",
                        "physical_type": "INT64",
                        "logical_type": None,
                        "repetition_type": "REQUIRED",
                        "nullable": False,
                        "is_suspicious": False,
                    },
                    {
                        "path": "customer_id",
                        "name": "customer_id",
                        "physical_type": "INT64",
                        "logical_type": None,
                        "repetition_type": "OPTIONAL",
                        "nullable": True,
                        "is_suspicious": False,
                    },
                ],
                "order_id:INT64:REQUIRED:0|customer_id:INT64:OPTIONAL:1",
            )
        return DummyMetadata(
            80,
            [
                {
                    "path": "order_id",
                    "name": "order_id",
                    "physical_type": "INT64",
                    "logical_type": None,
                    "repetition_type": "REQUIRED",
                    "nullable": False,
                    "is_suspicious": False,
                },
                {
                    "path": "customer_id",
                    "name": "customer_id",
                    "physical_type": "INT64",
                    "logical_type": None,
                    "repetition_type": "OPTIONAL",
                    "nullable": True,
                    "is_suspicious": False,
                },
            ],
            "order_id:INT64:REQUIRED:0|customer_id:INT64:OPTIONAL:1",
        )

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_parquet_footer_metadata", fake_metadata)

    detail = get_data_lake_table_detail(db, connection["id"], inventory.id)

    assert detail.inventory.id == inventory.id
    assert detail.schema_status == "exact"
    assert detail.row_count == 200
    assert detail.row_count_method == "exact"
    assert detail.column_count == 2
    assert detail.sample_files[0].key.endswith("file-1.parquet")
    assert detail.sample_files[0].row_count == 120
    assert detail.freshness_status in {"fresh", "recent"}
    assert any(signal.key == "row_count_method" for signal in detail.quality_signals)


def test_data_lake_table_detail_falls_back_when_head_object_fails(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=32)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-fallback",
            description=None,
            bucket="datalake-t2c-data-integracao",
            region="us-east-2",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    inventory = _create_detail_inventory(db, connection)

    def fake_head(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=422, detail="s3_object_head_failed: Object head failed")

    def fake_bytes(*, byte_range=None, suffix_length=None, **_kwargs):  # noqa: ANN001
        if suffix_length == 8:
            return (
                {"content-range": "bytes 1016-1023/1024", "content-length": "8"},
                b"\x0c\x00\x00\x00PAR1",
            )
        if byte_range == (1004, 1015):
            return {}, b"footer-bytes"
        raise AssertionError(f"Unexpected byte read: {byte_range} / {suffix_length}")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_head", fake_head)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_bytes", fake_bytes)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._parse_file_metadata", lambda _payload: _fake_parquet_metadata(11))

    detail = get_data_lake_table_detail(db, connection["id"], inventory.id)

    assert detail.schema_status == "exact"
    assert detail.row_count == 11
    assert detail.row_count_method == "exact"
    assert detail.row_count_source_files == 1
    assert detail.column_count == 1
    assert detail.sample_files[0].row_count == 11
    assert all("s3_object_head_failed" not in note for note in detail.technical_notes)


def test_data_lake_detail_aws_request_bytes_accepts_query_string(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_sign_headers(**_kwargs):  # noqa: ANN001
        return "prefix=bronze%2F&list-type=2", {"host": "example.amazonaws.com"}

    def fake_request(method, url, headers, content, timeout, follow_redirects):  # noqa: ANN001
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        return SimpleNamespace(status_code=200, headers={"content-length": "0"}, content=b"")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._aws_sign_headers", fake_sign_headers)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail.httpx.request", fake_request)

    status_code, headers, content = _aws_request_bytes(
        method="GET",
        url="https://example.amazonaws.com/bucket/key",
        region="us-east-2",
        service="s3",
        credentials={"aws_access_key_id": "AKIA", "aws_secret_access_key": "secret"},
        query_params={"prefix": "bronze/"},
    )

    assert status_code == 200
    assert headers["content-length"] == "0"
    assert content == b""
    assert captured["url"] == "https://example.amazonaws.com/bucket/key?prefix=bronze%2F&list-type=2"


def test_data_lake_detail_builds_virtual_host_s3_object_url() -> None:
    url = _s3_object_url(
        bucket="datalake-t2c-data-integracao",
        region="us-east-2",
        key="bronze/clientes/ano=2026/mes=04/dia=22/clientes.parquet",
    )

    assert url == "https://datalake-t2c-data-integracao.s3.us-east-2.amazonaws.com/bronze/clientes/ano=2026/mes=04/dia=22/clientes.parquet"


def test_data_lake_detail_sends_signed_range_header(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class DummyResponse:
        status_code = 206
        headers = {"content-length": "8", "content-range": "bytes 0-7/20"}
        content = b"PAR1PAR1"

    def fake_request(method, url, *, headers, content, timeout, follow_redirects):  # noqa: ANN001
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["content"] = content
        captured["timeout"] = timeout
        captured["follow_redirects"] = follow_redirects
        return DummyResponse()

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail.httpx.request", fake_request)

    status_code, _headers, _body = _aws_request_bytes(
        method="GET",
        url="https://bucket.s3.us-east-2.amazonaws.com/bronze/clientes/ano=2026/mes=04/file.parquet",
        region="us-east-2",
        service="s3",
        credentials={"aws_access_key_id": "AKIA_TEST", "aws_secret_access_key": "SECRET_TEST"},
        extra_headers={"range": "bytes=0-7"},
    )

    sent_headers = captured["headers"]
    assert status_code == 206
    assert isinstance(sent_headers, dict)
    assert sent_headers["range"] == "bytes=0-7"
    assert "range" in sent_headers["Authorization"]


def test_data_lake_detail_uses_row_group_row_count_when_top_level_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        "t2c_data.features.integrations.data_lake_detail._parse_file_metadata",
        lambda _payload: {
            "num_rows": 0,
            "schema": [
                {"num_children": 1},
                {"name": "id", "type": 1, "repetition_type": 0, "num_children": 0},
            ],
            "row_groups": [
                {
                    "num_rows": 12,
                    "columns": [
                        {"meta_data": {"path_in_schema": ["id"], "num_values": 12, "statistics": {"null_count": 0}}}
                    ],
                },
                {
                    "num_rows": 8,
                    "columns": [
                        {"meta_data": {"path_in_schema": ["id"], "num_values": 8, "statistics": {"null_count": 0}}}
                    ],
                },
            ],
        },
    )

    metadata = _build_parquet_file_metadata_from_footer(b"footer-bytes", bucket="bucket", region="us-east-2", key="key.parquet")

    assert metadata.num_rows == 20
    assert metadata.row_group_count == 2


def test_data_lake_detail_ignores_invalid_pyarrow_footer_reconstruction(monkeypatch) -> None:
    import pyarrow._parquet as pq_private

    class InvalidPyArrowMetadata:
        num_columns = -1
        num_row_groups = -1
        num_rows = -11

        class schema:  # noqa: N801
            @staticmethod
            def column(_index):  # noqa: ANN001
                raise AssertionError("Invalid reconstructed metadata should not expose columns")

    monkeypatch.setattr(pq_private, "_reconstruct_filemetadata", lambda _payload: InvalidPyArrowMetadata())
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._parse_file_metadata", lambda _payload: _fake_parquet_metadata(11))

    metadata = _build_parquet_file_metadata_from_footer(
        b"x" * 64,
        bucket="datalake-t2c-data-integracao",
        region="us-east-2",
        key="bronze/clientes/ano=2026/mes=04/dia=22/clientes.parquet",
    )

    assert metadata.num_rows == 11
    assert len(metadata.columns) == 1
    assert metadata.row_group_count == 1


def test_data_lake_detail_recovers_row_count_with_pyarrow_fallback(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=35)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-pyarrow",
            description=None,
            bucket="datalake-t2c-data-integracao",
            region="us-east-2",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    inventory = _create_detail_inventory(db, connection)

    table = pa.table({"id": [1, 2, 3, 4], "cliente_uuid": ["a", "b", "c", "d"]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    parquet_bytes = sink.getvalue().to_pybytes()
    footer_len = int.from_bytes(parquet_bytes[-8:-4], "little")
    footer_bytes = parquet_bytes[-8 - footer_len : -8]

    def fake_head(*_args, **_kwargs):  # noqa: ANN001
        return {"content-length": str(len(parquet_bytes))}, ""

    def fake_bytes(*, byte_range=None, suffix_length=None, **_kwargs):  # noqa: ANN001
        if suffix_length == 8:
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8}-{len(parquet_bytes) - 1}/{len(parquet_bytes)}", "content-length": "8"},
                parquet_bytes[-8:],
            )
        if byte_range == (len(parquet_bytes) - 8, len(parquet_bytes) - 1):
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8}-{len(parquet_bytes) - 1}/{len(parquet_bytes)}", "content-length": "8"},
                parquet_bytes[-8:],
            )
        if byte_range == (len(parquet_bytes) - 8 - footer_len, len(parquet_bytes) - 9):
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8 - footer_len}-{len(parquet_bytes) - 9}/{len(parquet_bytes)}", "content-length": str(len(footer_bytes))},
                footer_bytes,
            )
        return ({"content-length": str(len(parquet_bytes))}, parquet_bytes)

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_head", fake_head)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_bytes", fake_bytes)
    monkeypatch.setattr(
        "t2c_data.features.integrations.data_lake_detail._parse_file_metadata",
        lambda _payload: {
            "num_rows": 0,
            "schema": [
                {"num_children": 1},
                {"name": "id", "type": 1, "repetition_type": 0, "num_children": 0},
                {"name": "cliente_uuid", "type": 6, "repetition_type": 0, "num_children": 0},
            ],
            "row_groups": [],
        },
    )

    detail = get_data_lake_table_detail(db, connection["id"], inventory.id)

    assert detail.row_count == 4
    assert detail.row_count_method == "exact"
    assert detail.row_count_confidence == "exact"


def test_data_lake_detail_consolidates_row_count_from_real_parquet_footer(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=37)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-real-footer",
            description=None,
            bucket="datalake-t2c-data-integracao",
            region="us-east-2",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    inventory = _create_detail_inventory(db, connection)

    table = pa.table({"id": [1, 2, 3, 4], "cliente_uuid": ["a", "b", "c", "d"]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    parquet_bytes = sink.getvalue().to_pybytes()
    footer_len = int.from_bytes(parquet_bytes[-8:-4], "little")
    footer_bytes = parquet_bytes[-8 - footer_len : -8]

    def fake_head(*_args, **_kwargs):  # noqa: ANN001
        return {"content-length": str(len(parquet_bytes))}, ""

    def fake_bytes(*, byte_range=None, suffix_length=None, **_kwargs):  # noqa: ANN001
        if suffix_length == 8:
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8}-{len(parquet_bytes) - 1}/{len(parquet_bytes)}", "content-length": "8"},
                parquet_bytes[-8:],
            )
        if byte_range == (len(parquet_bytes) - 8, len(parquet_bytes) - 1):
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8}-{len(parquet_bytes) - 1}/{len(parquet_bytes)}", "content-length": "8"},
                parquet_bytes[-8:],
            )
        if byte_range == (len(parquet_bytes) - 8 - footer_len, len(parquet_bytes) - 9):
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8 - footer_len}-{len(parquet_bytes) - 9}/{len(parquet_bytes)}", "content-length": str(len(footer_bytes))},
                footer_bytes,
            )
        return ({"content-length": str(len(parquet_bytes))}, parquet_bytes)

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_head", fake_head)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_bytes", fake_bytes)

    detail = get_data_lake_table_detail(db, connection["id"], inventory.id)

    assert detail.row_count == 4
    assert detail.row_count_method == "exact"
    assert detail.row_count_confidence == "exact"
    assert detail.row_count_source_files == 1
    assert detail.sample_files[0].row_count == 4


def test_data_lake_table_detail_recovers_table_row_count_when_file_metadata_zero(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=36)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-table-rowcount",
            description=None,
            bucket="datalake-t2c-data-integracao",
            region="us-east-2",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    inventory = _create_detail_inventory(db, connection)
    inventory.parquet_files_count = 2
    inventory.files_count = 2
    inventory.sample_parquet_files_json = [
        {
            "key": "bronze/clientes/ano=2026/mes=04/dia=22/clientes_20260422T195219_200.parquet",
            "size": 1024,
            "last_modified": "2026-04-22T19:52:19+00:00",
        },
        {
            "key": "bronze/clientes/ano=2026/mes=04/dia=22/clientes_20260422T205000_200.parquet",
            "size": 1024,
            "last_modified": "2026-04-22T20:50:00+00:00",
        },
    ]
    db.add(inventory)
    db.commit()
    db.refresh(inventory)

    monkeypatch.setattr(
        "t2c_data.features.integrations.data_lake_detail._read_parquet_footer_metadata",
        lambda *_args, **_kwargs: SimpleNamespace(num_rows=0, columns=[{"path": "id", "name": "id", "physical_type": "INT64", "logical_type": None, "repetition_type": "REQUIRED", "nullable": False, "is_suspicious": False}], schema_signature="id:INT64:REQUIRED:0", row_group_count=1, column_stats={}),
    )
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._recover_table_row_count_from_samples", lambda **_kwargs: 5)

    detail = get_data_lake_table_detail(db, connection["id"], inventory.id)

    assert detail.row_count == 5
    assert detail.row_count_method == "exact"
    assert detail.row_count_confidence == "exact"


def test_data_lake_table_detail_recovers_table_row_count_with_pyarrow_s3fs_fallback(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=38)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-s3fs-rowcount",
            description=None,
            bucket="datalake-t2c-data-integracao",
            region="us-east-2",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    inventory = _create_detail_inventory(db, connection)

    table = pa.table({"id": [1, 2, 3, 4, 5, 6], "cliente_uuid": ["a", "b", "c", "d", "e", "f"]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    parquet_bytes = sink.getvalue().to_pybytes()
    footer_len = int.from_bytes(parquet_bytes[-8:-4], "little")
    footer_bytes = parquet_bytes[-8 - footer_len : -8]

    def fake_head(*_args, **_kwargs):  # noqa: ANN001
        return {"content-length": str(len(parquet_bytes))}, ""

    def fake_bytes(*, byte_range=None, suffix_length=None, **_kwargs):  # noqa: ANN001
        if suffix_length == 8:
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8}-{len(parquet_bytes) - 1}/{len(parquet_bytes)}", "content-length": "8"},
                parquet_bytes[-8:],
            )
        if byte_range == (len(parquet_bytes) - 8, len(parquet_bytes) - 1):
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8}-{len(parquet_bytes) - 1}/{len(parquet_bytes)}", "content-length": "8"},
                parquet_bytes[-8:],
            )
        if byte_range == (len(parquet_bytes) - 8 - footer_len, len(parquet_bytes) - 9):
            return (
                {"content-range": f"bytes {len(parquet_bytes) - 8 - footer_len}-{len(parquet_bytes) - 9}/{len(parquet_bytes)}", "content-length": str(len(footer_bytes))},
                footer_bytes,
            )
        return ({"content-length": str(len(parquet_bytes))}, parquet_bytes)

    class FakeFile:
        def __enter__(self):  # noqa: D401
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: D401, ANN001
            return False

    class FakeS3FileSystem:
        def __init__(self, *args, **kwargs):  # noqa: ANN001
            self.args = args
            self.kwargs = kwargs

        def open_input_file(self, path):  # noqa: ANN001
            assert path == "datalake-t2c-data-integracao/bronze/clientes/ano=2026/mes=04/dia=22/clientes_20260422T195219_200.parquet"
            return FakeFile()

    class FakeParquetFile:
        def __init__(self, _file_obj):  # noqa: ANN001
            self.metadata = SimpleNamespace(num_rows=6)

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_head", fake_head)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_bytes", fake_bytes)
    monkeypatch.setattr(
        "t2c_data.features.integrations.data_lake_detail._read_parquet_footer_metadata",
        lambda *_args, **_kwargs: SimpleNamespace(num_rows=0, columns=[{"path": "id", "name": "id", "physical_type": "INT64", "logical_type": None, "repetition_type": "REQUIRED", "nullable": False, "is_suspicious": False}], schema_signature="id:INT64:REQUIRED:0", row_group_count=1, column_stats={}),
    )
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._pyarrow_parquet_row_count", lambda _payload: 0)
    import pyarrow.fs as pa_fs
    import pyarrow.parquet as pq_mod

    monkeypatch.setattr(pa_fs, "S3FileSystem", FakeS3FileSystem)
    monkeypatch.setattr(pq_mod, "ParquetFile", FakeParquetFile)

    detail = get_data_lake_table_detail(db, connection["id"], inventory.id)

    assert detail.row_count == 6
    assert detail.row_count_method == "exact"
    assert detail.row_count_confidence == "exact"


def test_data_lake_table_detail_classifies_s3_access_and_not_found(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=33)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-errors",
            description=None,
            bucket="datalake-t2c-data-integracao",
            region="us-east-2",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    inventory = _create_detail_inventory(db, connection)

    def fake_access_denied(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=403, detail="AccessDenied: Access Denied")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_head", fake_access_denied)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_bytes", fake_access_denied)

    detail_access = get_data_lake_table_detail(db, connection["id"], inventory.id)
    assert detail_access.schema_status == "unavailable"
    assert detail_access.technical_errors[0].category == "s3_access"
    assert any("s3_access" in note for note in detail_access.technical_notes)
    assert any("AccessDenied" in note for note in detail_access.technical_notes)

    def fake_not_found(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=404, detail="NoSuchKey: The specified key does not exist.")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_head", fake_not_found)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_bytes", fake_not_found)

    detail_missing = get_data_lake_table_detail(db, connection["id"], inventory.id)
    assert detail_missing.schema_status == "unavailable"
    assert detail_missing.technical_errors[0].category == "s3_not_found"
    assert any("s3_not_found" in note for note in detail_missing.technical_notes)
    assert any("NoSuchKey" in note for note in detail_missing.technical_notes)


def test_data_lake_table_detail_classifies_invalid_parquet_footer(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV_AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")
    db = _build_session()
    user = SimpleNamespace(id=34)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **kwargs: None)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-corrupt",
            description=None,
            bucket="datalake-t2c-data-integracao",
            region="us-east-2",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    inventory = _create_detail_inventory(db, connection)

    def fake_head(*_args, **_kwargs):  # noqa: ANN001
        return {"content-length": "1024"}, ""

    def fake_bytes(*, byte_range=None, suffix_length=None, **_kwargs):  # noqa: ANN001
        if suffix_length == 8:
            return (
                {"content-range": "bytes 1016-1023/1024", "content-length": "8"},
                b"\x0c\x00\x00\x00PAR1",
            )
        if byte_range == (1016, 1023):
            return {"content-length": "8"}, b"\x0c\x00\x00\x00PAR1"
        if byte_range == (1004, 1015):
            return {}, b"invalid-footer"
        raise AssertionError(f"Unexpected byte read: {byte_range} / {suffix_length}")

    def fake_parse(_payload):  # noqa: ANN001
        raise ValueError("Corrupted parquet footer")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_head", fake_head)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._read_s3_object_bytes", fake_bytes)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_detail._parse_file_metadata", fake_parse)

    detail = get_data_lake_table_detail(db, connection["id"], inventory.id)

    assert detail.schema_status == "unavailable"
    assert detail.technical_errors[0].category == "parquet_read"
    assert any("parquet" in note.lower() for note in detail.technical_notes)
