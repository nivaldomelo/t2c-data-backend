from __future__ import annotations

import json
import asyncio
import csv
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import Request
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api import platform as platform_api
from t2c_data.features.platform.cockpit_ops import (
    build_platform_cockpit_queue_page,
    build_platform_cockpit_recommended_actions,
)
from t2c_data.features.platform.job_diagnostics import diagnose_integration_job
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.platform import IntegrationSyncJob


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


def _request(path: str = "/ops/cockpit/export.csv") -> Request:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": [(b"host", b"testserver")],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "root_path": "",
    }
    return Request(scope)


def _seed_tables(db: Session) -> list[SimpleNamespace]:
    datasource = DataSource(
        name="local",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="user",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table_one = TableEntity(name="orders", table_type="table", schema=schema)
    table_two = TableEntity(name="customers", table_type="table", schema=schema)
    db.add_all([datasource, database, schema, table_one, table_two])
    db.commit()
    db.refresh(table_one)
    db.refresh(table_two)
    return [
        SimpleNamespace(
            table_id=table_one.id,
            table_name="orders",
            table_fqn="bronze.orders",
            schema_name="bronze",
            database_name="andromeda",
            datasource_name="local",
            owner_defined=False,
            open_incidents=2,
            critical_open_incidents=1,
            dq_score=65.0,
            classification_defined=False,
            has_personal_data=True,
            has_sensitive_personal_data=False,
        ),
        SimpleNamespace(
            table_id=table_two.id,
            table_name="customers",
            table_fqn="bronze.customers",
            schema_name="bronze",
            database_name="andromeda",
            datasource_name="local",
            owner_defined=True,
            open_incidents=0,
            critical_open_incidents=0,
            dq_score=95.0,
            classification_defined=True,
            has_personal_data=False,
            has_sensitive_personal_data=False,
        ),
    ]


def test_diagnose_integration_job_marks_stalled_and_overdue() -> None:
    job = SimpleNamespace(
        source="dq",
        job_type="rules",
        status="running",
        started_at=datetime.now(timezone.utc) - timedelta(hours=30),
        finished_at=None,
        next_expected_run_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )

    diagnostic = diagnose_integration_job(job)

    assert diagnostic["diagnostic_status"] == "stalled"
    assert diagnostic["diagnostic_severity"] == "critical"
    assert diagnostic["is_stalled"] is True
    assert diagnostic["is_overdue_next_run"] is True
    assert diagnostic["diagnostic_probable_cause_code"] in {"stalled_execution", "dq_spark_failed"}
    assert diagnostic["diagnostic_runbook_url"] == "/docs/runbooks/dq-failed.md"


def test_diagnose_integration_job_redacts_secret_values_from_context_and_error() -> None:
    job = SimpleNamespace(
        source="dq",
        job_type="rules",
        status="failed",
        started_at=datetime.now(timezone.utc) - timedelta(minutes=15),
        finished_at=datetime.now(timezone.utc),
        context_json={
            "error": "jdbc:postgresql://user:super-secret@db.local/catalog",
            "jdbc_password": "super-secret",
            "aws_secret_access_key": "SECRET_TEST",
        },
        error="jdbc:postgresql://user:super-secret@db.local/catalog",
    )

    diagnostic = diagnose_integration_job(job)
    rendered = json.dumps(diagnostic, ensure_ascii=False, default=str)

    assert "super-secret" not in rendered
    assert "SECRET_TEST" not in rendered
    assert "jdbc_password" not in rendered
    assert "********" in rendered


def test_queue_page_and_recommended_actions_use_backend_contract(monkeypatch) -> None:
    db = _build_session()
    tables = _seed_tables(db)

    ingestion_payload = {
        "available": True,
        "message": None,
        "generated_at": datetime.now(timezone.utc),
        "high_volume_failed_threshold_rows": 100000,
        "items": [
            {
                "table_id": tables[0].table_id,
                "table_name": tables[0].table_name,
                "table_fqn": tables[0].table_fqn,
                "schema_name": tables[0].schema_name,
                "database_name": tables[0].database_name,
                "datasource_name": tables[0].datasource_name,
                "pipeline_name": "mysql_pg_orders",
                "dag_id": "dag_orders",
                "task_name": "task_orders",
                "latest_status_label": "Falha",
                "last_status": "failed",
                "last_success_at": None,
                "last_execution_finished_at": None,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "rows_processed": 200000,
                "records_processed": 200000,
                "last_error": "Unknown column updated_at",
                "pipeline_history_href": "/ops/ingestion/history?tableId=1",
                "target_url": "/explorer?tableId=1",
            },
            {
                "table_id": tables[1].table_id,
                "table_name": tables[1].table_name,
                "table_fqn": tables[1].table_fqn,
                "schema_name": tables[1].schema_name,
                "database_name": tables[1].database_name,
                "datasource_name": tables[1].datasource_name,
                "pipeline_name": "mysql_pg_customers",
                "dag_id": "dag_customers",
                "task_name": "task_customers",
                "latest_status_label": "Sucesso",
                "last_status": "success",
                "last_success_at": datetime.now(timezone.utc).isoformat(),
                "last_execution_finished_at": datetime.now(timezone.utc).isoformat(),
                "last_run_started_at": datetime.now(timezone.utc).isoformat(),
                "last_run_finished_at": datetime.now(timezone.utc).isoformat(),
                "rows_processed": 120,
                "records_processed": 120,
                "last_error": None,
                "pipeline_history_href": "/ops/ingestion/history?tableId=2",
                "target_url": "/explorer?tableId=2",
            },
        ],
        "unmapped_items": [
            {
                "table_id": 999,
                "table_name": "events",
                "table_fqn": "bronze.events",
                "schema_name": "bronze",
                "database_name": "andromeda",
                "datasource_name": "local",
                "target_url": "/explorer?tableId=999",
                "hint": "Sem pipeline mapeado.",
            }
        ],
    }

    stalled_job = IntegrationSyncJob(
        job_key="dq:rules_scheduler",
        source="dq",
        job_type="rules_scheduler",
        trigger_mode="scheduled",
        status="running",
        started_at=datetime.now(timezone.utc) - timedelta(hours=30),
        next_expected_run_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    stalled_job.id = 42  # type: ignore[attr-defined]

    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.load_dashboard_profiles_with_fallback",
        lambda session, now, current_user=None: (tables, None),
    )
    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.load_ingestion_operational_overview_from_source",
        lambda *args, **kwargs: ingestion_payload,
    )
    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.analytics_summary",
        lambda *args, **kwargs: {
            "legacy_api_hits": 7,
            "top_legacy_modules": [{"label": "auth", "value": 7}],
        },
    )
    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.legacy_api_usage_stats_by_module",
        lambda *args, **kwargs: {"auth": {"hits_in_window": 7, "hits_total": 7, "last_hit_at": datetime.now(timezone.utc)}},
    )
    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.integration_jobs_status_snapshot",
        lambda *args, **kwargs: {
            "generated_at": datetime.now(timezone.utc),
            "total": 1,
            "running": 1,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "next_expected_run_at": None,
            "items": [
                {
                    "id": stalled_job.id,
                    "job_key": stalled_job.job_key,
                    "source": stalled_job.source,
                    "job_type": stalled_job.job_type,
                    "target_type": None,
                    "target_id": None,
                    "target_name": None,
                    "trigger_mode": stalled_job.trigger_mode,
                    "status": stalled_job.status,
                    "started_at": stalled_job.started_at,
                    "finished_at": None,
                    "next_expected_run_at": stalled_job.next_expected_run_at,
                    "diagnostic_status": "stalled",
                    "diagnostic_severity": "critical",
                    "diagnostic_label": "Travado há 1 dia",
                    "diagnostic_description": "Job travado para teste.",
                    "diagnostic_recommended_action": "Revisar job travado.",
                    "is_stalled": True,
                    "is_overdue_next_run": True,
                    "running_duration_seconds": 108000,
                }
            ],
        },
    )
    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.owner_review_due",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.privacy_review_due",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        "t2c_data.features.platform.cockpit_ops.certification_review_due",
        lambda *args, **kwargs: True,
    )

    queue_page = build_platform_cockpit_queue_page(db, current_user=None, category="operacao", page=1, page_size=1)
    assert queue_page["total"] >= 2
    assert len(queue_page["items"]) == 1
    assert queue_page["items"][0]["type"] in {"pipeline_failure", "degraded_pipeline"}

    recommended = build_platform_cockpit_recommended_actions(db, current_user=None, limit=5)
    titles = [item["title"] for item in recommended["items"]]
    assert any(title == "Revisar job travado" for title in titles)
    assert recommended["total"] >= 2


def test_platform_cockpit_export_csv_streams_bom_and_rows(monkeypatch) -> None:
    db = _build_session()
    monkeypatch.setattr(
        platform_api,
        "build_platform_cockpit_export_rows",
        lambda *args, **kwargs: [
            {
                "record_type": "recommended_action",
                "severity": "critical",
                "title": "Revisar job travado",
                "asset_name": None,
                "database": None,
                "schema": None,
                "pipeline_name": None,
                "dag_id": None,
                "task_id": None,
                "status": "actionable",
                "reason": "Job travado.",
                "impact": "Pode bloquear execuções.",
                "recommended_action": "Ver jobs",
                "route": "/ops/cockpit#jobs",
                "updated_at": datetime.now(timezone.utc),
                "metadata_json": {},
            }
        ],
    )

    response = platform_api.platform_cockpit_export_csv(
        request=_request(),
        category=None,
        status_filter=None,
        severity=None,
        q=None,
        db=db,
        current_user=SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com"),
    )

    async def _read_body() -> bytes:
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return b"".join(chunks)

    body = asyncio.run(_read_body())
    content = body.decode("utf-8-sig")
    lines = list(csv.reader(content.splitlines()))

    assert body.startswith(b"\xef\xbb\xbf")
    assert lines[0][0] == "record_type"
    assert lines[1][0] == "recommended_action"
