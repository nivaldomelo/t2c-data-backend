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

from t2c_data.features.integrations.data_lake import create_data_lake_connection
from t2c_data.features.integrations.data_lake_governance import update_data_lake_inventory_table_governance
from t2c_data.features.integrations.data_lake_operations import load_data_lake_operations_summary, load_data_lake_troubleshooting
from t2c_data.features.integrations.data_lake_scheduler import run_data_lake_scan_scheduler_cycle
from t2c_data.features.integrations.data_lake_schedules import (
    get_data_lake_scan_schedule,
    list_data_lake_scan_schedules,
    scheduler_status_snapshot,
    upsert_data_lake_scan_schedule,
)
from t2c_data.models import Base, DataLakeInventoryTable, DataLakeTableObservation, IntegrationSyncJob
from t2c_data.models.catalog import DataOwner
from t2c_data.schemas.integrations import DataLakeConnectionIn, DataLakeInventoryTableGovernanceIn, DataLakeScanScheduleIn

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


def _disable_audit(monkeypatch) -> None:
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_governance.write_audit_log_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_schedules.write_audit_log_sync", lambda *_args, **_kwargs: None)


def test_data_lake_governance_update_and_operations_summary(monkeypatch) -> None:
    _disable_audit(monkeypatch)
    db = _build_session()
    user = SimpleNamespace(id=71)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-governance",
            description="Integração para governança",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    owner = DataOwner(name="Analyst Owner", email="owner@example.com", area="Analytics", description="Responsável", is_active=True)
    db.add(owner)
    db.commit()
    db.refresh(owner)

    now = datetime.now(timezone.utc)
    table = DataLakeInventoryTable(
        connection_id=connection["id"],
        layer="bronze",
        table_name="orders",
        path_base="bronze/orders",
        files_count=2,
        parquet_files_count=2,
        non_parquet_files_count=0,
        size_total_bytes=1024,
        last_modified_at=now,
        has_partitions=True,
        partition_pattern_detected="key_value",
        status_scan="scanned",
        data_last_scan_at=now,
        sample_parquet_files_json=[{"key": "bronze/orders/file-1.parquet", "size": 512, "last_modified": now.isoformat()}],
        scan_run_id=None,
        error_message=None,
    )
    db.add(table)
    db.commit()
    db.refresh(table)

    observation = DataLakeTableObservation(
        connection_id=connection["id"],
        table_id=table.id,
        source_kind="detail",
        observed_at=now,
        freshness_status="fresh",
        freshness_age_seconds=3600,
        freshness_sla_hours=24,
        quality_score=93.5,
        row_count=2048,
        row_count_method="exact",
        row_count_confidence="exact",
        size_total_bytes=1024,
        schema_variants_count=1,
        null_columns_count=0,
        missing_columns_count=0,
        unreadable_files_count=0,
        drift_detected=False,
    )
    db.add(observation)
    db.commit()

    updated = update_data_lake_inventory_table_governance(
        db,
        connection["id"],
        table.id,
        DataLakeInventoryTableGovernanceIn(
            data_owner_id=owner.id,
            domain_name="Vendas",
            description="Tabela de pedidos do Data Lake",
            classification="internal",
            criticality="high",
            is_monitored=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    assert updated.catalog_ready is True
    assert updated.governance_status == "ready"
    assert updated.data_owner_id == owner.id
    assert updated.is_monitored is True

    summary = load_data_lake_operations_summary(db, connection["id"])
    assert summary.tables_total == 1
    assert summary.tables_scanned == 1
    assert summary.average_quality_score == 93.5
    assert summary.layer_summaries[0].layer == "bronze"

    troubleshooting = load_data_lake_troubleshooting(db, connection["id"])
    assert troubleshooting.connection_id == connection["id"]
    assert troubleshooting.status in {"ok", "attention"}


def test_data_lake_scan_schedule_lifecycle(monkeypatch) -> None:
    _disable_audit(monkeypatch)
    db = _build_session()
    user = SimpleNamespace(id=72)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-scheduler",
            description="Integração agendada",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix="silver",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    schedule = upsert_data_lake_scan_schedule(
        db,
        connection["id"],
        DataLakeScanScheduleIn(
            schedule_mode="daily",
            schedule_enabled=True,
            schedule_time="08:15",
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    assert schedule.connection_id == connection["id"]
    assert schedule.schedule_enabled is True
    assert schedule.schedule_summary

    schedules = list_data_lake_scan_schedules(db, connection["id"])
    assert len(schedules) == 1
    assert schedules[0].schedule_mode == "daily"

    snapshot = scheduler_status_snapshot(db)
    assert snapshot["scheduler_name"] == "data_lake_scan"
    assert snapshot["scheduled_sources_total"] == 1


def test_data_lake_scheduler_enqueues_job_instead_of_running_scan(monkeypatch) -> None:
    _disable_audit(monkeypatch)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_scheduler._advisory_lock", lambda _session: True)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_scheduler._release_advisory_lock", lambda _session: None)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_scheduler._scheduler_status_table_exists", lambda _session: False)
    db = _build_session()
    session_factory = sessionmaker(bind=db.get_bind(), autoflush=False, autocommit=False, class_=Session)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_scheduler.SessionLocal", session_factory)
    user = SimpleNamespace(id=73)

    connection = create_data_lake_connection(
        db,
        DataLakeConnectionIn(
            name="lake-scheduler-queued",
            description="Integração agendada por fila",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix="silver",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )

    upsert_data_lake_scan_schedule(
        db,
        connection["id"],
        DataLakeScanScheduleIn(
            schedule_mode="interval",
            schedule_enabled=True,
            schedule_every_minutes=5,
        ),
        current_user=user,
        audit_kwargs={"user_id": user.id},
    )
    schedule = get_data_lake_scan_schedule(db, connection["id"])
    assert schedule is not None
    schedule.schedule_next_run_at = None
    db.add(schedule)
    db.commit()

    summary = run_data_lake_scan_scheduler_cycle(force=True)

    assert summary["processed_schedules"] == 1
    assert summary["processed"][0]["status"] == "queued"
    assert summary["processed"][0]["job_id"] is not None

    job = db.scalar(
        select(IntegrationSyncJob).where(
            IntegrationSyncJob.source == "s3",
            IntegrationSyncJob.job_type == "inventory_scan",
            IntegrationSyncJob.target_id == connection["id"],
        )
    )
    assert job is not None
    assert job.status == "queued"
