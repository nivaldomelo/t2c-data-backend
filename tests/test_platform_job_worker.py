from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.platform.jobs import enqueue_integration_job, finish_integration_job_record
from t2c_data.features.platform.job_worker import process_next_integration_job
from t2c_data.features.integrations.data_lake import create_data_lake_connection
from t2c_data.features.integrations.data_lake_inventory import enqueue_data_lake_inventory_scan
from t2c_data.features.scanner.application import enqueue_datasource_scan
from t2c_data.features.scanner.spark_execution import SparkDatasourceScanExecutionOutcome
from t2c_data.models import Base, DataLakeInventoryScanRun, DataSource, IntegrationSyncJob, MetabaseInstance, MetabaseSyncRun, PlatformWorkerHeartbeat, ScanRun
from t2c_data.schemas.integrations import DataLakeConnectionIn, DataLakeInventoryScanOut, DataLakeInventoryScanRunOut, DataLakeInventorySummaryOut

if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]


def _build_session_factory():
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
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def _seed_datasource(session: Session) -> DataSource:
    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="analytics",
        username="tester",
    )
    datasource.set_secret_values({"password": "super-secret"})
    session.add(datasource)
    session.commit()
    session.refresh(datasource)
    return datasource


def _seed_metabase_instance(session: Session) -> MetabaseInstance:
    instance = MetabaseInstance(
        name="metabase-prod",
        base_url="https://metabase.local",
        enabled=True,
        sync_dashboards=True,
        sync_questions=True,
        sync_collections=True,
    )
    instance.auth_secret = "secret-token"
    session.add(instance)
    session.commit()
    session.refresh(instance)
    return instance


def _seed_data_lake_connection(session: Session) -> dict:
    return create_data_lake_connection(
        session,
        DataLakeConnectionIn(
            name="lake-ops",
            description="Data Lake worker",
            bucket="catalog-datalake",
            region="sa-east-1",
            prefix="bronze",
            auth_type="default_environment",
            is_active=True,
        ),
        current_user=type("UserRef", (), {"id": 42})(),
        audit_kwargs={"user_id": 42},
    )


def test_process_next_integration_job_executes_queued_datasource_scan(monkeypatch) -> None:
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        datasource = _seed_datasource(session)
        scan_run, job = enqueue_datasource_scan(session, datasource=datasource, started_by=7, trigger_mode="manual")
        assert job is not None
        assert job.status == "queued"
        assert "password" not in str(job.payload_json)

    def _fake_execute(
        session: Session,
        *,
        datasource: DataSource,
        scan_run: ScanRun,
        started_by: int | None = None,
        integration_job_id: int | None = None,
        worker_heartbeat_at=None,
    ):
        assert datasource.id > 0
        assert started_by == 7
        assert scan_run is not None
        assert integration_job_id is not None
        assert worker_heartbeat_at is not None
        scan_run.status = "succeeded"
        scan_run.summary = {
            "tables": 5,
            "row_counts": {"failed": 1},
            "execution_engine": "spark",
            "legacy_status": "partial_success",
            "spark_app_id": "application_1234_0001",
        }
        session.add(scan_run)
        session.commit()
        session.refresh(scan_run)
        return SparkDatasourceScanExecutionOutcome(
            scan_run=scan_run,
            job_status="success",
            job_records=5,
            job_error=None,
            job_context={
                "datasource_id": datasource.id,
                "scan_run_id": scan_run.id,
                "status": "succeeded",
                "spark_app_id": "application_1234_0001",
            },
        )

    monkeypatch.setattr("t2c_data.features.platform.job_worker.execute_spark_datasource_scan", _fake_execute)

    processed = process_next_integration_job(source="datasource", job_type="scan", session_factory=SessionLocal)

    assert processed is not None
    assert processed.status == "success"
    assert processed.records_processed == 5
    assert processed.result_summary_json["legacy_status"] == "partial_success"
    assert processed.result_summary_json["execution_engine"] == "spark"

    with SessionLocal() as session:
        persisted_job = session.get(IntegrationSyncJob, job.id)
        persisted_run = session.get(ScanRun, scan_run.id)
        heartbeats = session.query(PlatformWorkerHeartbeat).all()
        assert persisted_job is not None
        assert persisted_job.status == "success"
        assert persisted_job.progress_pct == 100.0
        assert persisted_run is not None
        assert persisted_run.status == "succeeded"
        assert len(heartbeats) == 1
        assert heartbeats[0].status == "idle"
        assert heartbeats[0].last_job_status == "success"


def test_process_next_integration_job_executes_queued_metabase_sync(monkeypatch) -> None:
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        instance = _seed_metabase_instance(session)
        sync_run = MetabaseSyncRun(
            instance_id=instance.id,
            status="queued",
            started_at=instance.created_at,
            summary_json={"phase": "queued"},
        )
        session.add(sync_run)
        session.flush()
        sync_run_id = sync_run.id
        job = enqueue_integration_job(
            session,
            source="metabase",
            job_type="sync",
            target_type="metabase_instance",
            target_id=instance.id,
            target_name=instance.name,
            requested_by_user_id=None,
            payload_json={"instance_id": instance.id, "sync_run_id": sync_run_id},
            context_json={"instance_id": instance.id},
        )
        assert job is not None
        queued_job_id = job.id

    def _fake_metabase_sync(
        session: Session,
        instance_id: int,
        *,
        commit: bool = True,
        force: bool = False,
        audit_kwargs=None,
        integration_job: IntegrationSyncJob | None = None,
        sync_run_id: int | None = None,
    ):
        assert instance_id > 0
        assert integration_job is not None
        assert sync_run_id is not None
        run = session.get(MetabaseSyncRun, sync_run_id)
        assert run is not None
        run.status = "success"
        run.finished_at = run.started_at
        run.dashboards_count = 2
        run.questions_count = 3
        run.collections_count = 1
        run.links_count = 9
        run.summary_json = {"dashboards": 2, "questions": 3, "collections": 1, "links": 9}
        session.add(run)
        finish_integration_job_record(
            session,
            integration_job,
            status="success",
            records_processed=15,
            context_json={"instance_id": instance_id, "sync_run_id": sync_run_id},
            result_summary_json={"dashboards": 2, "questions": 3, "collections": 1, "links": 9},
            progress_pct=100.0,
        )
        return run

    monkeypatch.setattr("t2c_data.features.metabase.service.run_metabase_instance_sync", _fake_metabase_sync)

    processed = process_next_integration_job(source="metabase", job_type="sync", session_factory=SessionLocal)

    assert processed is not None
    assert processed.status == "success"
    assert processed.records_processed == 15
    assert processed.result_summary_json == {"dashboards": 2, "questions": 3, "collections": 1, "links": 9}

    with SessionLocal() as session:
        persisted_job = session.get(IntegrationSyncJob, queued_job_id)
        persisted_run = session.get(MetabaseSyncRun, sync_run_id)
        assert persisted_job is not None
        assert persisted_job.status == "success"
        assert persisted_job.progress_pct == 100.0
        assert persisted_run is not None
        assert persisted_run.status == "success"


def test_process_next_integration_job_executes_queued_platform_maintenance(monkeypatch) -> None:
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        job = enqueue_integration_job(
            session,
            source="platform",
            job_type="maintenance",
            target_type="platform_scheduler",
            target_id=1,
            target_name="platform maintenance",
            requested_by_user_id=None,
            payload_json={"trigger": "scheduled", "scheduler_mode": "dedicated"},
            context_json={"trigger": "scheduled", "scheduler_mode": "dedicated"},
        )
        assert job is not None
        queued_job_id = job.id

    def _fake_platform_maintenance(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
        return finish_integration_job_record(
            session,
            job,
            status="success",
            context_json={"trigger": "scheduled", "scheduler_mode": "dedicated"},
            result_summary_json={"trigger": "scheduled", "scheduler_mode": "dedicated", "job_id": job.id},
            progress_pct=100.0,
        ) or job

    monkeypatch.setattr("t2c_data.features.platform.job_worker.process_platform_maintenance_job", _fake_platform_maintenance)

    processed = process_next_integration_job(source="platform", job_type="maintenance", session_factory=SessionLocal)

    assert processed is not None
    assert processed.status == "success"
    assert processed.result_summary_json == {"trigger": "scheduled", "scheduler_mode": "dedicated", "job_id": queued_job_id}

    with SessionLocal() as session:
        persisted_job = session.get(IntegrationSyncJob, queued_job_id)
        assert persisted_job is not None
        assert persisted_job.status == "success"
        assert persisted_job.progress_pct == 100.0


def test_process_next_integration_job_executes_queued_export_job(monkeypatch) -> None:
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        job = enqueue_integration_job(
            session,
            source="export",
            job_type="audit.history.csv",
            target_type="export",
            target_name="auditoria.csv",
            requested_by_user_id=7,
            payload_json={
                "job_type": "audit.history.csv",
                "export_format": "csv",
                "date_from": "2026-05-01T00:00:00+00:00",
                "date_to": "2026-05-02T00:00:00+00:00",
            },
            context_json={"filters": {"date_from": "2026-05-01T00:00:00+00:00"}},
        )
        assert job is not None
        queued_job_id = job.id

    def _fake_process_export_job(session: Session, job: IntegrationSyncJob) -> IntegrationSyncJob:
        return finish_integration_job_record(
            session,
            job,
            status="success",
            records_processed=3,
            context_json={"job_type": job.job_type, "export_format": "csv"},
            result_summary_json={"job_type": job.job_type, "export_format": "csv", "row_count": 3},
            progress_pct=100.0,
        ) or job

    monkeypatch.setattr("t2c_data.features.platform.job_worker.process_export_job", _fake_process_export_job)

    processed = process_next_integration_job(source="export", job_type="audit.history.csv", session_factory=SessionLocal)

    assert processed is not None
    assert processed.status == "success"
    assert processed.records_processed == 3
    assert processed.result_summary_json == {"job_type": "audit.history.csv", "export_format": "csv", "row_count": 3}

    with SessionLocal() as session:
        persisted_job = session.get(IntegrationSyncJob, queued_job_id)
        assert persisted_job is not None
        assert persisted_job.status == "success"
        assert persisted_job.progress_pct == 100.0


def test_process_next_integration_job_executes_queued_data_lake_scan(monkeypatch) -> None:
    SessionLocal = _build_session_factory()
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory.write_audit_log_sync", lambda *_args, **_kwargs: None)
    with SessionLocal() as session:
        connection = _seed_data_lake_connection(session)
        queued = enqueue_data_lake_inventory_scan(
            session,
            connection["id"],
            current_user=type("UserRef", (), {"id": 42})(),
            audit_kwargs={"user_id": 42},
            correlation_id="corr-datalake-worker",
        )
        assert queued.job_id is not None
        queued_job_id = queued.job_id
        queued_scan_run_id = queued.scan_run.id

    def _fake_run(session: Session, connection_id: int, *, current_user=None, audit_kwargs=None, trigger_mode="manual", schedule_id=None, request_runner=None, scan_run=None):
        assert connection_id == connection["id"]
        assert scan_run is not None
        scan_run.status = "success"
        scan_run.started_at = scan_run.started_at or queued.scan_run.created_at
        scan_run.finished_at = scan_run.started_at
        scan_run.discovered_tables_count = 4
        scan_run.discovered_parquet_files_count = 9
        scan_run.total_bytes = 1024
        session.add(scan_run)
        session.commit()
        session.refresh(scan_run)
        return DataLakeInventoryScanOut(
            scan_run=DataLakeInventoryScanRunOut.model_validate(
                {
                    "id": scan_run.id,
                    "connection_id": scan_run.connection_id,
                    "status": scan_run.status,
                    "scanned_layers_count": scan_run.scanned_layers_count,
                    "discovered_tables_count": scan_run.discovered_tables_count,
                    "discovered_parquet_files_count": scan_run.discovered_parquet_files_count,
                    "total_bytes": scan_run.total_bytes,
                    "trigger_mode": scan_run.trigger_mode,
                    "schedule_id": scan_run.schedule_id,
                    "error_message": scan_run.error_message,
                    "started_at": scan_run.started_at,
                    "finished_at": scan_run.finished_at,
                    "scanned_by_user_id": scan_run.scanned_by_user_id,
                    "created_at": scan_run.created_at,
                    "updated_at": scan_run.updated_at,
                }
            ),
            summary=DataLakeInventorySummaryOut(
                connection_id=connection["id"],
                connection_name=connection["name"],
                total_tables=4,
                bronze_tables=4,
                silver_tables=0,
                gold_tables=0,
                total_parquet_files=9,
                total_bytes=1024,
                tables_without_parquet=0,
                tables_without_recent_update=0,
                layers_detected=["bronze"],
                last_scan_at=scan_run.finished_at,
                latest_scan_status="success",
                latest_scan_message=None,
                latest_scan_run_id=scan_run.id,
            ),
        )

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory._run_data_lake_inventory_scan", _fake_run)

    processed = process_next_integration_job(source="s3", job_type="inventory_scan", session_factory=SessionLocal)

    assert processed is not None
    assert processed.status == "success"
    assert processed.records_processed == 4
    assert processed.result_summary_json == {
        "scan_run_id": queued_scan_run_id,
        "status": "success",
        "tables": 4,
        "parquet_files": 9,
        "layers": 1,
        "total_bytes": 1024,
    }

    with SessionLocal() as session:
        persisted_job = session.get(IntegrationSyncJob, queued_job_id)
        persisted_scan_run = session.get(DataLakeInventoryScanRun, queued_scan_run_id)
        assert persisted_job is not None
        assert persisted_job.status == "success"
        assert persisted_job.progress_pct == 100.0
        assert persisted_scan_run is not None
        assert persisted_scan_run.status == "success"


def test_process_next_integration_job_marks_failed_data_lake_scan(monkeypatch) -> None:
    SessionLocal = _build_session_factory()
    monkeypatch.setattr("t2c_data.features.integrations.data_lake.write_audit_log_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory.write_audit_log_sync", lambda *_args, **_kwargs: None)
    with SessionLocal() as session:
        connection = _seed_data_lake_connection(session)
        queued = enqueue_data_lake_inventory_scan(
            session,
            connection["id"],
            current_user=type("UserRef", (), {"id": 51})(),
            audit_kwargs={"user_id": 51},
        )
        assert queued.job_id is not None
        queued_job_id = queued.job_id
        queued_scan_run_id = queued.scan_run.id

    def _failing_run(*_args, **_kwargs):
        raise RuntimeError("inventory failed")

    monkeypatch.setattr("t2c_data.features.integrations.data_lake_inventory._run_data_lake_inventory_scan", _failing_run)

    processed = process_next_integration_job(source="s3", job_type="inventory_scan", session_factory=SessionLocal)

    assert processed is not None
    assert processed.status == "failed"

    with SessionLocal() as session:
        persisted_job = session.get(IntegrationSyncJob, queued_job_id)
        persisted_scan_run = session.get(DataLakeInventoryScanRun, queued_scan_run_id)
        assert persisted_job is not None
        assert persisted_job.status == "failed"
        assert persisted_job.error == "inventory failed"
        assert persisted_scan_run is not None
        assert persisted_scan_run.status == "error"
