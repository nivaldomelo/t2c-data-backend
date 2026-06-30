from __future__ import annotations

import os
import unittest
from datetime import timedelta

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException
from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.platform.jobs import (
    claim_queued_integration_job,
    enqueue_integration_job,
    finish_integration_job,
    finish_integration_job_record,
    integration_jobs_status_snapshot,
    list_integration_jobs_history,
    maybe_start_integration_job,
    record_asset_row_count_snapshot,
    run_platform_job,
)
from t2c_data.models import Base, AssetRowCountSnapshot, IntegrationSyncJob, MetabaseInstance, MetabaseSyncRun, PlatformDomainEvent, Role, User, UserInboxNotification
from t2c_data.schemas.platform import IntegrationSyncJobRunIn

if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]


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


class PlatformJobsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = _build_session()
        self.addCleanup(self.session.close)

    def _seed_metabase_instance(self) -> MetabaseInstance:
        instance = MetabaseInstance(
            name="metabase-prod",
            base_url="https://metabase.local",
            enabled=True,
            sync_dashboards=True,
            sync_questions=True,
            sync_collections=True,
        )
        instance.auth_secret = "secret"
        self.session.add(instance)
        self.session.commit()
        self.session.refresh(instance)
        return instance

    def test_jobs_status_and_history_reflect_latest_executions(self) -> None:
        first = maybe_start_integration_job(
            self.session,
            source="s3",
            job_type="inventory_scan",
            target_type="data_lake_connection",
            target_id=11,
            target_name="lake-bronze",
            trigger_mode="scheduled",
        )
        assert first is not None
        finish_integration_job(
            self.session,
            first,
            status="success",
            records_processed=8,
            context_json={"connection_id": 11},
        )

        second = maybe_start_integration_job(
            self.session,
            source="metabase",
            job_type="sync",
            target_type="metabase_instance",
            target_id=7,
            target_name="metabase-prod",
            trigger_mode="manual",
        )
        assert second is not None
        finish_integration_job(
            self.session,
            second,
            status="failed",
            records_processed=0,
            error="sync failed",
            context_json={"instance_id": 7},
        )

        snapshot = integration_jobs_status_snapshot(self.session, limit=10)
        assert snapshot["total"] == 2
        assert snapshot["queued"] == 0
        assert snapshot["success"] == 1
        assert snapshot["failed"] == 1
        assert snapshot["partial_success"] == 0
        assert snapshot["items"][0]["source"] == "metabase"
        assert snapshot["items"][0]["status"] == "failed"

        history = list_integration_jobs_history(self.session, page=1, page_size=10)
        assert history.total == 2
        assert history.items[0].source == "metabase"
        assert history.items[1].source == "s3"

    def test_jobs_history_caps_page_size_for_large_requests(self) -> None:
        handle = maybe_start_integration_job(
            self.session,
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=41,
            target_name="warehouse",
        )
        assert handle is not None
        finish_integration_job(self.session, handle, status="success", records_processed=3)

        history = list_integration_jobs_history(self.session, page=1, page_size=999)

        assert history.page == 1
        assert history.page_size == 100
        assert history.total == 1
        assert len(history.items) == 1

    def test_maybe_start_integration_job_blocks_duplicate_running_job(self) -> None:
        handle = maybe_start_integration_job(
            self.session,
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=3,
            target_name="warehouse",
        )
        assert handle is not None
        try:
            with self.assertRaises(HTTPException) as ctx:
                maybe_start_integration_job(
                    self.session,
                    source="datasource",
                    job_type="scan",
                    target_type="datasource",
                    target_id=3,
                    target_name="warehouse",
                )
            assert ctx.exception.status_code == 409
        finally:
            finish_integration_job(self.session, handle, status="success", records_processed=1)

    def test_maybe_start_integration_job_allows_force_on_stale_running_job(self) -> None:
        first = maybe_start_integration_job(
            self.session,
            source="metabase",
            job_type="sync",
            target_type="metabase_instance",
            target_id=9,
            target_name="metabase-prod",
        )
        assert first is not None
        first.job.started_at = first.job.started_at - timedelta(hours=26)
        self.session.add(first.job)
        self.session.commit()

        second = maybe_start_integration_job(
            self.session,
            source="metabase",
            job_type="sync",
            target_type="metabase_instance",
            target_id=9,
            target_name="metabase-prod",
            force_stale_running_job=True,
        )
        assert second is not None
        assert second.job.id != first.job.id
        assert second.job.status == "running"
        assert self.session.get(IntegrationSyncJob, first.job.id).status == "failed"

        finish_integration_job(self.session, second, status="success", records_processed=1)

    def test_record_asset_row_count_snapshot_persists_snapshot(self) -> None:
        job = maybe_start_integration_job(
            self.session,
            source="s3",
            job_type="inventory_scan",
            target_type="data_lake_connection",
            target_id=21,
            target_name="lake-gold",
        )
        assert job is not None
        finish_integration_job(self.session, job, status="success", records_processed=15)

        snapshot = record_asset_row_count_snapshot(
            self.session,
            asset_type="datalake_table",
            asset_id=88,
            asset_name="clientes",
            asset_fqn="datalake/clientes",
            row_count=15,
            row_count_method="footer",
            row_count_confidence="high",
            integration_sync_job_id=job.job.id,
            context_json={"source": "s3"},
        )

        assert snapshot is not None
        row = self.session.scalar(select(AssetRowCountSnapshot))
        assert row is not None
        assert row.asset_type == "datalake_table"
        assert row.row_count == 15
        assert row.integration_sync_job_id == job.job.id

        jobs = self.session.scalars(select(IntegrationSyncJob)).all()
        assert len(jobs) == 1

    def test_jobs_status_snapshot_counts_partial_success(self) -> None:
        handle = maybe_start_integration_job(
            self.session,
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=31,
            target_name="warehouse",
        )
        assert handle is not None
        finish_integration_job(
            self.session,
            handle,
            status="partial_success",
            records_processed=12,
            context_json={"row_counts": {"failed": 1, "success": 11}},
        )

        snapshot = integration_jobs_status_snapshot(self.session, limit=10)
        assert snapshot["queued"] == 0
        assert snapshot["partial_success"] == 1
        assert snapshot["items"][0]["status"] == "partial_success"

    def test_enqueue_claim_and_finish_dedicated_job_flow(self) -> None:
        queued = enqueue_integration_job(
            self.session,
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=55,
            target_name="warehouse",
            requested_by_user_id=None,
            payload_json={"datasource_id": 55, "scan_run_id": 12},
            context_json={"datasource_id": 55},
        )
        assert queued is not None
        assert queued.status == "queued"
        assert queued.queued_at is not None
        assert queued.progress_pct == 0.0

        claimed = claim_queued_integration_job(self.session, source="datasource", job_type="scan")
        assert claimed is not None
        assert claimed.id == queued.id
        assert claimed.status == "running"
        assert claimed.progress_pct == 1.0

        finished = finish_integration_job_record(
            self.session,
            claimed,
            status="success",
            records_processed=8,
            result_summary_json={"tables": 8},
            progress_pct=100.0,
        )
        assert finished is not None
        assert finished.status == "success"
        assert finished.records_processed == 8
        assert finished.result_summary_json == {"tables": 8}

    def test_failed_job_emits_causal_diagnostic_and_internal_alert(self) -> None:
        admin_role = Role(name="admin", description="Administrator")
        admin_user = User(email="admin@example.com", password_hash="x", is_active=True, roles=[admin_role])
        self.session.add_all([admin_role, admin_user])
        self.session.commit()

        queued = enqueue_integration_job(
            self.session,
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=77,
            target_name="warehouse",
            requested_by_user_id=admin_user.id,
            context_json={"datasource_id": 77},
        )
        assert queued is not None

        claimed = claim_queued_integration_job(self.session, source="datasource", job_type="scan")
        assert claimed is not None

        finished = finish_integration_job_record(
            self.session,
            claimed,
            status="failed",
            error="authentication failed for user",
            context_json={
                "datasource_id": 77,
                "error_code": "invalid_credentials",
                "error_detail": "password authentication failed",
            },
            result_summary_json={"tables": 0},
        )
        assert finished is not None

        snapshot = integration_jobs_status_snapshot(self.session, limit=10)
        item = snapshot["items"][0]
        assert item["diagnostic_probable_cause_code"] == "invalid_credentials"
        assert item["diagnostic_runbook_url"] == "/docs/runbooks/scan-failed.md"
        assert item["diagnostic_impact"]
        assert item["diagnostic_recurrence_count"] == 1
        assert "Credenciais inválidas" in str(item["diagnostic_probable_cause"])
        assert item["diagnostic_correlation_id"]

        events = self.session.scalars(select(PlatformDomainEvent)).all()
        assert len(events) == 1
        assert events[0].event_key == "platform.alert.datasource_scan"

        inbox = self.session.scalars(select(UserInboxNotification)).all()
        assert len(inbox) == 1
        assert inbox[0].source_entity_type == "integration_sync_job"
        assert inbox[0].href == f"/ops-cockpit?jobId={finished.id}"

    def test_run_platform_job_enqueues_metabase_sync(self) -> None:
        instance = self._seed_metabase_instance()
        admin_role = Role(name="admin", description="Administrator")
        admin_user = User(email="admin-metabase@example.com", password_hash="x", is_active=True, roles=[admin_role])
        self.session.add_all([admin_role, admin_user])
        self.session.commit()

        payload = IntegrationSyncJobRunIn(
            source="metabase",
            job_type="sync",
            target_type="metabase_instance",
            target_id=instance.id,
            target_name=instance.name,
            trigger_mode="manual",
        )

        job = run_platform_job(
            self.session,
            payload=payload,
            current_user=admin_user,
            audit_kwargs={},
        )

        assert job.source == "metabase"
        assert job.job_type == "sync"
        assert job.status == "queued"
        assert job.target_id == instance.id

        persisted = self.session.scalar(
            select(IntegrationSyncJob)
            .where(
                IntegrationSyncJob.source == "metabase",
                IntegrationSyncJob.job_type == "sync",
                IntegrationSyncJob.target_id == instance.id,
            )
            .order_by(IntegrationSyncJob.id.desc())
            .limit(1)
        )
        assert persisted is not None
        assert persisted.status == "queued"
        sync_run = self.session.scalar(
            select(MetabaseSyncRun).where(MetabaseSyncRun.instance_id == instance.id).order_by(MetabaseSyncRun.id.desc()).limit(1)
        )
        assert sync_run is not None
        assert persisted.payload_json == {"instance_id": instance.id, "sync_run_id": sync_run.id, "force": False, "reason": "manual"}

    def test_run_platform_job_enqueues_platform_maintenance(self) -> None:
        admin_role = Role(name="admin", description="Administrator")
        admin_user = User(email="admin-platform@example.com", password_hash="x", is_active=True, roles=[admin_role])
        self.session.add_all([admin_role, admin_user])
        self.session.commit()

        payload = IntegrationSyncJobRunIn(
            source="platform",
            job_type="maintenance",
            trigger_mode="scheduled",
        )

        job = run_platform_job(
            self.session,
            payload=payload,
            current_user=admin_user,
            audit_kwargs={},
        )

        assert job.source == "platform"
        assert job.job_type == "maintenance"
        assert job.status == "queued"
        assert job.target_type == "platform_scheduler"
        assert job.target_id == 1

        persisted = self.session.scalar(
            select(IntegrationSyncJob)
            .where(
                IntegrationSyncJob.source == "platform",
                IntegrationSyncJob.job_type == "maintenance",
                IntegrationSyncJob.target_type == "platform_scheduler",
            )
            .order_by(IntegrationSyncJob.id.desc())
            .limit(1)
        )
        assert persisted is not None
        assert persisted.status == "queued"
        assert persisted.payload_json == {
            "trigger": "scheduled",
            "scheduler_mode": "worker",
        }

    def test_enqueue_blocks_duplicate_active_job(self) -> None:
        first = enqueue_integration_job(
            self.session,
            source="datasource",
            job_type="scan",
            target_type="datasource",
            target_id=77,
            target_name="warehouse",
        )
        assert first is not None

        with self.assertRaises(HTTPException) as ctx:
            enqueue_integration_job(
                self.session,
                source="datasource",
                job_type="scan",
                target_type="datasource",
                target_id=77,
                target_name="warehouse",
            )
        assert ctx.exception.status_code == 409


if __name__ == "__main__":
    unittest.main()
