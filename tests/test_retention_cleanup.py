from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.config import settings
from t2c_data.features.platform.retention import run_retention_cleanup_job
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models import Base
from t2c_data.models.audit import AccessLog, AccessLogArchive, AuditLog, AuditLogArchive
from t2c_data.models.auth import User, UserAccessEvent, UserSession
from t2c_data.models.dq import DQEvidenceSample, DQObservabilityBaseline, DQObservabilityEvent, DQRun, DQTableMetric
from t2c_data.models.governance import CertificationDecisionEvent, GovernanceSettings, PrivacyReviewEvent
from t2c_data.models.incident import Incident, IncidentEvent
from t2c_data.models.operations import OperationalFailureEvent
from t2c_data.models.platform import AssetRowCountSnapshot, IntegrationSyncJob, PlatformDomainEvent, RetentionCleanupRun


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True, connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def _seed_policy(db: Session) -> None:
    settings_row = GovernanceSettings(
        id=1,
        audit_log_retention_days=1,
        audit_log_archive_retention_days=365,
        access_log_retention_days=1,
        access_log_archive_retention_days=365,
        platform_usage_event_retention_days=1,
        search_result_click_retention_days=1,
    )
    db.add(settings_row)
    db.commit()


def test_retention_cleanup_job_removes_expired_data_and_records_run(monkeypatch) -> None:
    session_factory = _session_factory()
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=10)

    monkeypatch.setattr(settings, "audit_event_retention_days", 1)
    monkeypatch.setattr(settings, "user_session_retention_days", 1)
    monkeypatch.setattr(settings, "user_access_event_retention_days", 1)
    monkeypatch.setattr(settings, "export_file_ttl_hours", 1)
    monkeypatch.setattr(settings, "dq_sample_retention_days", 1)
    monkeypatch.setattr(settings, "profiling_sample_retention_days", 1)
    monkeypatch.setattr(settings, "incident_evidence_retention_days", 1)
    monkeypatch.setattr(settings, "temp_file_ttl_hours", 1)
    monkeypatch.setattr(settings, "row_count_snapshot_retention_days", 1)
    monkeypatch.setattr(settings, "certification_history_retention_days", 1)
    monkeypatch.setattr(settings, "privacy_review_event_retention_days", 1)
    monkeypatch.setattr(settings, "system_log_retention_days", 1)

    temp_root = Path(tempfile.gettempdir())
    stale_temp_file = temp_root / "profiling-run-999-test.json"
    stale_temp_file.write_text("{}", encoding="utf-8")
    stale_temp_file.touch()
    stale_time = (now - timedelta(days=2)).timestamp()
    os.utime(stale_temp_file, (stale_time, stale_time))

    export_public_id = "retention-export"
    export_path = temp_root / "andromeda_exports" / export_public_id / "export.csv"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text("id,name\n1,teste\n", encoding="utf-8")

    with session_factory() as db:
        _seed_policy(db)
        datasource = DataSource(id=1, name="warehouse", db_type="postgres", host="localhost", port=5432, database="analytics", username="tester")
        db.add(datasource)
        db.flush()
        database = Database(id=1, datasource_id=1, name="analytics")
        db.add(database)
        db.flush()
        schema = Schema(id=1, database_id=1, name="sales")
        db.add(schema)
        db.flush()
        table = TableEntity(id=1, schema_id=1, name="clientes", table_type="table")
        db.add(table)
        db.flush()
        db.add(User(id=1, email="user@example.com", password_hash="x"))
        db.add(
            UserSession(
                id=1,
                user_id=1,
                jti="jti-1",
                started_at=old,
                last_seen_at=old,
                ended_at=None,
                duration_seconds=None,
                end_reason=None,
                expires_at=old + timedelta(hours=1),
                revoked_at=None,
                ip_address="127.0.0.1",
                user_agent="pytest",
                auth_method="password",
                mfa_used=False,
                success=True,
            )
        )
        db.add(
            UserAccessEvent(
                id=1,
                user_id=1,
                session_id=1,
                event_type="page_view",
                page_key="explorer",
                route_path="/explorer",
                http_method="GET",
                resource_type="table",
                resource_id="1",
                resource_fqn="warehouse.sales.clientes",
                datasource_id=1,
                schema_name="sales",
                table_id=1,
                table_name="clientes",
                column_id=1,
                column_name="cpf",
                action="view",
                sensitivity_level="high",
                has_personal_data=True,
                has_sensitive_data=True,
                privacy_classification="personal",
                metadata_json={"context": "old"},
                ip_address="127.0.0.1",
                user_agent="pytest",
                request_id="req-1",
                correlation_id="corr-1",
                created_at=old,
            )
        )
        db.add(
            AuditLog(
                id=1,
                created_at=old,
                user_id=1,
                actor_name="User",
                user_email="user@example.com",
                action="test.update",
                entity_type="table",
                entity_id="1",
                source_module="governance",
            )
        )
        db.add(
            AccessLog(
                id=1,
                created_at=old,
                user_id=1,
                actor_name="User",
                user_email="user@example.com",
                route="/api/v1/test",
                method="GET",
                status_code=200,
                request_id="req-1",
                api_version="v1",
                module_name="test",
            )
        )
        db.add(
            IntegrationSyncJob(
                id=77,
                job_key="export:test:1",
                source="export",
                job_type="audit.history.csv",
                target_type="export",
                target_id=None,
                target_name=None,
                trigger_mode="manual",
                status="success",
                started_at=old,
                queued_at=old,
                finished_at=old,
                requested_by_user_id=1,
                artifact_public_id=export_public_id,
                artifact_filename="export.csv",
                artifact_content_type="text/csv; charset=utf-8",
                artifact_storage_path=str(export_path),
                artifact_available_at=old,
                artifact_expires_at=old + timedelta(minutes=1),
                artifact_size_bytes=export_path.stat().st_size,
            )
        )
        db.add(DQRun(id=1, table_id=1, datasource_id=1, status="success", execution_engine="spark", created_at=old))
        db.add(DQTableMetric(id=1, run_id=1, table_id=1, row_count=10, column_count=3, completeness_pct_avg=100.0, dq_score=99.0, duplicates_count=0, failed_rules=0, created_at=old))
        db.add(
            DQObservabilityBaseline(
                run_id=1,
                table_id=1,
                metric_key="volume",
                metric_scope="table",
                current_value=10.0,
                baseline_value=9.0,
                mean_value=9.5,
                median_value=9.0,
                min_value=8.0,
                max_value=10.0,
                tolerance_abs=None,
                tolerance_pct=None,
                window_size=14,
                calculated_at=old,
                details_json={},
                created_at=old,
            )
        )
        db.add(
            DQObservabilityEvent(
                run_id=1,
                table_id=1,
                metric_key="freshness",
                event_type="stale",
                status="open",
                severity="high",
                detected_at=old,
                details_json={},
                created_at=old,
            )
        )
        db.add(
            DQEvidenceSample(
                dq_run_id=1,
                table_id=1,
                evidence_type="rule_violation",
                origin="dq_rule",
                status="masked",
                sample_size=1,
                affected_rows_count=1,
                masked_fields_json=["cpf"],
                sample_rows_json=[{"cpf": {"value": "[masked]", "redacted": True}}],
                evidence_json={},
                created_at=old,
            )
        )
        db.add(
            Incident(
                id=1,
                title="Incidente antigo",
                description="evidência velha",
                entity_type="table",
                table_fqn="warehouse.sales.clientes",
                detected_at=old,
                closed_at=old,
                status="closed",
                severity="sev3",
                evidence_json={"cpf": "123.456.789-00"},
            )
        )
        db.add(
            IncidentEvent(
                id=1,
                incident_id=1,
                event_type="note",
                title="Nota antiga",
                detail="evidência",
                evidence_json={"cnpj": "00.000.000/0001-00"},
                created_at=old,
            )
        )
        db.add(
            AssetRowCountSnapshot(
                id=1,
                asset_type="table",
                asset_id=1,
                asset_name="clientes",
                asset_fqn="warehouse.sales.clientes",
                source="s3",
                observed_at=old,
                row_count=10,
                row_count_method="estimate",
                row_count_confidence="medium",
                context_json={},
                created_at=old,
            )
        )
        db.add(
            CertificationDecisionEvent(
                id=1,
                asset_id=1,
                asset_name="clientes",
                database_name="warehouse",
                schema_name="sales",
                table_name="clientes",
                new_status="certified",
                decision_type="approve",
                decision_source="manual",
                reviewer="Admin",
                reviewer_email="admin@example.com",
                created_at=old,
            )
        )
        db.add(
            PrivacyReviewEvent(
                id=1,
                table_id=1,
                table_name="clientes",
                database_name="warehouse",
                schema_name="sales",
                review_type="manual",
                review_source="manual",
                reviewer_name="Admin",
                reviewer_email="admin@example.com",
                created_at=old,
            )
        )
        db.add(
            OperationalFailureEvent(
                id=1,
                occurred_at=old,
                category_code="platform",
                severity="medium",
                source="platform.scheduler",
                message="erro antigo",
                route="/api/v1/test",
                created_at=old,
            )
        )
        db.add(
            PlatformDomainEvent(
                id=1,
                event_key="platform.test.old",
                category="platform",
                severity="low",
                title="Evento antigo",
                source_module="platform",
                source_action="platform.test.old",
                actor_user_id=1,
                actor_name="Admin",
                actor_email="admin@example.com",
                manual_mode="manual",
                created_at=old,
            )
        )
        db.commit()

        summary = run_retention_cleanup_job(db, trigger_source="manual")

        assert summary["status"] == "success"
        assert summary["audit_archived"] == 1
        assert summary["access_archived"] == 1
        assert summary["user_sessions"]["user_sessions_closed"] == 1
        assert summary["user_access_events"]["user_access_events_deleted"] == 1
        assert summary["export_files"]["export_files_deleted"] == 1
        assert summary["dq_samples"]["dq_evidence_samples_deleted"] == 1
        assert summary["incident_evidence"]["incident_evidence_cleared"] == 1
        assert summary["row_count_snapshots"]["row_count_snapshots_deleted"] == 1
        assert summary["certification_history"]["certification_history_deleted"] == 1
        assert summary["privacy_review_events"]["privacy_review_events_deleted"] == 1
        assert summary["system_logs"]["operational_failure_events_deleted"] == 1
        assert summary["system_logs"]["platform_domain_events_deleted"] == 1
        assert summary["profiling_samples"]["profiling_samples_deleted"] == 1
        assert summary["temporary_files"]["temporary_files_deleted"] == 0
        assert summary["errors"] == []

        session_row = db.scalar(select(UserSession).where(UserSession.id == 1))
        assert session_row is not None
        assert session_row.ended_at is not None
        assert session_row.end_reason == "expired"
        assert session_row.duration_seconds is not None
        assert db.scalar(select(UserAccessEvent).where(UserAccessEvent.id == 1)) is None
        assert db.scalar(select(AuditLog).where(AuditLog.id == 1)) is None
        assert db.scalar(select(AccessLog).where(AccessLog.id == 1)) is None
        assert db.scalar(select(AssetRowCountSnapshot).where(AssetRowCountSnapshot.id == 1)) is None
        assert db.scalar(select(CertificationDecisionEvent).where(CertificationDecisionEvent.id == 1)) is None
        assert db.scalar(select(PrivacyReviewEvent).where(PrivacyReviewEvent.id == 1)) is None
        assert db.scalar(select(OperationalFailureEvent).where(OperationalFailureEvent.id == 1)) is None
        assert db.scalar(select(PlatformDomainEvent).where(PlatformDomainEvent.id == 1)) is None
        assert db.scalar(select(RetentionCleanupRun).where(RetentionCleanupRun.id == 1)) is not None
        assert not export_path.exists()
        assert not stale_temp_file.exists()

        audit_archive = db.scalar(select(AuditLogArchive).where(AuditLogArchive.id == 1))
        access_archive = db.scalar(select(AccessLogArchive).where(AccessLogArchive.id == 1))
        assert audit_archive is not None
        assert access_archive is not None
