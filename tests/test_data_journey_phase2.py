from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog import correlation as correlation_module
from t2c_data.features.catalog import operational_context as operational_context_module
from t2c_data.features.catalog.correlation import build_table_correlation_summary
from t2c_data.features.data_quality import rule_management as dq_rule_management_module
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleRun
from t2c_data.models.incident import Incident


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


def test_data_journey_correlation_surfaces_dq_rule_by_table_id(monkeypatch) -> None:
    class _OperationalSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    db = _build_session()

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="products",
        table_type="table",
        schema=schema,
        owner="Owner",
        owner_email="owner@andromeda.local",
        certification_status="eligible",
        certification_criticality="high",
    )
    db.add_all([datasource, database, schema, table])
    db.flush()

    rule = DQRule(
        table_id=table.id,
        table_fqn="local-andromeda.bronze.products",
        name="Preco maior que zero",
        severity="critical",
        rule_type="row_violation",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=60,
    )
    db.add(rule)
    db.flush()

    db.add_all(
            [
                DQRuleRun(rule_id=rule.id, status="failed", execution_engine="spark", violations_count=7),
            DQJobRun(
                job_type="rules",
                status="success",
                execution_engine="spark",
                table_id=table.id,
                table_fqn=rule.table_fqn,
                datasource_id=datasource.id,
                result_json={"requested_rule_ids": [rule.id]},
            ),
            Incident(
                title="Falha de qualidade",
                description="Incidente derivado da regra de DQ",
                entity_type="table",
                source_type="dq_rule",
                source_ref_id=rule.id,
                table_fqn="bronze.products",
                status="open",
                severity="sev1",
                detected_at=datetime.now(timezone.utc),
            ),
        ]
    )
    db.commit()
    db.refresh(table)
    db.refresh(rule)

    monkeypatch.setattr(correlation_module, "can_view_table", lambda current_user, current_table: True)
    monkeypatch.setattr(dq_rule_management_module, "can_view_table", lambda current_user, current_table: True)
    monkeypatch.setattr(
        correlation_module,
        "get_governance_settings_snapshot",
        lambda _db: SimpleNamespace(airflow_ui_base_url=None),
    )
    monkeypatch.setattr(
        correlation_module,
        "load_table_operational_context",
        lambda *_args, **_kwargs: {
            "table_id": table.id,
            "table_name": table.name,
            "table_fqn": "local-andromeda.bronze.products",
            "datasource_id": datasource.id,
            "datasource_name": datasource.name,
            "database_id": database.id,
            "database_name": database.name,
            "schema_id": schema.id,
            "schema_name": schema.name,
            "owner_name": table.owner or "Owner",
            "owner_defined": True,
            "data_owner_id": None,
            "criticality_score": 90,
            "criticality_label": "Alta",
            "criticality_tone": "critical",
            "dq_score": 72.0,
            "dq_status_label": "Atenção",
            "certification_status": "eligible",
            "certification_status_label": "Elegível",
            "dictionary_complete": True,
            "description_complete": True,
            "tags_count": 1,
            "terms_count": 1,
            "open_incidents": 1,
            "critical_open_incidents": 1,
            "eligible_for_certification": True,
            "sensitivity_level": None,
            "sensitivity_label": "Não classificado",
            "owner_review_due": False,
            "privacy_review_due": False,
            "certification_review_due": False,
            "last_review_at": None,
            "last_updated_at": datetime.now(timezone.utc),
            "last_sync_at": None,
            "recommended_actions": [],
            "actions": [],
            "links": {
                "explorer": "/explorer",
                "change_management": "/changes",
                "lineage": "/lineage",
                "data_quality": "/data-quality",
                "incidents": "/incidents/tickets",
                "audit": "/audit",
                "certification": "/certification",
                "owners": "/data-owners",
                "privacy": "/privacy-access",
                "datasource": "/datasources",
                "database": "/explorer",
                "schema": "/explorer",
                "metabase_consumption": "/dashboard",
            },
        },
    )
    monkeypatch.setattr(
        correlation_module,
        "evaluate_table_dq_incident_signals",
        lambda *_args, **_kwargs: {
            "table_id": table.id,
            "generated_incident_id": None,
            "generated_mode": None,
            "open_incidents": 1,
            "suggestions": [],
            "links": {
                "explorer": "/explorer",
                "change_management": "/changes",
                "lineage": "/lineage",
                "data_quality": "/data-quality",
                "incidents": "/incidents/tickets",
                "audit": "/audit",
                "certification": "/certification",
                "owners": "/data-owners",
                "privacy": "/privacy-access",
                "datasource": "/datasources",
                "database": "/explorer",
                "schema": "/explorer",
                "metabase_consumption": "/dashboard",
            },
        },
    )
    monkeypatch.setattr(
        correlation_module,
        "get_latest_metrics_by_table_id",
        lambda **_kwargs: {
            "dq_score": 72.0,
            "effective_dq_score": 72.0,
            "failed_rules": 1,
            "freshness_seconds": 1800,
            "run_at": datetime.now(timezone.utc),
        },
    )
    monkeypatch.setattr(
        correlation_module,
        "operational_session_for_datasource",
        lambda _datasource: _OperationalSession(),
    )
    monkeypatch.setattr(
        correlation_module,
        "load_table_ingestion_summary",
        lambda *_args, **_kwargs: {
            "linked": True,
            "state": "failing",
            "table_schema": schema.name,
            "table_name": table.name,
            "pipeline_count": 1,
            "pipelines": [],
            "primary_pipeline": {
                "latest_status_label": "Falha",
                "last_error": "Pipeline falhou",
                "last_failure_at": datetime.now(timezone.utc),
                "last_execution_finished_at": datetime.now(timezone.utc),
                "last_execution_started_at": datetime.now(timezone.utc),
                "last_success_at": None,
                "pipeline_name": "dq-products",
                "dag_id": "dq-products",
                "task_name": "profile",
                "watermark_value": None,
                "rows_processed": None,
            },
        },
    )
    monkeypatch.setattr(
        correlation_module,
        "build_governance_score",
        lambda **_kwargs: {
            "score": 68,
            "max_score": 100,
            "label": "Atenção",
            "tone": "warning",
            "completed_factors": 6,
            "partial_factors": 1,
            "total_factors": 10,
            "summary": "Há pendências operacionais e de DQ.",
            "factors": [],
        },
    )
    monkeypatch.setattr(correlation_module, "summarize_table_governance_score_trend", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(correlation_module, "owner_review_due", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(correlation_module, "privacy_review_due", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(correlation_module, "certification_review_due", lambda *_args, **_kwargs: False)

    summary = build_table_correlation_summary(
        db=db,
        table_id=table.id,
        current_user=SimpleNamespace(roles=[], email="viewer@andromeda.local"),
    )

    assert summary.table_id == table.id
    assert summary.dq.correlated_rules
    assert summary.dq.correlated_rules[0].id == rule.id
    assert summary.dq.correlated_rules[0].open_incident_id is not None
    assert summary.incidents.open_count == 1
    assert summary.signals.operational_failure is True


def test_data_journey_operational_context_surfaces_review_workflow_gaps(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    profile = SimpleNamespace(
        table_id=1,
        table_name="customers",
        table_fqn="local-andromeda.bronze.customers",
        datasource_name="local-andromeda",
        database_name="andromeda",
        schema_name="bronze",
        owner_name=None,
        owner_defined=False,
        data_owner_id=None,
        dq_score=95.0,
        dictionary_complete=True,
        description_complete=True,
        tags_count=2,
        terms_count=1,
        open_incidents=0,
        critical_open_incidents=0,
        eligible_for_certification=False,
        classification_defined=True,
        sensitivity_level="personal_data",
        has_personal_data=True,
        has_sensitive_personal_data=False,
        certification_status="revalidation_pending",
        certification_criticality="medium",
        certification_review_at=now,
        certification_expires_at=now,
        certification_decided_at=now,
        owner_reviewed_at=None,
        privacy_reviewed_at=now.replace(year=now.year - 1),
        last_review_at=now.replace(year=now.year - 1),
        last_updated_at=now,
        last_sync_at=None,
        search_clicks_30d=12,
        active_dq_rules_count=1,
    )

    monkeypatch.setattr(operational_context_module, "compute_priority_score", lambda *_args, **_kwargs: (75, []))
    monkeypatch.setattr(operational_context_module, "compute_profile_priority_score", lambda *_args, **_kwargs: 82)
    monkeypatch.setattr(operational_context_module, "recommended_actions", lambda *_args, **_kwargs: ["revisar-governanca"])
    monkeypatch.setattr(operational_context_module, "resolve_certification_status_for_profile", lambda *_args, **_kwargs: "revalidation_pending")

    payload = operational_context_module.table_operational_context_payload(
        profile,
        datasource_id=10,
        database_id=20,
        schema_id=30,
    )

    assert payload["owner_defined"] is False
    assert payload["owner_name"] == "Não definido"
    assert payload["certification_status"] == "revalidation_pending"
    assert payload["certification_status_label"] == "Pendente de revalidação"
    assert payload["privacy_review_due"] is True
    assert payload["certification_review_due"] is True
    assert payload["privacy_review_next_at"] is not None
    assert payload["certification_next_review_at"] == profile.certification_expires_at
    assert payload["review_due_label"] == "privacidade, certificação"
    action_keys = {item["key"] for item in payload["actions"]}
    assert "define_owner" in action_keys
    assert "review_privacy" in action_keys
    assert "revalidate_certification" in action_keys


if __name__ == "__main__":
    print("Run with pytest")
